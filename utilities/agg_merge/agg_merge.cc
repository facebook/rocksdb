//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/agg_merge/agg_merge.h"

#include <assert.h>

#include <deque>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "port/lang.h"
#include "port/likely.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/agg_merge.h"
#include "rocksdb/utilities/options_type.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, std::unique_ptr<Aggregator>> func_map;
const std::string kUnnamedFuncName = "";
const std::string kErrorFuncName = "kErrorFuncName";

Status AddAggregator(const std::string& function_name,
                     std::unique_ptr<Aggregator>&& agg) {
  if (function_name == kErrorFuncName) {
    return Status::InvalidArgument(
        "Cannot register function name kErrorFuncName");
  }
  func_map.emplace(function_name, std::move(agg));
  return Status::OK();
}

AggMergeOperator::AggMergeOperator() {}

std::string EncodeAggFuncAndPayloadNoCheck(const Slice& function_name,
                                           const Slice& value) {
  std::string result;
  PutLengthPrefixedSlice(&result, function_name);
  result += value.ToString();
  return result;
}

Status EncodeAggFuncAndPayload(const Slice& function_name, const Slice& payload,
                               std::string& output) {
  if (function_name == kErrorFuncName) {
    return Status::InvalidArgument("Cannot use error function name");
  }
  if (function_name != kUnnamedFuncName &&
      func_map.find(function_name.ToString()) == func_map.end()) {
    return Status::InvalidArgument("Function name not registered");
  }
  output = EncodeAggFuncAndPayloadNoCheck(function_name, payload);
  return Status::OK();
}

bool ExtractAggFuncAndValue(const Slice& op, Slice& func, Slice& value) {
  value = op;
  return GetLengthPrefixedSlice(&value, &func);
}

bool ExtractList(const Slice& encoded_list, std::vector<Slice>& decoded_list) {
  decoded_list.clear();
  Slice list_slice = encoded_list;
  Slice item;
  while (GetLengthPrefixedSlice(&list_slice, &item)) {
    decoded_list.push_back(item);
  }
  return list_slice.empty();
}

class AggMergeOperator::Accumulator {
 public:
  bool Add(const Slice& op, bool is_partial_aggregation) {
    if (ignore_operands_) {
      return true;
    }
    Slice my_func;
    Slice my_value;
    bool ret = ExtractAggFuncAndValue(op, my_func, my_value);
    if (!ret) {
      ignore_operands_ = true;
      return true;
    }

    // Determine whether we need to do partial merge.
    if (is_partial_aggregation && !my_func.empty()) {
      auto f = func_map.find(my_func.ToString());
      if (f == func_map.end() || !f->second->DoPartialAggregate()) {
        return false;
      }
    }

    if (!func_valid_) {
      if (my_func != kUnnamedFuncName) {
        func_ = my_func;
        func_valid_ = true;
      }
    } else if (func_ != my_func) {
      // User switched aggregation function. Need to aggregate the older
      // one first.

      // Previous aggreagion can't be done in partial merge
      if (is_partial_aggregation) {
        func_valid_ = false;
        ignore_operands_ = true;
        return false;
      }

      // We could consider stashing an iterator into the hash of aggregators
      // to avoid repeated lookups when the aggregator doesn't change.
      auto f = func_map.find(func_.ToString());
      if (f == func_map.end() || !f->second->Aggregate(values_, scratch_)) {
        func_valid_ = false;
        ignore_operands_ = true;
        return true;
      }
      std::swap(scratch_, aggregated_);
      values_.clear();
      values_.push_back(aggregated_);
      func_ = my_func;
    }
    values_.push_back(my_value);
    return true;
  }

  // Return false if aggregation fails.
  // One possible reason
  bool GetResult(std::string& result) {
    if (!func_valid_) {
      return false;
    }
    auto f = func_map.find(func_.ToString());
    if (f == func_map.end()) {
      return false;
    }
    if (!f->second->Aggregate(values_, scratch_)) {
      return false;
    }
    result = EncodeAggFuncAndPayloadNoCheck(func_, scratch_);
    return true;
  }

  void Clear() {
    func_.clear();
    values_.clear();
    aggregated_.clear();
    scratch_.clear();
    ignore_operands_ = false;
    func_valid_ = false;
  }

 private:
  Slice func_;
  std::vector<Slice> values_;
  std::string aggregated_;
  std::string scratch_;
  bool ignore_operands_ = false;
  bool func_valid_ = false;
};

// Creating and using a new Accumulator might invoke multiple malloc and is
// expensive if it needs to be done when processing each merge operation.
// AggMergeOperator's merge operators can be invoked concurrently by multiple
// threads so we cannot simply create one Aggregator and reuse.
// We use thread local instances instead.
AggMergeOperator::Accumulator& AggMergeOperator::GetTLSAccumulator() {
  static thread_local Accumulator tls_acc;
  tls_acc.Clear();
  return tls_acc;
}

void AggMergeOperator::PackAllMergeOperands(const MergeOperationInput& merge_in,
                                            MergeOperationOutput& merge_out) {
  merge_out.new_value = "";
  PutLengthPrefixedSlice(&merge_out.new_value, kErrorFuncName);
  if (merge_in.existing_value != nullptr) {
    PutLengthPrefixedSlice(&merge_out.new_value, *merge_in.existing_value);
  }
  for (const Slice& op : merge_in.operand_list) {
    PutLengthPrefixedSlice(&merge_out.new_value, op);
  }
}

bool AggMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                   MergeOperationOutput* merge_out) const {
  Accumulator& agg = GetTLSAccumulator();
  if (merge_in.existing_value != nullptr) {
    agg.Add(*merge_in.existing_value, /*is_partial_aggregation=*/false);
  }
  for (const Slice& e : merge_in.operand_list) {
    agg.Add(e, /*is_partial_aggregation=*/false);
  }

  bool succ = agg.GetResult(merge_out->new_value);
  if (!succ) {
    // If aggregation can't happen, pack all merge operands. In contrast to
    // merge operator, we don't want to fail the DB. If users insert wrong
    // format or call unregistered an aggregation function, we still hope
    // the DB can continue functioning with other keys.
    PackAllMergeOperands(merge_in, *merge_out);
  }
  agg.Clear();
  return true;
}

bool AggMergeOperator::PartialMergeMulti(const Slice& /*key*/,
                                         const std::deque<Slice>& operand_list,
                                         std::string* new_value,
                                         Logger* /*logger*/) const {
  Accumulator& agg = GetTLSAccumulator();
  bool do_aggregation = true;
  for (const Slice& item : operand_list) {
    do_aggregation = agg.Add(item, /*is_partial_aggregation=*/true);
    if (!do_aggregation) {
      break;
    }
  }
  if (do_aggregation) {
    do_aggregation = agg.GetResult(*new_value);
  }
  agg.Clear();
  return do_aggregation;
}

std::shared_ptr<MergeOperator> GetAggMergeOperator() {
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<MergeOperator>, instance)
  (std::make_shared<AggMergeOperator>());
  assert(instance);
  return instance;
}
}  // namespace ROCKSDB_NAMESPACE
