//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "agg_merge.h"

#include <assert.h>

#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "port/likely.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/agg_merge.h"
#include "rocksdb/utilities/options_type.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, std::unique_ptr<Aggregator>> func_map;
// A reserved function name for aggregation failure.
const std::string kErrorFuncName = "";

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

std::string EncodeAggFuncAndValue(const Slice& function_name,
                                  const Slice& value) {
  std::string result;
  PutLengthPrefixedSlice(&result, function_name);
  result += value.ToString();
  return result;
}

bool ExtractAggFuncAndValue(const Slice& op, Slice& func, Slice& value) {
  value = op;
  return GetLengthPrefixedSlice(&value, &func);
}

bool ExtractList(const Slice& encoded_list, std::vector<Slice>& decoded_list) {
  decoded_list.clear();
  Slice list_slice = encoded_list;
  bool ret;
  Slice item;
  while ((ret = GetLengthPrefixedSlice(&list_slice, &item))) {
    decoded_list.push_back(item);
  }
  return list_slice.empty();
}

class Accumulator {
 public:
  bool Add(const Slice& op, bool is_initial_value,
           bool is_partial_aggregation) {
    if (ignore_operands_) {
      return true;
    }
    Slice my_func;
    Slice my_value;
    bool ret = ExtractAggFuncAndValue(op, my_func, my_value);
    if (!ret || (!func_.empty() && func_ != my_func)) {
      if (is_initial_value) {
        // We allow initial value not to have the function name part.
        my_value = op;
        my_func = func_;
      } else {
        // We got some unexpected merge operands. Ignore this and all
        // subsequence ones.
        ignore_operands_ = true;
        return true;
      }
    }
    if (is_partial_aggregation && !func_.empty()) {
      auto f = func_map.find(func_.ToString());
      if (f == func_map.end() || f->second->DoPartialAggregate()) {
        return false;
      }
    }
    func_ = my_func;
    values_.push_back(my_value);
    return true;
  }

  // Return false if aggregation fails.
  // One possible reason
  bool GetResult(std::string* result) {
    if (func_.empty()) {
      return false;
    }
    auto f = func_map.find(func_.ToString());
    if (f == func_map.end()) {
      return false;
    }
    if (!f->second->Aggregate(values_, &scratch_)) {
      return false;
    }
    *result = EncodeAggFuncAndValue(func_, scratch_);
    return true;
  }

  void Clear() {
    func_.clear();
    values_.clear();
    scratch_.clear();
    ignore_operands_ = false;
  }

 private:
  Slice func_;
  std::vector<Slice> values_;
  std::string scratch_;
  bool ignore_operands_ = false;
};

// Creating and using a new Accumulator might invoke multiple malloc and is
// expensive if it needs to be done when processing each merge operation.
// AggMergeOperator's merge operators can be invoked concurrently by multiple
// threads so we cannot simply create one Aggregator and reuse.
// We use thread local instances instead.
Accumulator& GetTLSAccumulator() {
  // The implementation is mostly copoied from Random::GetTLSInstance()
  // If the same pattern is used more frequently, we might create a utility
  // function for that.
  static __thread Accumulator* tls_instance;
  static __thread std::aligned_storage<sizeof(Accumulator)>::type
      tls_instance_bytes;

  auto rv = tls_instance;
  if (UNLIKELY(rv == nullptr)) {
    rv = new (&tls_instance_bytes) Accumulator();
    tls_instance = rv;
  }
  rv->Clear();
  return *rv;
}

void AggMergeOperator::PackAllMergeOperands(const MergeOperationInput& merge_in,
                                            MergeOperationOutput* merge_out) {
  merge_out->new_value = "";
  PutLengthPrefixedSlice(&merge_out->new_value, kErrorFuncName);
  if (merge_in.existing_value != nullptr) {
    PutLengthPrefixedSlice(&merge_out->new_value, *merge_in.existing_value);
  }
  for (const Slice& op : merge_in.operand_list) {
    PutLengthPrefixedSlice(&merge_out->new_value, op);
  }
}

bool AggMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                   MergeOperationOutput* merge_out) const {
  Accumulator& agg = GetTLSAccumulator();
  for (auto it = merge_in.operand_list.rbegin();
       it != merge_in.operand_list.rend(); it++) {
    agg.Add(*it, /*is_initial_value=*/false,
            /*is_partial_aggregation=*/false);
  }
  if (merge_in.existing_value != nullptr) {
    agg.Add(*merge_in.existing_value, /*is_initial_value=*/true,
            /*is_partial_aggregation=*/false);
  }
  bool succ = agg.GetResult(&merge_out->new_value);
  if (!succ) {
    // If aggregation can't happen, pack all merge operands. In contrast to
    // merge operator, we don't want to fail the DB. If users insert wrong
    // format or call unregistered an aggregation function, we still hope
    // the DB can continue functioning with other keys.
    PackAllMergeOperands(merge_in, merge_out);
  }
  return true;
}

bool AggMergeOperator::PartialMergeMulti(const Slice& /*key*/,
                                         const std::deque<Slice>& operand_list,
                                         std::string* new_value,
                                         Logger* /*logger*/) const {
  Accumulator& agg = GetTLSAccumulator();
  for (auto it = operand_list.rbegin(); it != operand_list.rend(); it++) {
    bool do_aggregation = agg.Add(*it, /*is_initial_value=*/false,
                                  /*is_partial_aggregation=*/true);
    if (!do_aggregation) {
      return false;
    }
  }
  if (!agg.GetResult(new_value)) {
    return false;
  }
  return true;
}

std::shared_ptr<MergeOperator> CreateAggMergeOperator() {
  return std::make_shared<AggMergeOperator>();
}
}  // namespace ROCKSDB_NAMESPACE
