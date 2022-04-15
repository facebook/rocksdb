//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <algorithm>
#include <cstddef>
#include <memory>
#include <unordered_map>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/agg_merge.h"
#include "utilities/cassandra/cassandra_options.h"

namespace ROCKSDB_NAMESPACE {
class AggMergeOperator : public MergeOperator {
 public:
  explicit AggMergeOperator();

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value, Logger* logger) const override;

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "AggMergeOperator.v1"; }

  bool AllowSingleOperand() const override { return true; }

  bool ShouldMerge(const std::vector<Slice>&) const override { return false; }

 private:
  class Accumulator;

  // Pack all merge operands into one value. This is called when aggregation
  // fails. The existing values are preserved and returned so that users can
  // debug the problem.
  static void PackAllMergeOperands(const MergeOperationInput& merge_in,
                                   MergeOperationOutput& merge_out);
  static Accumulator& GetTLSAccumulator();
};

extern std::string EncodeAggFuncAndPayloadNoCheck(const Slice& function_name,
                                                  const Slice& value);
}  // namespace ROCKSDB_NAMESPACE
