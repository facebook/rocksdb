//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "utilities/cassandra/cassandra_options.h"

namespace ROCKSDB_NAMESPACE {
namespace cassandra {

/**
 * A MergeOperator for rocksdb that implements Cassandra row value merge.
 */
class CassandraValueMergeOperator : public MergeOperator {
public:
 explicit CassandraValueMergeOperator(int32_t gc_grace_period_in_seconds,
                                      size_t operands_limit = 0);

 virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                          MergeOperationOutput* merge_out) const override;

 virtual bool PartialMergeMulti(const Slice& key,
                                const std::deque<Slice>& operand_list,
                                std::string* new_value,
                                Logger* logger) const override;

 const char* Name() const override { return kClassName(); }
 static const char* kClassName() { return "CassandraValueMergeOperator"; }

 virtual bool AllowSingleOperand() const override { return true; }

 virtual bool ShouldMerge(const std::vector<Slice>& operands) const override {
   return options_.operands_limit > 0 &&
          operands.size() >= options_.operands_limit;
 }

private:
 CassandraOptions options_;
};
} // namespace cassandra
}  // namespace ROCKSDB_NAMESPACE
