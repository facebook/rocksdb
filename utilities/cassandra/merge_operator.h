//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {
struct DBOptions;
class  ExtensionLoader;
  
namespace cassandra {

struct CassandraMergeOperatorOptions {
  size_t  operands_limit;
  uint32_t gc_grace_period_in_seconds;
};
/**
 * A MergeOperator for rocksdb that implements Cassandra row value merge.
 */
class CassandraValueMergeOperator : public MergeOperator {
public:
#ifndef ROCKSDB_LITE
  static void RegisterFactory(DBOptions & dbOpts);
  static void RegisterFactory(ExtensionLoader & loader);
#endif
public:
 explicit CassandraValueMergeOperator(int32_t gc_grace_period_in_seconds,
                                      size_t operands_limit = 0) {
   options_.gc_grace_period_in_seconds = gc_grace_period_in_seconds;
   options_.operands_limit = operands_limit;
 }

 virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                          MergeOperationOutput* merge_out) const override;

 virtual bool PartialMergeMulti(const Slice& key,
                                const std::deque<Slice>& operand_list,
                                std::string* new_value,
                                Logger* logger) const override;

 virtual void *GetOptionsPtr() override { return &options_; }
 virtual const char *GetOptionsPrefix() const override { return "cassandra.rocksdb."; }
 virtual const OptionTypeMap *GetOptionsMap() const override;
 virtual const char* Name() const override;

 virtual bool AllowSingleOperand() const override { return true; }

 virtual bool ShouldMerge(const std::vector<Slice>& operands) const override {
   return options_.operands_limit > 0 && operands.size() >= options_.operands_limit;
 }

private:
 CassandraMergeOperatorOptions options_;
};
} // namespace cassandra
} // namespace rocksdb
