//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {
namespace cassandra {

/**
 * A MergeOperator for rocksdb that implements Cassandra row value merge.
 */
class CassandraValueMergeOperator : public MergeOperator {
public:
  static std::shared_ptr<MergeOperator> CreateSharedInstance();

  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override;

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value,
                                 Logger* logger) const override;

  virtual const char* Name() const override;

  virtual bool AllowSingleOperand() const override { return true; }
};
} // namespace cassandra
} // namespace rocksdb
