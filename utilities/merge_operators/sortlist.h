//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// A test MergeOperator for RocksDB that implements Merge Sort.
// It is built using the MergeOperator interface. This is useful for
// testing/benchmarking
#pragma once
#include <deque>
#include <string>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {

class SortList : public MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override;

  virtual bool PartialMerge(const Slice& /*key*/, const Slice& left_operand,
                            const Slice& right_operand, std::string* new_value,
                            Logger* /*logger*/) const override;

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value,
                                 Logger* logger) const override;

  virtual const char* Name() const override;

  void Make(std::vector<int>& operand, Slice slice) const;

  std::vector<int> Merge(std::vector<int>& left, std::vector<int>& right) const;
};

}  // namespace rocksdb
