//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// A MergeOperator for RocksDB that implements Merge Sort.
// It is built using the MergeOperator interface. The operator works by taking
// an input which contains one or more merge operands where each operand is a
// list of sorted ints and merges them to form a large sorted list.
#pragma once

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class SortList : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMerge(const Slice& /*key*/, const Slice& left_operand,
                    const Slice& right_operand, std::string* new_value,
                    Logger* /*logger*/) const override;

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value, Logger* logger) const override;

  const char* Name() const override;

  void MakeVector(std::vector<int>& operand, Slice slice) const;

 private:
  std::vector<int> Merge(std::vector<int>& left, std::vector<int>& right) const;
};

}  // namespace ROCKSDB_NAMESPACE
