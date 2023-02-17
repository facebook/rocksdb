//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Merge operator that picks the maximum operand, Comparison is based on
// Slice::compare

#pragma once

#include "rocksdb/merge_operator.h"

namespace ROCKSDB_NAMESPACE {
class Logger;
class Slice;

class MaxOperator : public MergeOperator {
 public:
  static const char* kClassName() { return "MaxOperator"; }
  static const char* kNickName() { return "max"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;
  bool PartialMerge(const Slice& /*key*/, const Slice& left_operand,
                    const Slice& right_operand, std::string* new_value,
                    Logger* /*logger*/) const override;
  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* /*logger*/) const override;
};

}  // namespace ROCKSDB_NAMESPACE
