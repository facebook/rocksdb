//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A merge operator that mimics Put semantics
// Since this merge-operator will not be used in production,
// it is implemented as a non-associative merge operator to illustrate the
// new interface and for testing purposes. (That is, we inherit from
// the MergeOperator class rather than the AssociativeMergeOperator
// which would be simpler in this case).
//
// From the client-perspective, semantics are the same.

#pragma once

#include "rocksdb/merge_operator.h"

namespace ROCKSDB_NAMESPACE {
class Logger;
class Slice;

class PutOperator : public MergeOperator {
 public:
  static const char* kClassName() { return "PutOperator"; }
  static const char* kNickName() { return "put_v1"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  bool FullMerge(const Slice& /*key*/, const Slice* /*existing_value*/,
                 const std::deque<std::string>& operand_sequence,
                 std::string* new_value, Logger* /*logger*/) const override;
  bool PartialMerge(const Slice& /*key*/, const Slice& left_operand,
                    const Slice& right_operand, std::string* new_value,
                    Logger* /*logger*/) const override;
  using MergeOperator::PartialMergeMulti;
  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* /*logger*/) const override;
};

class PutOperatorV2 : public PutOperator {
 public:
  static const char* kNickName() { return "put"; }
  const char* NickName() const override { return kNickName(); }

  bool FullMerge(const Slice& /*key*/, const Slice* /*existing_value*/,
                 const std::deque<std::string>& /*operand_sequence*/,
                 std::string* /*new_value*/, Logger* /*logger*/) const override;

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;
};

}  // namespace ROCKSDB_NAMESPACE
