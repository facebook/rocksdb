// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
/**
 * A TEST MergeOperator for rocksdb that implements string append.
 * It is built using the MergeOperator interface rather than the simpler
 * AssociativeMergeOperator interface. This is useful for testing/benchmarking.
 * While the two operators are semantically the same, all production code
 * should use the StringAppendOperator defined in stringappend.{h,cc}. The
 * operator defined in the present file is primarily for testing.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include <deque>
#include <string>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class StringAppendTESTOperator : public MergeOperator {
 public:
  // Constructor with delimiter
  explicit StringAppendTESTOperator(char delim_char);
  explicit StringAppendTESTOperator(const std::string& delim);

  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override;

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value,
                                 Logger* logger) const override;

  static const char* kClassName() { return "StringAppendTESTOperator"; }
  static const char* kNickName() { return "stringappendtest"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

 private:
  // A version of PartialMerge that actually performs "partial merging".
  // Use this to simulate the exact behaviour of the StringAppendOperator.
  bool _AssocPartialMergeMulti(const Slice& key,
                               const std::deque<Slice>& operand_list,
                               std::string* new_value, Logger* logger) const;

  std::string delim_;  // The delimiter is inserted between elements
};

}  // namespace ROCKSDB_NAMESPACE
