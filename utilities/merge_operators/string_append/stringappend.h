//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A MergeOperator for rocksdb that implements string append.

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class StringAppendOperator : public AssociativeMergeOperator {
 public:
  // Constructor: specify delimiter
  explicit StringAppendOperator(char delim_char);
  explicit StringAppendOperator(const std::string& delim);

  bool Merge(const Slice& key, const Slice* existing_value, const Slice& value,
             std::string* new_value, Logger* logger) const override;

  static const char* kClassName() { return "StringAppendOperator"; }
  static const char* kNickName() { return "stringappend"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

 private:
  std::string delim_;  // The delimiter is inserted between elements
};

}  // namespace ROCKSDB_NAMESPACE
