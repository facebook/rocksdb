//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A 'model' merge operator with uint64 addition semantics
// Implemented as an AssociativeMergeOperator for simplicity and example.

#pragma once

#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
class Logger;
class Slice;

class UInt64AddOperator : public AssociativeMergeOperator {
 public:
  static const char* kClassName() { return "UInt64AddOperator"; }
  static const char* kNickName() { return "uint64add"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  bool Merge(const Slice& /*key*/, const Slice* existing_value,
             const Slice& value, std::string* new_value,
             Logger* logger) const override;

 private:
  // Takes the string and decodes it into a uint64_t
  // On error, prints a message and returns 0
  uint64_t DecodeInteger(const Slice& value, Logger* logger) const;
};

}  // namespace ROCKSDB_NAMESPACE
