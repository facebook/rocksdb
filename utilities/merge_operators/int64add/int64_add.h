//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A 'model' merge operator with int64 addition semantics
// operands and database value should be variable length encoded
// int64_t values, as encoded/decoded by `util/coding.h`.

#pragma once

#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
class Logger;
class Slice;

class Int64AddOperator : public AssociativeMergeOperator {
 public:
  static const char* kClassName() { return "Int64AddOperator"; }
  static const char* kNickName() { return "int64add"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  bool Merge(const Slice& /*key*/, const Slice* existing_value,
             const Slice& value, std::string* new_value,
             Logger* logger) const override;
};

}  // namespace ROCKSDB_NAMESPACE
