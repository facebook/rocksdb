// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>

#include "logging/logging.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace { // anonymous namespace

using ROCKSDB_NAMESPACE::AssociativeMergeOperator;
using ROCKSDB_NAMESPACE::InfoLogLevel;
using ROCKSDB_NAMESPACE::Logger;
using ROCKSDB_NAMESPACE::Slice;

// A 'model' merge operator with uint64 addition semantics
// Implemented as an AssociativeMergeOperator for simplicity and example.
class UInt64AddOperator : public AssociativeMergeOperator {
 public:
  bool Merge(const Slice& /*key*/, const Slice* existing_value,
             const Slice& value, std::string* new_value,
             Logger* logger) const override {
    uint64_t orig_value = 0;
    if (existing_value){
      orig_value = DecodeInteger(*existing_value, logger);
    }
    uint64_t operand = DecodeInteger(value, logger);

    assert(new_value);
    new_value->clear();
    ROCKSDB_NAMESPACE::PutFixed64(new_value, orig_value + operand);

    return true;  // Return true always since corruption will be treated as 0
  }

  static const char* kClassName() { return "UInt64AddOperator"; }
  static const char* kNickName() { return "uint64add"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

 private:
  // Takes the string and decodes it into a uint64_t
  // On error, prints a message and returns 0
  uint64_t DecodeInteger(const Slice& value, Logger* logger) const {
    uint64_t result = 0;

    if (value.size() == sizeof(uint64_t)) {
      result = ROCKSDB_NAMESPACE::DecodeFixed64(value.data());
    } else if (logger != nullptr) {
      // If value is corrupted, treat it as 0
      ROCKS_LOG_ERROR(logger,
                      "uint64 value corruption, size: %" ROCKSDB_PRIszt
                      " > %" ROCKSDB_PRIszt,
                      value.size(), sizeof(uint64_t));
    }

    return result;
  }
};

}  // anonymous namespace

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<MergeOperator> MergeOperators::CreateUInt64AddOperator() {
  return std::make_shared<UInt64AddOperator>();
}

}  // namespace ROCKSDB_NAMESPACE
