// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/merge_operators/uint64add.h"

#include <memory>

#include "logging/logging.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {  // anonymous namespace

bool UInt64AddOperator::Merge(const Slice& /*key*/, const Slice* existing_value,
                              const Slice& value, std::string* new_value,
                              Logger* logger) const {
  uint64_t orig_value = 0;
  if (existing_value) {
    orig_value = DecodeInteger(*existing_value, logger);
  }
  uint64_t operand = DecodeInteger(value, logger);

  assert(new_value);
  new_value->clear();
  PutFixed64(new_value, orig_value + operand);

  return true;  // Return true always since corruption will be treated as 0
}

uint64_t UInt64AddOperator::DecodeInteger(const Slice& value,
                                          Logger* logger) const {
  uint64_t result = 0;

  if (value.size() == sizeof(uint64_t)) {
    result = DecodeFixed64(value.data());
  } else if (logger != nullptr) {
    // If value is corrupted, treat it as 0
    ROCKS_LOG_ERROR(logger,
                    "uint64 value corruption, size: %" ROCKSDB_PRIszt
                    " > %" ROCKSDB_PRIszt,
                    value.size(), sizeof(uint64_t));
  }

  return result;
}

std::shared_ptr<MergeOperator> MergeOperators::CreateUInt64AddOperator() {
  return std::make_shared<UInt64AddOperator>();
}

}  // namespace ROCKSDB_NAMESPACE
