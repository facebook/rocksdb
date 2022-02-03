//  Copyright (c) Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <assert.h>

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// This is a data structure specifically designed as a "Set" for a
// pretty small scale of Enum structure. For now, it can support up
// to 64 element, and it is expandable in the future.
template <typename ENUM_TYPE, ENUM_TYPE MAX_VALUE>
class SmallEnumSet {
 public:
  SmallEnumSet() : state_(0) {}

  ~SmallEnumSet() {}

  // Return true if the input enum is included in the "Set" (i.e., changes the
  // internal scalar state successfully), otherwise, it will return false.
  bool Add(const ENUM_TYPE value) {
    static_assert(MAX_VALUE <= 63, "Size currently limited to 64");
    assert(value >= 0 && value <= MAX_VALUE);
    uint64_t old_state = state_;
    uint64_t tmp = 1;
    state_ |= (tmp << value);
    return old_state != state_;
  }

  // Return true if the input enum is contained in the "Set".
  bool Contains(const ENUM_TYPE value) {
    static_assert(MAX_VALUE <= 63, "Size currently limited to 64");
    assert(value >= 0 && value <= MAX_VALUE);
    uint64_t tmp = 1;
    return state_ & (tmp << value);
  }

 private:
  uint64_t state_;
};
}  // namespace ROCKSDB_NAMESPACE
