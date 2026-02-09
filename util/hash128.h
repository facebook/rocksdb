//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

// 128-bit hash gets it own header so that more popular hash.h doesn't
// depend on math128.h

#include "rocksdb/slice.h"
#include "util/math128.h"

namespace ROCKSDB_NAMESPACE {

// Stable/persistent 128-bit hash for non-cryptographic applications.
Unsigned128 Hash128(const char* data, size_t n, uint64_t seed);

// Specific optimization without seed (same as seed = 0)
Unsigned128 Hash128(const char* data, size_t n);

inline Unsigned128 GetSliceHash128(const Slice& key) {
  return Hash128(key.data(), key.size());
}

}  // namespace ROCKSDB_NAMESPACE
