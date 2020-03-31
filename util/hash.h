//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#pragma once
#include <stddef.h>
#include <stdint.h>

#include "rocksdb/slice.h"
#include "util/murmurhash.h"

namespace rocksdb {

// Non-persistent hash. Only used for in-memory data structure
// The hash results are applicable to change.
extern uint64_t NPHash64(const char* data, size_t n, uint32_t seed);

extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

inline uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

inline uint64_t GetSliceNPHash64(const Slice& s) {
  return NPHash64(s.data(), s.size(), 0);
}

inline uint32_t GetSliceHash(const Slice& s) {
  return Hash(s.data(), s.size(), 397);
}

inline uint64_t NPHash64(const char* data, size_t n, uint32_t seed) {
  // Right now murmurhash2B is used. It should able to be freely
  // changed to a better hash, without worrying about backward
  // compatibility issue.
  return MURMUR_HASH(data, static_cast<int>(n),
                     static_cast<unsigned int>(seed));
}

// std::hash compatible interface.
struct SliceHasher {
  uint32_t operator()(const Slice& s) const { return GetSliceHash(s); }
};

// An alternative to % for mapping a hash value to an arbitrary range. See
// https://github.com/lemire/fastrange and
// https://github.com/pdillinger/wormhashing/blob/2c4035a4462194bf15f3e9fc180c27c513335225/bloom_simulation_tests/foo.cc#L57
inline uint32_t fastrange32(uint32_t a, uint32_t h) {
  uint64_t product = static_cast<uint64_t>(a) * h;
  return static_cast<uint32_t>(product >> 32);
}

}  // namespace rocksdb
