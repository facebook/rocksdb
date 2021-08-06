//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This file is split from hash.cc because the xxh3p.h header might not be
// compatible with newer xxhash.h headers, because we forked off a prototype
// version of the hash function that now needs to be maintained.

#include "util/hash.h"
#include "util/xxh3p.h"

namespace ROCKSDB_NAMESPACE {

// We are standardizing on a preview release of XXH3, because that's
// the best available at time of standardizing.
//
// In testing (mostly Intel Skylake), this hash function is much more
// thorough than Hash32 and is almost universally faster. Hash() only
// seems faster when passing runtime-sized keys of the same small size
// (less than about 24 bytes) thousands of times in a row; this seems
// to allow the branch predictor to work some magic. XXH3's speed is
// much less dependent on branch prediction.
//
// Hashing with a prefix extractor is potentially a common case of
// hashing objects of small, predictable size. We could consider
// bundling hash functions specialized for particular lengths with
// the prefix extractors.
uint64_t Hash64(const char* data, size_t n, uint64_t seed) {
  return XXH3p_64bits_withSeed(data, n, seed);
}

uint64_t Hash64(const char* data, size_t n) {
  // Same as seed = 0
  return XXH3p_64bits(data, n);
}

}  // namespace ROCKSDB_NAMESPACE
