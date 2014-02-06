// Copyright (c) 2013, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "dynamic_bloom.h"

#include "rocksdb/slice.h"
#include "util/hash.h"

namespace rocksdb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}
}

DynamicBloom::DynamicBloom(uint32_t total_bits,
                           uint32_t (*hash_func)(const Slice& key),
                           uint32_t num_probes)
    : hash_func_(hash_func),
      kTotalBits((total_bits + 7) / 8 * 8),
      kNumProbes(num_probes) {
  assert(hash_func_);
  assert(kNumProbes > 0);
  assert(kTotalBits > 0);
  data_.reset(new unsigned char[kTotalBits / 8]());
}

DynamicBloom::DynamicBloom(uint32_t total_bits,
                           uint32_t num_probes)
    : DynamicBloom(total_bits, &BloomHash, num_probes) {
}

}  // rocksdb
