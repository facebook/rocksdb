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
      total_bits_((total_bits + 7) / 8 * 8),
      num_probes_(num_probes) {
  assert(hash_func_);
  assert(num_probes_ > 0);
  assert(total_bits_ > 0);
  data_.reset(new unsigned char[total_bits_ / 8]());
}

DynamicBloom::DynamicBloom(uint32_t total_bits,
                           uint32_t num_probes)
    : hash_func_(&BloomHash),
      total_bits_((total_bits + 7) / 8 * 8),
      num_probes_(num_probes) {
  assert(num_probes_ > 0);
  assert(total_bits_ > 0);
  data_.reset(new unsigned char[total_bits_ / 8]());
}

void DynamicBloom::Add(const Slice& key) {
  AddHash(hash_func_(key));
}

void DynamicBloom::AddHash(uint32_t h) {
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  for (uint32_t i = 0; i < num_probes_; i++) {
    const uint32_t bitpos = h % total_bits_;
    data_[bitpos/8] |= (1 << (bitpos % 8));
    h += delta;
  }
}

bool DynamicBloom::MayContain(const Slice& key) {
  return (MayContainHash(hash_func_(key)));
}

bool DynamicBloom::MayContainHash(uint32_t h) {
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  for (uint32_t i = 0; i < num_probes_; i++) {
    const uint32_t bitpos = h % total_bits_;
    if ((data_[bitpos/8] & (1 << (bitpos % 8)))
        == 0) return false;
    h += delta;
  }
  return true;
}

}
