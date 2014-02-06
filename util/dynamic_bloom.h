// Copyright (c) 2013, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <atomic>
#include <memory>

namespace rocksdb {

class Slice;

class DynamicBloom {
 public:
  // total_bits: fixed total bits for the bloom
  // hash_func:  customized hash function
  // num_probes: number of hash probes for a single key
  DynamicBloom(uint32_t total_bits,
               uint32_t (*hash_func)(const Slice& key),
               uint32_t num_probes = 6);

  explicit DynamicBloom(uint32_t total_bits, uint32_t num_probes = 6);

  // Assuming single threaded access to this function.
  void Add(const Slice& key);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t hash);

  // Multithreaded access to this function is OK
  bool MayContain(const Slice& key);

  // Multithreaded access to this function is OK
  bool MayContainHash(uint32_t hash);

 private:
  uint32_t (*hash_func_)(const Slice& key);
  const uint32_t kTotalBits;
  const uint32_t kNumProbes;
  std::unique_ptr<unsigned char[]> data_;
};

inline void DynamicBloom::Add(const Slice& key) { AddHash(hash_func_(key)); }

inline bool DynamicBloom::MayContain(const Slice& key) {
  return (MayContainHash(hash_func_(key)));
}

inline bool DynamicBloom::MayContainHash(uint32_t h) {
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  for (uint32_t i = 0; i < kNumProbes; i++) {
    const uint32_t bitpos = h % kTotalBits;
    if (((data_[bitpos / 8]) & (1 << (bitpos % 8))) == 0) {
      return false;
    }
    h += delta;
  }
  return true;
}

inline void DynamicBloom::AddHash(uint32_t h) {
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  for (uint32_t i = 0; i < kNumProbes; i++) {
    const uint32_t bitpos = h % kTotalBits;
    data_[bitpos / 8] |= (1 << (bitpos % 8));
    h += delta;
  }
}

}  // rocksdb
