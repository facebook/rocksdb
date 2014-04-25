// Copyright (c) 2013, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <atomic>
#include <memory>

#include <util/arena.h>

namespace rocksdb {

class Slice;

class DynamicBloom {
 public:
  // total_bits: fixed total bits for the bloom
  // num_probes: number of hash probes for a single key
  // cl_per_block: block size in cache lines. When this is non-zero, a
  //               query/set is done within a block to improve cache locality.
  // hash_func:  customized hash function
  // huge_page_tlb_size:  if >0, try to allocate bloom bytes from huge page TLB
  //                      withi this page size. Need to reserve huge pages for
  //                      it to be allocated, like:
  //                         sysctl -w vm.nr_hugepages=20
  //                     See linux doc Documentation/vm/hugetlbpage.txt
  explicit DynamicBloom(uint32_t total_bits, uint32_t cl_per_block = 0,
                        uint32_t num_probes = 6,
                        uint32_t (*hash_func)(const Slice& key) = nullptr,
                        size_t huge_page_tlb_size = 0);

  ~DynamicBloom() {}

  // Assuming single threaded access to this function.
  void Add(const Slice& key);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t hash);

  // Multithreaded access to this function is OK
  bool MayContain(const Slice& key);

  // Multithreaded access to this function is OK
  bool MayContainHash(uint32_t hash);

 private:
  const bool kBlocked;
  const uint32_t kBitsPerBlock;
  const uint32_t kTotalBits;
  const uint32_t kNumBlocks;
  const uint32_t kNumProbes;

  uint32_t (*hash_func_)(const Slice& key);
  unsigned char* data_;
  unsigned char* raw_;

  Arena arena_;
};

inline void DynamicBloom::Add(const Slice& key) { AddHash(hash_func_(key)); }

inline bool DynamicBloom::MayContain(const Slice& key) {
  return (MayContainHash(hash_func_(key)));
}

inline bool DynamicBloom::MayContainHash(uint32_t h) {
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  if (kBlocked) {
    uint32_t b = ((h >> 11 | (h << 21)) % kNumBlocks) * kBitsPerBlock;
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      const uint32_t bitpos = b + h % kBitsPerBlock;
      if (((data_[bitpos / 8]) & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
  } else {
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      const uint32_t bitpos = h % kTotalBits;
      if (((data_[bitpos / 8]) & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
  }
  return true;
}

inline void DynamicBloom::AddHash(uint32_t h) {
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  if (kBlocked) {
    uint32_t b = ((h >> 11 | (h << 21)) % kNumBlocks) * kBitsPerBlock;
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      const uint32_t bitpos = b + h % kBitsPerBlock;
      data_[bitpos / 8] |= (1 << (bitpos % 8));
      h += delta;
    }
  } else {
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      const uint32_t bitpos = h % kTotalBits;
      data_[bitpos / 8] |= (1 << (bitpos % 8));
      h += delta;
    }
  }
}

}  // rocksdb
