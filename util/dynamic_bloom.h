// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>

#include "rocksdb/slice.h"

#include "port/port.h"

#include <atomic>
#include <memory>

namespace rocksdb {

class Slice;
class Allocator;
class Logger;

class DynamicBloom {
 public:
  // allocator: pass allocator to bloom filter, hence trace the usage of memory
  // total_bits: fixed total bits for the bloom
  // num_probes: number of hash probes for a single key
  // locality:  If positive, optimize for cache line locality, 0 otherwise.
  // hash_func:  customized hash function
  // huge_page_tlb_size:  if >0, try to allocate bloom bytes from huge page TLB
  //                      within this page size. Need to reserve huge pages for
  //                      it to be allocated, like:
  //                         sysctl -w vm.nr_hugepages=20
  //                     See linux doc Documentation/vm/hugetlbpage.txt
  explicit DynamicBloom(Allocator* allocator,
                        uint32_t total_bits, uint32_t locality = 0,
                        uint32_t num_probes = 6,
                        uint32_t (*hash_func)(const Slice& key) = nullptr,
                        size_t huge_page_tlb_size = 0,
                        Logger* logger = nullptr);

  explicit DynamicBloom(uint32_t num_probes = 6,
                        uint32_t (*hash_func)(const Slice& key) = nullptr);

  void SetTotalBits(Allocator* allocator, uint32_t total_bits,
                    uint32_t locality, size_t huge_page_tlb_size,
                    Logger* logger);

  ~DynamicBloom() {}

  // Assuming single threaded access to this function.
  void Add(const Slice& key);

  // Like Add, but may be called concurrent with other functions.
  void AddConcurrently(const Slice& key);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t hash);

  // Like AddHash, but may be called concurrent with other functions.
  void AddHashConcurrently(uint32_t hash);

  // Multithreaded access to this function is OK
  bool MayContain(const Slice& key) const;

  // Multithreaded access to this function is OK
  bool MayContainHash(uint32_t hash) const;

  void Prefetch(uint32_t h);

  uint32_t GetNumBlocks() const { return kNumBlocks; }

  Slice GetRawData() const {
    return Slice(reinterpret_cast<char*>(data_), GetTotalBits() / 8);
  }

  void SetRawData(unsigned char* raw_data, uint32_t total_bits,
                  uint32_t num_blocks = 0);

  uint32_t GetTotalBits() const { return kTotalBits; }

  bool IsInitialized() const { return kNumBlocks > 0 || kTotalBits > 0; }

 private:
  uint32_t kTotalBits;
  uint32_t kNumBlocks;
  const uint32_t kNumProbes;

  uint32_t (*hash_func_)(const Slice& key);
  std::atomic<uint8_t>* data_;

  // or_func(ptr, mask) should effect *ptr |= mask with the appropriate
  // concurrency safety, working with bytes.
  template <typename OrFunc>
  void AddHash(uint32_t hash, const OrFunc& or_func);
};

inline void DynamicBloom::Add(const Slice& key) { AddHash(hash_func_(key)); }

inline void DynamicBloom::AddConcurrently(const Slice& key) {
  AddHashConcurrently(hash_func_(key));
}

inline void DynamicBloom::AddHash(uint32_t hash) {
  AddHash(hash, [](std::atomic<uint8_t>* ptr, uint8_t mask) {
    ptr->store(ptr->load(std::memory_order_relaxed) | mask,
               std::memory_order_relaxed);
  });
}

inline void DynamicBloom::AddHashConcurrently(uint32_t hash) {
  AddHash(hash, [](std::atomic<uint8_t>* ptr, uint8_t mask) {
    // Happens-before between AddHash and MaybeContains is handled by
    // access to versions_->LastSequence(), so all we have to do here is
    // avoid races (so we don't give the compiler a license to mess up
    // our code) and not lose bits.  std::memory_order_relaxed is enough
    // for that.
    if ((mask & ptr->load(std::memory_order_relaxed)) != mask) {
      ptr->fetch_or(mask, std::memory_order_relaxed);
    }
  });
}

inline bool DynamicBloom::MayContain(const Slice& key) const {
  return (MayContainHash(hash_func_(key)));
}

inline void DynamicBloom::Prefetch(uint32_t h) {
  if (kNumBlocks != 0) {
    uint32_t b = ((h >> 11 | (h << 21)) % kNumBlocks) * (CACHE_LINE_SIZE * 8);
    PREFETCH(&(data_[b / 8]), 0, 3);
  }
}

inline bool DynamicBloom::MayContainHash(uint32_t h) const {
  assert(IsInitialized());
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  if (kNumBlocks != 0) {
    uint32_t b = ((h >> 11 | (h << 21)) % kNumBlocks) * (CACHE_LINE_SIZE * 8);
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      // Since CACHE_LINE_SIZE is defined as 2^n, this line will be optimized
      //  to a simple and operation by compiler.
      const uint32_t bitpos = b + (h % (CACHE_LINE_SIZE * 8));
      uint8_t byteval = data_[bitpos / 8].load(std::memory_order_relaxed);
      if ((byteval & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      // Rotate h so that we don't reuse the same bytes.
      h = h / (CACHE_LINE_SIZE * 8) +
          (h % (CACHE_LINE_SIZE * 8)) * (0x20000000U / CACHE_LINE_SIZE);
      h += delta;
    }
  } else {
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      const uint32_t bitpos = h % kTotalBits;
      uint8_t byteval = data_[bitpos / 8].load(std::memory_order_relaxed);
      if ((byteval & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
  }
  return true;
}

template <typename OrFunc>
inline void DynamicBloom::AddHash(uint32_t h, const OrFunc& or_func) {
  assert(IsInitialized());
  const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
  if (kNumBlocks != 0) {
    uint32_t b = ((h >> 11 | (h << 21)) % kNumBlocks) * (CACHE_LINE_SIZE * 8);
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      // Since CACHE_LINE_SIZE is defined as 2^n, this line will be optimized
      // to a simple and operation by compiler.
      const uint32_t bitpos = b + (h % (CACHE_LINE_SIZE * 8));
      or_func(&data_[bitpos / 8], (1 << (bitpos % 8)));
      // Rotate h so that we don't reuse the same bytes.
      h = h / (CACHE_LINE_SIZE * 8) +
          (h % (CACHE_LINE_SIZE * 8)) * (0x20000000U / CACHE_LINE_SIZE);
      h += delta;
    }
  } else {
    for (uint32_t i = 0; i < kNumProbes; ++i) {
      const uint32_t bitpos = h % kTotalBits;
      or_func(&data_[bitpos / 8], (1 << (bitpos % 8)));
      h += delta;
    }
  }
}

}  // rocksdb
