// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/slice.h"

#include "port/port.h"
#include "util/hash.h"

#include <atomic>
#include <memory>

namespace rocksdb {

class Slice;
class Allocator;
class Logger;

// A Bloom filter intended only to be used in memory, never serialized in a way
// that could lead to schema incompatibility. Supports opt-in lock-free
// concurrent access.
class DynamicBloom {
 public:
  // allocator: pass allocator to bloom filter, hence trace the usage of memory
  // total_bits: fixed total bits for the bloom
  // num_probes: number of hash probes for a single key
  // hash_func:  customized hash function
  // huge_page_tlb_size:  if >0, try to allocate bloom bytes from huge page TLB
  //                      within this page size. Need to reserve huge pages for
  //                      it to be allocated, like:
  //                         sysctl -w vm.nr_hugepages=20
  //                     See linux doc Documentation/vm/hugetlbpage.txt
  explicit DynamicBloom(Allocator* allocator,
                        uint32_t total_bits,
                        uint32_t num_probes = 6,
                        size_t huge_page_tlb_size = 0,
                        Logger* logger = nullptr);

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

 private:
  // Length of the structure, in 64-bit words. For this structure, "word"
  // will always refer to 64-bit words.
  uint32_t kLen;
  // We make the k probes in pairs, two for each 64-bit read/write. Thus,
  // this stores k/2, the number of words to double-probe.
  const uint32_t kNumDoubleProbes;

  std::atomic<uint64_t>* data_;

  // or_func(ptr, mask) should effect *ptr |= mask with the appropriate
  // concurrency safety, working with bytes.
  template <typename OrFunc>
  void AddHash(uint32_t hash, const OrFunc& or_func);
};

inline void DynamicBloom::Add(const Slice& key) { AddHash(BloomHash(key)); }

inline void DynamicBloom::AddConcurrently(const Slice& key) {
  AddHashConcurrently(BloomHash(key));
}

inline void DynamicBloom::AddHash(uint32_t hash) {
  AddHash(hash, [](std::atomic<uint64_t>* ptr, uint64_t mask) {
    ptr->store(ptr->load(std::memory_order_relaxed) | mask,
               std::memory_order_relaxed);
  });
}

inline void DynamicBloom::AddHashConcurrently(uint32_t hash) {
  AddHash(hash, [](std::atomic<uint64_t>* ptr, uint64_t mask) {
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
  return (MayContainHash(BloomHash(key)));
}

#if defined(_MSC_VER)
#pragma warning(push)
// local variable is initialized but not referenced
#pragma warning(disable : 4189)
#endif
inline void DynamicBloom::Prefetch(uint32_t h32) {
  size_t a = fastrange32(kLen, h32);
  PREFETCH(data_ + a, 0, 3);
}
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

inline bool DynamicBloom::MayContainHash(uint32_t h32) const {
  size_t a = fastrange32(kLen, h32);
  PREFETCH(data_ + a, 0, 3);
  uint64_t h = h32;
  for (unsigned i = 0;;) {
    h *= 0x9e3779b97f4a7c13ULL;
    for (int j = 0; j < 5; ++j, ++i) {
      uint64_t mask = ((uint64_t)1 << (h & 63))
                    | ((uint64_t)1 << ((h >> 6) & 63));
      uint64_t val = data_[a ^ i].load(std::memory_order_relaxed);
      if (i + 1 >= kNumDoubleProbes) {
        return (val & mask) == mask;
      } else if ((val & mask) != mask) {
        return false;
      }
      h = (h >> 12) | (h << 52);
    }
  }
}

template <typename OrFunc>
inline void DynamicBloom::AddHash(uint32_t h32, const OrFunc& or_func) {
  size_t a = fastrange32(kLen, h32);
  PREFETCH(data_ + a, 0, 3);
  uint64_t h = h32;
  for (unsigned i = 0;;) {
    h *= 0x9e3779b97f4a7c13ULL;
    for (int j = 0; j < 5; ++j, ++i) {
      uint64_t mask = ((uint64_t)1 << (h & 63))
                    | ((uint64_t)1 << ((h >> 6) & 63));
      or_func(&data_[a ^ i], mask);
      if (i + 1 >= kNumDoubleProbes) {
        return;
      }
      h = (h >> 12) | (h << 52);
    }
  }
}

}  // rocksdb
