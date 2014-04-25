// Copyright (c) 2013, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "dynamic_bloom.h"

#include <algorithm>

#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/hash.h"

namespace rocksdb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}
}

DynamicBloom::DynamicBloom(uint32_t total_bits, uint32_t cl_per_block,
                           uint32_t num_probes,
                           uint32_t (*hash_func)(const Slice& key),
                           size_t huge_page_tlb_size)
    : kBlocked(cl_per_block > 0),
      kBitsPerBlock(std::min(cl_per_block, num_probes) * CACHE_LINE_SIZE * 8),
      kTotalBits((kBlocked ? (total_bits + kBitsPerBlock - 1) / kBitsPerBlock *
                                 kBitsPerBlock
                           : total_bits + 7) /
                 8 * 8),
      kNumBlocks(kBlocked ? kTotalBits / kBitsPerBlock : 1),
      kNumProbes(num_probes),
      hash_func_(hash_func == nullptr ? &BloomHash : hash_func) {
  assert(kBlocked ? kTotalBits > 0 : kTotalBits >= kBitsPerBlock);
  assert(kNumProbes > 0);

  uint32_t sz = kTotalBits / 8;
  if (kBlocked) {
    sz += CACHE_LINE_SIZE - 1;
  }
  raw_ = reinterpret_cast<unsigned char*>(
      arena_.AllocateAligned(sz, huge_page_tlb_size));
  memset(raw_, 0, sz);
  if (kBlocked && (reinterpret_cast<uint64_t>(raw_) % CACHE_LINE_SIZE)) {
    data_ = raw_ + CACHE_LINE_SIZE -
      reinterpret_cast<uint64_t>(raw_) % CACHE_LINE_SIZE;
  } else {
    data_ = raw_;
  }
}

}  // rocksdb
