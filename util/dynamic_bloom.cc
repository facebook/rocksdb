// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "dynamic_bloom.h"

#include <algorithm>

#include "memory/allocator.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/hash.h"

namespace rocksdb {

namespace {

unsigned roundUpToPow2(unsigned x) {
  unsigned rv = 1;
  while (rv < x) {
    rv <<= 1;
  }
  return rv;
}

}

DynamicBloom::DynamicBloom(Allocator* allocator, uint32_t total_bits,
                           uint32_t num_probes,
                           size_t huge_page_tlb_size, Logger* logger)
    // Round down, except round up with 1
    : kNumDoubleProbes((num_probes + (num_probes == 1)) / 2) {
  assert(num_probes % 2 == 0); // limitation of current implementation
  assert(kNumDoubleProbes > 0);

  // Determine how much to round off + align by so that x ^ i (that's xor) is
  // a valid u64 index if x is a valid u64 index and 0 <= i < kNumDoubleProbes.
  unsigned block_bytes = /*bytes/u64*/ 8 *
                         /*u64s*/ std::max(1U, roundUpToPow2(kNumDoubleProbes));
  kLen = (total_bits + (/*bits/byte*/ 8 * block_bytes - 1)) /
         /*bits/u64*/ 64;
  assert(kLen > 0);

  uint32_t sz = kLen * /*bytes/u64*/ 8;
  sz += block_bytes - 1;
  assert(allocator);

  char* raw = allocator->AllocateAligned(sz, huge_page_tlb_size, logger);
  memset(raw, 0, sz);
  auto block_offset = reinterpret_cast<uintptr_t>(raw) % block_bytes;
  if (block_offset > 0) {
    raw += block_bytes - block_offset;
  }
  data_ = reinterpret_cast<std::atomic<uint64_t>*>(raw);
}

}  // rocksdb
