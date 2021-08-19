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

namespace ROCKSDB_NAMESPACE {

namespace {

uint32_t roundUpToPow2(uint32_t x) {
  uint32_t rv = 1;
  while (rv < x) {
    rv <<= 1;
  }
  return rv;
}
}

DynamicBloom::DynamicBloom(Allocator* allocator, uint32_t total_bits,
                           uint32_t num_probes, size_t huge_page_tlb_size,
                           Logger* logger)
    // Round down, except round up with 1
    : kNumDoubleProbes((num_probes + (num_probes == 1)) / 2) {
  assert(num_probes % 2 == 0);  // limitation of current implementation
  assert(num_probes <= 10);     // limitation of current implementation
  assert(kNumDoubleProbes > 0);

  // Determine how much to round off + align by so that x ^ i (that's xor) is
  // a valid u64 index if x is a valid u64 index and 0 <= i < kNumDoubleProbes.
  uint32_t block_bytes = /*bytes/u64*/ 8 *
                         /*u64s*/ std::max(1U, roundUpToPow2(kNumDoubleProbes));
  uint32_t block_bits = block_bytes * 8;
  uint32_t blocks = (total_bits + block_bits - 1) / block_bits;
  uint32_t sz = blocks * block_bytes;
  kLen = sz / /*bytes/u64*/ 8;
  assert(kLen > 0);
#ifndef NDEBUG
  for (uint32_t i = 0; i < kNumDoubleProbes; ++i) {
    // Ensure probes starting at last word are in range
    assert(((kLen - 1) ^ i) < kLen);
  }
#endif

  // Padding to correct for allocation not originally aligned on block_bytes
  // boundary
  sz += block_bytes - 1;
  assert(allocator);

  char* raw = allocator->AllocateAligned(sz, huge_page_tlb_size, logger);
  memset(raw, 0, sz);
  auto block_offset = reinterpret_cast<uintptr_t>(raw) % block_bytes;
  if (block_offset > 0) {
    // Align on block_bytes boundary
    raw += block_bytes - block_offset;
  }
  static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t),
                "Expecting zero-space-overhead atomic");
  data_ = reinterpret_cast<std::atomic<uint64_t>*>(raw);
}

uint64_t DynamicBloom::UniqueEntryEstimate() const {
  int one_bits = 0;
  uint64_t data_bit_len = kLen * sizeof(uint64_t);

  assert(kLen > 0);
  // Sample size is given by:
  // sample_size = n0/(1+(n0/N))
  // where n0 = (1.96**2)*0.5*(1-0.5)/(0.05**2) ~= 385
  //             for 95% confidence, 5% error margin
  //       N = population size (in our situation, total
  //           number of bits in the Bloom filter).
  uint32_t sample_bit_size =
      static_cast<uint32_t>(ceil(385.0 / (1.0 + (385.0 / (data_bit_len)))));
  uint32_t sample_byte_size =
      (sample_bit_size + sizeof(uint64_t) - 1) / sizeof(uint64_t);
  // Update sample bit size for later.
  sample_bit_size = sample_byte_size * sizeof(uint64_t);
  assert(sample_byte_size <= kLen);

  for (uint32_t i = 0; i < sample_byte_size; i++) {
    one_bits += BitsSetToOne<uint64_t>(data_[i]);
  }

  // Handle specific case where all bits sampled are set to 1.
  if (static_cast<uint32_t>(one_bits) == sample_bit_size)
    return rocksdb::port::kMaxUint64;

  // Else, from Samidass & Baldi (2007): # of items in a Bloom Filter
  // can be approximated with:
  // n_approx = -(m/k)ln [ 1- (X/m)], where:
  // m: length (size) of the filter (bits)
  // k: number of has functions (probes)
  // X: number of bits set to one
  // In our case:
  // (X/m) = one_bits_in_sample/sample_bit_size
  return static_cast<uint64_t>(
      std::ceil(-((1.0 * data_bit_len) / (2 * kNumDoubleProbes)) *
                std::log(1.0 - (one_bits * 1.0 / sample_bit_size))));
}

}  // namespace ROCKSDB_NAMESPACE
