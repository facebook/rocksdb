//  Copyright (c) 2019-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Implementation details of various Bloom filter implementations used in
// RocksDB. (DynamicBloom is in a separate file for now because it
// supports concurrent write.)

#pragma once
#include <stddef.h>
#include <stdint.h>

#include "rocksdb/slice.h"

namespace rocksdb {

// A legacy Bloom filter implementation with no locality of probes (slow).
// It uses double hashing to generate a sequence of hash values.
// Asymptotic analysis is in [Kirsch,Mitzenmacher 2006], but known to have
// subtle accuracy flaws for practical sizes [Dillinger,Manolios 2004].
//
// DO NOT REUSE - faster and more predictably accurate implementations
// are available at
// https://github.com/pdillinger/wormhashing/blob/master/bloom_simulation_tests/foo.cc
// See e.g. RocksDB DynamicBloom.
//
class LegacyNoLocalityBloomImpl {
 public:
  static inline void AddHash(uint32_t h, uint32_t total_bits, int num_probes,
                             char *data) {
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (int i = 0; i < num_probes; i++) {
      const uint32_t bitpos = h % total_bits;
      data[bitpos / 8] |= (1 << (bitpos % 8));
      h += delta;
    }
  }

  static inline bool HashMayMatch(uint32_t h, uint32_t total_bits,
                                  int num_probes, const char *data) {
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (int i = 0; i < num_probes; i++) {
      const uint32_t bitpos = h % total_bits;
      if ((data[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
    return true;
  }
};

// A legacy Bloom filter implementation with probes local to a single
// cache line (fast). Because SST files might be transported between
// platforms, the cache line size is a parameter rather than hard coded.
// (But if specified as a constant parameter, an optimizing compiler
// should take advantage of that.)
//
// When ExtraRotates is false, this implementation is notably deficient in
// accuracy. Specifically, it uses double hashing with a 1/512 chance of the
// increment being zero (when cache line size is 512 bits). Thus, there's a
// 1/512 chance of probing only one index, which we'd expect to incur about
// a 1/2 * 1/512 or absolute 0.1% FP rate penalty. More detail at
// https://github.com/facebook/rocksdb/issues/4120
//
// DO NOT REUSE - faster and more predictably accurate implementations
// are available at
// https://github.com/pdillinger/wormhashing/blob/master/bloom_simulation_tests/foo.cc
// See e.g. RocksDB DynamicBloom.
//
template <bool ExtraRotates>
class LegacyLocalityBloomImpl {
 private:
  static inline uint32_t GetLine(uint32_t h, uint32_t num_lines) {
    uint32_t offset_h = ExtraRotates ? (h >> 11) | (h << 21) : h;
    return offset_h % num_lines;
  }

 public:
  static inline void AddHash(uint32_t h, uint32_t num_lines, int num_probes,
                             char *data, int log2_cache_line_bytes) {
    const int log2_cache_line_bits = log2_cache_line_bytes + 3;

    char *data_at_offset =
        data + (GetLine(h, num_lines) << log2_cache_line_bytes);
    const uint32_t delta = (h >> 17) | (h << 15);
    for (int i = 0; i < num_probes; ++i) {
      // Mask to bit-within-cache-line address
      const uint32_t bitpos = h & ((1 << log2_cache_line_bits) - 1);
      data_at_offset[bitpos / 8] |= (1 << (bitpos % 8));
      if (ExtraRotates) {
        h = (h >> log2_cache_line_bits) | (h << (32 - log2_cache_line_bits));
      }
      h += delta;
    }
  }

  static inline void PrepareHashMayMatch(uint32_t h, uint32_t num_lines,
                                         const char *data,
                                         uint32_t /*out*/ *byte_offset,
                                         int log2_cache_line_bytes) {
    uint32_t b = GetLine(h, num_lines) << log2_cache_line_bytes;
    PREFETCH(data + b, 0 /* rw */, 1 /* locality */);
    PREFETCH(data + b + ((1 << log2_cache_line_bytes) - 1), 0 /* rw */,
             1 /* locality */);
    *byte_offset = b;
  }

  static inline bool HashMayMatch(uint32_t h, uint32_t num_lines,
                                  int num_probes, const char *data,
                                  int log2_cache_line_bytes) {
    uint32_t b = GetLine(h, num_lines) << log2_cache_line_bytes;
    return HashMayMatchPrepared(h, num_probes, data + b, log2_cache_line_bytes);
  }

  static inline bool HashMayMatchPrepared(uint32_t h, int num_probes,
                                          const char *data_at_offset,
                                          int log2_cache_line_bytes) {
    const int log2_cache_line_bits = log2_cache_line_bytes + 3;

    const uint32_t delta = (h >> 17) | (h << 15);
    for (int i = 0; i < num_probes; ++i) {
      // Mask to bit-within-cache-line address
      const uint32_t bitpos = h & ((1 << log2_cache_line_bits) - 1);
      if (((data_at_offset[bitpos / 8]) & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      if (ExtraRotates) {
        h = (h >> log2_cache_line_bits) | (h << (32 - log2_cache_line_bits));
      }
      h += delta;
    }
    return true;
  }
};

}  // namespace rocksdb
