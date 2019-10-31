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
#include "util/hash.h"

#ifdef HAVE_AVX2
#include <immintrin.h>
#endif

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

// A fast, flexible, and accurate cache-local Bloom implementation with
// SIMD-optimized query performance (currently using AVX2 on Intel). Write
// performance and non-SIMD read are very good, benefiting from fastrange32
// used in place of % and single-cycle multiplication on recent processors.
//
// Most other SIMD Bloom implementations sacrifice flexibility and/or
// accuracy by requiring num_probes to be a power of two and restricting
// where each probe can occur in a cache line. This implementation sacrifices
// SIMD-optimization for add (might still be possible, especially with AVX512)
// in favor of allowing any num_probes, not crossing cache line boundary,
// and accuracy close to theoretical best accuracy for a cache-local Bloom.
// E.g. theoretical best for 10 bits/key, num_probes=6, and 512-bit bucket
// (Intel cache line size) is 0.9535% FP rate. This implementation yields
// about 0.957%. (Compare to LegacyLocalityBloomImpl<false> at 1.138%, or
// about 0.951% for 1024-bit buckets, cache line size for some ARM CPUs.)
//
// This implementation can use a 32-bit hash (let h2 be h1 * 0x9e3779b9) or
// a 64-bit hash (split into two uint32s). With many millions of keys, the
// false positive rate associated with using a 32-bit hash can dominate the
// false positive rate of the underlying filter. At 10 bits/key setting, the
// inflection point is about 40 million keys, so 32-bit hash is a bad idea
// with 10s of millions of keys or more.
//
// Despite accepting a 64-bit hash, this implementation uses 32-bit fastrange
// to pick a cache line, which can be faster than 64-bit in some cases.
// This only hurts accuracy as you get into 10s of GB for a single filter,
// and accuracy abruptly breaks down at 256GB (2^32 cache lines). Switch to
// 64-bit fastrange if you need filters so big. ;)
//
// Using only a 32-bit input hash within each cache line has negligible
// impact for any reasonable cache line / bucket size, for arbitrary filter
// size, and potentially saves intermediate data size in some cases vs.
// tracking full 64 bits. (Even in an implementation using 64-bit arithmetic
// to generate indices, I might do the same, as a single multiplication
// suffices to generate a sufficiently mixed 64 bits from 32 bits.)
//
// This implementation is currently tied to Intel cache line size, 64 bytes ==
// 512 bits. If there's sufficient demand for other cache line sizes, this is
// a pretty good implementation to extend, but slight performance enhancements
// are possible with an alternate implementation (probably not very compatible
// with SIMD):
// (1) Use rotation in addition to multiplication for remixing
// (like murmur hash). (Using multiplication alone *slightly* hurts accuracy
// because lower bits never depend on original upper bits.)
// (2) Extract more than one bit index from each re-mix. (Only if rotation
// or similar is part of remix, because otherwise you're making the
// multiplication-only problem worse.)
// (3) Re-mix full 64 bit hash, to get maximum number of bit indices per
// re-mix.
//
class FastLocalBloomImpl {
 public:
  static inline void AddHash(uint32_t h1, uint32_t h2, uint32_t len_bytes,
                             int num_probes, char *data) {
    uint32_t bytes_to_cache_line = fastrange32(len_bytes >> 6, h1) << 6;
    char *data_at_cache_line = data + bytes_to_cache_line;
    PREFETCH(data_at_cache_line, 1 /* rw */, 1 /* locality */);
    AddHashPrepared(h2, num_probes, data_at_cache_line);
  }

  static inline void AddHashPrepared(uint32_t h2, int num_probes,
                                     char *data_at_cache_line) {
    uint32_t h = h2;
    for (int i = 0; i < num_probes; ++i, h *= uint32_t{0x9e3779b9}) {
      // 9-bit address within 512 bit cache line
      int bitpos = h >> (32 - 9);
      data_at_cache_line[bitpos >> 3] |= (uint8_t(1) << (bitpos & 7));
    }
  }

  static inline void PrepareHashMayMatch(uint32_t h1, uint32_t len_bytes,
                                         const char *data,
                                         uint32_t /*out*/ *byte_offset) {
    uint32_t bytes_to_cache_line = fastrange32(len_bytes >> 6, h1) << 6;
    PREFETCH(data + bytes_to_cache_line, 0 /* rw */, 1 /* locality */);
    *byte_offset = bytes_to_cache_line;
  }

  static inline bool HashMayMatch(uint32_t h1, uint32_t h2, uint32_t len_bytes,
                                  int num_probes, const char *data) {
    uint32_t bytes_to_cache_line = fastrange32(len_bytes >> 6, h1) << 6;
    return HashMayMatchPrepared(h2, num_probes, data + bytes_to_cache_line);
  }

  static inline bool HashMayMatchPrepared(uint32_t h2, int num_probes,
                                          const char *data_at_cache_line) {
    uint32_t h = h2;
#ifdef HAVE_AVX2
    int rem_probes = num_probes;

    // NOTE: For better performance for num_probes in {1, 2, 9, 10, 17, 18,
    // etc.} one can insert specialized code for rem_probes <= 2, bypassing
    // the SIMD code in those cases. There is a detectable but minor overhead
    // applied to other values of num_probes (when not statically determined),
    // but smoother performance curve vs. num_probes. But for now, when
    // in doubt, don't add unnecessary code.

    // Powers of 32-bit golden ratio, mod 2**32.
    const __m256i multipliers =
        _mm256_setr_epi32(0x00000001, 0x9e3779b9, 0xe35e67b1, 0x734297e9,
                          0x35fbe861, 0xdeb7c719, 0x448b211, 0x3459b749);

    for (;;) {
      // Eight copies of hash
      __m256i hash_vector = _mm256_set1_epi32(h);

      // Same effect as repeated multiplication by 0x9e3779b9 thanks to
      // associativity of multiplication.
      hash_vector = _mm256_mullo_epi32(hash_vector, multipliers);

      // Shift right by 28 bits to get 4-bit addresses of 32-bit values within
      // a cache line.
      const __m256i word_addresses = _mm256_srli_epi32(hash_vector, 28);

      // Gather 32-bit values spread over 512 bits by 4-bit address.
      //
      // Option 1: AVX2 gather (seems to be a little slow - understandable)
      // const __m256i value_vector =
      //     _mm256_i32gather_epi32(static_cast<const int
      //     *>(data_at_cache_line),
      //                            word_addresses,
      //                            /*bytes / i32*/ 4);
      // END Option 1
      // Potentially unaligned as we're not *always* cache-aligned -> loadu
      const __m256i *mm_data =
          reinterpret_cast<const __m256i *>(data_at_cache_line);
      __m256i lower = _mm256_loadu_si256(mm_data);
      __m256i upper = _mm256_loadu_si256(mm_data + 1);
      // Option 2: AVX512VL permute hack
      // Only negligibly faster than Option 3, so not yet worth supporting
      // const __m256i value_vector =
      //    _mm256_permutex2var_epi32(lower, word_addresses, upper);
      // END Option 2
      // Option 3: AVX2 permute+blend hack
      // Use lowest three bits to order probing values, as if all from same
      // 256 bit piece.
      lower = _mm256_permutevar8x32_epi32(lower, word_addresses);
      upper = _mm256_permutevar8x32_epi32(upper, word_addresses);
      // Just top 1 bit of address, to select between lower and upper.
      const __m256i upper_lower_selector = _mm256_srai_epi32(hash_vector, 31);
      // Finally: the next 8 probed 32-bit values, in probing sequence order.
      const __m256i value_vector =
          _mm256_blendv_epi8(lower, upper, upper_lower_selector);
      // END Option 3

      // We might not need to probe all 8, so build a mask for selecting only
      // what we need. The k_selector(s) could be pre-computed but that
      // doesn't seem to make a noticeable performance difference.
      const __m256i zero_to_seven = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
      __m256i k_selector =
          _mm256_sub_epi32(zero_to_seven, _mm256_set1_epi32(rem_probes));
      // Keep only high bit; negative after subtract -> use/select
      k_selector = _mm256_srli_epi32(k_selector, 31);

      // Strip off the 4 bit word address (shift left)
      __m256i bit_addresses = _mm256_slli_epi32(hash_vector, 4);
      // And keep only 5-bit (32 - 27) bit-within-32-bit-word addresses.
      bit_addresses = _mm256_srli_epi32(bit_addresses, 27);
      // Build a bit mask
      const __m256i bit_mask = _mm256_sllv_epi32(k_selector, bit_addresses);

      // Like ((~value_vector) & bit_mask) == 0)
      bool match = _mm256_testc_si256(value_vector, bit_mask) != 0;

      // This check first so that it's easy for branch predictor to optimize
      // num_probes <= 8 case, making it free of unpredictable branches.
      if (rem_probes <= 8) {
        return match;
      } else if (!match) {
        return false;
      }
      // otherwise
      // Need another iteration. 0xab25f4c1 == golden ratio to the 8th power
      h *= 0xab25f4c1;
      rem_probes -= 8;
    }
#else
    for (int i = 0; i < num_probes; ++i, h *= uint32_t{0x9e3779b9}) {
      // 9-bit address within 512 bit cache line
      int bitpos = h >> (32 - 9);
      if ((data_at_cache_line[bitpos >> 3] & (char(1) << (bitpos & 7))) == 0) {
        return false;
      }
    }
    return true;
#endif
  }
};

}  // namespace rocksdb
