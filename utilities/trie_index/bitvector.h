//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  *****************************************************************
//  EXPERIMENTAL - subject to change while under development
//  *****************************************************************

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Bit-vector with O(1) rank and select operations.
//
// This is the foundational data structure for the LOUDS-based succinct trie.
// It stores a sequence of bits and supports:
//   - rank1(pos): Count of 1-bits in positions [0, pos)
//   - select1(i): Position of the i-th 1-bit (0-indexed)
//   - rank0(pos): Count of 0-bits in positions [0, pos)
//   - select0(i): Position of the i-th 0-bit (0-indexed)
//
// Implementation uses a two-level lookup table for rank (sampling every 512
// bits) and a combination of binary search + popcount for select. The rank
// operation is O(1); select is O(log(n/512)) but fast in practice due to
// small constants and branch prediction.
//
// Memory layout (serialized):
//   [num_bits: uint64_t]
//   [num_ones: uint64_t]
//   [raw bits: ceil(num_bits/64) uint64_t words, 64-bit aligned]
//   [rank LUT: (num_bits/512 + 1) uint32_t cumulative rank values]
//
// The rank LUT uses uint32_t entries (not uint64_t), which halves the LUT
// memory overhead. This is safe because the maximum cumulative popcount
// equals num_bits, and uint32_t can hold values up to ~4 billion. This
// limits individual bitvectors to ~4 billion bits, which is far beyond
// any realistic trie index: the largest bitvector (d_labels_) uses 256
// bits per dense trie node, so the limit corresponds to ~16 million dense
// nodes. A typical SST file has 16K-64K data blocks, producing a trie
// with at most a few hundred thousand nodes. An assertion in BuildRankLUT
// guards against overflow.
//
// The rank LUT stores cumulative popcount at every 512-bit (8-word) boundary.
// For positions between boundaries, we compute the remaining popcount using
// hardware popcount on the intermediate words.
// ============================================================================

// Number of bits per rank lookup table entry. Must be a power of 2 and a
// multiple of 64. 512 is chosen because it gives a good balance between LUT
// size overhead (~0.8% of bitvector size with uint32_t entries) and the number
// of popcounts needed per rank query (at most 8).
inline constexpr uint64_t kBitsPerRankSample = 512;
inline constexpr uint64_t kWordsPerRankSample = kBitsPerRankSample / 64;

// Portable popcount using RocksDB's BitsSetToOne, which handles MSVC,
// GCC, and Clang with hardware POPCNT when available.
inline uint64_t Popcount(uint64_t x) {
  return static_cast<uint64_t>(BitsSetToOne(x));
}

// Count trailing zeros. Returns 64 if x == 0. Uses RocksDB's
// CountTrailingZeroBits for portability (MSVC + GCC/Clang).
inline uint64_t Ctz(uint64_t x) {
  return x == 0 ? 64 : static_cast<uint64_t>(CountTrailingZeroBits(x));
}

// Select the i-th set bit (0-indexed) within a 64-bit word.
// Uses popcount-based binary search: O(log 64) = O(6) popcounts.
// This is significantly faster than the naive bit-clearing loop for
// large values of i, and matches the SuRF reference implementation's
// select64_popcount_search().
// Precondition: i < Popcount(word).
inline uint64_t Select64(uint64_t word, uint64_t i) {
  // Binary search: narrow down which 32-bit half, then 16, 8, 4, 2, 1.
  uint64_t pos = 0;
  uint64_t pc;

  pc = Popcount(word & 0xFFFFFFFFULL);
  if (i >= pc) {
    word >>= 32;
    pos += 32;
    i -= pc;
  }
  pc = Popcount(word & 0xFFFFULL);
  if (i >= pc) {
    word >>= 16;
    pos += 16;
    i -= pc;
  }
  pc = Popcount(word & 0xFFULL);
  if (i >= pc) {
    word >>= 8;
    pos += 8;
    i -= pc;
  }
  pc = Popcount(word & 0xFULL);
  if (i >= pc) {
    word >>= 4;
    pos += 4;
    i -= pc;
  }
  pc = Popcount(word & 0x3ULL);
  if (i >= pc) {
    word >>= 2;
    pos += 2;
    i -= pc;
  }
  pc = word & 1;
  if (i >= pc) {
    pos += 1;
  }
  return pos;
}

// ============================================================================
// BitvectorBuilder: Builds a bitvector incrementally, bit by bit.
// ============================================================================
class BitvectorBuilder {
 public:
  BitvectorBuilder() : num_bits_(0) {}

  // Append a single bit (0 or 1).
  void Append(bool bit) {
    if (num_bits_ % 64 == 0) {
      words_.push_back(0);
    }
    if (bit) {
      words_.back() |= (uint64_t(1) << (num_bits_ % 64));
    }
    num_bits_++;
  }

  // Append a full 64-bit word of `nbits` bits (1..64). The bits are taken
  // from the low `nbits` positions of `word` in LSB-first order. This is
  // used by the LOUDS-Dense builder to emit 256-bit label bitmaps as 4
  // word-level operations instead of 256 individual Append() calls.
  // Precondition: num_bits_ must be 64-bit aligned (i.e., num_bits_ % 64 == 0).
  void AppendWord(uint64_t word, uint64_t nbits) {
    assert(nbits > 0 && nbits <= 64);
    assert(num_bits_ % 64 == 0);  // Must be word-aligned.
    words_.push_back(word);
    num_bits_ += nbits;
  }

  // Append `count` copies of `bit`. Optimized to operate at word granularity
  // when possible, which is significantly faster than the bit-by-bit loop for
  // large counts (e.g., appending 256 zeros for an empty dense node).
  void AppendMultiple(bool bit, uint64_t count) {
    if (count == 0) return;

    // Fill partial word at the end of the current buffer.
    uint64_t partial = num_bits_ % 64;
    if (partial > 0) {
      uint64_t fill = std::min(count, 64 - partial);
      if (bit) {
        // Set bits [partial, partial + fill) in the last word.
        uint64_t mask =
            ((fill == 64) ? ~uint64_t(0) : ((uint64_t(1) << fill) - 1))
            << partial;
        words_.back() |= mask;
      }
      // For bit==0, no action needed (words are zero-initialized).
      num_bits_ += fill;
      count -= fill;
    }

    // Append full 64-bit words.
    uint64_t full_word = bit ? ~uint64_t(0) : uint64_t(0);
    while (count >= 64) {
      words_.push_back(full_word);
      num_bits_ += 64;
      count -= 64;
    }

    // Append remaining bits (< 64).
    if (count > 0) {
      if (bit) {
        words_.push_back((uint64_t(1) << count) - 1);
      } else {
        words_.push_back(0);
      }
      num_bits_ += count;
    }
  }

  uint64_t NumBits() const { return num_bits_; }

  // Pre-allocate capacity for at least `num_bits` bits. Avoids repeated
  // reallocations when the final size is known or can be estimated.
  void Reserve(uint64_t num_bits) { words_.reserve((num_bits + 63) / 64); }

  // Access a specific bit. For testing/debugging only.
  bool GetBit(uint64_t pos) const {
    assert(pos < num_bits_);
    return (words_[pos / 64] >> (pos % 64)) & 1;
  }

  // Return the underlying word array.
  const std::vector<uint64_t>& Words() const { return words_; }

 private:
  std::vector<uint64_t> words_;
  uint64_t num_bits_;
};

// ============================================================================
// Bitvector: Immutable bitvector with O(1) rank and O(log n) select.
//
// Created from serialized data (e.g., read from an SST file meta-block) or
// from a BitvectorBuilder. The bitvector does NOT own the underlying memory
// when created from a Slice — the caller must ensure the data outlives this
// object.
// ============================================================================
class Bitvector {
 public:
  Bitvector()
      : words_(nullptr),
        rank_lut_(nullptr),
        num_bits_(0),
        num_ones_(0),
        num_words_(0),
        num_rank_samples_(0) {}

  // Bitvector contains raw pointers (words_, rank_lut_) that may point into
  // owned_data_ or into external memory (InitFromData). Copying would create
  // dangling pointers, so copy is deleted. Move is safe only when the pointers
  // point into owned_data_ (BuildFrom case) because std::string's move
  // constructor preserves the buffer address for SSO-exceeding strings. For
  // the InitFromData case, the pointers reference external memory and are
  // unaffected by moving owned_data_ (which is empty).
  Bitvector(const Bitvector&) = delete;
  Bitvector& operator=(const Bitvector&) = delete;
  Bitvector(Bitvector&& other) noexcept
      : words_(other.words_),
        rank_lut_(other.rank_lut_),
        num_bits_(other.num_bits_),
        num_ones_(other.num_ones_),
        num_words_(other.num_words_),
        num_rank_samples_(other.num_rank_samples_),
        owned_data_(std::move(other.owned_data_)) {
    // If this bitvector owns its data, the pointers must be re-seated into
    // our owned_data_ buffer. std::string move may or may not preserve the
    // buffer address (SSO optimization), so always re-seat.
    if (!owned_data_.empty()) {
      words_ = reinterpret_cast<const uint64_t*>(owned_data_.data());
      size_t words_bytes = num_words_ * sizeof(uint64_t);
      rank_lut_ =
          reinterpret_cast<const uint32_t*>(owned_data_.data() + words_bytes);
    }
    other.words_ = nullptr;
    other.rank_lut_ = nullptr;
    other.num_bits_ = 0;
    other.num_ones_ = 0;
    other.num_words_ = 0;
    other.num_rank_samples_ = 0;
  }
  Bitvector& operator=(Bitvector&& other) noexcept {
    if (this != &other) {
      words_ = other.words_;
      rank_lut_ = other.rank_lut_;
      num_bits_ = other.num_bits_;
      num_ones_ = other.num_ones_;
      num_words_ = other.num_words_;
      num_rank_samples_ = other.num_rank_samples_;
      owned_data_ = std::move(other.owned_data_);
      if (!owned_data_.empty()) {
        words_ = reinterpret_cast<const uint64_t*>(owned_data_.data());
        size_t words_bytes = num_words_ * sizeof(uint64_t);
        rank_lut_ =
            reinterpret_cast<const uint32_t*>(owned_data_.data() + words_bytes);
      }
      other.words_ = nullptr;
      other.rank_lut_ = nullptr;
      other.num_bits_ = 0;
      other.num_ones_ = 0;
      other.num_words_ = 0;
      other.num_rank_samples_ = 0;
    }
    return *this;
  }

  // Initialize from serialized data. The data pointer must remain valid for
  // the lifetime of this object. On success, sets `*bytes_consumed` to the
  // number of bytes read from the input. Returns Status::OK() on success,
  // or Status::Corruption() if the data is malformed.
  Status InitFromData(const char* data, size_t data_size,
                      size_t* bytes_consumed);

  // Serialize to a string. Appends serialized data to `output`.
  void Serialize(std::string* output) const;

  // Build from a BitvectorBuilder. This allocates owned memory.
  void BuildFrom(const BitvectorBuilder& builder);

  // ---- Core Operations ----

  // Get the bit at position `pos`.
  inline bool GetBit(uint64_t pos) const {
    assert(pos < num_bits_);
    return (words_[pos / 64] >> (pos % 64)) & 1;
  }

  // rank1(pos): Count of 1-bits in positions [0, pos).
  // pos can be in [0, num_bits_].
  inline uint64_t Rank1(uint64_t pos) const {
    assert(pos <= num_bits_);
    // Which rank sample does `pos` fall into?
    uint64_t sample_idx = pos / kBitsPerRankSample;
    uint64_t sample_rank = rank_lut_[sample_idx];
    // Count remaining 1-bits in words between the sample boundary and `pos`.
    uint64_t word_start = sample_idx * kWordsPerRankSample;
    uint64_t word_end = pos / 64;
    for (uint64_t w = word_start; w < word_end; w++) {
      sample_rank += Popcount(words_[w]);
    }
    // Count bits within the final partial word [0, pos % 64).
    uint64_t remaining = pos % 64;
    if (remaining > 0) {
      // Mask off the bits at and above position `remaining`.
      uint64_t mask = (uint64_t(1) << remaining) - 1;
      sample_rank += Popcount(words_[word_end] & mask);
    }
    return sample_rank;
  }

  // rank0(pos): Count of 0-bits in positions [0, pos).
  inline uint64_t Rank0(uint64_t pos) const { return pos - Rank1(pos); }

  // select1(i): Position of the i-th 1-bit (0-indexed).
  // Returns num_bits_ if there are fewer than (i+1) 1-bits.
  uint64_t Select1(uint64_t i) const;

  // select0(i): Position of the i-th 0-bit (0-indexed).
  // Returns num_bits_ if there are fewer than (i+1) 0-bits.
  uint64_t Select0(uint64_t i) const;

  // Find the next set bit at or after position `pos`.
  // Returns num_bits_ if no set bit is found.
  // Used by the trie for finding the next sibling label in dense nodes.
  uint64_t NextSetBit(uint64_t pos) const;

  // Find the distance from `pos` to the next set bit (exclusive).
  // Returns the distance in bits. Used by sparse level to compute node size.
  // pos must point to a set bit.
  uint64_t DistanceToNextSetBit(uint64_t pos) const;

  // ---- Accessors ----
  uint64_t NumBits() const { return num_bits_; }
  uint64_t NumOnes() const { return num_ones_; }
  uint64_t NumZeros() const { return num_bits_ - num_ones_; }

  // Size in bytes of the serialized representation.
  size_t SerializedSize() const;

 private:
  // Build the rank LUT from current words. Used by BuildFrom().
  void BuildRankLUT();

  // Pointer to the raw bit words. May point into external data (InitFromData)
  // or into owned_data_ (BuildFrom).
  const uint64_t* words_;

  // Pointer to the rank lookup table (uint32_t entries to halve LUT size).
  // Same ownership semantics as words_.
  const uint32_t* rank_lut_;

  uint64_t num_bits_;
  uint64_t num_ones_;
  uint64_t num_words_;
  uint64_t num_rank_samples_;

  // Owned storage when built from BitvectorBuilder (not from serialized data).
  std::string owned_data_;
};

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
