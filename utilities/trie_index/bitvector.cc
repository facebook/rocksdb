//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trie_index/bitvector.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "port/lang.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace trie_index {

// ============================================================================
// Bitvector serialization format:
//
//   [num_bits   : uint64_t (fixed 8 bytes)]
//   [num_ones   : uint64_t (fixed 8 bytes)]
//   [words      : num_words * 8 bytes, where num_words = ceil(num_bits/64)]
//   [rank_lut   : num_rank_samples * 4 bytes (uint32_t entries)]
//
// num_rank_samples = num_bits / kBitsPerRankSample + 1
// The +1 is for the sentinel entry at the end that stores total popcount.
//
// Using uint32_t for rank LUT entries halves the LUT memory overhead compared
// to uint64_t. See bitvector.h for why this is safe for trie index bitvectors.
// ============================================================================

size_t Bitvector::SerializedSize() const {
  // 2 uint64_t for header + words (uint64_t) + rank LUT (uint32_t) + padding.
  // Pad the rank LUT to 8-byte alignment so the next data structure in the
  // serialized buffer starts at an 8-byte boundary.
  size_t rank_bytes = num_rank_samples_ * sizeof(uint32_t);
  size_t rank_padded = (rank_bytes + 7) & ~size_t(7);  // round up to 8
  return 2 * sizeof(uint64_t) + num_words_ * sizeof(uint64_t) + rank_padded;
}

void Bitvector::Serialize(std::string* output) const {
  size_t old_size = output->size();
  size_t needed = SerializedSize();
  output->resize(old_size + needed);
  char* dst = &(*output)[old_size];

  // Write header: num_bits, num_ones
  memcpy(dst, &num_bits_, sizeof(uint64_t));
  dst += sizeof(uint64_t);
  memcpy(dst, &num_ones_, sizeof(uint64_t));
  dst += sizeof(uint64_t);

  // Write words
  if (num_words_ > 0) {
    memcpy(dst, words_, num_words_ * sizeof(uint64_t));
    dst += num_words_ * sizeof(uint64_t);
  }

  // Write rank LUT (uint32_t entries) with padding to 8-byte alignment.
  if (num_rank_samples_ > 0) {
    size_t rank_bytes = num_rank_samples_ * sizeof(uint32_t);
    memcpy(dst, rank_lut_, rank_bytes);
    // Zero-fill padding bytes (if any) to maintain deterministic output.
    size_t rank_padded = (rank_bytes + 7) & ~size_t(7);
    if (rank_padded > rank_bytes) {
      memset(dst + rank_bytes, 0, rank_padded - rank_bytes);
    }
  }
}

Status Bitvector::InitFromData(const char* data, size_t data_size,
                               size_t* bytes_consumed) {
  const char* start = data;

  // Read header
  if (data_size < 2 * sizeof(uint64_t)) {
    return Status::Corruption("Bitvector: insufficient data for header");
  }
  memcpy(&num_bits_, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  memcpy(&num_ones_, data, sizeof(uint64_t));
  data += sizeof(uint64_t);
  data_size -= 2 * sizeof(uint64_t);

  // Validate header fields from untrusted data.
  if (num_ones_ > num_bits_) {
    return Status::Corruption("Bitvector: num_ones exceeds num_bits");
  }
  // The rank LUT uses uint32_t entries, so num_bits must fit in uint32_t.
  // This also prevents integer overflow in derived size computations below.
  if (num_bits_ > UINT32_MAX) {
    return Status::Corruption(
        "Bitvector: num_bits exceeds uint32_t range for rank LUT");
  }

  // Compute derived sizes
  num_words_ = (num_bits_ + 63) / 64;
  num_rank_samples_ = num_bits_ / kBitsPerRankSample + 1;

  // Read words
  size_t words_bytes = num_words_ * sizeof(uint64_t);
  if (data_size < words_bytes) {
    return Status::Corruption("Bitvector: insufficient data for words");
  }
  // The data pointer must be 8-byte aligned for safe uint64_t access.
  // This is guaranteed by the serialization format (header is 16 bytes,
  // and the overall buffer comes from aligned block cache allocations).
  if (reinterpret_cast<uintptr_t>(data) % alignof(uint64_t) != 0) {
    return Status::Corruption("Bitvector: words data not 8-byte aligned");
  }
  words_ = reinterpret_cast<const uint64_t*>(data);
  data += words_bytes;
  data_size -= words_bytes;

  // Read rank LUT (uint32_t entries, padded to 8-byte alignment)
  size_t rank_bytes = num_rank_samples_ * sizeof(uint32_t);
  size_t rank_padded = (rank_bytes + 7) & ~size_t(7);
  if (data_size < rank_padded) {
    return Status::Corruption("Bitvector: insufficient data for rank LUT");
  }
  if (reinterpret_cast<uintptr_t>(data) % alignof(uint32_t) != 0) {
    return Status::Corruption("Bitvector: rank LUT not 4-byte aligned");
  }
  rank_lut_ = reinterpret_cast<const uint32_t*>(data);
  data += rank_padded;

  *bytes_consumed = static_cast<size_t>(data - start);
  return Status::OK();
}

void Bitvector::BuildFrom(const BitvectorBuilder& builder) {
  num_bits_ = builder.NumBits();
  num_words_ = (num_bits_ + 63) / 64;
  num_rank_samples_ = num_bits_ / kBitsPerRankSample + 1;

  // Allocate owned storage for words + rank LUT in a single buffer.
  size_t words_bytes = num_words_ * sizeof(uint64_t);
  size_t rank_bytes = num_rank_samples_ * sizeof(uint32_t);
  owned_data_.resize(words_bytes + rank_bytes, '\0');

  // Copy words
  if (num_words_ > 0) {
    memcpy(&owned_data_[0], builder.Words().data(), words_bytes);
  }
  words_ = reinterpret_cast<const uint64_t*>(owned_data_.data());
  rank_lut_ =
      reinterpret_cast<const uint32_t*>(owned_data_.data() + words_bytes);
  // words_bytes is always a multiple of 8 (num_words_ * 8), and std::string
  // allocators typically return 8+ byte aligned memory, so rank_lut_ should
  // be at least 4-byte aligned.
  assert(reinterpret_cast<uintptr_t>(rank_lut_) % alignof(uint32_t) == 0);

  BuildRankLUT();
}

void Bitvector::BuildRankLUT() {
  // The rank LUT is mutable during construction, so we const_cast here.
  // After BuildRankLUT() completes, rank_lut_ is treated as read-only.
  uint32_t* lut = const_cast<uint32_t*>(rank_lut_);

  // Guard: uint32_t can hold values up to ~4 billion. Since the maximum
  // cumulative rank equals num_bits_, assert it fits.
  assert(num_bits_ <= UINT32_MAX);

  uint64_t cumulative = 0;
  for (uint64_t s = 0; s < num_rank_samples_; s++) {
    lut[s] = static_cast<uint32_t>(cumulative);
    // Sum popcount for the kWordsPerRankSample words in this sample interval.
    uint64_t word_start = s * kWordsPerRankSample;
    uint64_t word_end = std::min(word_start + kWordsPerRankSample, num_words_);
    for (uint64_t w = word_start; w < word_end; w++) {
      cumulative += Popcount(words_[w]);
    }
  }
  // Total number of 1-bits is the cumulative count after all words.
  num_ones_ = cumulative;
}

uint64_t Bitvector::Select1(uint64_t i) const {
  if (i >= num_ones_) {
    return num_bits_;
  }

  // Binary search on the rank LUT to find the sample interval containing
  // the i-th 1-bit.
  uint64_t lo = 0;
  uint64_t hi = num_rank_samples_ - 1;
  while (lo < hi) {
    uint64_t mid = lo + (hi - lo + 1) / 2;
    if (rank_lut_[mid] <= i) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }

  // `lo` is the rank sample interval. Now scan words within this interval.
  uint64_t remaining = i - rank_lut_[lo];
  uint64_t word_start = lo * kWordsPerRankSample;
  uint64_t word_end = std::min(word_start + kWordsPerRankSample, num_words_);

  for (uint64_t w = word_start; w < word_end; w++) {
    uint64_t pc = Popcount(words_[w]);
    if (remaining < pc) {
      // The target bit is in this word. Find the `remaining`-th 1-bit
      // using popcount-based binary search within the 64-bit word.
      // This is O(log 64) = O(6) instead of O(remaining) for the
      // bit-clearing loop, and matches the SuRF reference implementation.
      return w * 64 + Select64(words_[w], remaining);
    }
    remaining -= pc;
  }

  // Should not reach here if i < num_ones_.
  assert(false);
  return num_bits_;
}

uint64_t Bitvector::Select0(uint64_t i) const {
  uint64_t num_zeros = num_bits_ - num_ones_;
  if (i >= num_zeros) {
    return num_bits_;
  }

  // Binary search on the rank LUT. For each sample s, the number of zeros
  // up to sample s is: (s * kBitsPerRankSample) - rank_lut_[s].
  uint64_t lo = 0;
  uint64_t hi = num_rank_samples_ - 1;
  while (lo < hi) {
    uint64_t mid = lo + (hi - lo + 1) / 2;
    uint64_t zeros_at_mid = mid * kBitsPerRankSample - rank_lut_[mid];
    if (zeros_at_mid <= i) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }

  uint64_t zeros_at_lo = lo * kBitsPerRankSample - rank_lut_[lo];
  uint64_t remaining = i - zeros_at_lo;
  uint64_t word_start = lo * kWordsPerRankSample;
  uint64_t word_end = std::min(word_start + kWordsPerRankSample, num_words_);

  for (uint64_t w = word_start; w < word_end; w++) {
    // For the last word, we may have fewer than 64 valid bits. We need to
    // mask out the padding bits so they don't count as zeros.
    uint64_t valid_bits = 64;
    if (w == num_words_ - 1 && num_bits_ % 64 != 0) {
      valid_bits = num_bits_ % 64;
    }
    uint64_t zeros_in_word = valid_bits - Popcount(words_[w]);
    if (remaining < zeros_in_word) {
      // The target 0-bit is in this word. Invert to get 1-bits at zero
      // positions, mask off padding, then use popcount-based binary search.
      uint64_t word = ~words_[w];
      if (valid_bits < 64) {
        word &= (uint64_t(1) << valid_bits) - 1;
      }
      return w * 64 + Select64(word, remaining);
    }
    remaining -= zeros_in_word;
  }

  assert(false);
  return num_bits_;
}

uint64_t Bitvector::NextSetBit(uint64_t pos) const {
  if (pos >= num_bits_) return num_bits_;

  uint64_t word_idx = pos / 64;
  uint64_t bit_idx = pos % 64;

  // Check remaining bits in the current word.
  uint64_t word = words_[word_idx] >> bit_idx;
  if (word != 0) {
    uint64_t result = pos + Ctz(word);
    return (result < num_bits_) ? result : num_bits_;
  }

  // Scan subsequent words.
  for (uint64_t w = word_idx + 1; w < num_words_; w++) {
    if (words_[w] != 0) {
      uint64_t result = w * 64 + Ctz(words_[w]);
      return (result < num_bits_) ? result : num_bits_;
    }
  }
  return num_bits_;
}

uint64_t Bitvector::DistanceToNextSetBit(uint64_t pos) const {
  assert(pos < num_bits_);
  assert(GetBit(pos));  // pos must be a set bit.

  // Find the next set bit after pos (exclusive).
  uint64_t next = NextSetBit(pos + 1);
  return next - pos;
}

}  // namespace trie_index
}  // namespace ROCKSDB_NAMESPACE
