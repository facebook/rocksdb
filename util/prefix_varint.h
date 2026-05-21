//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "port/likely.h"
#include "rocksdb/slice.h"
#include "util/cast_util.h"
#include "util/coding_lean.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

// Prefix varint is an established little-endian, low-bit-prefix alternative to
// the continuation-bit varint/LEB128 format used by RocksDB's existing
// Varint32/64 helpers. It exists for callers that naturally read one byte
// first, such as disk APIs that can issue a second read for the remaining
// bytes once the encoded length is known.
//
// Another motivation is CPU efficiency: for some in-memory workloads this
// format may decode more efficiently than continuation-bit varint because the
// length is known from the first byte and the payload bits become contiguous
// after a shift. That tradeoff still needs evaluation in RocksDB.
// TODO: Benchmark PrefixVarint against continuation-bit varint for both
// disk-read and in-memory callers before treating it as a CPU optimization.
//
// This format family is not new. LLVM/MLIR bytecode uses the same
// "PrefixVarInt" encoding, including the 9-byte `00000000 <fixed64>` form for
// full-width 64-bit values.
//
// The low-order zero-bit count in the first byte determines the total encoded
// length, so the first byte alone tells the caller how many additional bytes
// are needed before decoding.
//
// Prefix-byte layout:
//   xxxxxxx1   -> 1 byte  (7 payload bits)
//   xxxxxx10   -> 2 bytes (14 payload bits)
//   xxxxx100   -> 3 bytes (21 payload bits)
//   xxxx1000   -> 4 bytes (28 payload bits)
//   xxx10000   -> 5 bytes (35 payload bits)
//   xx100000   -> 6 bytes (42 payload bits)
//   x1000000   -> 7 bytes (49 payload bits)
//   10000000   -> 8 bytes (56 payload bits)
//   00000000   -> 9 bytes (64 payload bits via fixed64 payload bytes)
//
// PrefixVarint32 uses the first five rows of this table, and therefore uses
// exactly the same encoded lengths as LEB128 varint.
//
// PrefixVarint64 uses the full MLIR PrefixVarInt table above. Relative to
// LEB128, the encoded lengths are the same except in the extreme uint64_t case,
// where PrefixVarint64 uses 9 bytes and LEB128 would use 10.
constexpr uint32_t kMaxPrefixVarint32Length = 5;
constexpr uint32_t kInvalidPrefixVarint32AddlByteCount =
    kMaxPrefixVarint32Length;

// PrefixVarint64 uses the same low-bit prefix for 1-8 byte encodings.
// A zero first byte is reserved for the 9-byte form and is followed by a
// little-endian fixed64 payload.
constexpr uint32_t kMaxPrefixVarint64Length = 9;

namespace detail {

template <typename T>
inline uint32_t PrefixVarintLengthImpl(T value) {
  const uint32_t bits = static_cast<uint32_t>(FloorLog2(value | T{1}) + 1);
  return (bits + 6) / 7;
}

// Reassembles the already-encoded bytes into a little-endian word so callers
// can strip the prefix bits and recover the payload with shifts/masks.
inline uint64_t LoadPrefixVarintEncodedWord(char first_byte,
                                            const char* addl_bytes,
                                            uint32_t addl_byte_count) {
  uint64_t encoded_word = static_cast<unsigned char>(first_byte);
  // TODO: Benchmark an unaligned word load plus masking here. MLIR's
  // parseMultiByteVarInt uses a bulk little-endian load for this path; that
  // may beat the current byte-at-a-time assembly for hot in-memory callers.
  for (uint32_t i = 0; i < addl_byte_count; ++i) {
    encoded_word |=
        static_cast<uint64_t>(static_cast<unsigned char>(addl_bytes[i]))
        << ((i + 1) * 8);
  }
  return encoded_word;
}

// Stores the low `byte_count` bytes of an already-formed encoded word.
inline char* StorePrefixVarintEncodedWord(char* dst, uint64_t encoded_word,
                                          uint32_t byte_count) {
  unsigned char* ptr = lossless_cast<unsigned char*>(dst);
  // TODO: Benchmark an unaligned word store here. MLIR's emitMultiByteVarInt
  // writes from an already-formed little-endian word, which may beat the
  // current byte-at-a-time write for hot in-memory callers.
  for (uint32_t i = 0; i < byte_count; ++i) {
    ptr[i] = static_cast<unsigned char>(encoded_word >> (i * 8));
  }
  return lossless_cast<char*>(ptr + byte_count);
}

}  // namespace detail

inline uint32_t PrefixVarint32Length(uint32_t value) {
  return detail::PrefixVarintLengthImpl(value);
}

inline uint32_t PrefixVarint64Length(uint64_t value) {
  const uint32_t len = detail::PrefixVarintLengthImpl(value);
  return std::min(len, kMaxPrefixVarint64Length);
}

inline char* EncodePrefixVarint32(char* dst, uint32_t value) {
  const uint32_t num_bytes = PrefixVarint32Length(value);
  const uint64_t encoded_word = (static_cast<uint64_t>(value) << num_bytes) |
                                (uint64_t{1} << (num_bytes - 1));
  return detail::StorePrefixVarintEncodedWord(dst, encoded_word, num_bytes);
}

// Encode `value` as a PrefixVarint64 to the buffer at `dst`, which should in
// general be at least kMaxPrefixVarint64Length bytes. Returns a pointer to the
// byte after the last encoded byte. kMinimumBytes is rarely used, to support
// "improper" (non-minimal) encoding.
template <uint32_t kMinimumBytes = 0>
inline char* EncodePrefixVarint64(char* dst, uint64_t value) {
  uint32_t num_bytes = PrefixVarint64Length(value);
  if constexpr (kMinimumBytes > 1) {
    static_assert(kMinimumBytes < kMaxPrefixVarint64Length);
    num_bytes = std::max(num_bytes, kMinimumBytes);
  }
  if (UNLIKELY(num_bytes == kMaxPrefixVarint64Length)) {
    dst[0] = 0;
    EncodeFixed64(dst + 1, value);
    return dst + kMaxPrefixVarint64Length;
  }

  const uint64_t encoded_word =
      (value << num_bytes) | (uint64_t{1} << (num_bytes - 1));
  return detail::StorePrefixVarintEncodedWord(dst, encoded_word, num_bytes);
}

inline void PutPrefixVarint32(std::string* dst, uint32_t value) {
  char buf[kMaxPrefixVarint32Length];
  char* ptr = EncodePrefixVarint32(buf, value);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutPrefixVarint64(std::string* dst, uint64_t value) {
  char buf[kMaxPrefixVarint64Length];
  char* ptr = EncodePrefixVarint64(buf, value);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

// Split-decode helper for callers that already have the first byte and need to
// know how many additional bytes to fetch before calling
// DecodePrefixVarint32(). Returns kInvalidPrefixVarint32AddlByteCount if
// `first_byte` cannot start a valid PrefixVarint32.
inline uint32_t PrefixVarint32AddlByteCount(char first_byte) {
  const uint32_t bits = static_cast<unsigned char>(first_byte);
  if (UNLIKELY(bits == 0)) {
    return kInvalidPrefixVarint32AddlByteCount;
  }
  const uint32_t addl_byte_count =
      static_cast<uint32_t>(CountTrailingZeroBits(bits));
  return addl_byte_count < kMaxPrefixVarint32Length
             ? addl_byte_count
             : kInvalidPrefixVarint32AddlByteCount;
}

// Split-decode helper for callers that already have the first byte and need to
// know how many additional bytes to fetch before calling
// DecodePrefixVarint64(). For the extreme 9-byte form, this returns 8.
inline uint32_t PrefixVarint64AddlByteCount(char first_byte) {
  const uint32_t bits = static_cast<unsigned char>(first_byte);
  return bits == 0 ? kMaxPrefixVarint64Length - 1
                   : static_cast<uint32_t>(CountTrailingZeroBits(bits));
}

// Decodes a PrefixVarint32 when the caller already has the first byte and the
// additional bytes requested by PrefixVarint32AddlByteCount(first_byte), which
// is useful for disk-read APIs. Returns false on invalid first byte, too few
// additional bytes, or overflow beyond uint32_t.
inline bool DecodePrefixVarint32(char first_byte, const char* addl_bytes,
                                 size_t addl_byte_count, uint32_t* value) {
  const uint32_t first = static_cast<unsigned char>(first_byte);
  // Optimize the overwhelmingly common single-byte case.
  if (LIKELY((first & 0x01u) != 0)) {
    *value = first >> 1;
    return true;
  }

  const uint32_t required_addl_byte_count =
      PrefixVarint32AddlByteCount(first_byte);
  if (required_addl_byte_count == kInvalidPrefixVarint32AddlByteCount ||
      addl_byte_count < required_addl_byte_count) {
    return false;
  }

  const uint64_t decoded =
      detail::LoadPrefixVarintEncodedWord(first_byte, addl_bytes,
                                          required_addl_byte_count) >>
      (required_addl_byte_count + 1);
  if ((decoded >> 32) != 0) {
    return false;
  }
  *value = static_cast<uint32_t>(decoded);
  return true;
}

// Decodes a PrefixVarint64 when the caller already has the first byte and the
// additional bytes requested by PrefixVarint64AddlByteCount(first_byte).
// Returns false only when too few additional bytes are provided.
inline bool DecodePrefixVarint64(char first_byte, const char* addl_bytes,
                                 size_t addl_byte_count, uint64_t* value) {
  const uint32_t first = static_cast<unsigned char>(first_byte);
  // Optimize the overwhelmingly common single-byte case.
  if (LIKELY((first & 0x01u) != 0)) {
    *value = first >> 1;
    return true;
  }

  const uint32_t required_addl_byte_count =
      PrefixVarint64AddlByteCount(first_byte);
  if (addl_byte_count < required_addl_byte_count) {
    return false;
  }
  if (UNLIKELY(required_addl_byte_count == kMaxPrefixVarint64Length - 1)) {
    *value = DecodeFixed64(addl_bytes);
    return true;
  }

  *value = detail::LoadPrefixVarintEncodedWord(first_byte, addl_bytes,
                                               required_addl_byte_count) >>
           (required_addl_byte_count + 1);
  return true;
}

// Parses one PrefixVarint32 from [p, limit). On success stores the decoded
// value in *value and returns the first byte after the encoding. On failure
// returns nullptr. It never reads past `limit`.
inline const char* GetPrefixVarint32Ptr(const char* p, const char* limit,
                                        uint32_t* value) {
  if (p >= limit) {
    return nullptr;
  }

  const uint32_t first = static_cast<unsigned char>(*p);
  // Optimize the overwhelmingly common single-byte case.
  if (LIKELY((first & 0x01u) != 0)) {
    *value = first >> 1;
    return p + 1;
  }

  const uint32_t addl_byte_count = PrefixVarint32AddlByteCount(*p);
  if (addl_byte_count == kInvalidPrefixVarint32AddlByteCount ||
      static_cast<size_t>(limit - p) < addl_byte_count + 1) {
    return nullptr;
  }
  if (!DecodePrefixVarint32(*p, p + 1, addl_byte_count, value)) {
    return nullptr;
  }
  return p + addl_byte_count + 1;
}

// Parses one PrefixVarint64 from [p, limit). On success stores the decoded
// value in *value and returns the first byte after the encoding. On failure
// returns nullptr. It never reads past `limit`.
inline const char* GetPrefixVarint64Ptr(const char* p, const char* limit,
                                        uint64_t* value) {
  if (p >= limit) {
    return nullptr;
  }

  const uint32_t first = static_cast<unsigned char>(*p);
  // Optimize the overwhelmingly common single-byte case.
  if (LIKELY((first & 0x01u) != 0)) {
    *value = first >> 1;
    return p + 1;
  }

  const uint32_t addl_byte_count = PrefixVarint64AddlByteCount(*p);
  if (static_cast<size_t>(limit - p) < addl_byte_count + 1) {
    return nullptr;
  }
  if (!DecodePrefixVarint64(*p, p + 1, addl_byte_count, value)) {
    return nullptr;
  }
  return p + addl_byte_count + 1;
}

// Slice adapter around GetPrefixVarint32Ptr(). Advances `input` only on
// success.
inline bool GetPrefixVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetPrefixVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  }
  *input = Slice(q, static_cast<size_t>(limit - q));
  return true;
}

// Slice adapter around GetPrefixVarint64Ptr(). Advances `input` only on
// success.
inline bool GetPrefixVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetPrefixVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  }
  *input = Slice(q, static_cast<size_t>(limit - q));
  return true;
}

}  // namespace ROCKSDB_NAMESPACE
