//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Encoding independent of machine byte order:
// * Fixed-length numbers are encoded with least-significant byte first
//   (little endian, native order on Intel and others)
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format
//
// Some related functions are provided in coding_lean.h

#pragma once
#include <algorithm>
#include <string>

#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/coding_lean.h"

// Some processors does not allow unaligned access to memory
#if defined(__sparc)
#define PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED
#endif

namespace ROCKSDB_NAMESPACE {

// The maximum length of a varint in bytes for 64-bit.
const uint32_t kMaxVarint64Length = 10;

// Standard Put... routines append to a string
void PutFixed16(std::string* dst, uint16_t value);
void PutFixed32(std::string* dst, uint32_t value);
void PutFixed64(std::string* dst, uint64_t value);
void PutVarint32(std::string* dst, uint32_t value);
void PutVarint32Varint32(std::string* dst, uint32_t value1, uint32_t value2);
void PutVarint32Varint32Varint32(std::string* dst, uint32_t value1,
                                 uint32_t value2, uint32_t value3);
void PutVarint64(std::string* dst, uint64_t value);
void PutVarint64Varint64(std::string* dst, uint64_t value1, uint64_t value2);
void PutVarint32Varint64(std::string* dst, uint32_t value1, uint64_t value2);
void PutVarint32Varint32Varint64(std::string* dst, uint32_t value1,
                                 uint32_t value2, uint64_t value3);
void PutLengthPrefixedSlice(std::string* dst, const Slice& value);
void PutLengthPrefixedSliceParts(std::string* dst,
                                 const SliceParts& slice_parts);
void PutLengthPrefixedSlicePartsWithPadding(std::string* dst,
                                            const SliceParts& slice_parts,
                                            size_t pad_sz);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
bool GetFixed64(Slice* input, uint64_t* value);
bool GetFixed32(Slice* input, uint32_t* value);
bool GetFixed16(Slice* input, uint16_t* value);
bool GetVarint32(Slice* input, uint32_t* value);
bool GetVarint64(Slice* input, uint64_t* value);
bool GetVarsignedint64(Slice* input, int64_t* value);
bool GetLengthPrefixedSlice(Slice* input, Slice* result);
// This function assumes data is well-formed.
Slice GetLengthPrefixedSlice(const char* data);

Slice GetSliceUntil(Slice* slice, char delimiter);

// Borrowed from
// https://github.com/facebook/fbthrift/blob/449a5f77f9f9bae72c9eb5e78093247eef185c04/thrift/lib/cpp/util/VarintUtils-inl.h#L202-L208
constexpr inline uint64_t i64ToZigzag(const int64_t l) {
  return (static_cast<uint64_t>(l) << 1) ^ static_cast<uint64_t>(l >> 63);
}
inline int64_t zigzagToI64(uint64_t n) {
  return (n >> 1) ^ -static_cast<int64_t>(n & 1);
}

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* v);
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* v);
inline const char* GetVarsignedint64Ptr(const char* p, const char* limit,
                                        int64_t* value) {
  uint64_t u = 0;
  const char* ret = GetVarint64Ptr(p, limit, &u);
  *value = zigzagToI64(u);
  return ret;
}

// Returns the length of the varint32 or varint64 encoding of "v"
int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
char* EncodeVarint32(char* dst, uint32_t value);
char* EncodeVarint64(char* dst, uint64_t value);

// Internal routine for use by fallback path of GetVarint32Ptr
const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value);
inline const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

// Pull the last 8 bits and cast it to a character
inline void PutFixed16(std::string* dst, uint16_t value) {
  if (port::kLittleEndian) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  } else {
    char buf[sizeof(value)];
    EncodeFixed16(buf, value);
    dst->append(buf, sizeof(buf));
  }
}

inline void PutFixed32(std::string* dst, uint32_t value) {
  if (port::kLittleEndian) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  } else {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
  }
}

inline void PutFixed64(std::string* dst, uint64_t value) {
  if (port::kLittleEndian) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  } else {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
  }
}

inline void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2) {
  char buf[10];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32Varint32(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint32_t v3) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline char* EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  char buf[kMaxVarint64Length];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarsignedint64(std::string* dst, int64_t v) {
  char buf[kMaxVarint64Length];
  // Using Zigzag format to convert signed to unsigned
  char* ptr = EncodeVarint64(buf, i64ToZigzag(v));
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

/**
 * @brief encode a signed int using 8 bits of every byte
 *
 * Efficient use of all 8 bits of as many bytes as necessary
 * because the length of the encoding is known independently,
 * e.g. as the length of the "value" in a (key,value)-pair
 *
 * -128..127 are a single byte sign extended value
 * -32768..32767 are a 2-byte sign extended value
 *
 * @param dst buffer to encode into
 * @param v value to encode
 */
inline void Put8BitVarsignedint64(std::string* dst, int64_t v) {
  if (v >= 0) {
    while (v > 0x7f) {
      unsigned char byte = static_cast<uint64_t>(v) & 0xff;
      dst->push_back(byte);
      v >>= 8;
    }
  } else {
    while (v < -0x80) {
      unsigned char byte = static_cast<uint64_t>(v) & 0xff;
      dst->push_back(byte);
      v >>= 8;
    }
  }
  dst->push_back(static_cast<unsigned char>(v));
}

inline void PutVarint64Varint64(std::string* dst, uint64_t v1, uint64_t v2) {
  char buf[20];
  char* ptr = EncodeVarint64(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint64(std::string* dst, uint32_t v1, uint64_t v2) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32Varint64(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint64_t v3) {
  char buf[20];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint64(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

inline void PutLengthPrefixedSliceParts(std::string* dst, size_t total_bytes,
                                        const SliceParts& slice_parts) {
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    total_bytes += slice_parts.parts[i].size();
  }
  PutVarint32(dst, static_cast<uint32_t>(total_bytes));
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    dst->append(slice_parts.parts[i].data(), slice_parts.parts[i].size());
  }
}

inline void PutLengthPrefixedSliceParts(std::string* dst,
                                        const SliceParts& slice_parts) {
  PutLengthPrefixedSliceParts(dst, /*total_bytes=*/0, slice_parts);
}

inline void PutLengthPrefixedSlicePartsWithPadding(
    std::string* dst, const SliceParts& slice_parts, size_t pad_sz) {
  PutLengthPrefixedSliceParts(dst, /*total_bytes=*/pad_sz, slice_parts);
  dst->append(pad_sz, '\0');
}

inline int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

inline bool GetFixed64(Slice* input, uint64_t* value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input->data());
  input->remove_prefix(sizeof(uint64_t));
  return true;
}

inline bool GetFixed32(Slice* input, uint32_t* value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input->data());
  input->remove_prefix(sizeof(uint32_t));
  return true;
}

inline bool GetFixed16(Slice* input, uint16_t* value) {
  if (input->size() < sizeof(uint16_t)) {
    return false;
  }
  *value = DecodeFixed16(input->data());
  input->remove_prefix(sizeof(uint16_t));
  return true;
}

inline bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline bool GetVarsignedint64(Slice* input, int64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarsignedint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

/**
 * @brief decode a signed int encoded using 8 bits of every byte, inverse of
 * `Put8BitVarsignedint64`
 *
 * Efficient use of all 8 bits of as many bytes as necessary
 * because the length of the encoding is known independently,
 * e.g. as the length of the "value" in a (key,value)-pair
 *
 * -128..127 are a single byte sign extended value
 * -32768..32767 are a 2-byte sign extended value
 *
 * @param input buffer to decode out of
 * @return an int64_t which is the decoded value of the buffer
 */
inline int64_t Get8BitVarsignedint64(Slice* input) {
  const char* start = input->data();
  const char* p = start + input->size();
  int64_t s = 0;
  if (start < p) {
    s = *(reinterpret_cast<const char*>(--p));
    while (start < p) {
      int64_t byte =
          static_cast<int64_t>(*(reinterpret_cast<const unsigned char*>(--p)));
      s = (s * 0x100) | byte;  // s = (s << 8) | byte; fails ubsan check,
                               // -ve << is undefined in C/C++
    }
  }
  return s;
}

inline bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len = 0;
  if (GetVarint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

inline Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len = 0;
  // +5: we assume "data" is not corrupted
  // unsigned char is 7 bits, uint32_t is 32 bits, need 5 unsigned char
  auto p = GetVarint32Ptr(data, data + 5 /* limit */, &len);
  return Slice(p, len);
}

inline Slice GetSliceUntil(Slice* slice, char delimiter) {
  uint32_t len = 0;
  for (len = 0; len < slice->size() && slice->data()[len] != delimiter; ++len) {
    // nothing
  }

  Slice ret(slice->data(), len);
  slice->remove_prefix(len + ((len < slice->size()) ? 1 : 0));
  return ret;
}

template <class T>
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
inline void
PutUnaligned(T* memory, const T& value) {
#if defined(PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED)
  char* nonAlignedMemory = reinterpret_cast<char*>(memory);
  memcpy(nonAlignedMemory, reinterpret_cast<const char*>(&value), sizeof(T));
#else
  *memory = value;
#endif
}

template <class T>
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
inline void
GetUnaligned(const T* memory, T* value) {
#if defined(PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED)
  char* nonAlignedMemory = reinterpret_cast<char*>(value);
  memcpy(nonAlignedMemory, reinterpret_cast<const char*>(memory), sizeof(T));
#else
  *value = *memory;
#endif
}

}  // namespace ROCKSDB_NAMESPACE
