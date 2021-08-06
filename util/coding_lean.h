//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Encoding independent of machine byte order:
// * Fixed-length numbers are encoded with least-significant byte first
//   (little endian, native order on Intel and others)
//
// More functions in coding.h

#pragma once

#include <cstdint>
#include <cstring>

#include "port/port.h"  // for port::kLittleEndian

namespace ROCKSDB_NAMESPACE {

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
// -- Implementation of the functions declared above
inline void EncodeFixed16(char* buf, uint16_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
  }
}

inline void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

inline void EncodeFixed64(char* buf, uint64_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint16_t DecodeFixed16(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint16_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint16_t>(static_cast<unsigned char>(ptr[0]))) |
            (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8));
  }
}

inline uint32_t DecodeFixed32(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

// Swaps between big and little endian. Can be used to in combination
// with the little-endian encoding/decoding functions to encode/decode
// big endian.
template <typename T>
inline T EndianSwapValue(T v) {
  static_assert(std::is_integral<T>::value, "non-integral type");

#ifdef _MSC_VER
  if (sizeof(T) == 2) {
    return static_cast<T>(_byteswap_ushort(static_cast<uint16_t>(v)));
  } else if (sizeof(T) == 4) {
    return static_cast<T>(_byteswap_ulong(static_cast<uint32_t>(v)));
  } else if (sizeof(T) == 8) {
    return static_cast<T>(_byteswap_uint64(static_cast<uint64_t>(v)));
  }
#else
  if (sizeof(T) == 2) {
    return static_cast<T>(__builtin_bswap16(static_cast<uint16_t>(v)));
  } else if (sizeof(T) == 4) {
    return static_cast<T>(__builtin_bswap32(static_cast<uint32_t>(v)));
  } else if (sizeof(T) == 8) {
    return static_cast<T>(__builtin_bswap64(static_cast<uint64_t>(v)));
  }
#endif
  // Recognized by clang as bswap, but not by gcc :(
  T ret_val = 0;
  for (size_t i = 0; i < sizeof(T); ++i) {
    ret_val |= ((v >> (8 * i)) & 0xff) << (8 * (sizeof(T) - 1 - i));
  }
  return ret_val;
}

}  // namespace ROCKSDB_NAMESPACE
