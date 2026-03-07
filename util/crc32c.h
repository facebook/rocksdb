//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>

#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
namespace crc32c {

std::string IsFastCrc32Supported();

// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
// crc32c of some string A.  Extend() is often used to maintain the
// crc32c of a stream of data.
uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

// Takes two unmasked crc32c values, and the length of the string from
// which `crc2` was computed, and computes a crc32c value for the
// concatenation of the original two input strings. Running time is
// ~ log(crc2len).
uint32_t Crc32cCombine(uint32_t crc1, uint32_t crc2, size_t crc2len);

// Return the crc32c of data[0,n-1]
inline uint32_t Value(const char* data, size_t n) { return Extend(0, data, n); }

static const uint32_t kMaskDelta = UINT32_C(0xa282ead8);

// Portable 32-bit rotate helpers.  The idiom ((x >> n) | (x << (32-n))) is
// recognized by GCC/Clang as a rotation, but MSVC historically did not
// recognize it and the shift-by-32 is undefined behaviour when n==0.
// Use compiler builtins / intrinsics where available.
#if defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 4)
#  define ROCKSDB_ROTR32(x, r) __builtin_rotateright32((x), (r))
#  define ROCKSDB_ROTL32(x, r) __builtin_rotateleft32((x), (r))
#elif defined(_MSC_VER)
#  include <intrin.h>
#  define ROCKSDB_ROTR32(x, r) _rotr((x), (r))
#  define ROCKSDB_ROTL32(x, r) _rotl((x), (r))
#else
#  define ROCKSDB_ROTR32(x, r) \
     (((uint32_t)(x) >> (r)) | ((uint32_t)(x) << (32 - (r))))
#  define ROCKSDB_ROTL32(x, r) \
     (((uint32_t)(x) << (r)) | ((uint32_t)(x) >> (32 - (r))))
#endif

// Return a masked representation of crc.
//
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs.  Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
inline uint32_t Mask(uint32_t crc) {
  // Rotate right by 15 bits and add a constant.
  return ROCKSDB_ROTR32(crc, 15) + kMaskDelta;
}

// Return the crc whose masked representation is masked_crc.
inline uint32_t Unmask(uint32_t masked_crc) {
  uint32_t rot = masked_crc - kMaskDelta;
  return ROCKSDB_ROTL32(rot, 15);
}

}  // namespace crc32c
}  // namespace ROCKSDB_NAMESPACE
