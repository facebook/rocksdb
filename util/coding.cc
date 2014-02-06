//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

#include <algorithm>
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb {

char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}

const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

void BitStreamPutInt(char* dst, size_t dstlen, size_t offset,
                     uint32_t bits, uint64_t value) {
  assert((offset + bits + 7)/8 <= dstlen);
  assert(bits <= 64);

  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);

  size_t byteOffset = offset / 8;
  size_t bitOffset = offset % 8;

  // This prevents unused variable warnings when compiling.
#ifndef NDEBUG
  // Store truncated value.
  uint64_t origValue = (bits < 64)?(value & (((uint64_t)1 << bits) - 1)):value;
  uint32_t origBits = bits;
#endif

  while (bits > 0) {
    size_t bitsToGet = std::min<size_t>(bits, 8 - bitOffset);
    unsigned char mask = ((1 << bitsToGet) - 1);

    ptr[byteOffset] = (ptr[byteOffset] & ~(mask << bitOffset)) +
                      ((value & mask) << bitOffset);

    value >>= bitsToGet;
    byteOffset += 1;
    bitOffset = 0;
    bits -= bitsToGet;
  }

  assert(origValue == BitStreamGetInt(dst, dstlen, offset, origBits));
}

uint64_t BitStreamGetInt(const char* src, size_t srclen, size_t offset,
                         uint32_t bits) {
  assert((offset + bits + 7)/8 <= srclen);
  assert(bits <= 64);

  const unsigned char* ptr = reinterpret_cast<const unsigned char*>(src);

  uint64_t result = 0;

  size_t byteOffset = offset / 8;
  size_t bitOffset = offset % 8;
  size_t shift = 0;

  while (bits > 0) {
    size_t bitsToGet = std::min<size_t>(bits, 8 - bitOffset);
    unsigned char mask = ((1 << bitsToGet) - 1);

    result += (uint64_t)((ptr[byteOffset] >> bitOffset) & mask) << shift;

    shift += bitsToGet;
    byteOffset += 1;
    bitOffset = 0;
    bits -= bitsToGet;
  }

  return result;
}

void BitStreamPutInt(std::string* dst, size_t offset, uint32_t bits,
                     uint64_t value) {
  assert((offset + bits + 7)/8 <= dst->size());

  const size_t kTmpBufLen = sizeof(value) + 1;
  char tmpBuf[kTmpBufLen];

  // Number of bytes of tmpBuf being used
  const size_t kUsedBytes = (offset%8 + bits)/8;

  // Copy relevant parts of dst to tmpBuf
  for (size_t idx = 0; idx <= kUsedBytes; ++idx) {
    tmpBuf[idx] = (*dst)[offset/8 + idx];
  }

  BitStreamPutInt(tmpBuf, kTmpBufLen, offset%8, bits, value);

  // Copy tmpBuf back to dst
  for (size_t idx = 0; idx <= kUsedBytes; ++idx) {
    (*dst)[offset/8 + idx] = tmpBuf[idx];
  }

  // Do the check here too as we are working with a buffer.
  assert(((bits < 64)?(value & (((uint64_t)1 << bits) - 1)):value) ==
         BitStreamGetInt(dst, offset, bits));
}

}  // namespace rocksdb
