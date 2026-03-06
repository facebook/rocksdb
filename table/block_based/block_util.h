//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>

#include "db/dbformat.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

// Decode the next block entry starting at "p", storing the number of shared key
// bytes, non_shared key bytes, and the length of the value in "*shared",
// "*non_shared", and "*value_length", respectively.  Will not dereference past
// "limit".
//
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
struct DecodeEntry {
  inline const char* operator()(const char* p, const char* limit,
                                uint32_t* shared, uint32_t* non_shared,
                                uint32_t* value_length,
                                uint32_t* value_offset) {
    // We need 2 bytes for shared and non_shared size. We also need one more
    // byte either for value size or the actual value in case of value delta
    // encoding.
    assert(limit - p >= 3);
    *shared = reinterpret_cast<const unsigned char*>(p)[0];
    *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
    *value_length = reinterpret_cast<const unsigned char*>(p)[2];
    if ((*shared | *non_shared | *value_length) < 128) {
      // Fast path: all three values are encoded in one byte each
      p += 3;
    } else {
      if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) {
        return nullptr;
      }
      if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) {
        return nullptr;
      }
      if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) {
        return nullptr;
      }
    }

    if (value_offset) {
      if ((p = GetVarint32Ptr(p, limit, value_offset)) == nullptr) {
        return nullptr;
      }
    }

    return p;
  }
};

struct DecodeKey {
  inline const char* operator()(const char* p, const char* limit,
                                uint32_t* shared, uint32_t* non_shared,
                                uint32_t* value_offset) {
    uint32_t value_length;
    return DecodeEntry()(p, limit, shared, non_shared, &value_length,
                         value_offset);
  }
};

// In format_version 4, which is used by index blocks, the value size is not
// encoded before the entry, as the value is known to be the handle with the
// known size.
struct DecodeKeyV4 {
  inline const char* operator()(const char* p, const char* limit,
                                uint32_t* shared, uint32_t* non_shared,
                                uint32_t* value_offset) {
    // We need 2 bytes for shared and non_shared size. We also need one more
    // byte either for value size or the actual value in case of value delta
    // encoding.
    if (limit - p < 3) {
      return nullptr;
    }
    *shared = reinterpret_cast<const unsigned char*>(p)[0];
    *non_shared = reinterpret_cast<const unsigned char*>(p)[1];
    if ((*shared | *non_shared) < 128) {
      // Fast path: all three values are encoded in one byte each
      p += 2;
    } else {
      if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) {
        return nullptr;
      }
      if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) {
        return nullptr;
      }
    }

    if (value_offset) {
      if ((p = GetVarint32Ptr(p, limit, value_offset)) == nullptr) {
        return nullptr;
      }
    }
    return p;
  }
};

struct DecodeEntryV4 {
  inline const char* operator()(const char* p, const char* limit,
                                uint32_t* shared, uint32_t* non_shared,
                                uint32_t* value_length,
                                uint32_t* value_offset) {
    assert(value_length);

    *value_length = 0;
    return DecodeKeyV4()(p, limit, shared, non_shared, value_offset);
  }
};

// Read first 8 bytes (starting at offset) as big-endian uint64_t, padding
// with zeros on the right if the key is shorter. This preserves
// lexicographic ordering.
//
// If s.size() <= offset, then returns 0.
inline uint64_t ReadBe64FromKey(Slice s, bool is_user_key, size_t offset) {
  if (!is_user_key) {
    assert(s.size() >= kNumInternalBytes);
    s = Slice(s.data(), s.size() - kNumInternalBytes);
  }
  offset = std::min(offset, s.size());
  size_t remaining = s.size() - offset;

  // fast path
  if (remaining >= 8) {
    uint64_t val;
    memcpy(&val, s.data() + offset, sizeof(val));
    if (port::kLittleEndian) {
      return EndianSwapValue(val);
    }
    return val;
  }

  uint64_t val = 0;
  for (size_t i = 0; i < remaining; i++) {
    val = (val << 8) | static_cast<uint8_t>(s.data()[offset + i]);
  }
  if (remaining > 0) {
    val <<= (8 - remaining) * 8;  // Pad zeros on the right
  }
  return val;
}

}  // namespace ROCKSDB_NAMESPACE
