//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/reader_common.h"

#include "util/crc32c.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {
void ForceReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle, true /* force_erase */);
}

Status VerifyChecksum(const ChecksumType type, const char* buf, size_t len,
                      uint32_t expected) {
  Status s;
  uint32_t actual = 0;
  switch (type) {
    case kNoChecksum:
      break;
    case kCRC32c:
      expected = crc32c::Unmask(expected);
      actual = crc32c::Value(buf, len);
      break;
    case kxxHash:
      actual = XXH32(buf, static_cast<int>(len), 0);
      break;
    case kxxHash64:
      actual = static_cast<uint32_t>(XXH64(buf, static_cast<int>(len), 0) &
                                     uint64_t{0xffffffff});
      break;
    default:
      s = Status::Corruption("unknown checksum type");
  }
  if (s.ok() && actual != expected) {
    s = Status::Corruption("properties block checksum mismatched");
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
