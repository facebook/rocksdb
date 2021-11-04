//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

constexpr uint32_t kCacheKeySize = 16;

class Cache;

class CacheKey {
 public:
  inline CacheKey() : hi64_(), lo64_() {}

  inline bool IsEmpty() const { return (hi64_ == 0) & (lo64_ == 0); }

  inline Slice AsSlice() const {
    static_assert(sizeof(CacheKey) == kCacheKeySize,
                  "Standardized on 16-byte cache key");
    assert(!IsEmpty());
    return Slice(reinterpret_cast<const char *>(this), kCacheKeySize);
  }

  static CacheKey CreateUniqueForCacheLifetime(Cache *cache);

  static CacheKey CreateUniqueForProcessLifetime();

 protected:
  friend class OffsetableCacheKey;
  CacheKey(uint64_t _hi64, uint64_t _lo64) : hi64_(_hi64), lo64_(_lo64) {}
  uint64_t hi64_;
  uint64_t lo64_;
};

class OffsetableCacheKey : private CacheKey {
 public:
  inline OffsetableCacheKey() : CacheKey() {}
  OffsetableCacheKey(const std::string &db_id, const std::string &db_session_id,
                     uint64_t file_number, uint64_t max_offset);

  inline bool IsEmpty() const {
    bool result = hi64_ == 0;
    assert(!(lo64_ > 0 && result));
    return result;
  }

  inline CacheKey WithOffset(uint64_t offset) const {
    assert(!IsEmpty());
    assert(offset <= max_offset_);
    return CacheKey(hi64_, lo64_ ^ offset);
  }

  static constexpr size_t kFilePrefixSize = sizeof(hi64_);

  inline Slice FilePrefixSlice() const {
    static_assert(sizeof(CacheKey) == kCacheKeySize,
                  "Standardized on 16-byte cache key");
    assert(!IsEmpty());
    assert(&this->hi64_ == static_cast<const void *>(this));
    return Slice(reinterpret_cast<const char *>(this), kFilePrefixSize);
  }

  static constexpr uint64_t kMaxOffsetStandardEncoding = 0xffffffffffU;

 private:
#ifndef NDEBUG
  uint64_t max_offset_ = 0;
#endif
};

}  // namespace ROCKSDB_NAMESPACE
