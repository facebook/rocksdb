//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class Cache;

// A standard holder for fixed-size block cache keys (and for related caches).
// They are created through one of these, each using its own range of values:
// * CacheKey::CreateUniqueForCacheLifetime
// * CacheKey::CreateUniqueForProcessLifetime
// * Default ctor ("empty" cache key)
// * OffsetableCacheKey->WithOffset
//
// The first two use atomic counters to guarantee uniqueness over the given
// lifetime and the last uses a form of universally unique identifier for
// uniqueness with very high probabilty (and guaranteed for files generated
// during a single process lifetime).
//
// CacheKeys are currently used by calling AsSlice() to pass as a key to
// Cache. For performance, the keys are endianness-dependent (though otherwise
// portable). (Persistable cache entries are not intended to cross platforms.)
class CacheKey {
 public:
  // For convenience, constructs an "empty" cache key that is never returned
  // by other means.
  inline CacheKey() : session_id64_(), offset_etc64_() {}

  inline bool IsEmpty() const {
    return (session_id64_ == 0) & (offset_etc64_ == 0);
  }

  // Use this cache key as a Slice (byte order is endianness-dependent)
  inline Slice AsSlice() const {
    static_assert(sizeof(*this) == 16, "Standardized on 16-byte cache key");
    assert(!IsEmpty());
    return Slice(reinterpret_cast<const char *>(this), sizeof(*this));
  }

  // Create a CacheKey that is unique among others associated with this Cache
  // instance. Depends on Cache::NewId. This is useful for block cache
  // "reservations".
  static CacheKey CreateUniqueForCacheLifetime(Cache *cache);

  // Create a CacheKey that is unique among others for the lifetime of this
  // process. This is useful for saving in a static data member so that
  // different DB instances can agree on a cache key for shared entities,
  // such as for CacheEntryStatsCollector.
  static CacheKey CreateUniqueForProcessLifetime();

 protected:
  friend class OffsetableCacheKey;
  CacheKey(uint64_t session_id64, uint64_t offset_etc64)
      : session_id64_(session_id64), offset_etc64_(offset_etc64) {}
  uint64_t session_id64_;
  uint64_t offset_etc64_;
};

// TODO: doc
class OffsetableCacheKey : private CacheKey {
 public:
  inline OffsetableCacheKey() : CacheKey() {}
  OffsetableCacheKey(const std::string &db_id, const std::string &db_session_id,
                     uint64_t file_number, uint64_t max_offset);

  inline bool IsEmpty() const {
    bool result = session_id64_ == 0;
    assert(!(offset_etc64_ > 0 && result));
    return result;
  }

  inline CacheKey WithOffset(uint64_t offset) const {
    assert(!IsEmpty());
    assert(offset <= max_offset_);
    return CacheKey(session_id64_, offset_etc64_ ^ offset);
  }

  static constexpr size_t kCommonPrefixSize = sizeof(session_id64_);

  inline Slice CommonPrefixSlice() const {
    assert(!IsEmpty());
    assert(&this->session_id64_ == static_cast<const void *>(this));
    return Slice(reinterpret_cast<const char *>(this), kCommonPrefixSize);
  }

  static constexpr uint64_t kMaxOffsetStandardEncoding = 0xffffffffffU;

 private:
#ifndef NDEBUG
  uint64_t max_offset_ = 0;
#endif
};

}  // namespace ROCKSDB_NAMESPACE
