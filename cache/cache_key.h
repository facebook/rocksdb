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
  inline CacheKey() : session_etc64_(), offset_etc64_() {}

  inline bool IsEmpty() const {
    return (session_etc64_ == 0) & (offset_etc64_ == 0);
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
  CacheKey(uint64_t session_etc64, uint64_t offset_etc64)
      : session_etc64_(session_etc64), offset_etc64_(offset_etc64) {}
  uint64_t session_etc64_;
  uint64_t offset_etc64_;
};

// A file-specific generator of cache keys, sometimes referred to as the
// "base" cache key for a file because all the cache keys for various offsets
// within the file are computed using simple arithmetic. The basis for the
// general approach is dicussed here: https://github.com/pdillinger/unique_id
// Heavily related to GetUniqueIdFromTableProperties.
//
// If the db_id, db_session_id, and file_number come from the file's table
// properties, then the keys will be stable across DB::Open/Close, backup/
// restore, import/export, etc.
//
// This class "is a" CacheKey only privately so that it is not misused as
// a ready-to-use CacheKey.
class OffsetableCacheKey : private CacheKey {
 public:
  // For convenience, constructs an "empty" cache key that should not be used.
  inline OffsetableCacheKey() : CacheKey() {}

  // Constructs an OffsetableCacheKey with the given information about a file.
  // max_offset is based on file size (see WithOffset) and is required here to
  // choose an appropriate (sub-)encoding. This constructor never generates an
  // "empty" base key.
  OffsetableCacheKey(const std::string &db_id, const std::string &db_session_id,
                     uint64_t file_number, uint64_t max_offset);

  inline bool IsEmpty() const {
    bool result = session_etc64_ == 0;
    assert(!(offset_etc64_ > 0 && result));
    return result;
  }

  // Construct a CacheKey for an offset within a file, which must be
  // <= max_offset provided in constructor. An offset is not necessarily a
  // byte offset if a smaller unique identifier of keyable offsets is used.
  //
  // This class was designed to make this hot code extremely fast.
  inline CacheKey WithOffset(uint64_t offset) const {
    assert(!IsEmpty());
    assert(offset <= max_offset_);
    return CacheKey(session_etc64_, offset_etc64_ ^ offset);
  }

  // The "common prefix" is a shared prefix for all the returned CacheKeys,
  // that also happens to usually be the same among many files in the same DB,
  // so is efficient and highly accurate (not perfectly) for DB-specific cache
  // dump selection (but not file-specific).
  static constexpr size_t kCommonPrefixSize = 8;
  inline Slice CommonPrefixSlice() const {
    static_assert(sizeof(session_etc64_) == kCommonPrefixSize,
                  "8 byte common prefix expected");
    assert(!IsEmpty());
    assert(&this->session_etc64_ == static_cast<const void *>(this));

    return Slice(reinterpret_cast<const char *>(this), kCommonPrefixSize);
  }

  // For any max_offset <= this value, the same encoding scheme is guaranteed.
  static constexpr uint64_t kMaxOffsetStandardEncoding = 0xffffffffffU;

 private:
#ifndef NDEBUG
  uint64_t max_offset_ = 0;
#endif
};

}  // namespace ROCKSDB_NAMESPACE
