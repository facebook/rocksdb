//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "table/unique_id_impl.h"

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
  inline CacheKey() : file_num_etc64_(), offset_etc64_() {}

  inline bool IsEmpty() const {
    return (file_num_etc64_ == 0) & (offset_etc64_ == 0);
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
  CacheKey(uint64_t file_num_etc64, uint64_t offset_etc64)
      : file_num_etc64_(file_num_etc64), offset_etc64_(offset_etc64) {}
  uint64_t file_num_etc64_;
  uint64_t offset_etc64_;
};

constexpr uint8_t kCacheKeySize = static_cast<uint8_t>(sizeof(CacheKey));

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
  // This constructor never generates an "empty" base key.
  OffsetableCacheKey(const std::string &db_id, const std::string &db_session_id,
                     uint64_t file_number);

  // Creates an OffsetableCacheKey from an SST unique ID, so that cache keys
  // can be derived from DB manifest data before reading the file from
  // storage--so that every part of the file can potentially go in a persistent
  // cache.
  //
  // Calling GetSstInternalUniqueId() on a db_id, db_session_id, and
  // file_number and passing the result to this function produces the same
  // base cache key as feeding those inputs directly to the constructor.
  //
  // This is a bijective transformation assuming either id is empty or
  // lower 64 bits is non-zero:
  // * Empty (all zeros) input -> empty (all zeros) output
  // * Lower 64 input is non-zero -> lower 64 output (file_num_etc64_) is
  //   non-zero
  static OffsetableCacheKey FromInternalUniqueId(UniqueIdPtr id);

  // This is the inverse transformation to the above, assuming either empty
  // or lower 64 bits (file_num_etc64_) is non-zero. Perhaps only useful for
  // testing.
  UniqueId64x2 ToInternalUniqueId();

  inline bool IsEmpty() const {
    bool result = file_num_etc64_ == 0;
    assert(!(offset_etc64_ > 0 && result));
    return result;
  }

  // Construct a CacheKey for an offset within a file. An offset is not
  // necessarily a byte offset if a smaller unique identifier of keyable
  // offsets is used.
  //
  // This class was designed to make this hot code extremely fast.
  inline CacheKey WithOffset(uint64_t offset) const {
    assert(!IsEmpty());
    return CacheKey(file_num_etc64_, offset_etc64_ ^ offset);
  }

  // The "common prefix" is a shared prefix for all the returned CacheKeys.
  // It is specific to the file but the same for all offsets within the file.
  static constexpr size_t kCommonPrefixSize = 8;
  inline Slice CommonPrefixSlice() const {
    static_assert(sizeof(file_num_etc64_) == kCommonPrefixSize,
                  "8 byte common prefix expected");
    assert(!IsEmpty());
    assert(&this->file_num_etc64_ == static_cast<const void *>(this));

    return Slice(reinterpret_cast<const char *>(this), kCommonPrefixSize);
  }
};

}  // namespace ROCKSDB_NAMESPACE
