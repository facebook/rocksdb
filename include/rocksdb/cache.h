// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_ROCKSDB_INCLUDE_CACHE_H_
#define STORAGE_ROCKSDB_INCLUDE_CACHE_H_

#include <memory>
#include <stdint.h>
#include "rocksdb/slice.h"

namespace rocksdb {

using std::shared_ptr;

class Cache;

// Create a new cache with a fixed size capacity. The cache is sharded
// to 2^numShardBits shards, by hash of the key. The total capacity
// is divided and evenly assigned to each shard. Inside each shard,
// the eviction is done in two passes: first try to free spaces by
// evicting entries that are among the most least used removeScanCountLimit
// entries and do not have reference other than by the cache itself, in
// the least-used order. If not enough space is freed, further free the
// entries in least used order.
//
// The functions without parameter numShardBits and/or removeScanCountLimit
// use default values. removeScanCountLimit's default value is 0, which
// means a strict LRU order inside each shard.
extern shared_ptr<Cache> NewLRUCache(size_t capacity);
extern shared_ptr<Cache> NewLRUCache(size_t capacity, int numShardBits);
extern shared_ptr<Cache> NewLRUCache(size_t capacity, int numShardBits,
                                     int removeScanCountLimit);

class Cache {
 public:
  Cache() { }

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle { };

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // returns the maximum configured capacity of the cache
  virtual size_t GetCapacity() = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;

  // No copying allowed
  Cache(const Cache&);
  void operator=(const Cache&);
};

}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_UTIL_CACHE_H_
