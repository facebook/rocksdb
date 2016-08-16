//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "util/sharded_cache.h"

#include "port/port.h"
#include "util/autovector.h"

namespace rocksdb {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//  In that case the entry is *not* in the LRU. (refs > 1 && in_cache == true)
// 2. Not referenced externally and in hash table. In that case the entry is
// in the LRU and can be freed. (refs == 1 && in_cache == true)
// 3. Referenced externally and not in hash table. In that case the entry is
// in not on LRU and not in table. (refs >= 1 && in_cache == false)
//
// All newly created LRUHandles are in state 1. If you call
// LRUCacheShard::Release
// on entry in state 1, it will go into state 2. To move from state 1 to
// state 3, either call LRUCacheShard::Erase or LRUCacheShard::Insert with the
// same key.
// To move from state 2 to state 1, use LRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful LRUCacheShard::Lookup/LRUCacheShard::Insert have a
// matching
// RUCache::Release (to move into state 2) or LRUCacheShard::Erase (for state 3)

struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;     // a number of refs to this entry
                     // cache itself is counted as 1
  bool in_cache;     // true, if this entry is referenced by the hash table
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  char key_data[1];  // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }

  void Free() {
    assert((refs == 1 && in_cache) || (refs == 0 && !in_cache));
    (*deleter)(key(), value);
    delete[] reinterpret_cast<char*>(this);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class LRUHandleTable {
 public:
  LRUHandleTable();
  ~LRUHandleTable();

  LRUHandle* Lookup(const Slice& key, uint32_t hash);
  LRUHandle* Insert(LRUHandle* h);
  LRUHandle* Remove(const Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToAllCacheEntries(T func) {
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->in_cache);
        func(h);
        h = n;
      }
    }
  }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;
};

// A single shard of sharded cache.
class LRUCacheShard : public CacheShard {
 public:
  LRUCacheShard();
  virtual ~LRUCacheShard();

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  virtual void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Like Cache methods, but with an extra "hash" parameter.
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Cache::Handle** handle) override;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) override;
  virtual void Release(Cache::Handle* handle) override;
  virtual void Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  virtual void EraseUnRefEntries() override;

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(LRUHandle* e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, autovector<LRUHandle*>* deleted);

  // Initialized before use.
  size_t capacity_;

  // Memory size for entries residing in the cache
  size_t usage_;

  // Memory size for entries residing only in the LRU list
  size_t lru_usage_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable port::Mutex mutex_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  LRUHandle lru_;

  LRUHandleTable table_;
};

}  // namespace rocksdb
