//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <memory>
#include <string>

#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/secondary_cache.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"

namespace ROCKSDB_NAMESPACE {
namespace fast_lru_cache {

// An experimental (under development!) alternative to LRUCache

struct LRUHandle {
  void* value;
  Cache::DeleterFn deleter;
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;

  enum Flags : uint8_t {
    // Whether this entry is referenced by the hash table.
    IN_CACHE = (1 << 0),
  };
  uint8_t flags;

  // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }

  // Increase the reference count by 1.
  void Ref() { refs++; }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    assert(refs > 0);
    refs--;
    return refs == 0;
  }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const { return refs > 0; }

  bool InCache() const { return flags & IN_CACHE; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      flags |= IN_CACHE;
    } else {
      flags &= ~IN_CACHE;
    }
  }

  void Free() {
    assert(refs == 0);
    if (deleter) {
      (*deleter)(key(), value);
    }
    delete[] reinterpret_cast<char*>(this);
  }

  // Calculate the memory usage by metadata.
  inline size_t CalcMetaCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    if (metadata_charge_policy != kFullChargeCacheMetadata) {
      return 0;
    } else {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      return malloc_usable_size(
          const_cast<void*>(static_cast<const void*>(this)));
#else
      // This is the size that is used when a new handle is created.
      return sizeof(LRUHandle) - 1 + key_length;
#endif
    }
  }

  inline void CalcTotalCharge(
      size_t charge, CacheMetadataChargePolicy metadata_charge_policy) {
    total_charge = charge + CalcMetaCharge(metadata_charge_policy);
  }

  inline size_t GetCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    size_t meta_charge = CalcMetaCharge(metadata_charge_policy);
    assert(total_charge >= meta_charge);
    return total_charge - meta_charge;
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class LRUHandleTable {
 public:
  explicit LRUHandleTable(int hash_bits);
  ~LRUHandleTable();

  LRUHandle* Lookup(const Slice& key, uint32_t hash);
  LRUHandle* Insert(LRUHandle* h);
  LRUHandle* Remove(const Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToEntriesRange(T func, uint32_t index_begin, uint32_t index_end) {
    for (uint32_t i = index_begin; i < index_end; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

  int GetLengthBits() const { return length_bits_; }

  // Return the address of the head of the chain in the bucket given
  // by the hash.
  inline LRUHandle** Head(uint32_t hash);

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash);

  // Number of hash bits (upper because lower bits used for sharding)
  // used for table index. Length == 1 << length_bits_
  int length_bits_;

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  std::unique_ptr<LRUHandle*[]> list_;
};

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) LRUCacheShard final : public CacheShard {
 public:
  LRUCacheShard(size_t capacity, size_t estimated_value_size,
                bool strict_capacity_limit,
                CacheMetadataChargePolicy metadata_charge_policy);
  ~LRUCacheShard() override = default;

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space.
  void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Like Cache methods, but with an extra "hash" parameter.
  Status Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
                Cache::DeleterFn deleter, Cache::Handle** handle,
                Cache::Priority priority) override;

  Status Insert(const Slice& key, uint32_t hash, void* value,
                const Cache::CacheItemHelper* helper, size_t charge,
                Cache::Handle** handle, Cache::Priority priority) override {
    return Insert(key, hash, value, charge, helper->del_cb, handle, priority);
  }

  Cache::Handle* Lookup(const Slice& key, uint32_t hash,
                        const Cache::CacheItemHelper* /*helper*/,
                        const Cache::CreateCallback& /*create_cb*/,
                        Cache::Priority /*priority*/, bool /*wait*/,
                        Statistics* /*stats*/) override {
    return Lookup(key, hash);
  }
  Cache::Handle* Lookup(const Slice& key, uint32_t hash) override;

  bool Release(Cache::Handle* handle, bool /*useful*/,
               bool erase_if_last_ref) override {
    return Release(handle, erase_if_last_ref);
  }
  bool IsReady(Cache::Handle* /*handle*/) override { return true; }
  void Wait(Cache::Handle* /*handle*/) override {}

  bool Ref(Cache::Handle* handle) override;
  bool Release(Cache::Handle* handle, bool erase_if_last_ref = false) override;
  void Erase(const Slice& key, uint32_t hash) override;

  size_t GetUsage() const override;
  size_t GetPinnedUsage() const override;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      uint32_t average_entries_per_lock, uint32_t* state) override;

  void EraseUnRefEntries() override;

  std::string GetPrintableOptions() const override;

 private:
  friend class LRUCache;
  // Insert an item into the hash table and, if handle is null, insert into
  // the LRU list. Older items are evicted as necessary. If the cache is full
  // and free_handle_on_fail is true, the item is deleted and handle is set to
  // nullptr.
  Status InsertItem(LRUHandle* item, Cache::Handle** handle,
                    bool free_handle_on_fail);

  void LRU_Remove(LRUHandle* e);
  void LRU_Insert(LRUHandle* e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.
  void EvictFromLRU(size_t charge, autovector<LRUHandle*>* deleted);

  // Returns the number of bits used to hash an element in the per-shard
  // table.
  static int GetHashBits(size_t capacity, size_t estimated_value_size,
                         CacheMetadataChargePolicy metadata_charge_policy);

  // Initialized before use.
  size_t capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  LRUHandle lru_;

  // Pointer to head of low-pri pool in LRU list.
  LRUHandle* lru_low_pri_;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  LRUHandleTable table_;

  // Memory size for entries residing in the cache.
  size_t usage_;

  // Memory size for entries residing only in the LRU list.
  size_t lru_usage_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;
};

class LRUCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache {
 public:
  LRUCache(size_t capacity, size_t estimated_value_size, int num_shard_bits,
           bool strict_capacity_limit,
           CacheMetadataChargePolicy metadata_charge_policy =
               kDontChargeCacheMetadata);
  ~LRUCache() override;
  const char* Name() const override { return "LRUCache"; }
  CacheShard* GetShard(uint32_t shard) override;
  const CacheShard* GetShard(uint32_t shard) const override;
  void* Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  uint32_t GetHash(Handle* handle) const override;
  DeleterFn GetDeleter(Handle* handle) const override;
  void DisownData() override;

 private:
  LRUCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
};
}  // namespace fast_lru_cache

std::shared_ptr<Cache> NewFastLRUCache(
    size_t capacity, size_t estimated_value_size, int num_shard_bits,
    bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy);

}  // namespace ROCKSDB_NAMESPACE
