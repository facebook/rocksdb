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
namespace lru_cache {

// LRU cache implementation. This class is not thread-safe.

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in a hash table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//    In that case the entry is *not* in the LRU list
//    (refs >= 1 && in_cache == true)
// 2. Not referenced externally AND in hash table.
//    In that case the entry is in the LRU list and can be freed.
//    (refs == 0 && in_cache == true)
// 3. Referenced externally AND not in hash table.
//    In that case the entry is not in the LRU list and not in hash table.
//    The entry can be freed when refs becomes 0.
//    (refs >= 1 && in_cache == false)
// 4. The handle is never inserted into the LRUCache (both hash table and LRU
//    list) and it doesn't experience the above three states.
//    The entry can be freed when refs becomes 0.
//    (refs >= 1 && in_cache == false && IS_STANDALONE == true)
// All newly created LRUHandles are in state 1 or 4. If you call
// LRUCacheShard::Release on entry in state 1, it will go into state 2.
// To move from state 1 to state 3, either call LRUCacheShard::Erase or
// LRUCacheShard::Insert with the same key (but possibly different value).
// To move from state 2 to state 1, use LRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful LRUCacheShard::Lookup/LRUCacheShard::Insert have a
// matching LRUCache::Release (to move into state 2) or LRUCacheShard::Erase
// (to move into state 3).

struct LRUHandle {
  void* value;
  union Info {
    Info() {}
    ~Info() {}
    Cache::DeleterFn deleter;
    const ShardedCache::CacheItemHelper* helper;
  } info_;
  // An entry is not added to the LRUHandleTable until the secondary cache
  // lookup is complete, so its safe to have this union.
  union {
    LRUHandle* next_hash;
    SecondaryCacheResultHandle* sec_handle;
  };
  LRUHandle* next;
  LRUHandle* prev;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;

  enum Flags : uint16_t {
    // Whether this entry is referenced by the hash table.
    IN_CACHE = (1 << 0),
    // Whether this entry is high priority entry.
    IS_HIGH_PRI = (1 << 1),
    // Whether this entry is in high-pri pool.
    IN_HIGH_PRI_POOL = (1 << 2),
    // Whether this entry has had any lookups (hits).
    HAS_HIT = (1 << 3),
    // Can this be inserted into the secondary cache.
    IS_SECONDARY_CACHE_COMPATIBLE = (1 << 4),
    // Is the handle still being read from a lower tier.
    IS_PENDING = (1 << 5),
    // Whether this handle is still in a lower tier
    IS_IN_SECONDARY_CACHE = (1 << 6),
    // Whether this entry is low priority entry.
    IS_LOW_PRI = (1 << 7),
    // Whether this entry is in low-pri pool.
    IN_LOW_PRI_POOL = (1 << 8),
    // Whether this entry is not inserted into the cache (both hash table and
    // LRU list).
    IS_STANDALONE = (1 << 9),
  };

  uint16_t flags;

#ifdef __SANITIZE_THREAD__
  // TSAN can report a false data race on flags, where one thread is writing
  // to one of the mutable bits and another thread is reading this immutable
  // bit. So precisely suppress that TSAN warning, we separate out this bit
  // during TSAN runs.
  bool is_secondary_cache_compatible_for_tsan;
#endif  // __SANITIZE_THREAD__

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
  bool IsHighPri() const { return flags & IS_HIGH_PRI; }
  bool InHighPriPool() const { return flags & IN_HIGH_PRI_POOL; }
  bool IsLowPri() const { return flags & IS_LOW_PRI; }
  bool InLowPriPool() const { return flags & IN_LOW_PRI_POOL; }
  bool HasHit() const { return flags & HAS_HIT; }
  bool IsSecondaryCacheCompatible() const {
#ifdef __SANITIZE_THREAD__
    return is_secondary_cache_compatible_for_tsan;
#else
    return flags & IS_SECONDARY_CACHE_COMPATIBLE;
#endif  // __SANITIZE_THREAD__
  }
  bool IsPending() const { return flags & IS_PENDING; }
  bool IsInSecondaryCache() const { return flags & IS_IN_SECONDARY_CACHE; }
  bool IsStandalone() const { return flags & IS_STANDALONE; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      flags |= IN_CACHE;
    } else {
      flags &= ~IN_CACHE;
    }
  }

  void SetPriority(Cache::Priority priority) {
    if (priority == Cache::Priority::HIGH) {
      flags |= IS_HIGH_PRI;
      flags &= ~IS_LOW_PRI;
    } else if (priority == Cache::Priority::LOW) {
      flags &= ~IS_HIGH_PRI;
      flags |= IS_LOW_PRI;
    } else {
      flags &= ~IS_HIGH_PRI;
      flags &= ~IS_LOW_PRI;
    }
  }

  void SetInHighPriPool(bool in_high_pri_pool) {
    if (in_high_pri_pool) {
      flags |= IN_HIGH_PRI_POOL;
    } else {
      flags &= ~IN_HIGH_PRI_POOL;
    }
  }

  void SetInLowPriPool(bool in_low_pri_pool) {
    if (in_low_pri_pool) {
      flags |= IN_LOW_PRI_POOL;
    } else {
      flags &= ~IN_LOW_PRI_POOL;
    }
  }

  void SetHit() { flags |= HAS_HIT; }

  void SetSecondaryCacheCompatible(bool compat) {
    if (compat) {
      flags |= IS_SECONDARY_CACHE_COMPATIBLE;
    } else {
      flags &= ~IS_SECONDARY_CACHE_COMPATIBLE;
    }
#ifdef __SANITIZE_THREAD__
    is_secondary_cache_compatible_for_tsan = compat;
#endif  // __SANITIZE_THREAD__
  }

  void SetIncomplete(bool incomp) {
    if (incomp) {
      flags |= IS_PENDING;
    } else {
      flags &= ~IS_PENDING;
    }
  }

  void SetIsInSecondaryCache(bool is_in_secondary_cache) {
    if (is_in_secondary_cache) {
      flags |= IS_IN_SECONDARY_CACHE;
    } else {
      flags &= ~IS_IN_SECONDARY_CACHE;
    }
  }

  void SetIsStandalone(bool is_standalone) {
    if (is_standalone) {
      flags |= IS_STANDALONE;
    } else {
      flags &= ~IS_STANDALONE;
    }
  }

  void Free() {
    assert(refs == 0);
#ifdef __SANITIZE_THREAD__
    // Here we can safely assert they are the same without a data race reported
    assert(((flags & IS_SECONDARY_CACHE_COMPATIBLE) != 0) ==
           is_secondary_cache_compatible_for_tsan);
#endif  // __SANITIZE_THREAD__
    if (!IsSecondaryCacheCompatible() && info_.deleter) {
      (*info_.deleter)(key(), value);
    } else if (IsSecondaryCacheCompatible()) {
      if (IsPending()) {
        assert(sec_handle != nullptr);
        SecondaryCacheResultHandle* tmp_sec_handle = sec_handle;
        tmp_sec_handle->Wait();
        value = tmp_sec_handle->Value();
        delete tmp_sec_handle;
      }
      if (value) {
        (*info_.helper->del_cb)(key(), value);
      }
    }
    delete[] reinterpret_cast<char*>(this);
  }

  inline size_t CalcuMetaCharge(
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

  // Calculate the memory usage by metadata.
  inline void CalcTotalCharge(
      size_t charge, CacheMetadataChargePolicy metadata_charge_policy) {
    total_charge = charge + CalcuMetaCharge(metadata_charge_policy);
  }

  inline size_t GetCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    size_t meta_charge = CalcuMetaCharge(metadata_charge_policy);
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
  // If the table uses more hash bits than `max_upper_hash_bits`,
  // it will eat into the bits used for sharding, which are constant
  // for a given LRUHandleTable.
  explicit LRUHandleTable(int max_upper_hash_bits);
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

  size_t GetOccupancyCount() const { return elems_; }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // Number of hash bits (upper because lower bits used for sharding)
  // used for table index. Length == 1 << length_bits_
  int length_bits_;

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  std::unique_ptr<LRUHandle*[]> list_;

  // Number of elements currently in the table.
  uint32_t elems_;

  // Set from max_upper_hash_bits (see constructor).
  const int max_length_bits_;
};

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) LRUCacheShard final : public CacheShard {
 public:
  LRUCacheShard(size_t capacity, bool strict_capacity_limit,
                double high_pri_pool_ratio, double low_pri_pool_ratio,
                bool use_adaptive_mutex,
                CacheMetadataChargePolicy metadata_charge_policy,
                int max_upper_hash_bits,
                const std::shared_ptr<SecondaryCache>& secondary_cache);
  virtual ~LRUCacheShard() override = default;

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space.
  virtual void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // Set percentage of capacity reserved for low-pri cache entries.
  void SetLowPriorityPoolRatio(double low_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge, Cache::DeleterFn deleter,
                        Cache::Handle** handle,
                        Cache::Priority priority) override {
    return Insert(key, hash, value, charge, deleter, nullptr, handle, priority);
  }
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        const Cache::CacheItemHelper* helper, size_t charge,
                        Cache::Handle** handle,
                        Cache::Priority priority) override {
    assert(helper);
    return Insert(key, hash, value, charge, nullptr, helper, handle, priority);
  }
  // If helper_cb is null, the values of the following arguments don't matter.
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash,
                                const ShardedCache::CacheItemHelper* helper,
                                const ShardedCache::CreateCallback& create_cb,
                                ShardedCache::Priority priority, bool wait,
                                Statistics* stats) override;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) override {
    return Lookup(key, hash, nullptr, nullptr, Cache::Priority::LOW, true,
                  nullptr);
  }
  virtual bool Release(Cache::Handle* handle, bool /*useful*/,
                       bool erase_if_last_ref) override {
    return Release(handle, erase_if_last_ref);
  }
  virtual bool IsReady(Cache::Handle* /*handle*/) override;
  virtual void Wait(Cache::Handle* /*handle*/) override {}
  virtual bool Ref(Cache::Handle* handle) override;
  virtual bool Release(Cache::Handle* handle,
                       bool erase_if_last_ref = false) override;
  virtual void Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;
  virtual size_t GetOccupancyCount() const override;
  virtual size_t GetTableAddressCount() const override;

  virtual void ApplyToSomeEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      uint32_t average_entries_per_lock, uint32_t* state) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;

  void TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri,
                       LRUHandle** lru_bottom_pri);

  // Retrieves number of elements in LRU, for unit test purpose only.
  // Not threadsafe.
  size_t TEST_GetLRUSize();

  // Retrieves high pri pool ratio
  double GetHighPriPoolRatio();

  // Retrieves low pri pool ratio
  double GetLowPriPoolRatio();

 private:
  friend class LRUCache;
  // Insert an item into the hash table and, if handle is null, insert into
  // the LRU list. Older items are evicted as necessary. If the cache is full
  // and free_handle_on_fail is true, the item is deleted and handle is set to
  // nullptr.
  Status InsertItem(LRUHandle* item, Cache::Handle** handle,
                    bool free_handle_on_fail);
  Status Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
                DeleterFn deleter, const Cache::CacheItemHelper* helper,
                Cache::Handle** handle, Cache::Priority priority);
  // Promote an item looked up from the secondary cache to the LRU cache.
  // The item may be still in the secondary cache.
  // It is only inserted into the hash table and not the LRU list, and only
  // if the cache is not at full capacity, as is the case during Insert.  The
  // caller should hold a reference on the LRUHandle. When the caller releases
  // the last reference, the item is added to the LRU list.
  // The item is promoted to the high pri or low pri pool as specified by the
  // caller in Lookup.
  void Promote(LRUHandle* e);
  void LRU_Remove(LRUHandle* e);
  void LRU_Insert(LRUHandle* e);

  // Overflow the last entry in high-pri pool to low-pri pool until size of
  // high-pri pool is no larger than the size specify by high_pri_pool_pct.
  void MaintainPoolSize();

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.
  void EvictFromLRU(size_t charge, autovector<LRUHandle*>* deleted);

  // Try to insert the evicted handles into the secondary cache.
  void TryInsertIntoSecondaryCache(autovector<LRUHandle*> evicted_handles);

  // Initialized before use.
  size_t capacity_;

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;

  // Memory size for entries in low-pri pool.
  size_t low_pri_pool_usage_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // Ratio of capacity reserved for low priority cache entries.
  double low_pri_pool_ratio_;

  // Low-pri pool size, equals to capacity * low_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double low_pri_pool_capacity_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  LRUHandle lru_;

  // Pointer to head of low-pri pool in LRU list.
  LRUHandle* lru_low_pri_;

  // Pointer to head of bottom-pri pool in LRU list.
  LRUHandle* lru_bottom_pri_;

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

  std::shared_ptr<SecondaryCache> secondary_cache_;
};

class LRUCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache {
 public:
  LRUCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
           double high_pri_pool_ratio, double low_pri_pool_ratio,
           std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
           bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
           CacheMetadataChargePolicy metadata_charge_policy =
               kDontChargeCacheMetadata,
           const std::shared_ptr<SecondaryCache>& secondary_cache = nullptr);
  virtual ~LRUCache();
  virtual const char* Name() const override { return "LRUCache"; }
  virtual CacheShard* GetShard(uint32_t shard) override;
  virtual const CacheShard* GetShard(uint32_t shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual DeleterFn GetDeleter(Handle* handle) const override;
  virtual void DisownData() override;
  virtual void WaitAll(std::vector<Handle*>& handles) override;
  std::string GetPrintableOptions() const override;

  // Retrieves number of elements in LRU, for unit test purpose only.
  size_t TEST_GetLRUSize();
  // Retrieves high pri pool ratio.
  double GetHighPriPoolRatio();

 private:
  LRUCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
  std::shared_ptr<SecondaryCache> secondary_cache_;
};

}  // namespace lru_cache

using LRUCache = lru_cache::LRUCache;
using LRUHandle = lru_cache::LRUHandle;
using LRUCacheShard = lru_cache::LRUCacheShard;

}  // namespace ROCKSDB_NAMESPACE
