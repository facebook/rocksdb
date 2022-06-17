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

#define KEY_LENGTH 16

namespace ROCKSDB_NAMESPACE {
namespace fast_lru_cache {

// An experimental (under development!) alternative to LRUCache

struct LRUHandle {
  void* value;
  Cache::DeleterFn deleter;
  LRUHandle* next;
  LRUHandle* prev;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;

  enum Flags : uint8_t {
    // Whether the handle can be found via a Lookup.
    IS_PRESENT = (1 << 0),
    // Whether the slot is in use by an element.
    // IS_PRESENT implies IS_ELEMENT.
    IS_ELEMENT = (1 << 1),
  };
  uint8_t flags;

  // Number of elements that hash to this slot or a lower one,
  // but ended up in a higher slot. This field is managed
  // by the hash table, but not the shard.
  uint32_t displacements;

  char key_data[KEY_LENGTH];

  Slice key() const { return Slice(key_data, KEY_LENGTH); }

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

  bool IsPresent() const { return flags & IS_PRESENT; }

  void SetIsPresent(bool is_present) {
    if (is_present) {
      flags |= IS_PRESENT;
    } else {
      flags &= ~IS_PRESENT;
    }
  }

  bool IsElement() const { return flags & IS_ELEMENT; }

  void SetIsElement(bool is_element) {
    if (is_element) {
      flags |= IS_ELEMENT;
    } else {
      flags &= ~IS_ELEMENT;
    }
  }

  void Free() {
    assert(refs == 0);
    if (deleter) {
      (*deleter)(key(), value);
    }
  }

  // Calculate the memory usage by metadata.
  inline size_t CalcMetaCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    if (metadata_charge_policy != kFullChargeCacheMetadata) {
      return 0;
    } else {
      return sizeof(LRUHandle);
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

  // A handle can represent 3 different things: an empty slot, a tombstone, or
  // an actual element.
  inline bool IsEmpty() {
    return !this->IsElement() && this->displacements == 0;
  }

  inline bool IsTombstone() {
    return !this->IsElement() && this->displacements > 0;
  }

  inline bool Matches(const Slice& some_key, uint32_t some_hash) {
    return this->IsElement() && this->hash == some_hash &&
           this->key() == some_key;
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

  // Returns a pointer to the handle matching the key/hash, or nullptr
  // if not present.
  LRUHandle* Lookup(const Slice& key, uint32_t hash);
  // Returns a pointer to the inserted handle. The argument old is
  // an output argument, which is a reference to a pointer to an
  // existing handle that matches the key/hash, or nullptr if no such
  // handle exists.
  LRUHandle* Insert(LRUHandle* h, LRUHandle** old);
  // Returns a pointer to the handle matching the key/hash, or nullptr
  // if not present. Assumes the element is in the table.
  LRUHandle* Remove(const Slice& key, uint32_t hash);

  void Exclude(LRUHandle* e);

  void Assign(int slot, LRUHandle* src);

  template <typename T>
  void ApplyToEntriesRange(T func, uint32_t index_begin, uint32_t index_end) {
    for (uint32_t i = index_begin; i < index_end; i++) {
      LRUHandle* h = &array_[i];
      if (h->IsPresent()) {
        func(h);
      }
    }
  }

  int GetLengthBits() const { return length_bits_; }

  uint32_t GetOccupancy() const { return occupancy_; }

 private:
  int FindElement(const Slice& key, uint32_t hash, int& probe,
                  int displacement);

  int FindPresentElement(const Slice& key, uint32_t hash, int& probe,
                         int displacement);

  int FindAvailableSlot(const Slice& key, int& probe, int displacement);

  int FindPresentElementOrAvailableSlot(const Slice& key, uint32_t hash,
                                        int& probe, int displacement);

  // Returns the index of the first probe that contains a handle e
  // such that cond(e) is true.  If the probe sequence ends without
  // a match, returns a pointer to the first slot available in the
  // sequence.  Otherwise, if no match is found and no slot is
  // available, returns -1. For every handle e probed, except any
  // matching slot, we update e->displacements += displacement.
  // The variable probe is updated such that consecutive calls to
  // FindSlot use every probe number, without repetition.
  int FindSlot(const Slice& key, std::function<bool(LRUHandle*)> cond,
               int& probe, int displacement);

  // Number of hash bits (upper because lower bits used for sharding)
  // used for table index. Length == 1 << length_bits_
  int length_bits_;

  uint32_t occupancy_;

  // TODO(guido) Cache alignment.
  // TODO(guido) unique_ptr?
  LRUHandle* array_;
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
  // Insert an item into the hash table and, if handle is null, insert into
  // the LRU list. Older items are evicted as necessary. If the cache is full
  // and free_handle_on_fail is true, the item is deleted and handle is set to
  // nullptr.
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
  mutable port::Mutex mutex_;
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
