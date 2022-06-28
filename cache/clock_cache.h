// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <array>
#include <memory>
#include <string>

#include "rocksdb/cache.h"
#include "cache/cache_key.h"
#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/secondary_cache.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"

#if !defined(ROCKSDB_LITE)
#define SUPPORT_CLOCK_CACHE // TODO(Guido) Do we need this?
#endif

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

// TODO(Guido) Update comment.

// LRU cache implementation using an open-address hash table.
//
// Every slot in the hash table is an LRUHandle. Because handles can be
// referenced externally, we can't discard them immediately once they are
// deleted (via a delete or an LRU eviction) or replaced by a new version
// (via an insert of the same key). The state of an element is defined by
// the following two properties:
// (R) Referenced: An element can be referenced externally (refs > 0), or not.
//    Importantly, an element can be evicted if and only if it's not
//    referenced. In particular, when an element becomes referenced, it's
//    temporarily taken out of the LRU list until all references to it
//    are dropped.
// (V) Visible: An element can visible for lookups (IS_VISIBLE set), or not.
//    Initially, every element is visible. An element that is not visible is
//    called a ghost.
// These properties induce 4 different states, with transitions defined as
// follows:
// - V --> not V: When a visible element is deleted or replaced by a new
//    version.
// - Not V --> V: This cannot happen. A ghost remains in that state until it's
//    not referenced any more, at which point it's ready to be removed from the
//    hash table. (A ghost simply waits to transition to the afterlife---it will
//    never be visible again.)
// - R --> not R: When all references to an element are dropped.
// - Not R --> R: When an unreferenced element becomes referenced. This can only
//    happen if the element is V, since references to an element can only be
//    created when it's visible.
//
// Internally, the cache uses an open-addressed hash table to index the handles.
// We use tombstone counters to keep track of displacements.
// Because of the tombstones and the two possible visibility states of an
// element, the table slots can be in 4 different states:
// 1. Visible element (IS_ELEMENT set and IS_VISIBLE set): The slot contains a
//    key-value element.
// 2. Ghost element (IS_ELEMENT set and IS_VISIBLE unset): The slot contains an
//    element that has been removed, but it's still referenced. It's invisible
//    to lookups.
// 3. Tombstone (IS_ELEMENT unset and displacements > 0): The slot contains a
//    tombstone.
// 4. Empty (IS_ELEMENT unset and displacements == 0): The slot is unused.
//    A slot that is an element can further have IS_VISIBLE set or not.
// When a ghost is removed from the table, it can either transition to being a
// tombstone or an empty slot, depending on the number of displacements of the
// slot. In any case, the slot becomes available. When a handle is inserted
// into that slot, it becomes a visible element again.

// The load factor p is a real number in (0, 1) such that at all
// times at most a fraction p of all slots, without counting tombstones,
// are occupied by elements. This means that the probability that a
// random probe hits an empty slot is at most p, and thus at most 1/p probes
// are required on average. For example, p = 70% implies that between 1 and 2
// probes are needed on average.
// Because the size of the hash table is always rounded up to the next
// power of 2, p is really an upper bound on the actual load factor---the
// actual load factor is anywhere between p/2 and p. This is a bit wasteful,
// but bear in mind that slots only hold metadata, not actual values.
// Since space cost is dominated by the values (the LSM blocks),
// overprovisioning the table with metadata only increases the total cache space
// usage by a tiny fraction.
constexpr double kLoadFactor = 0.35;

// Arbitrary seeds.
constexpr uint32_t kProbingSeed1 = 0xbc9f1d34;
constexpr uint32_t kProbingSeed2 = 0x7a2bb9d5;

// An experimental (under development!) alternative to LRUCache

struct ClockHandle {
  void* value;
  Cache::DeleterFn deleter;
  uint32_t hash;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;

  static constexpr int kIsVisibleOffset = 0;
  static constexpr int kIsElementOffset = 1;
  static constexpr int kClockPriorityOffset = 2;

  enum Flags : uint8_t {
    // Whether the handle is visible to Lookups.
    IS_VISIBLE      = (1 << kIsVisibleOffset),
    // Whether the slot is in use by an element.
    IS_ELEMENT      = (1 << kIsElementOffset),
    // Clock priorities. Represents how close a handle is from
    // being evictable.
    CLOCK_PRIORITY  = (3 << kClockPriorityOffset),
  };
  uint8_t flags;

  enum ClockPriority : uint8_t {
    NONE   = (0 << kClockPriorityOffset),    // Not an element in the eyes of clock.
    LOW    = (1 << kClockPriorityOffset),    // Immediately evictable.
    MEDIUM = (2 << kClockPriorityOffset),
    HIGH   = (3 << kClockPriorityOffset)
    // Priority is CLOCK_NONE if and only if
    // (i) the handle is not an element, or
    // (ii) the handle is an element but it is being referenced.
  };

  // The number of elements that hash to this slot or a lower one,
  // but wind up in a higher slot.
  uint32_t displacements;

  std::array<char, kCacheKeySize> key_data;

  ClockHandle() {
    value = nullptr;
    deleter = nullptr;
    hash = 0;
    total_charge = 0;
    refs = 0;
    flags = 0;
    SetIsVisible(false);
    SetIsElement(false);
    SetPriority(ClockPriority::NONE);
    displacements = 0;
    key_data.fill(0);
  }

  Slice key() const { return Slice(key_data.data(), kCacheKeySize); }

  // Increase the reference count by 1.
  void Ref() { refs++; }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    assert(refs > 0);
    refs--;
    return refs == 0;
  }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const {
    return refs > 0;
  }

  bool IsVisible() const { return flags & IS_VISIBLE; }

  void SetIsVisible(bool is_visible) {
    if (is_visible) {
      flags |= IS_VISIBLE;
    } else {
      flags &= ~IS_VISIBLE;
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

  ClockPriority GetPriority() const {
    return static_cast<ClockPriority>(flags & Flags::CLOCK_PRIORITY);
  }

  void SetPriority(ClockPriority priority) {
    flags &= ~Flags::CLOCK_PRIORITY;
    flags |= priority;
  }

  void DecreasePriority() {
    uint8_t p = static_cast<uint8_t>(flags & Flags::CLOCK_PRIORITY) >> kClockPriorityOffset;
    assert(p > 0);
    p--;
    flags &= ~Flags::CLOCK_PRIORITY;
    ClockPriority new_priority = static_cast<ClockPriority>(p << kClockPriorityOffset);
    flags |= new_priority;
  }

  void FreeData() {
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
      // #ifdef ROCKSDB_MALLOC_USABLE_SIZE
      //       return malloc_usable_size(
      //           const_cast<void*>(static_cast<const void*>(this)));
      // #else
      // TODO(Guido) malloc_usable_size only works when we call it on
      // a pointer allocated with malloc. Because our handles are all
      // allocated in a single shot as an array, the user can't call
      // CalcMetaCharge (or CalcTotalCharge or GetCharge) on a handle
      // pointer returned by the cache. Moreover, malloc_usable_size
      // expects a heap-allocated handle, but sometimes in our code we
      // wish to pass a stack-allocated handle (this is only a performance
      // concern).
      // What is the right way to compute metadata charges with pre-allocated
      // handles?
      return sizeof(ClockHandle);
      // #endif
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

  inline bool IsEmpty() {
    return !this->IsElement() && this->displacements == 0;
  }

  inline bool IsTombstone() {
    return !this->IsElement() && this->displacements > 0;
  }

  inline bool Matches(const Slice& some_key) {
    return this->IsElement() && this->key() == some_key;
  }
};  // struct ClockHandle


class ClockHandleTable {
 public:
  explicit ClockHandleTable(uint8_t hash_bits);
  ~ClockHandleTable();

  // Returns a pointer to a visible element matching the key/hash, or
  // nullptr if not present.
  ClockHandle* Lookup(const Slice& key);

  // Inserts a copy of h into the hash table.
  // Returns a pointer to the inserted handle, or nullptr if no slot
  // available was found. If an existing visible element matching the
  // key/hash is already present in the hash table, the argument old
  // is set to pointe to it; otherwise, it's set to nullptr.
  ClockHandle* Insert(ClockHandle* h, ClockHandle** old);

  // Removes h from the hash table. The handle must already be off
  // the clock list.
  void Remove(ClockHandle* h);

  // Turns a visible element h into a ghost (i.e., not visible).
  void Exclude(ClockHandle* h);

  // Assigns a copy of h to the given slot.
  void Assign(int slot, ClockHandle* h);

  template <typename T>
  void ApplyToEntriesRange(T func, uint32_t index_begin, uint32_t index_end) {
    for (uint32_t i = index_begin; i < index_end; i++) {
      ClockHandle* h = &array_[i];
      if (h->IsVisible()) {
        func(h);
      }
    }
  }

  uint8_t GetLengthBits() const { return length_bits_; }

  uint32_t GetOccupancy() const { return occupancy_; }

 private:
  friend class ClockCacheShard;

  int FindVisibleElement(const Slice& key, int& probe,
                         int displacement);

  int FindAvailableSlot(const Slice& key, int& probe, int displacement);

  int FindVisibleElementOrAvailableSlot(const Slice& key,
                                        int& probe, int displacement);

  // Returns the index of the first slot probed (hashing with
  // the given key) with a handle e such that cond(e) is true.
  // Otherwise, if no match is found, returns -1.
  // For every handle e probed except the final slot, updates
  // e->displacements += displacement.
  // The argument probe is modified such that consecutive calls
  // to FindSlot continue probing right after where the previous
  // call left.
  int FindSlot(const Slice& key, std::function<bool(ClockHandle*)> cond,
               int& probe, int displacement);

  // Number of hash bits used for table index.
  // The size of the table is 1 << length_bits_.
  uint8_t length_bits_;

  // Number of elements in the table.
  uint32_t occupancy_;

  std::unique_ptr<ClockHandle[]> array_;
}; // class ClockHandleTable


// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) ClockCacheShard final : public CacheShard {
 public:
  ClockCacheShard(size_t capacity, size_t estimated_value_size,
                bool strict_capacity_limit,
                CacheMetadataChargePolicy metadata_charge_policy);
  ~ClockCacheShard() override = default;

  // Separate from constructor so caller can easily make an array of ClockCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space.
  void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Like Cache methods, but with an extra "hash" parameter.
  // Insert an item into the hash table and, if handle is null, insert into
  // the clock list. Older items are evicted as necessary. If the cache is full
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
  friend class ClockCache;
  void ClockRemove(ClockHandle* e);
  void ClockInsert(ClockHandle* e);

  // Free some space following strict clock policy until enough space
  // to hold (usage_ + charge) is freed or the clock list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.
  void EvictFromClock(size_t charge, autovector<ClockHandle>* deleted);

  // Returns the charge of a single handle.
  static size_t CalcEstimatedHandleCharge(
      size_t estimated_value_size,
      CacheMetadataChargePolicy metadata_charge_policy);

  // Returns the number of bits used to hash an element in the hash
  // table.
  static uint8_t CalcHashBits(size_t capacity, size_t estimated_value_size,
                              CacheMetadataChargePolicy metadata_charge_policy);

  // Initialized before use.
  size_t capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  uint32_t clock_pointer_;

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
  ClockHandleTable table_;

  // Memory size for entries residing in the cache.
  size_t usage_;

  // Memory size for unpinned entries in the clock list.
  size_t clock_usage_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;
}; // class ClockCacheShard


class ClockCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache {
 public:
  ClockCache(size_t capacity, size_t estimated_value_size, int num_shard_bits,
           bool strict_capacity_limit,
           CacheMetadataChargePolicy metadata_charge_policy =
               kDontChargeCacheMetadata);
  ~ClockCache() override;
  const char* Name() const override { return "ClockCache"; }
  CacheShard* GetShard(uint32_t shard) override;
  const CacheShard* GetShard(uint32_t shard) const override;
  void* Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  uint32_t GetHash(Handle* handle) const override;
  DeleterFn GetDeleter(Handle* handle) const override;
  void DisownData() override;

 private:
  ClockCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
}; // class ClockCache

}  // namespace clock_cache

}  // namespace ROCKSDB_NAMESPACE
