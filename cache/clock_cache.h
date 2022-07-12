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
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "cache/cache_key.h"
#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/secondary_cache.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

// Clock cache implementation. This is based on FastLRUCache's open-addressed
// hash table. Importantly, it stores elements in an array, and resolves
// collision using a probing strategy. Visibility and referenceability of
// elements works as usual. See fast_lru_cache.h for a detailed description.
//
// The main difference with FastLRUCache is, not surprisingly, the eviction
// algorithm
// ---instead of an LRU list, we maintain a circular list with the elements
// available for eviction, which the clock algorithm traverses to pick the next
// victim. The clock list is represented using the array of handles, and we
// simply mark those elements that are present in the list. This is done using
// different clock flags, namely NONE, LOW, MEDIUM, HIGH, that represent
// priorities: NONE means that the element is not part of the clock list, and
// LOW to HIGH represent how close an element is from being evictable (LOW being
// immediately evictable). When the clock pointer steps on an element that is
// not immediately evictable, it decreases its priority.

constexpr double kLoadFactor = 0.35;  // See fast_lru_cache.h.

constexpr double kStrictLoadFactor = 0.7;  // See fast_lru_cache.h.

// Arbitrary seeds.
constexpr uint32_t kProbingSeed1 = 0xbc9f1d34;
constexpr uint32_t kProbingSeed2 = 0x7a2bb9d5;

// An experimental (under development!) alternative to LRUCache

struct ClockHandle {
  void* value;
  Cache::DeleterFn deleter;
  uint32_t hash;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  std::array<char, kCacheKeySize> key_data;

  static constexpr int kExternalRefsOffset = 0;
  static constexpr int kSharedRefsOffset = 15;
  static constexpr int kExclusiveRefOffset = 30;
  static constexpr int kWillDeleteOffset = 31;

  enum Refs : uint32_t {
    // Number of external references to the slot.
    EXTERNAL_REFS = ((uint32_t{1} << 15) - 1)
                    << kExternalRefsOffset,  // Bits 0, ..., 14
    // Total number of internal plus external references to the slot.
    SHARED_REFS = ((uint32_t{1} << 15) - 1)
                  << kSharedRefsOffset,  // Bits 15, ..., 29
    // Whether a thread has an exclusive reference to the slot.
    EXCLUSIVE_REF = uint32_t{1} << kExclusiveRefOffset,  // Bit 30
    // Whether the handle will be deleted soon. This means that new internal
    // or external references to this handle cannot be taken from the moment
    // it's set. There is an exception: external references can be created
    // from existing external references, or converting from existing internal
    // references.
    WILL_DELETE = uint32_t{1} << kWillDeleteOffset  // Bit 31
  };

  static constexpr uint32_t kOneInternalRef = 0x8000;
  static constexpr uint32_t kOneExternalRef = 0x8001;

  std::atomic<uint32_t> refs;

  static constexpr int kIsElementOffset = 1;
  static constexpr int kClockPriorityOffset = 2;
  static constexpr int kIsHitOffset = 4;
  static constexpr int kCachePriorityOffset = 5;

  enum Flags : uint8_t {
    // Whether the slot is in use by an element.
    IS_ELEMENT = 1 << kIsElementOffset,
    // Clock priorities. Represents how close a handle is from being evictable.
    CLOCK_PRIORITY = 3 << kClockPriorityOffset,
    // Whether the handle has been looked up after its insertion.
    HAS_HIT = 1 << kIsHitOffset,
    // The value of Cache::Priority for the handle.
    CACHE_PRIORITY = 1 << kCachePriorityOffset,
  };

  std::atomic<uint8_t> flags;

  enum ClockPriority : uint8_t {
    NONE = (0 << kClockPriorityOffset),
    LOW = (1 << kClockPriorityOffset),
    MEDIUM = (2 << kClockPriorityOffset),
    HIGH = (3 << kClockPriorityOffset)
    // Priority LOW means immediately evictable, and HIGH means farthest from
    // evictable. Priority is NONE if and only if:
    // (i) the handle is not an element, or
    // (ii) the handle is an externally referenced element, or
    // (iii) the handle is an element that was externally referenced but it's
    //      not any more, and the clock_pointer_ has not swept through the
    //      slot since the element stopped being referenced.
  };

  // The number of elements that hash to this slot or a lower one, but wind
  // up in this slot or a higher one.
  std::atomic<uint32_t> displacements;

  ClockHandle()
      : value(nullptr),
        deleter(nullptr),
        hash(0),
        total_charge(0),
        refs(0),
        flags(0),
        displacements(0) {
    SetWillDelete(false);
    SetIsElement(false);
    SetClockPriority(ClockPriority::NONE);
    SetCachePriority(Cache::Priority::LOW);
    key_data.fill(0);
  }

  ClockHandle(const ClockHandle& other)
      : value(other.value),
        deleter(other.deleter),
        hash(other.hash),
        total_charge(other.total_charge),
        key_data(other.key_data) {
    refs.store(other.refs);
    flags.store(other.flags);
    displacements.store(other.displacements);
    SetWillDelete(other.WillDelete());
    SetIsElement(other.IsElement());
    SetClockPriority(other.GetClockPriority());
    SetCachePriority(other.GetCachePriority());
  }

  void operator=(const ClockHandle& other) {
    // TODO(Guido) Is there a better way to implement copy ctor + operator=?
    value = other.value;
    deleter = other.deleter;
    hash = other.hash;
    total_charge = other.total_charge;
    refs.store(other.refs);
    key_data = other.key_data;
    flags.store(other.flags);
    SetWillDelete(other.WillDelete());
    SetIsElement(other.IsElement());
    SetClockPriority(other.GetClockPriority());
    SetCachePriority(other.GetCachePriority());
    displacements.store(other.displacements);
  }

  Slice key() const { return Slice(key_data.data(), kCacheKeySize); }

  bool HasExternalRefs() const { return (refs & EXTERNAL_REFS) > 0; }

  bool IsElement() const { return flags & IS_ELEMENT; }

  void SetIsElement(bool is_element) {
    if (is_element) {
      flags |= IS_ELEMENT;
    } else {
      flags &= ~IS_ELEMENT;
    }
  }

  bool HasHit() const { return flags & HAS_HIT; }

  void SetHit() { flags |= HAS_HIT; }

  bool IsInClock() const {
    return GetClockPriority() != ClockHandle::ClockPriority::NONE;
  }

  Cache::Priority GetCachePriority() const {
    return static_cast<Cache::Priority>(flags & CACHE_PRIORITY);
  }

  void SetCachePriority(Cache::Priority priority) {
    if (priority == Cache::Priority::HIGH) {
      flags |= Flags::CACHE_PRIORITY;
    } else {
      flags &= ~Flags::CACHE_PRIORITY;
    }
  }

  ClockPriority GetClockPriority() const {
    return static_cast<ClockPriority>(flags & Flags::CLOCK_PRIORITY);
  }

  void SetClockPriority(ClockPriority priority) {
    flags &= ~Flags::CLOCK_PRIORITY;
    flags |= priority;
  }

  void DecreaseClockPriority() {
    uint8_t p = static_cast<uint8_t>(flags & Flags::CLOCK_PRIORITY) >>
                kClockPriorityOffset;
    assert(p > 0);
    p--;
    flags &= ~Flags::CLOCK_PRIORITY;
    ClockPriority new_priority =
        static_cast<ClockPriority>(p << kClockPriorityOffset);
    flags |= new_priority;
  }

  void FreeData() {
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

  inline bool IsEmpty() const {
    return !this->IsElement() && this->displacements == 0;
  }

  inline bool IsTombstone() const {
    return !this->IsElement() && this->displacements > 0;
  }

  inline bool Matches(const Slice& some_key, uint32_t some_hash) const {
    return this->IsElement() && this->hash == some_hash &&
           this->key() == some_key;
  }

  bool WillDelete() const { return refs & WILL_DELETE; }

  void SetWillDelete(bool will_delete) {
    if (will_delete) {
      refs |= WILL_DELETE;
    } else {
      refs &= ~WILL_DELETE;
    }
  }

  inline bool ExternalRef() {
    if (!((refs += kOneExternalRef) & (EXCLUSIVE_REF | WILL_DELETE))) {
      return true;
    }
    refs -= kOneExternalRef;
    return false;
  }

  // Retrieves refs and modify the external ref count in a single atomic
  // operation.
  inline uint32_t ReleaseExternalRef() { return refs -= kOneExternalRef; }

  // Take an external ref, assuming there is at least one external reference
  // to the handle.
  // TODO(Guido) Is it okay to assume that the existing external reference
  // survives until this function returns?
  void Ref() { refs += kOneExternalRef; }

  inline bool InternalRef() {
    if (!((refs += kOneInternalRef) & (EXCLUSIVE_REF | WILL_DELETE))) {
      return true;
    }
    refs -= kOneInternalRef;
    return false;
  }

  inline void ReleaseInternalRef() { refs -= kOneInternalRef; }

  inline bool ExclusiveRef() {
    uint32_t will_delete = refs & WILL_DELETE;
    uint32_t expected = will_delete;
    return refs.compare_exchange_strong(expected, EXCLUSIVE_REF | will_delete);
  }

  // Spins until an exclusive ref is taken. Stops when an external reference is
  // detected (in this case the wait would presumably be too long).
  inline bool ConditionalSpinExclusiveRef() {
    uint32_t expected = 0;
    uint32_t will_delete = 0;
    while (
        !refs.compare_exchange_strong(expected, EXCLUSIVE_REF | will_delete)) {
      if (expected & EXTERNAL_REFS) {
        return false;
      }
      will_delete = expected & WILL_DELETE;
      expected = will_delete;
    }
    return true;
  }

  inline void ReleaseExclusiveRef() { refs.fetch_and(~EXCLUSIVE_REF); }

  // All the following conversion functions guarantee that no exclusive
  // refs to the handle can be taken by a different thread during the
  // process.
  inline void ExclusiveToInternalRef() {
    refs += kOneInternalRef;
    ReleaseExclusiveRef();
  }

  inline void ExclusiveToExternalRef() {
    refs += kOneExternalRef;
    ReleaseExclusiveRef();
  }

  // TODO(Guido) Spinning indefinitely is dangerous. Do we want to bound the
  // loop and prepare the algorithms to react to a failure?
  inline void InternalToExclusiveRef() {
    uint32_t expected = kOneInternalRef;
    uint32_t will_delete = 0;
    while (
        !refs.compare_exchange_strong(expected, EXCLUSIVE_REF | will_delete)) {
      will_delete = expected & WILL_DELETE;
      expected = kOneInternalRef | will_delete;
    }
  }

  inline void InternalToExternalRef() { refs++; }

  // TODO(Guido) Same concern.
  inline void ExternalToExclusiveRef() {
    uint32_t expected = kOneExternalRef;
    uint32_t will_delete = 0;
    while (
        !refs.compare_exchange_strong(expected, EXCLUSIVE_REF | will_delete)) {
      will_delete = expected & WILL_DELETE;
      expected = kOneExternalRef | will_delete;
    }
  }

};  // struct ClockHandle

class ClockHandleTable {
 public:
  explicit ClockHandleTable(int hash_bits);
  ~ClockHandleTable();

  // Returns a pointer to a visible element matching the key/hash, or
  // nullptr if not present.
  ClockHandle* Lookup(const Slice& key, uint32_t hash);

  // Inserts a copy of h into the hash table.
  // Returns a pointer to the inserted handle, or nullptr if no slot
  // available was found. If an existing visible element matching the
  // key/hash is already present in the hash table, the argument old
  // is set to point to it; otherwise, it's set to nullptr.
  // Returns an exclusive reference to h, and no references to old.
  ClockHandle* Insert(ClockHandle* h, ClockHandle** old);

  // Removes h from the hash table. The handle must already be off
  // the clock list.
  // Assumes the thread has an exclusive reference to h.
  void Remove(ClockHandle* h);

  // Assigns a copy of src to dst.
  // Assumes the thread has an exclusive reference to dst.
  void Assign(ClockHandle* dst, ClockHandle* src);

  template <typename T>
  void ApplyToEntriesRange(T func, uint32_t index_begin, uint32_t index_end,
                           bool ok_will_delete) {
    for (uint32_t i = index_begin; i < index_end; i++) {
      ClockHandle* h = &array_[i];
      if (h->ExclusiveRef()) {
        if (h->IsElement() && (ok_will_delete || !h->WillDelete())) {
          func(h);
        }
        h->ReleaseExclusiveRef();
      }
    }
  }

  template <typename T>
  void ConstApplyToEntriesRange(T func, uint32_t index_begin,
                                uint32_t index_end, bool ok_will_delete) const {
    for (uint32_t i = index_begin; i < index_end; i++) {
      ClockHandle* h = &array_[i];
      if (h->ExclusiveRef()) {
        if (h->IsElement() && (ok_will_delete || !h->WillDelete())) {
          func(h);
        }
        h->ReleaseExclusiveRef();
      }
    }
  }

  uint32_t GetTableSize() const { return uint32_t{1} << length_bits_; }

  int GetLengthBits() const { return length_bits_; }

  uint32_t GetOccupancyLimit() const { return occupancy_limit_; }

  uint32_t GetOccupancy() const { return occupancy_; }

  // Returns x mod 2^{length_bits_}.
  uint32_t ModTableSize(uint32_t x) { return x & length_bits_mask_; }

 private:
  friend class ClockCacheShard;

  int FindElement(const Slice& key, uint32_t hash, int& probe,
                  int displacement);

  int FindAvailableSlot(const Slice& key, int& probe, int displacement);

  int FindElementOrAvailableSlot(const Slice& key, uint32_t hash, int& probe,
                                 int displacement);

  // Returns the index of the first slot probed (hashing with
  // the given key) with a handle e such that cond(e) is true.
  // Otherwise, if no match is found, returns -1.
  // For every handle e probed, the FindSlot updates
  // e->displacements += displacement.
  // The argument probe is modified such that consecutive calls
  // to FindSlot continue probing right after where the previous
  // call left.
  // The functional argument cond is always called having an
  // internal reference.
  // If a matching handle is found, FindSlot yields an internal
  // reference to it.
  int FindSlot(const Slice& key, std::function<bool(const ClockHandle*)> cond,
               int& probe, int displacement);

  // After a failed FindSlot call, this function rolls back all
  // displacement updates done by a sequence of FindSlot calls,
  // starting from the 0-th probe.
  void Rollback(const Slice& key, int probe, int displacement);

  // Number of hash bits used for table index.
  // The size of the table is 1 << length_bits_.
  int length_bits_;

  // For faster computation of ModTableSize.
  const uint32_t length_bits_mask_;

  // Number of elements in the table.
  uint32_t occupancy_;

  // Maximum number of elements the user can store in the table.
  uint32_t occupancy_limit_;

  std::unique_ptr<ClockHandle[]> array_;
};  // class ClockHandleTable

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

  void ClockInsert(ClockHandle* h);

  void ClockRemove(ClockHandle* h);

  void Evict(ClockHandle* h);

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
  static int CalcHashBits(size_t capacity, size_t estimated_value_size,
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

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;
};  // class ClockCacheShard

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
};  // class ClockCache

}  // namespace clock_cache

}  // namespace ROCKSDB_NAMESPACE
