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

// Block cache implementation using a lock-free open-address hash table
// and clock eviction.

///////////////////////////////////////////////////////////////////////////////
//                          Part 1: Handles
//
// Every slot in the hash table is a ClockHandle. A handle can be in a few
// different states, that stem from the fact that handles can be externally
// referenced and, thus, can't always be immediately evicted when a delete
// operation is executed or when they are replaced by a new version (via an
// insert of the same key). Concretely, the state of a handle is defined by the
// following two properties:
// (R) Externally referenced: A handle can be referenced externally, or not.
//    Importantly, a handle can be evicted if and only if it's not
//    referenced. In particular, when an handle becomes referenced, it's
//    temporarily taken out of clock until all references to it are released.
// (M) Marked for deletion (or invisible): An handle is marked for deletion
//    when an operation attempts to delete it, but the handle is externally
//    referenced, so it can't be immediately deleted. When this mark is placed,
//    lookups will no longer be able to find it. Consequently, no more external
//    references will be taken to the handle. When a handle is marked for
//    deletion, we also say it's invisible.
// These properties induce 4 different states, with transitions defined as
// follows:
// - Not M --> M: When a handle is deleted or replaced by a new version, but
//    not immediately evicted.
// - M --> not M: This cannot happen. Once a handle is marked for deletion,
//    there is no can't go back.
// - R --> not R: When all references to an handle are released.
// - Not R --> R: When an unreferenced handle becomes referenced. This can only
//    happen if the handle is visible, since references to an handle can only be
//    created when it's visible.
//
///////////////////////////////////////////////////////////////////////////////
//                      Part 2: Hash table structure
//
// Internally, the cache uses an open-addressed hash table to index the handles.
// We use tombstone counters to keep track of displacements. Probes are
// generated with double-hashing (but the code can be easily modified to use
// other probing schemes, like linear hashing). Because of the tombstones and
// the two possible visibility states of a handle, the table slots (we use the
// word "slot" to refer to handles that are not necessary valid key-value
// elements) can be in 4 different states:
// 1. Visible element: The slot contains an element in not M state.
// 2. To-be-deleted element: The slot contains an element in M state.
// 3. Tombstone: The slot doesn't contain an element, but there is some other
//    element that probed this slot during its insertion.
// 4. Empty: The slot is unused.
// When a ghost is removed from the table, it can either transition to being a
// tombstone or an empty slot, depending on the number of displacements of the
// slot. In any case, the slot becomes available. When a handle is inserted
// into that slot, it becomes a visible element again.
//
///////////////////////////////////////////////////////////////////////////////
//                      Part 3: The clock algorithm
//
// We maintain a circular buffer with the handles available for eviction,
// which the clock algorithm traverses (using a "clock pointer") to pick the
// next victim. We use the hash table array as the circular buffer, and mark
// the handles that are evictable. For this we use different clock flags, namely
// NONE, LOW, MEDIUM, HIGH, that represent priorities: LOW, MEDIUM and HIGH
// represent how close an element is from being evictable, LOW being immediately
// evictable. NONE means the slot is not evictable. This is due to one of the
// following reasons:
// (i) the slot doesn't contain an element, or
// (ii) the slot contains an element that is in R state, or
// (iii) the slot contains an element that was in R state but it's
//      not any more, and the clock pointer has not swept through the
//      slot since the element stopped being referenced.
//
// The priority NONE is really only important for case (iii), as in the other
// two cases there are other metadata fields that already capture the state.
// When an element stops being referenced (and is not deleted), the clock
// algorithm must acknowledge this, and assign a non-NONE priority to make
// the element evictable again.
//
///////////////////////////////////////////////////////////////////////////////
//                      Part 4: Synchronization
//
// We provide the following synchronization guarantees:
// - Lookup is lock-free.
// - Release is lock-free, unless (i) no references to the element are left,
//   and (ii) it was marked for deletion or the user wishes to delete if
//   releasing the last reference.
// - Insert and Erase still use a per-shard lock.
//
// Our hash table is lock-free, in the sense that system-wide progress is
// guaranteed, i.e., some thread is always able to make progress.
//
///////////////////////////////////////////////////////////////////////////////

// The load factor p is a real number in (0, 1) such that at all
// times at most a fraction p of all slots, without counting tombstones,
// are occupied by elements. This means that the probability that a
// random probe hits an empty slot is at most p, and thus at most 1/p probes
// are required on average. For example, p = 70% implies that between 1 and 2
// probes are needed on average (bear in mind that this reasoning doesn't
// consider the effects of clustering over time).
// Because the size of the hash table is always rounded up to the next
// power of 2, p is really an upper bound on the actual load factor---the
// actual load factor is anywhere between p/2 and p. This is a bit wasteful,
// but bear in mind that slots only hold metadata, not actual values.
// Since space cost is dominated by the values (the LSM blocks),
// overprovisioning the table with metadata only increases the total cache space
// usage by a tiny fraction.
constexpr double kLoadFactor = 0.35;

// The user can exceed kLoadFactor if the sizes of the inserted values don't
// match estimated_value_size, or if strict_capacity_limit == false. To
// avoid performance to plunge, we set a strict upper bound on the load factor.
constexpr double kStrictLoadFactor = 0.7;

// Arbitrary seeds.
constexpr uint32_t kProbingSeed1 = 0xbc9f1d34;
constexpr uint32_t kProbingSeed2 = 0x7a2bb9d5;

// An experimental (under development!) alternative to LRUCache.

struct ClockHandle {
  void* value;
  Cache::DeleterFn deleter;
  uint32_t hash;
  size_t total_charge;
  std::array<char, kCacheKeySize> key_data;

  static constexpr uint8_t kExternalRefsOffset = 0;
  static constexpr uint8_t kSharedRefsOffset = 15;
  static constexpr uint8_t kExclusiveRefOffset = 30;
  static constexpr uint8_t kWillBeDeletedOffset = 31;

  enum Refs : uint32_t {
    // Number of external references to the slot.
    EXTERNAL_REFS = ((uint32_t{1} << 15) - 1)
                    << kExternalRefsOffset,  // Bits 0, ..., 14
    // Number of internal references plus external references to the slot.
    SHARED_REFS = ((uint32_t{1} << 15) - 1)
                  << kSharedRefsOffset,  // Bits 15, ..., 29
    // Whether a thread has an exclusive reference to the slot.
    EXCLUSIVE_REF = uint32_t{1} << kExclusiveRefOffset,  // Bit 30
    // Whether the handle will be deleted soon. When this bit is set, new
    // internal
    // or external references to this handle stop being accepted.
    // There is an exception: external references can be created from
    // existing external references, or converting from existing internal
    // references.
    WILL_BE_DELETED = uint32_t{1} << kWillBeDeletedOffset  // Bit 31

    // Shared references (i.e., external and internal references) and exclusive
    // references are our custom implementation of RW locks---external and
    // internal references are read locks, and exclusive references are write
    // locks. We prioritize readers, which never block; in fact, they don't even
    // use compare-and-swap operations. Using our own implementation of RW locks
    // allows us to save many atomic operations by packing data more carefully.
    // In particular:
    // - Combining EXTERNAL_REFS and SHARED_REFS allows us to convert an
    // internal
    //    reference into an external reference in a single atomic arithmetic
    //    operation.
    // - Combining SHARED_REFS and WILL_BE_DELETED allows us to attempt to take
    //    a shared reference and check whether the entry is marked for deletion
    //    in a single atomic arithmetic operation.
  };

  static constexpr uint32_t kOneInternalRef = 0x8000;
  static constexpr uint32_t kOneExternalRef = 0x8001;

  std::atomic<uint32_t> refs;

  static constexpr uint8_t kIsElementOffset = 1;
  static constexpr uint8_t kClockPriorityOffset = 2;
  static constexpr uint8_t kIsHitOffset = 4;
  static constexpr uint8_t kCachePriorityOffset = 5;

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
  };

  // The number of elements that hash to this slot or a lower one, but wind
  // up in this slot or a higher one.
  std::atomic<uint32_t> displacements;

  // Synchronization rules:
  // - Use a shared reference when we want the handle's identity
  //    members (key_data, hash, value and IS_ELEMENT flag) to
  //    remain untouched, but not modify them. The only updates
  //    that a shared reference allows are:
  //      * set CLOCK_PRIORITY to NONE;
  //      * set the HAS_HIT bit.
  //    Notice that these two types of updates are idempotent, so
  //    they don't require synchronization across shared references.
  // - Use an exclusive reference when we want identity members
  //    to remain untouched, as well as modify any identity member
  //    or flag.
  // - displacements can be modified without holding a reference.
  // - refs is only modified through appropriate functions to
  //    take or release references.

  ClockHandle()
      : value(nullptr),
        deleter(nullptr),
        hash(0),
        total_charge(0),
        refs(0),
        flags(0),
        displacements(0) {
    SetWillBeDeleted(false);
    SetIsElement(false);
    SetClockPriority(ClockPriority::NONE);
    SetCachePriority(Cache::Priority::LOW);
    key_data.fill(0);
  }

  ClockHandle(const ClockHandle& other) { *this = other; }

  void operator=(const ClockHandle& other) {
    value = other.value;
    deleter = other.deleter;
    hash = other.hash;
    total_charge = other.total_charge;
    refs.store(other.refs);
    key_data = other.key_data;
    flags.store(other.flags);
    SetWillBeDeleted(other.WillBeDeleted());
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
      flags &= static_cast<uint8_t>(~IS_ELEMENT);
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
      flags &= static_cast<uint8_t>(~Flags::CACHE_PRIORITY);
    }
  }

  ClockPriority GetClockPriority() const {
    return static_cast<ClockPriority>(flags & Flags::CLOCK_PRIORITY);
  }

  void SetClockPriority(ClockPriority priority) {
    flags &= static_cast<uint8_t>(~Flags::CLOCK_PRIORITY);
    flags |= priority;
  }

  void DecreaseClockPriority() {
    uint8_t p = static_cast<uint8_t>(flags & Flags::CLOCK_PRIORITY) >>
                kClockPriorityOffset;
    assert(p > 0);
    p--;
    flags &= static_cast<uint8_t>(~Flags::CLOCK_PRIORITY);
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

  bool WillBeDeleted() const { return refs & WILL_BE_DELETED; }

  void SetWillBeDeleted(bool will_be_deleted) {
    if (will_be_deleted) {
      refs |= WILL_BE_DELETED;
    } else {
      refs &= ~WILL_BE_DELETED;
    }
  }

  // The following functions are for taking and releasing refs.

  // Tries to take an external ref. Returns true iff it succeeds.
  inline bool TryExternalRef() {
    if (!((refs += kOneExternalRef) & (EXCLUSIVE_REF | WILL_BE_DELETED))) {
      return true;
    }
    refs -= kOneExternalRef;
    return false;
  }

  // Releases an external ref. Returns the new value (this is useful to
  // avoid an extra atomic read).
  inline uint32_t ReleaseExternalRef() { return refs -= kOneExternalRef; }

  // Take an external ref, assuming there is already one external ref
  // to the handle.
  void Ref() {
    // TODO(Guido) Is it okay to assume that the existing external reference
    // survives until this function returns?
    refs += kOneExternalRef;
  }

  // Tries to take an internal ref. Returns true iff it succeeds.
  inline bool TryInternalRef() {
    if (!((refs += kOneInternalRef) & (EXCLUSIVE_REF | WILL_BE_DELETED))) {
      return true;
    }
    refs -= kOneInternalRef;
    return false;
  }

  inline void ReleaseInternalRef() { refs -= kOneInternalRef; }

  // Tries to take an exclusive ref. Returns true iff it succeeds.
  inline bool TryExclusiveRef() {
    uint32_t will_be_deleted = refs & WILL_BE_DELETED;
    uint32_t expected = will_be_deleted;
    return refs.compare_exchange_strong(expected,
                                        EXCLUSIVE_REF | will_be_deleted);
  }

  // Repeatedly tries to take an exclusive reference, but stops as soon
  // as an external reference is detected (in this case the wait would
  // presumably be too long).
  inline bool TrySpinExclusiveRef() {
    uint32_t expected = 0;
    uint32_t will_be_deleted = 0;
    while (!refs.compare_exchange_strong(expected,
                                         EXCLUSIVE_REF | will_be_deleted)) {
      if (expected & EXTERNAL_REFS) {
        return false;
      }
      will_be_deleted = expected & WILL_BE_DELETED;
      expected = will_be_deleted;
    }
    return true;
  }

  inline void ReleaseExclusiveRef() { refs.fetch_and(~EXCLUSIVE_REF); }

  // The following functions are for upgrading and downgrading refs.
  // They guarantee atomicity, i.e., no exclusive refs to the handle
  // can be taken by a different thread during the conversion.

  inline void ExclusiveToInternalRef() {
    refs += kOneInternalRef;
    ReleaseExclusiveRef();
  }

  inline void ExclusiveToExternalRef() {
    refs += kOneExternalRef;
    ReleaseExclusiveRef();
  }

  // TODO(Guido) Do we want to bound the loop and prepare the
  // algorithms to react to a failure?
  inline void InternalToExclusiveRef() {
    uint32_t expected = kOneInternalRef;
    uint32_t will_be_deleted = 0;
    while (!refs.compare_exchange_strong(expected,
                                         EXCLUSIVE_REF | will_be_deleted)) {
      will_be_deleted = expected & WILL_BE_DELETED;
      expected = kOneInternalRef | will_be_deleted;
    }
  }

  inline void InternalToExternalRef() {
    refs += kOneExternalRef - kOneInternalRef;
  }

  // TODO(Guido) Same concern.
  inline void ExternalToExclusiveRef() {
    uint32_t expected = kOneExternalRef;
    uint32_t will_be_deleted = 0;
    while (!refs.compare_exchange_strong(expected,
                                         EXCLUSIVE_REF | will_be_deleted)) {
      will_be_deleted = expected & WILL_BE_DELETED;
      expected = kOneExternalRef | will_be_deleted;
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

  // Removes h from the hash table. The handle must already be off clock.
  void Remove(ClockHandle* h);

  // Extracts the element information from a handle (src), and assigns it
  // to a hash table slot (dst). Doesn't touch displacements and refs,
  // which are maintained by the hash table algorithm.
  void Assign(ClockHandle* dst, ClockHandle* src);

  template <typename T>
  void ApplyToEntriesRange(T func, uint32_t index_begin, uint32_t index_end,
                           bool apply_if_will_be_deleted) {
    for (uint32_t i = index_begin; i < index_end; i++) {
      ClockHandle* h = &array_[i];
      if (h->TryExclusiveRef()) {
        if (h->IsElement() &&
            (apply_if_will_be_deleted || !h->WillBeDeleted())) {
          // Hand the internal ref over to func, which is now responsible
          // to release it.
          func(h);
        } else {
          h->ReleaseExclusiveRef();
        }
      }
    }
  }

  template <typename T>
  void ConstApplyToEntriesRange(T func, uint32_t index_begin,
                                uint32_t index_end,
                                bool apply_if_will_be_deleted) const {
    for (uint32_t i = index_begin; i < index_end; i++) {
      ClockHandle* h = &array_[i];
      if (h->TryExclusiveRef()) {
        if (h->IsElement() &&
            (apply_if_will_be_deleted || !h->WillBeDeleted())) {
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

  int FindElement(const Slice& key, uint32_t hash, uint32_t& probe);

  int FindAvailableSlot(const Slice& key, uint32_t& probe);

  int FindElementOrAvailableSlot(const Slice& key, uint32_t hash,
                                 uint32_t& probe);

  // Returns the index of the first slot probed (hashing with
  // the given key) with a handle e such that match(e) is true.
  // At every step, the function first tests whether match(e) holds.
  // If it's false, it evaluates abort(e) to decide whether the
  // search should be aborted, and in the affirmative returns -1.
  // For every handle e probed except the last one, the function runs
  // update(e). We say a probe to a handle e is aborting if match(e) is
  // false and abort(e) is true. The argument probe is one more than the
  // last non-aborting probe during the call. This is so that that the
  // variable can be used to keep track of progress across consecutive
  // calls to FindSlot.
  inline int FindSlot(const Slice& key, std::function<bool(ClockHandle*)> match,
                      std::function<bool(ClockHandle*)> stop,
                      std::function<void(ClockHandle*)> update,
                      uint32_t& probe);

  // After a failed FindSlot call (i.e., with answer -1), this function
  // decrements all displacements, starting from the 0-th probe.
  void Rollback(const Slice& key, uint32_t probe);

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
  // Insert an item into the hash table and, if handle is null, make it
  // evictable by the clock algorithm. Older items are evicted as necessary.
  // If the cache is full and free_handle_on_fail is true, the item is deleted
  // and handle is set to nullptr.
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

  // Makes an element evictable by clock.
  void ClockOn(ClockHandle* h);

  // Makes an element non-evictable.
  void ClockOff(ClockHandle* h);

  // Requires an exclusive ref on h.
  void Evict(ClockHandle* h);

  // Free some space following strict clock policy until enough space
  // to hold (usage_ + charge) is freed or there are no evictable elements.
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

// Only for internal testing, temporarily replacing NewClockCache.
// TODO(Guido) Remove once NewClockCache constructs a ClockCache again.
extern std::shared_ptr<Cache> ExperimentalNewClockCache(
    size_t capacity, size_t estimated_value_size, int num_shard_bits,
    bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy);

}  // namespace ROCKSDB_NAMESPACE
