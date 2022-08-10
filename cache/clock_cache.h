// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <sys/types.h>

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

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

// Forward declaration of friend class.
class ClockCacheTest;

// An experimental alternative to LRUCache, using a lock-free, open-addressed
// hash table and clock eviction.

// ----------------------------------------------------------------------------
// 1. INTRODUCTION
//
// In RocksDB, a Cache is a concurrent unordered dictionary that supports
// external references (a.k.a. user references). A ClockCache is a type of Cache
// that uses the clock algorithm as its eviction policy. Internally, a
// ClockCache is an open-addressed hash table that stores all KV pairs in a
// large array. Every slot in the hash table is a ClockHandle, which holds a KV
// pair plus some additional metadata that controls the different aspects of the
// cache: external references, the hashing mechanism, concurrent access and the
// clock algorithm.
//
//
// 2. EXTERNAL REFERENCES
//
// An externally referenced handle can't be deleted (either evicted by the clock
// algorithm, or explicitly deleted) or replaced by a new version (via an insert
// of the same key) until all external references to it have been released by
// the users. ClockHandles have two members to support external references:
// - EXTERNAL_REFS counter: The number of external refs. When EXTERNAL_REFS > 0,
//    the handle is externally referenced. Updates that intend to modify the
//    handle will refrain from doing so. Eventually, when all references are
//    released, we have EXTERNAL_REFS == 0, and updates can operate normally on
//    the handle.
// - WILL_BE_DELETED flag: An handle is marked for deletion when an operation
//    decides the handle should be deleted. This happens either when the last
//    reference to a handle is released (and the release operation is instructed
//    to delete on last reference) or on when a delete operation is called on
//    the item. This flag is needed because an externally referenced handle
//    can't be immediately deleted. In these cases, the flag will be later read
//    and acted upon by the eviction algorithm. Importantly, WILL_BE_DELETED is
//    used not only to defer deletions, but also as a barrier for external
//    references: once WILL_BE_DELETED is set, lookups (which are the most
//    common way to acquire new external references) will ignore the handle.
//    For this reason, when WILL_BE_DELETED is set, we say the handle is
//    invisible (and, otherwise, that it's visible).
//
//
// 3. HASHING AND COLLISION RESOLUTION
//
// ClockCache uses an open-addressed hash table to store the handles.
// We use a variant of tombstones to manage collisions: every slot keeps a
// count of how many KV pairs that are currently in the cache have probed the
// slot in an attempt to insert. Probes are generated with double-hashing
// (although the code can be easily modified to use other probing schemes, like
// linear probing).
//
// A slot in the hash table can be in a few different states:
// - Element: The slot contains an element. This is indicated with the
//    IS_ELEMENT flag. Element can be sub-classified depending on the
//    value of WILL_BE_DELETED:
//    * Visible element.
//    * Invisible element.
// - Tombstone: The slot doesn't contain an element, but there is some other
//    element that probed this slot during its insertion.
// - Empty: The slot is unused---it's neither an element nor a tombstone.
//
// A slot cycles through the following sequence of states:
// empty or tombstone --> visible element --> invisible element -->
// empty or tombstone. Initially a slot is available---it's either
// empty or a tombstone. As soon as a KV pair is written into the slot, it
// becomes a visible element. At some point, the handle will be deleted
// by an explicit delete operation, the eviction algorithm, or an overwriting
// insert. In either case, the handle is marked for deletion. When the an
// attempt to delete the element finally succeeds, the slot is freed up
// and becomes available again.
//
//
// 4. CONCURRENCY
//
// ClockCache is lock-free. At a high level, we synchronize the operations
// using a read-prioritized, non-blocking variant of RW locks on every slot of
// the hash table. To do this we generalize the concept of reference:
// - Internal reference: Taken by a thread that is attempting to read a slot
//    or do a very precise type of update.
// - Exclusive reference: Taken by a thread that is attempting to write a
//    a slot extensively.
//
// We defer the precise definitions to the comments in the code below.
// A crucial feature of our references is that attempting to take one never
// blocks the thread. Another important feature is that readers are
// prioritized, as they use extremely fast synchronization primitives---they
// use atomic arithmetic/bit operations, but no compare-and-swaps (which are
// much slower).
//
// Internal references are used by threads to read slots during a probing
// sequence, making them the most common references (probing is performed
// in almost every operation, not just lookups). During a lookup, once
// the target element is found, and just before the handle is handed over
// to the user, an internal reference is converted into an external reference.
// During an update operation, once the target slot is found, an internal
// reference is converted into an exclusive reference. Interestingly, we
// can't atomically upgrade from internal to exclusive, or we may run into a
// deadlock. Releasing the internal reference and then taking an exclusive
// reference avoids the deadlock, but then the handle may change inbetween.
// One of the key observations we use in our implementation is that we can
// make up for this lack of atomicity using IS_ELEMENT and WILL_BE_DELETED.
//
// Distinguishing internal from external references is useful for two reasons:
// - Internal references are short lived, but external references are typically
//    not. This is helpful when acquiring an exclusive ref: if there are any
//    external references to the item, it's probably not worth waiting until
//    they go away.
// - We can precisely determine when there are no more external references to a
//    handle, and proceed to mark it for deletion. This is useful when users
//    release external references.
//
//
// 5. CLOCK ALGORITHM
//
// The clock algorithm circularly sweeps through the hash table to find the next
// victim. Recall that handles that are referenced are not evictable; the clock
// algorithm never picks those. We use different clock priorities: NONE, LOW,
// MEDIUM and HIGH. Priorities LOW, MEDIUM and HIGH represent how close an
// element is from being evicted, LOW being the closest to evicted. NONE means
// the slot is not evictable. NONE priority is used in one of the following
// cases:
// (a) the slot doesn't contain an element, or
// (b) the slot contains an externally referenced element, or
// (c) the slot contains an element that used to be externally referenced,
//      and the clock pointer has not swept through the slot since the element
//      stopped being externally referenced.
// ----------------------------------------------------------------------------

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
// avoid a performance drop, we set a strict upper bound on the load factor.
constexpr double kStrictLoadFactor = 0.7;

// Maximum number of spins when trying to acquire a ref.
// TODO(Guido) This value was set arbitrarily. Is it appropriate?
// What's the best way to bound the spinning?
constexpr uint32_t kSpinsPerTry = 100000;

// Arbitrary seeds.
constexpr uint32_t kProbingSeed1 = 0xbc9f1d34;
constexpr uint32_t kProbingSeed2 = 0x7a2bb9d5;

struct ClockHandle {
  void* value;
  Cache::DeleterFn deleter;
  uint32_t hash;
  size_t total_charge;
  std::array<char, kCacheKeySize> key_data;

  static constexpr uint8_t kIsElementOffset = 0;
  static constexpr uint8_t kClockPriorityOffset = 1;
  static constexpr uint8_t kIsHitOffset = 3;
  static constexpr uint8_t kCachePriorityOffset = 4;

  enum Flags : uint8_t {
    // Whether the slot is in use by an element.
    IS_ELEMENT = 1 << kIsElementOffset,
    // Clock priorities. Represents how close a handle is from being evictable.
    CLOCK_PRIORITY = 3 << kClockPriorityOffset,
    // Whether the handle has been looked up after its insertion.
    HAS_HIT = 1 << kIsHitOffset,
    // The value of Cache::Priority of the handle.
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

  static constexpr uint8_t kExternalRefsOffset = 0;
  static constexpr uint8_t kSharedRefsOffset = 15;
  static constexpr uint8_t kExclusiveRefOffset = 30;
  static constexpr uint8_t kWillBeDeletedOffset = 31;

  enum Refs : uint32_t {
    // Synchronization model:
    // - An external reference guarantees that hash, value, key_data
    //    and the IS_ELEMENT flag are not modified. Doesn't allow
    //    any writes.
    // - An internal reference has the same guarantees as an
    //    external reference, and additionally allows the following
    //    idempotent updates on the handle:
    //      * set CLOCK_PRIORITY to NONE;
    //      * set the HAS_HIT bit;
    //      * set the WILL_BE_DELETED bit.
    // - A shared reference is either an external reference or an
    //    internal reference.
    // - An exclusive reference guarantees that no other thread has a shared
    //    or exclusive reference to the handle, and allows writes
    //    on the handle.

    // Number of external references to the slot.
    EXTERNAL_REFS = ((uint32_t{1} << 15) - 1)
                    << kExternalRefsOffset,  // Bits 0, ..., 14
    // Number of internal references plus external references to the slot.
    SHARED_REFS = ((uint32_t{1} << 15) - 1)
                  << kSharedRefsOffset,  // Bits 15, ..., 29
    // Whether a thread has an exclusive reference to the slot.
    EXCLUSIVE_REF = uint32_t{1} << kExclusiveRefOffset,  // Bit 30
    // Whether the handle will be deleted soon. When this bit is set, new
    // internal references to this handle stop being accepted.
    // External references may still be granted---they can be created from
    // existing external references, or converting from existing internal
    // references.
    WILL_BE_DELETED = uint32_t{1} << kWillBeDeletedOffset  // Bit 31

    // Having these 4 fields in a single variable allows us to support the
    // following operations efficiently:
    // - Convert an internal reference into an external reference in a single
    //    atomic arithmetic operation.
    // - Attempt to take a shared reference using a single atomic arithmetic
    //    operation. This is because we can increment the internal ref count
    //    as well as checking whether the entry is marked for deletion using a
    //    single atomic arithmetic operation (and one non-atomic comparison).
  };

  static constexpr uint32_t kOneInternalRef = 0x8000;
  static constexpr uint32_t kOneExternalRef = 0x8001;

  std::atomic<uint32_t> refs;

  // True iff the handle is allocated separately from hash table.
  bool detached;

  ClockHandle()
      : value(nullptr),
        deleter(nullptr),
        hash(0),
        total_charge(0),
        flags(0),
        displacements(0),
        refs(0),
        detached(false) {
    SetWillBeDeleted(false);
    SetIsElement(false);
    SetClockPriority(ClockPriority::NONE);
    SetCachePriority(Cache::Priority::LOW);
    key_data.fill(0);
  }

  // The copy ctor and assignment operator are only used to copy a handle
  // for immediate deletion. (We need to copy because the slot may become
  // re-used before the deletion is completed.) We only copy the necessary
  // members to carry out the deletion. In particular, we don't need
  // the atomic members.
  ClockHandle(const ClockHandle& other) { *this = other; }

  void operator=(const ClockHandle& other) {
    value = other.value;
    deleter = other.deleter;
    key_data = other.key_data;
    hash = other.hash;
    total_charge = other.total_charge;
  }

  Slice key() const { return Slice(key_data.data(), kCacheKeySize); }

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

  // flags functions.

  bool IsElement() const { return flags & Flags::IS_ELEMENT; }

  void SetIsElement(bool is_element) {
    if (is_element) {
      flags |= Flags::IS_ELEMENT;
    } else {
      flags &= static_cast<uint8_t>(~Flags::IS_ELEMENT);
    }
  }

  bool HasHit() const { return flags & HAS_HIT; }

  void SetHit() { flags |= HAS_HIT; }

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

  bool IsInClock() const {
    return GetClockPriority() != ClockHandle::ClockPriority::NONE;
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

  bool IsDetached() { return detached; }

  void SetDetached() { detached = true; }

  inline bool IsEmpty() const {
    return !this->IsElement() && this->displacements == 0;
  }

  inline bool IsTombstone() const {
    return !this->IsElement() && this->displacements > 0;
  }

  inline bool Matches(const Slice& some_key, uint32_t some_hash) const {
    return this->hash == some_hash && this->key() == some_key;
  }

  // refs functions.

  inline bool WillBeDeleted() const { return refs & WILL_BE_DELETED; }

  void SetWillBeDeleted(bool will_be_deleted) {
    if (will_be_deleted) {
      refs |= WILL_BE_DELETED;
    } else {
      refs &= ~WILL_BE_DELETED;
    }
  }

  uint32_t ExternalRefs() const {
    return (refs & EXTERNAL_REFS) >> kExternalRefsOffset;
  }

  // Tries to take an internal ref. Returns true iff it succeeds.
  inline bool TryInternalRef() {
    if (!((refs += kOneInternalRef) & (EXCLUSIVE_REF | WILL_BE_DELETED))) {
      return true;
    }
    refs -= kOneInternalRef;
    return false;
  }

  // Tries to take an external ref. Returns true iff it succeeds.
  inline bool TryExternalRef() {
    if (!((refs += kOneExternalRef) & EXCLUSIVE_REF)) {
      return true;
    }
    refs -= kOneExternalRef;
    return false;
  }

  // Tries to take an exclusive ref. Returns true iff it succeeds.
  // TODO(Guido) After every TryExclusiveRef call, we always call
  // WillBeDeleted(). We could save an atomic read by having an output parameter
  // with the last value of refs.
  inline bool TryExclusiveRef() {
    uint32_t will_be_deleted = refs & WILL_BE_DELETED;
    uint32_t expected = will_be_deleted;
    return refs.compare_exchange_strong(expected,
                                        EXCLUSIVE_REF | will_be_deleted);
  }

  // Repeatedly tries to take an exclusive reference, but aborts as soon
  // as an external or exclusive reference is detected (since the wait
  // would presumably be too long).
  inline bool SpinTryExclusiveRef() {
    uint32_t expected = 0;
    uint32_t will_be_deleted = 0;
    uint32_t spins = kSpinsPerTry;
    while (!refs.compare_exchange_strong(expected,
                                         EXCLUSIVE_REF | will_be_deleted) &&
           spins--) {
      std::this_thread::yield();
      if (expected & (EXTERNAL_REFS | EXCLUSIVE_REF)) {
        return false;
      }
      will_be_deleted = expected & WILL_BE_DELETED;
      expected = will_be_deleted;
    }
    return true;
  }

  // Take an external ref, assuming there is already one external ref
  // to the handle.
  void Ref() {
    // TODO(Guido) Is it okay to assume that the existing external reference
    // survives until this function returns?
    refs += kOneExternalRef;
  }

  inline void ReleaseExternalRef() { refs -= kOneExternalRef; }

  inline void ReleaseInternalRef() { refs -= kOneInternalRef; }

  inline void ReleaseExclusiveRef() { refs.fetch_and(~EXCLUSIVE_REF); }

  // Downgrade an exclusive ref to external.
  inline void ExclusiveToExternalRef() {
    refs += kOneExternalRef;
    ReleaseExclusiveRef();
  }

  // Convert an internal ref into external.
  inline void InternalToExternalRef() {
    refs += kOneExternalRef - kOneInternalRef;
  }

};  // struct ClockHandle

class ClockHandleTable {
 public:
  explicit ClockHandleTable(size_t capacity, int hash_bits);
  ~ClockHandleTable();

  // Returns a pointer to a visible handle matching the key/hash, or
  // nullptr if not present. When an actual handle is produced, an
  // internal reference is handed over.
  ClockHandle* Lookup(const Slice& key, uint32_t hash);

  // Inserts a copy of h into the hash table. Returns a pointer to the
  // inserted handle, or nullptr if no available slot was found. Every
  // existing visible handle matching the key is already present in the
  // hash table is marked as WILL_BE_DELETED. The deletion is also attempted,
  // and, if the attempt is successful, the handle is inserted into the
  // autovector deleted. When take_reference is true, the function hands
  // over an external reference on the handle, and otherwise no reference is
  // produced.
  ClockHandle* Insert(ClockHandle* h, autovector<ClockHandle>* deleted,
                      bool take_reference);

  // Assigns h the appropriate clock priority, making it evictable.
  void ClockOn(ClockHandle* h);

  // Makes h non-evictable.
  void ClockOff(ClockHandle* h);

  // Runs the clock eviction algorithm until usage_ + charge is at most
  // capacity_.
  void ClockRun(size_t charge);

  // Remove h from the hash table. Requires an exclusive ref to h.
  void Remove(ClockHandle* h, autovector<ClockHandle>* deleted);

  // Remove from the hash table all handles with matching key/hash along a
  // probe sequence, starting from the given probe number. Doesn't
  // require any references.
  void RemoveAll(const Slice& key, uint32_t hash, uint32_t& probe,
                 autovector<ClockHandle>* deleted);

  void RemoveAll(const Slice& key, uint32_t hash,
                 autovector<ClockHandle>* deleted) {
    uint32_t probe = 0;
    RemoveAll(key, hash, probe, deleted);
  }

  // Tries to remove h from the hash table. If the attempt is successful,
  // the function hands over an exclusive ref to h.
  bool TryRemove(ClockHandle* h, autovector<ClockHandle>* deleted);

  // Similar to TryRemove, except that it spins, increasing the chances of
  // success. Requires that the caller thread has no shared ref to h.
  bool SpinTryRemove(ClockHandle* h, autovector<ClockHandle>* deleted);

  // Call this function after an Insert, Remove, RemoveAll, TryRemove
  // or SpinTryRemove. It frees the deleted values and updates the hash table
  // metadata.
  void Free(autovector<ClockHandle>* deleted);

  void ApplyToEntriesRange(std::function<void(ClockHandle*)> func,
                           uint32_t index_begin, uint32_t index_end,
                           bool apply_if_will_be_deleted) {
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

  void ConstApplyToEntriesRange(std::function<void(const ClockHandle*)> func,
                                uint32_t index_begin, uint32_t index_end,
                                bool apply_if_will_be_deleted) const {
    for (uint32_t i = index_begin; i < index_end; i++) {
      ClockHandle* h = &array_[i];
      // We take an external ref because we are handing over control
      // to a user-defined function, and because the handle will not be
      // modified.
      if (h->TryExternalRef()) {
        if (h->IsElement() &&
            (apply_if_will_be_deleted || !h->WillBeDeleted())) {
          func(h);
        }
        h->ReleaseExternalRef();
      }
    }
  }

  uint32_t GetTableSize() const { return uint32_t{1} << length_bits_; }

  int GetLengthBits() const { return length_bits_; }

  uint32_t GetOccupancyLimit() const { return occupancy_limit_; }

  uint32_t GetOccupancy() const { return occupancy_; }

  size_t GetUsage() const { return usage_; }

  size_t GetCapacity() const { return capacity_; }

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Returns x mod 2^{length_bits_}.
  uint32_t ModTableSize(uint32_t x) { return x & length_bits_mask_; }

 private:
  // Extracts the element information from a handle (src), and assigns it
  // to a hash table slot (dst). Doesn't touch displacements and refs,
  // which are maintained by the hash table algorithm.
  void Assign(ClockHandle* dst, ClockHandle* src);

  // Returns the first slot in the probe sequence, starting from the given
  // probe number, with a handle e such that match(e) is true. At every
  // step, the function first tests whether match(e) holds. If this is false,
  // it evaluates abort(e) to decide whether the search should be aborted,
  // and in the affirmative returns -1. For every handle e probed except
  // the last one, the function runs update(e).
  // The probe parameter is modified as follows. We say a probe to a handle
  // e is aborting if match(e) is false and abort(e) is true. Then the final
  // value of probe is one more than the last non-aborting probe during the
  // call. This is so that that the variable can be used to keep track of
  // progress across consecutive calls to FindSlot.
  inline ClockHandle* FindSlot(const Slice& key,
                               std::function<bool(ClockHandle*)> match,
                               std::function<bool(ClockHandle*)> stop,
                               std::function<void(ClockHandle*)> update,
                               uint32_t& probe);

  // Returns an available slot for the given key. All copies of the
  // key found along the probing sequence until an available slot is
  // found are marked for deletion. On each of them, a deletion is
  // attempted, and when the attempt succeeds the slot is assigned to
  // the new copy of the element.
  ClockHandle* FindAvailableSlot(const Slice& key, uint32_t hash,
                                 uint32_t& probe,
                                 autovector<ClockHandle>* deleted);

  // After a failed FindSlot call (i.e., with answer -1) in
  // FindAvailableSlot, this function fixes all displacements's
  // starting from the 0-th probe, until the given probe.
  void Rollback(const Slice& key, uint32_t probe);

  // Number of hash bits used for table index.
  // The size of the table is 1 << length_bits_.
  const int length_bits_;

  // For faster computation of ModTableSize.
  const uint32_t length_bits_mask_;

  // Maximum number of elements the user can store in the table.
  const uint32_t occupancy_limit_;

  // Maximum total charge of all elements stored in the table.
  size_t capacity_;

  // We partition the following members into different cache lines
  // to avoid false sharing among Lookup, Release, Erase and Insert
  // operations in ClockCacheShard.

  ALIGN_AS(CACHE_LINE_SIZE)
  // Array of slots comprising the hash table.
  std::unique_ptr<ClockHandle[]> array_;

  ALIGN_AS(CACHE_LINE_SIZE)
  // Clock algorithm sweep pointer.
  std::atomic<uint32_t> clock_pointer_;

  ALIGN_AS(CACHE_LINE_SIZE)
  // Number of elements in the table.
  std::atomic<uint32_t> occupancy_;

  // Memory size for entries residing in the cache.
  std::atomic<size_t> usage_;
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

  std::string GetPrintableOptions() const override { return std::string{}; }

 private:
  friend class ClockCache;
  friend class ClockCacheTest;

  ClockHandle* DetachedInsert(ClockHandle* h);

  // Returns the charge of a single handle.
  static size_t CalcEstimatedHandleCharge(
      size_t estimated_value_size,
      CacheMetadataChargePolicy metadata_charge_policy);

  // Returns the number of bits used to hash an element in the hash
  // table.
  static int CalcHashBits(size_t capacity, size_t estimated_value_size,
                          CacheMetadataChargePolicy metadata_charge_policy);

  // Whether to reject insertion if cache reaches its full capacity.
  std::atomic<bool> strict_capacity_limit_;

  // Handles allocated separately from the table.
  std::atomic<size_t> detached_usage_;

  ClockHandleTable table_;
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

  int num_shards_;
};  // class ClockCache

}  // namespace clock_cache

// Only for internal testing, temporarily replacing NewClockCache.
// TODO(Guido) Remove once NewClockCache constructs a ClockCache again.
extern std::shared_ptr<Cache> ExperimentalNewClockCache(
    size_t capacity, size_t estimated_value_size, int num_shard_bits,
    bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy);

}  // namespace ROCKSDB_NAMESPACE
