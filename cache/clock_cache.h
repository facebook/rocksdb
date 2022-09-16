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
#include <cstddef>
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

namespace hyper_clock_cache {

// Forward declaration of friend class.
class ClockCacheTest;

// HyperClockCache is an experimental alternative to LRUCache.
//
// Benefits
// --------
// * Fully lock free (no waits or spins) for efficiency under high concurrency
// * Optimized for hot path reads. For concurrency control, most Lookup() and
// essentially all Release() are a single atomic add operation.
// * Eviction on insertion is fully parallel and lock-free.
// * Uses a generalized + aging variant of CLOCK eviction that might outperform
// LRU in some cases. (For background, see
// https://en.wikipedia.org/wiki/Page_replacement_algorithm)
//
// Costs
// -----
// * Hash table is not resizable (for lock-free efficiency) so capacity is not
// dynamically changeable. Rely on an estimated average value (block) size for
// space+time efficiency. (See estimated_entry_charge option details.)
// * Insert usually does not (but might) overwrite a previous entry associated
// with a cache key. This is OK for RocksDB uses of Cache.
// * Only supports keys of exactly 16 bytes, which is what RocksDB uses for
// block cache (not row cache or table cache).
// * SecondaryCache is not supported.
// * Cache priorities are less aggressively enforced. Unlike LRUCache, enough
// transient LOW or BOTTOM priority items can evict HIGH priority entries that
// are not referenced recently (or often) enough.
// * If pinned entries leave little or nothing eligible for eviction,
// performance can degrade substantially, because of clock eviction eating
// CPU looking for evictable entries and because Release does not
// pro-actively delete unreferenced entries when the cache is over-full.
// Specifically, this makes this implementation more susceptible to the
// following combination:
//   * num_shard_bits is high (e.g. 6)
//   * capacity small (e.g. some MBs)
//   * some large individual entries (e.g. non-partitioned filters)
// where individual entries occupy a large portion of their shard capacity.
// This should be mostly mitigated by the implementation picking a lower
// number of cache shards than LRUCache for a given capacity (when
// num_shard_bits is not overridden; see calls to GetDefaultCacheShardBits()).
// * With strict_capacity_limit=false, respecting the capacity limit is not as
// aggressive as LRUCache. The limit might be transiently exceeded by a very
// small number of entries even when not strictly necessary, and slower to
// recover after pinning forces limit to be substantially exceeded. (Even with
// strict_capacity_limit=true, RocksDB will nevertheless transiently allocate
// memory before discovering it is over the block cache capacity, so this
// should not be a detectable regression in respecting memory limits, except
// on exceptionally small caches.)
// * In some cases, erased or duplicated entries might not be freed
// immediately. They will eventually be freed by eviction from further Inserts.
// * Internal metadata can overflow if the number of simultaneous references
// to a cache handle reaches many millions.
//
// High-level eviction algorithm
// -----------------------------
// A score (or "countdown") is maintained for each entry, initially determined
// by priority. The score is incremented on each Lookup, up to a max of 3,
// though is easily returned to previous state if useful=false with Release.
// During CLOCK-style eviction iteration, entries with score > 0 are
// decremented if currently unreferenced and entries with score == 0 are
// evicted if currently unreferenced. Note that scoring might not be perfect
// because entries can be referenced transiently within the cache even when
// there are no outside references to the entry.
//
// Cache sharding like LRUCache is used to reduce contention on usage+eviction
// state, though here the performance improvement from more shards is small,
// and (as noted above) potentially detrimental if shard capacity is too close
// to largest entry size. Here cache sharding mostly only affects cache update
// (Insert / Erase) performance, not read performance.
//
// Read efficiency (hot path)
// --------------------------
// Mostly to minimize the cost of accessing metadata blocks with
// cache_index_and_filter_blocks=true, we focus on optimizing Lookup and
// Release. In terms of concurrency, at a minimum, these operations have
// to do reference counting (and Lookup has to compare full keys in a safe
// way). Can we fold in all the other metadata tracking *for free* with
// Lookup and Release doing a simple atomic fetch_add/fetch_sub? (Assume
// for the moment that Lookup succeeds on the first probe.)
//
// We have a clever way of encoding an entry's reference count and countdown
// clock so that Lookup and Release are each usually a single atomic addition.
// In a single metadata word we have both an "acquire" count, incremented by
// Lookup, and a "release" count, incremented by Release. If useful=false,
// Release can instead decrement the acquire count. Thus the current ref
// count is (acquires - releases), and the countdown clock is min(3, acquires).
// Note that only unreferenced entries (acquires == releases) are eligible
// for CLOCK manipulation and eviction. We tolerate use of more expensive
// compare_exchange operations for cache writes (insertions and erasures).
//
// In a cache receiving many reads and little or no writes, it is possible
// for the acquire and release counters to overflow. Assuming the *current*
// refcount never reaches to many millions, we only have to correct for
// overflow in both counters in Release, not in Lookup. The overflow check
// should be only 1-2 CPU cycles per Release because it is a predictable
// branch on a simple condition on data already in registers.
//
// Slot states
// -----------
// We encode a state indicator into the same metadata word with the
// acquire and release counters. This allows bigger state transitions to
// be atomic. States:
//
// * Empty - slot is not in use and unowned. All other metadata and data is
// in an undefined state.
// * Construction - slot is exclusively owned by one thread, the thread
// successfully entering this state, for populating or freeing data.
// * Shareable (group) - slot holds an entry with counted references for
// pinning and reading, including
//   * Visible - slot holds an entry that can be returned by Lookup
//   * Invisible - slot holds an entry that is not visible to Lookup
//     (erased by user) but can be read by existing references, and ref count
//     changed by Ref and Release.
//
// A special case is "detached" entries, which are heap-allocated handles
// not in the table. They are always Invisible and freed on zero refs.
//
// State transitions:
// Empty -> Construction (in Insert): The encoding of state enables Insert to
// perform an optimistic atomic bitwise-or to take ownership if a slot is
// empty, or otherwise make no state change.
//
// Construction -> Visible (in Insert): This can be a simple assignment to the
// metadata word because the current thread has exclusive ownership and other
// metadata is meaningless.
//
// Visible -> Invisible (in Erase): This can be a bitwise-and while holding
// a shared reference, which is safe because the change is idempotent (in case
// of parallel Erase). By the way, we never go Invisible->Visible.
//
// Shareable -> Construction (in Evict part of Insert, in Erase, and in
// Release if Invisible): This is for starting to freeing/deleting an
// unreferenced entry. We have to use compare_exchange to ensure we only make
// this transition when there are zero refs.
//
// Construction -> Empty (in same places): This is for completing free/delete
// of an entry. A "release" atomic store suffices, as we have exclusive
// ownership of the slot but have to ensure none of the data member reads are
// re-ordered after committing the state transition.
//
// Insert
// ------
// If Insert were to guarantee replacing an existing entry for a key, there
// would be complications for concurrency and efficiency. First, consider how
// many probes to get to an entry. To ensure Lookup never waits and
// availability of a key is uninterrupted, we would need to use a different
// slot for a new entry for the same key. This means it is most likely in a
// later probing position than the old version, which should soon be removed.
// (Also, an entry is too big to replace atomically, even if no current refs.)
//
// However, overwrite capability is not really needed by RocksDB. Also, we
// know from our "redundant" stats that overwrites are very rare for the block
// cache, so we should not spend much to make them effective.
//
// So instead we Insert as soon as we find an empty slot in the probing
// sequence without seeing an existing (visible) entry for the same key. This
// way we only insert if we can improve the probing performance, and we don't
// need to probe beyond our insert position, assuming we are willing to let
// the previous entry for the same key die of old age (eventual eviction from
// not being used). We can reach a similar state with concurrent insertions,
// where one will pass over the other while it is "under construction."
// This temporary duplication is acceptable for RocksDB block cache because
// we know redundant insertion is rare.
//
// Another problem to solve is what to return to the caller when we find an
// existing entry whose probing position we cannot improve on, or when the
// table occupancy limit has been reached. If strict_capacity_limit=false,
// we must never fail Insert, and if a Handle* is provided, we have to return
// a usable Cache handle on success. The solution to this (typically rare)
// problem is "detached" handles, which are usable by the caller but not
// actually available for Lookup in the Cache. Detached handles are allocated
// independently on the heap and specially marked so that they are freed on
// the heap when their last reference is released.
//
// Usage on capacity
// -----------------
// Insert takes different approaches to usage tracking depending on
// strict_capacity_limit setting. If true, we enforce a kind of strong
// consistency where compare-exchange is used to ensure the usage number never
// exceeds its limit, and provide threads with an authoritative signal on how
// much "usage" they have taken ownership of. With strict_capacity_limit=false,
// we use a kind of "eventual consistency" where all threads Inserting to the
// same cache shard might race on reserving the same space, but the
// over-commitment will be worked out in later insertions. It is kind of a
// dance because we don't want threads racing each other too much on paying
// down the over-commitment (with eviction) either.
//
// Eviction
// --------
// A key part of Insert is evicting some entries currently unreferenced to
// make room for new entries. The high-level eviction algorithm is described
// above, but the details are also interesting. A key part is parallelizing
// eviction with a single CLOCK pointer. This works by each thread working on
// eviction pre-emptively incrementing the CLOCK pointer, and then CLOCK-
// updating or evicting the incremented-over slot(s). To reduce contention at
// the cost of possibly evicting too much, each thread increments the clock
// pointer by 4, so commits to updating at least 4 slots per batch. As
// described above, a CLOCK update will decrement the "countdown" of
// unreferenced entries, or evict unreferenced entries with zero countdown.
// Referenced entries are not updated, because we (presumably) don't want
// long-referenced entries to age while referenced. Note however that we
// cannot distinguish transiently referenced entries from cache user
// references, so some CLOCK updates might be somewhat arbitrarily skipped.
// This is OK as long as it is rare enough that eviction order is still
// pretty good.
//
// There is no synchronization on the completion of the CLOCK updates, so it
// is theoretically possible for another thread to cycle back around and have
// two threads racing on CLOCK updates to the same slot. Thus, we cannot rely
// on any implied exclusivity to make the updates or eviction more efficient.
// These updates use an opportunistic compare-exchange (no loop), where a
// racing thread might cause the update to be skipped without retry, but in
// such case the update is likely not needed because the most likely update
// to an entry is that it has become referenced. (TODO: test efficiency of
// avoiding compare-exchange loop)
//
// Release
// -------
// In the common case, Release is a simple atomic increment of the release
// counter. There is a simple overflow check that only does another atomic
// update in extremely rare cases, so costs almost nothing.
//
// If the Release specifies "not useful", we can instead decrement the
// acquire counter, which returns to the same CLOCK state as before Lookup
// or Ref.
//
// Adding a check for over-full cache on every release to zero-refs would
// likely be somewhat expensive, increasing read contention on cache shard
// metadata. Instead we are less aggressive about deleting entries right
// away in those cases.
//
// However Release tries to immediately delete entries reaching zero refs
// if (a) erase_if_last_ref is set by the caller, or (b) the entry is already
// marked invisible. Both of these are checks on values already in CPU
// registers so do not increase cross-CPU contention when not applicable.
// When applicable, they use a compare-exchange loop to take exclusive
// ownership of the slot for freeing the entry. These are rare cases
// that should not usually affect performance.
//
// Erase
// -----
// Searches for an entry like Lookup but moves it to Invisible state if found.
// This state transition is with bit operations so is idempotent and safely
// done while only holding a shared "read" reference. Like Release, it makes
// a best effort to immediately release an Invisible entry that reaches zero
// refs, but there are some corner cases where it will only be freed by the
// clock eviction process.

// ----------------------------------------------------------------------- //

// The load factor p is a real number in (0, 1) such that at all
// times at most a fraction p of all slots, without counting tombstones,
// are occupied by elements. This means that the probability that a random
// probe hits an occupied slot is at most p, and thus at most 1/p probes
// are required on average. For example, p = 70% implies that between 1 and 2
// probes are needed on average (bear in mind that this reasoning doesn't
// consider the effects of clustering over time, which should be negligible
// with double hashing).
// Because the size of the hash table is always rounded up to the next
// power of 2, p is really an upper bound on the actual load factor---the
// actual load factor is anywhere between p/2 and p. This is a bit wasteful,
// but bear in mind that slots only hold metadata, not actual values.
// Since space cost is dominated by the values (the LSM blocks),
// overprovisioning the table with metadata only increases the total cache space
// usage by a tiny fraction.
constexpr double kLoadFactor = 0.7;

// The user can exceed kLoadFactor if the sizes of the inserted values don't
// match estimated_value_size, or in some rare cases with
// strict_capacity_limit == false. To avoid degenerate performance, we set a
// strict upper bound on the load factor.
constexpr double kStrictLoadFactor = 0.84;

using CacheKeyBytes = std::array<char, kCacheKeySize>;

struct ClockHandleBasicData {
  void* value = nullptr;
  Cache::DeleterFn deleter = nullptr;
  CacheKeyBytes key = {};
  size_t total_charge = 0;

  Slice KeySlice() const { return Slice(key.data(), kCacheKeySize); }

  void FreeData() const {
    if (deleter) {
      (*deleter)(KeySlice(), value);
    }
  }
};

struct ClockHandleMoreData : public ClockHandleBasicData {
  uint32_t hash = 0;
};

// Target size to be exactly a common cache line size (see static_assert in
// clock_cache.cc)
struct ALIGN_AS(64U) ClockHandle : public ClockHandleMoreData {
  // Constants for handling the atomic `meta` word, which tracks most of the
  // state of the handle. The meta word looks like this:
  // low bits                                                     high bits
  // -----------------------------------------------------------------------
  // | acquire counter          | release counter           | state marker |
  // -----------------------------------------------------------------------

  // For reading or updating counters in meta word.
  static constexpr uint8_t kCounterNumBits = 30;
  static constexpr uint64_t kCounterMask = (uint64_t{1} << kCounterNumBits) - 1;

  static constexpr uint8_t kAcquireCounterShift = 0;
  static constexpr uint64_t kAcquireIncrement = uint64_t{1}
                                                << kAcquireCounterShift;
  static constexpr uint8_t kReleaseCounterShift = kCounterNumBits;
  static constexpr uint64_t kReleaseIncrement = uint64_t{1}
                                                << kReleaseCounterShift;

  // For reading or updating the state marker in meta word
  static constexpr uint8_t kStateShift = 2U * kCounterNumBits;

  // Bits contribution to state marker.
  // Occupied means any state other than empty
  static constexpr uint8_t kStateOccupiedBit = 0b100;
  // Shareable means the entry is reference counted (visible or invisible)
  // (only set if also occupied)
  static constexpr uint8_t kStateShareableBit = 0b010;
  // Visible is only set if also shareable
  static constexpr uint8_t kStateVisibleBit = 0b001;

  // Complete state markers (not shifted into full word)
  static constexpr uint8_t kStateEmpty = 0b000;
  static constexpr uint8_t kStateConstruction = kStateOccupiedBit;
  static constexpr uint8_t kStateInvisible =
      kStateOccupiedBit | kStateShareableBit;
  static constexpr uint8_t kStateVisible =
      kStateOccupiedBit | kStateShareableBit | kStateVisibleBit;

  // Constants for initializing the countdown clock. (Countdown clock is only
  // in effect with zero refs, acquire counter == release counter, and in that
  // case the countdown clock == both of those counters.)
  static constexpr uint8_t kHighCountdown = 3;
  static constexpr uint8_t kLowCountdown = 2;
  static constexpr uint8_t kBottomCountdown = 1;
  // During clock update, treat any countdown clock value greater than this
  // value the same as this value.
  static constexpr uint8_t kMaxCountdown = kHighCountdown;
  // TODO: make these coundown values tuning parameters for eviction?

  // See above
  std::atomic<uint64_t> meta{};
  // The number of elements that hash to this slot or a lower one, but wind
  // up in this slot or a higher one.
  std::atomic<uint32_t> displacements{};

  // True iff the handle is allocated separately from hash table.
  bool detached = false;
};  // struct ClockHandle

class ClockHandleTable {
 public:
  explicit ClockHandleTable(int hash_bits, bool initial_charge_metadata);
  ~ClockHandleTable();

  Status Insert(const ClockHandleMoreData& proto, ClockHandle** handle,
                Cache::Priority priority, size_t capacity,
                bool strict_capacity_limit);

  ClockHandle* Lookup(const CacheKeyBytes& key, uint32_t hash);

  bool Release(ClockHandle* handle, bool useful, bool erase_if_last_ref);

  void Ref(ClockHandle& handle);

  void Erase(const CacheKeyBytes& key, uint32_t hash);

  void ConstApplyToEntriesRange(std::function<void(const ClockHandle&)> func,
                                uint32_t index_begin, uint32_t index_end,
                                bool apply_if_will_be_deleted) const;

  void EraseUnRefEntries();

  uint32_t GetTableSize() const { return uint32_t{1} << length_bits_; }

  int GetLengthBits() const { return length_bits_; }

  uint32_t GetOccupancyLimit() const { return occupancy_limit_; }

  uint32_t GetOccupancy() const {
    return occupancy_.load(std::memory_order_relaxed);
  }

  size_t GetUsage() const { return usage_.load(std::memory_order_relaxed); }

  size_t GetDetachedUsage() const {
    return detached_usage_.load(std::memory_order_relaxed);
  }

  // Acquire/release N references
  void TEST_RefN(ClockHandle& handle, size_t n);
  void TEST_ReleaseN(ClockHandle* handle, size_t n);

 private:  // functions
  // Returns x mod 2^{length_bits_}.
  uint32_t ModTableSize(uint32_t x) { return x & length_bits_mask_; }

  // Runs the clock eviction algorithm trying to reclaim at least
  // requested_charge. Returns how much is evicted, which could be less
  // if it appears impossible to evict the requested amount without blocking.
  void Evict(size_t requested_charge, size_t* freed_charge,
             uint32_t* freed_count);

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
  inline ClockHandle* FindSlot(uint32_t hash,
                               std::function<bool(ClockHandle*)> match,
                               std::function<bool(ClockHandle*)> stop,
                               std::function<void(ClockHandle*)> update,
                               uint32_t& probe);

  // Re-decrement all displacements in probe path starting from beginning
  // until (not including) the given handle
  void Rollback(uint32_t hash, const ClockHandle* h);

 private:  // data
  // Number of hash bits used for table index.
  // The size of the table is 1 << length_bits_.
  const int length_bits_;

  // For faster computation of ModTableSize.
  const uint32_t length_bits_mask_;

  // Maximum number of elements the user can store in the table.
  const uint32_t occupancy_limit_;

  // Array of slots comprising the hash table.
  const std::unique_ptr<ClockHandle[]> array_;

  // We partition the following members into different cache lines
  // to avoid false sharing among Lookup, Release, Erase and Insert
  // operations in ClockCacheShard.

  ALIGN_AS(CACHE_LINE_SIZE)
  // Clock algorithm sweep pointer.
  std::atomic<uint64_t> clock_pointer_{};

  ALIGN_AS(CACHE_LINE_SIZE)
  // Number of elements in the table.
  std::atomic<uint32_t> occupancy_{};

  // Memory usage by entries tracked by the cache (including detached)
  std::atomic<size_t> usage_{};

  // Part of usage by detached entries (not in table)
  std::atomic<size_t> detached_usage_{};
};  // class ClockHandleTable

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) ClockCacheShard final : public CacheShard {
 public:
  ClockCacheShard(size_t capacity, size_t estimated_value_size,
                  bool strict_capacity_limit,
                  CacheMetadataChargePolicy metadata_charge_policy);
  ~ClockCacheShard() override = default;

  // TODO: document limitations
  void SetCapacity(size_t capacity) override;

  void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  Status Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
                Cache::DeleterFn deleter, Cache::Handle** handle,
                Cache::Priority priority) override;

  Cache::Handle* Lookup(const Slice& key, uint32_t hash) override;

  bool Release(Cache::Handle* handle, bool useful,
               bool erase_if_last_ref) override;

  bool Release(Cache::Handle* handle, bool erase_if_last_ref = false) override;

  bool Ref(Cache::Handle* handle) override;

  void Erase(const Slice& key, uint32_t hash) override;

  size_t GetUsage() const override;

  size_t GetPinnedUsage() const override;

  size_t GetOccupancyCount() const override;

  size_t GetTableAddressCount() const override;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      uint32_t average_entries_per_lock, uint32_t* state) override;

  void EraseUnRefEntries() override;

  std::string GetPrintableOptions() const override { return std::string{}; }

  // SecondaryCache not yet supported
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

  bool IsReady(Cache::Handle* /*handle*/) override { return true; }

  void Wait(Cache::Handle* /*handle*/) override {}

  // Acquire/release N references
  void TEST_RefN(Cache::Handle* handle, size_t n);
  void TEST_ReleaseN(Cache::Handle* handle, size_t n);

 private:  // functions
  friend class ClockCache;
  friend class ClockCacheTest;

  ClockHandle* DetachedInsert(const ClockHandleMoreData& h);

  // Returns the number of bits used to hash an element in the hash
  // table.
  static int CalcHashBits(size_t capacity, size_t estimated_value_size,
                          CacheMetadataChargePolicy metadata_charge_policy);

 private:  // data
  ClockHandleTable table_;

  // Maximum total charge of all elements stored in the table.
  std::atomic<size_t> capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  std::atomic<bool> strict_capacity_limit_;
};  // class ClockCacheShard

class HyperClockCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache {
 public:
  HyperClockCache(size_t capacity, size_t estimated_value_size,
                  int num_shard_bits, bool strict_capacity_limit,
                  CacheMetadataChargePolicy metadata_charge_policy =
                      kDontChargeCacheMetadata);

  ~HyperClockCache() override;

  const char* Name() const override { return "HyperClockCache"; }

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
};  // class HyperClockCache

}  // namespace hyper_clock_cache

}  // namespace ROCKSDB_NAMESPACE
