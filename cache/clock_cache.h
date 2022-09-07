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

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

// Forward declaration of friend class.
class ClockCacheTest;

// ClockCache is an experimental alternative to LRUCache.
//
// Benefits
// --------
// * Fully lock free (no waits or spins) for efficiency under high concurrency
// * Optimized for hot path reads. For concurrency control, most Lookup() and
// essentially all Release() are a single atomic add operation.
// * Uses a generalized + aging variant of CLOCK eviction that outperforms LRU
// at least on some RocksDB benchmarks.
// * Eviction on insertion is fully parallel and lock-free.
//
// Costs
// -----
// * Hash table is not resizable (for lock-free efficiency) so capacity is not
// dynamically changeable. Rely on an estimated average value (block) size for
// space+time efficiency.
// * With strict_capacity_limit=false, respecting the capacity limit is not as
// aggressive as LRUCache. The limit might be transiently exceeded by a very
// small number of entries even when not strictly necessary, and slower to
// recover after pinning forces limit to be substantially exceeded. (Even with
// strict_capacity_limit=true, RocksDB will nevertheless transiently allocate
// memory before discovering it is over the block cache capacity, so this
// should not be a detectable regression in respecting memory limits, except
// on exceptionally small caches.)
// * More susceptible to degraded performance with combination
//   * num_shard_bits is high (e.g. 6)
//   * capacity small (e.g. some MBs)
//   * some large individual entries (e.g. non-partitioned filters)
// where individual entries occupy a large portion of their shard capacity.
// * In some rare cases, erased or duplicated entries might not be freed
// immediately. They will eventually be freed by eviction from further Inserts.
// * SecondaryCache is not supported.
// * Insert usually does not (but might) overwrite a previous entry associated
// with a cache key. This is OK for RocksDB uses of Cache.
// * Only supports keys of exactly 16 bytes, which is what RocksDB uses.
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
// TODO
//
//
// Erase/Release
// -------------
// TODO
//

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
constexpr double kLoadFactor = 0.35;

// The user can exceed kLoadFactor if the sizes of the inserted values don't
// match estimated_value_size, or in some rare cases with
// strict_capacity_limit == false. To avoid degenerate performance, we set a
// strict upper bound on the load factor.
constexpr double kStrictLoadFactor = 0.7;

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
  static constexpr uint8_t kCounterNumBits = 29;
  static constexpr uint64_t kCounterMask = (uint64_t{1} << kCounterNumBits) - 1;
  static constexpr uint8_t kCounterWithOverflowNumBits = kCounterNumBits + 1;
  static constexpr uint64_t kCounterWithOverflowMask =
      (uint64_t{1} << kCounterWithOverflowNumBits) - 1;

  static constexpr uint8_t kAcquireCounterShift = 0;
  static constexpr uint64_t kAcquireIncrement = uint64_t{1}
                                                << kAcquireCounterShift;
  static constexpr uint8_t kReleaseCounterShift = kCounterWithOverflowNumBits;
  static constexpr uint64_t kReleaseIncrement = uint64_t{1}
                                                << kReleaseCounterShift;

  static constexpr uint8_t kStateShift = 2U * kCounterWithOverflowNumBits;

  static constexpr uint8_t kStateOccupiedBit = 0b100;
  static constexpr uint8_t kStateSharableBit = 0b010;
  static constexpr uint8_t kStateVisibleBit = 0b001;

  static constexpr uint8_t kStateEmpty = 0b000;
  static constexpr uint8_t kStateConstruction = kStateOccupiedBit;
  static constexpr uint8_t kStateInvisible =
      kStateOccupiedBit | kStateSharableBit;
  static constexpr uint8_t kStateVisible =
      kStateOccupiedBit | kStateSharableBit | kStateVisibleBit;

  static constexpr uint8_t kHighCountdown = 3;
  static constexpr uint8_t kLowCountdown = 2;
  static constexpr uint8_t kBottomCountdown = 1;
  // TODO: make a tuning parameter?
  static constexpr uint8_t kMaxCountdown = kHighCountdown;

  // TODO: doc
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

  uint32_t GetOccupancy() const { return occupancy_; }

  size_t GetUsage() const { return usage_; }

  size_t GetDetachedUsage() const {
    return detached_usage_.load(std::memory_order_relaxed);
  }

 private:  // functions
  // Returns x mod 2^{length_bits_}.
  uint32_t ModTableSize(uint32_t x) { return x & length_bits_mask_; }

  // Runs the clock eviction algorithm trying to reclaim at least
  // requested_charge. Returns how much is evicted, which could be less
  // if it appears impossible to evict the requested amount without blocking.
  size_t Evict(size_t requested_charge);

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

  // After a failed FindSlot call (i.e., with answer -1) in
  // FindAvailableSlot, this function fixes all displacements
  // starting from the 0-th probe, until the given probe.
  void Rollback(uint32_t hash, uint32_t probe);

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
  std::atomic<uint32_t> clock_pointer_{};

  ALIGN_AS(CACHE_LINE_SIZE)
  // Number of elements in the table.
  std::atomic<uint32_t> occupancy_{};

  // Memory usage by entries trtacked by the cache (including detached)
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
