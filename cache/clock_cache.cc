//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/clock_cache.h"

#include <algorithm>
#include <atomic>
#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <numeric>
#include <string>
#include <thread>
#include <type_traits>

#include "cache/cache_key.h"
#include "cache/secondary_cache_adapter.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "rocksdb/env.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

namespace {
inline uint64_t GetRefcount(uint64_t meta) {
  return ((meta >> ClockHandle::kAcquireCounterShift) -
          (meta >> ClockHandle::kReleaseCounterShift)) &
         ClockHandle::kCounterMask;
}

inline uint64_t GetInitialCountdown(Cache::Priority priority) {
  // Set initial clock data from priority
  // TODO: configuration parameters for priority handling and clock cycle
  // count?
  switch (priority) {
    case Cache::Priority::HIGH:
      return ClockHandle::kHighCountdown;
    default:
      assert(false);
      FALLTHROUGH_INTENDED;
    case Cache::Priority::LOW:
      return ClockHandle::kLowCountdown;
    case Cache::Priority::BOTTOM:
      return ClockHandle::kBottomCountdown;
  }
}

inline void MarkEmpty(ClockHandle& h) {
#ifndef NDEBUG
  // Mark slot as empty, with assertion
  uint64_t meta = h.meta.exchange(0, std::memory_order_release);
  assert(meta >> ClockHandle::kStateShift == ClockHandle::kStateConstruction);
#else
  // Mark slot as empty
  h.meta.store(0, std::memory_order_release);
#endif
}

inline void FreeDataMarkEmpty(ClockHandle& h, MemoryAllocator* allocator) {
  // NOTE: in theory there's more room for parallelism if we copy the handle
  // data and delay actions like this until after marking the entry as empty,
  // but performance tests only show a regression by copying the few words
  // of data.
  h.FreeData(allocator);

  MarkEmpty(h);
}

// Called to undo the effect of referencing an entry for internal purposes,
// so it should not be marked as having been used.
inline void Unref(const ClockHandle& h, uint64_t count = 1) {
  // Pretend we never took the reference
  // WART: there's a tiny chance we release last ref to invisible
  // entry here. If that happens, we let eviction take care of it.
  uint64_t old_meta = h.meta.fetch_sub(ClockHandle::kAcquireIncrement * count,
                                       std::memory_order_release);
  assert(GetRefcount(old_meta) != 0);
  (void)old_meta;
}

inline bool ClockUpdate(ClockHandle& h) {
  uint64_t meta = h.meta.load(std::memory_order_relaxed);

  uint64_t acquire_count =
      (meta >> ClockHandle::kAcquireCounterShift) & ClockHandle::kCounterMask;
  uint64_t release_count =
      (meta >> ClockHandle::kReleaseCounterShift) & ClockHandle::kCounterMask;
  if (acquire_count != release_count) {
    // Only clock update entries with no outstanding refs
    return false;
  }
  if (!((meta >> ClockHandle::kStateShift) & ClockHandle::kStateShareableBit)) {
    // Only clock update Shareable entries
    return false;
  }
  if ((meta >> ClockHandle::kStateShift == ClockHandle::kStateVisible) &&
      acquire_count > 0) {
    // Decrement clock
    uint64_t new_count =
        std::min(acquire_count - 1, uint64_t{ClockHandle::kMaxCountdown} - 1);
    // Compare-exchange in the decremented clock info, but
    // not aggressively
    uint64_t new_meta =
        (uint64_t{ClockHandle::kStateVisible} << ClockHandle::kStateShift) |
        (meta & ClockHandle::kHitBitMask) |
        (new_count << ClockHandle::kReleaseCounterShift) |
        (new_count << ClockHandle::kAcquireCounterShift);
    h.meta.compare_exchange_strong(meta, new_meta, std::memory_order_relaxed);
    return false;
  }
  // Otherwise, remove entry (either unreferenced invisible or
  // unreferenced and expired visible).
  if (h.meta.compare_exchange_strong(meta,
                                     (uint64_t{ClockHandle::kStateConstruction}
                                      << ClockHandle::kStateShift) |
                                         (meta & ClockHandle::kHitBitMask),
                                     std::memory_order_acquire)) {
    // Took ownership.
    return true;
  } else {
    // Compare-exchange failing probably
    // indicates the entry was used, so skip it in that case.
    return false;
  }
}

// If an entry doesn't receive clock updates but is repeatedly referenced &
// released, the acquire and release counters could overflow without some
// intervention. This is that intervention, which should be inexpensive
// because it only incurs a simple, very predictable check. (Applying a bit
// mask in addition to an increment to every Release likely would be
// relatively expensive, because it's an extra atomic update.)
//
// We do have to assume that we never have many millions of simultaneous
// references to a cache handle, because we cannot represent so many
// references with the difference in counters, masked to the number of
// counter bits. Similarly, we assume there aren't millions of threads
// holding transient references (which might be "undone" rather than
// released by the way).
//
// Consider these possible states for each counter:
// low: less than kMaxCountdown
// medium: kMaxCountdown to half way to overflow + kMaxCountdown
// high: half way to overflow + kMaxCountdown, or greater
//
// And these possible states for the combination of counters:
// acquire / release
// -------   -------
// low       low       - Normal / common, with caveats (see below)
// medium    low       - Can happen while holding some refs
// high      low       - Violates assumptions (too many refs)
// low       medium    - Violates assumptions (refs underflow, etc.)
// medium    medium    - Normal (very read heavy cache)
// high      medium    - Can happen while holding some refs
// low       high      - This function is supposed to prevent
// medium    high      - Violates assumptions (refs underflow, etc.)
// high      high      - Needs CorrectNearOverflow
//
// Basically, this function detects (high, high) state (inferred from
// release alone being high) and bumps it back down to (medium, medium)
// state with the same refcount and the same logical countdown counter
// (everything > kMaxCountdown is logically the same). Note that bumping
// down to (low, low) would modify the countdown counter, so is "reserved"
// in a sense.
//
// If near-overflow correction is triggered here, there's no guarantee
// that another thread hasn't freed the entry and replaced it with another.
// Therefore, it must be the case that the correction does not affect
// entries unless they are very old (many millions of acquire-release cycles).
// (Our bit manipulation is indeed idempotent and only affects entries in
// exceptional cases.) We assume a pre-empted thread will not stall that long.
// If it did, the state could be corrupted in the (unlikely) case that the top
// bit of the acquire counter is set but not the release counter, and thus
// we only clear the top bit of the acquire counter on resumption. It would
// then appear that there are too many refs and the entry would be permanently
// pinned (which is not terrible for an exceptionally rare occurrence), unless
// it is referenced enough (at least kMaxCountdown more times) for the release
// counter to reach "high" state again and bumped back to "medium." (This
// motivates only checking for release counter in high state, not both in high
// state.)
inline void CorrectNearOverflow(uint64_t old_meta,
                                std::atomic<uint64_t>& meta) {
  // We clear both top-most counter bits at the same time.
  constexpr uint64_t kCounterTopBit = uint64_t{1}
                                      << (ClockHandle::kCounterNumBits - 1);
  constexpr uint64_t kClearBits =
      (kCounterTopBit << ClockHandle::kAcquireCounterShift) |
      (kCounterTopBit << ClockHandle::kReleaseCounterShift);
  // A simple check that allows us to initiate clearing the top bits for
  // a large portion of the "high" state space on release counter.
  constexpr uint64_t kCheckBits =
      (kCounterTopBit | (ClockHandle::kMaxCountdown + 1))
      << ClockHandle::kReleaseCounterShift;

  if (UNLIKELY(old_meta & kCheckBits)) {
    meta.fetch_and(~kClearBits, std::memory_order_relaxed);
  }
}

inline bool BeginSlotInsert(const ClockHandleBasicData& proto, ClockHandle& h,
                            uint64_t initial_countdown, bool* already_matches) {
  assert(*already_matches == false);
  // Optimistically transition the slot from "empty" to
  // "under construction" (no effect on other states)
  uint64_t old_meta = h.meta.fetch_or(
      uint64_t{ClockHandle::kStateOccupiedBit} << ClockHandle::kStateShift,
      std::memory_order_acq_rel);
  uint64_t old_state = old_meta >> ClockHandle::kStateShift;

  if (old_state == ClockHandle::kStateEmpty) {
    // We've started inserting into an available slot, and taken
    // ownership.
    return true;
  } else if (old_state != ClockHandle::kStateVisible) {
    // Slot not usable / touchable now
    return false;
  }
  // Existing, visible entry, which might be a match.
  // But first, we need to acquire a ref to read it. In fact, number of
  // refs for initial countdown, so that we boost the clock state if
  // this is a match.
  old_meta =
      h.meta.fetch_add(ClockHandle::kAcquireIncrement * initial_countdown,
                       std::memory_order_acq_rel);
  // Like Lookup
  if ((old_meta >> ClockHandle::kStateShift) == ClockHandle::kStateVisible) {
    // Acquired a read reference
    if (h.hashed_key == proto.hashed_key) {
      // Match. Release in a way that boosts the clock state
      old_meta =
          h.meta.fetch_add(ClockHandle::kReleaseIncrement * initial_countdown,
                           std::memory_order_acq_rel);
      // Correct for possible (but rare) overflow
      CorrectNearOverflow(old_meta, h.meta);
      // Insert detached instead (only if return handle needed)
      *already_matches = true;
      return false;
    } else {
      // Mismatch.
      Unref(h, initial_countdown);
    }
  } else if (UNLIKELY((old_meta >> ClockHandle::kStateShift) ==
                      ClockHandle::kStateInvisible)) {
    // Pretend we never took the reference
    Unref(h, initial_countdown);
  } else {
    // For other states, incrementing the acquire counter has no effect
    // so we don't need to undo it.
    // Slot not usable / touchable now.
  }
  return false;
}

inline void FinishSlotInsert(const ClockHandleBasicData& proto, ClockHandle& h,
                             uint64_t initial_countdown, bool keep_ref) {
  // Save data fields
  ClockHandleBasicData* h_alias = &h;
  *h_alias = proto;

  // Transition from "under construction" state to "visible" state
  uint64_t new_meta = uint64_t{ClockHandle::kStateVisible}
                      << ClockHandle::kStateShift;

  // Maybe with an outstanding reference
  new_meta |= initial_countdown << ClockHandle::kAcquireCounterShift;
  new_meta |= (initial_countdown - keep_ref)
              << ClockHandle::kReleaseCounterShift;

#ifndef NDEBUG
  // Save the state transition, with assertion
  uint64_t old_meta = h.meta.exchange(new_meta, std::memory_order_release);
  assert(old_meta >> ClockHandle::kStateShift ==
         ClockHandle::kStateConstruction);
#else
  // Save the state transition
  h.meta.store(new_meta, std::memory_order_release);
#endif
}

bool TryInsert(const ClockHandleBasicData& proto, ClockHandle& h,
               uint64_t initial_countdown, bool keep_ref,
               bool* already_matches) {
  bool b = BeginSlotInsert(proto, h, initial_countdown, already_matches);
  if (b) {
    FinishSlotInsert(proto, h, initial_countdown, keep_ref);
  }
  return b;
}

// Func must be const HandleImpl& -> void callable
template <class HandleImpl, class Func>
void ConstApplyToEntriesRange(const Func& func, const HandleImpl* begin,
                              const HandleImpl* end,
                              bool apply_if_will_be_deleted) {
  uint64_t check_state_mask = ClockHandle::kStateShareableBit;
  if (!apply_if_will_be_deleted) {
    check_state_mask |= ClockHandle::kStateVisibleBit;
  }

  for (const HandleImpl* h = begin; h < end; ++h) {
    // Note: to avoid using compare_exchange, we have to be extra careful.
    uint64_t old_meta = h->meta.load(std::memory_order_relaxed);
    // Check if it's an entry visible to lookups
    if ((old_meta >> ClockHandle::kStateShift) & check_state_mask) {
      // Increment acquire counter. Note: it's possible that the entry has
      // completely changed since we loaded old_meta, but incrementing acquire
      // count is always safe. (Similar to optimistic Lookup here.)
      old_meta = h->meta.fetch_add(ClockHandle::kAcquireIncrement,
                                   std::memory_order_acquire);
      // Check whether we actually acquired a reference.
      if ((old_meta >> ClockHandle::kStateShift) &
          ClockHandle::kStateShareableBit) {
        // Apply func if appropriate
        if ((old_meta >> ClockHandle::kStateShift) & check_state_mask) {
          func(*h);
        }
        // Pretend we never took the reference
        Unref(*h);
        // No net change, so don't need to check for overflow
      } else {
        // For other states, incrementing the acquire counter has no effect
        // so we don't need to undo it. Furthermore, we cannot safely undo
        // it because we did not acquire a read reference to lock the
        // entry in a Shareable state.
      }
    }
  }
}

}  // namespace

void ClockHandleBasicData::FreeData(MemoryAllocator* allocator) const {
  if (helper->del_cb) {
    helper->del_cb(value, allocator);
  }
}

template <class HandleImpl>
HandleImpl* BaseClockTable::StandaloneInsert(
    const ClockHandleBasicData& proto) {
  // Heap allocated separate from table
  HandleImpl* h = new HandleImpl();
  ClockHandleBasicData* h_alias = h;
  *h_alias = proto;
  h->SetStandalone();
  // Single reference (standalone entries only created if returning a refed
  // Handle back to user)
  uint64_t meta = uint64_t{ClockHandle::kStateInvisible}
                  << ClockHandle::kStateShift;
  meta |= uint64_t{1} << ClockHandle::kAcquireCounterShift;
  h->meta.store(meta, std::memory_order_release);
  // Keep track of how much of usage is standalone
  standalone_usage_.fetch_add(proto.GetTotalCharge(),
                              std::memory_order_relaxed);
  return h;
}

template <class Table>
typename Table::HandleImpl* BaseClockTable::CreateStandalone(
    ClockHandleBasicData& proto, size_t capacity, bool strict_capacity_limit,
    bool allow_uncharged) {
  Table& derived = static_cast<Table&>(*this);
  typename Table::InsertState state;
  derived.StartInsert(state);

  const size_t total_charge = proto.GetTotalCharge();
  if (strict_capacity_limit) {
    Status s = ChargeUsageMaybeEvictStrict<Table>(
        total_charge, capacity,
        /*need_evict_for_occupancy=*/false, state);
    if (!s.ok()) {
      if (allow_uncharged) {
        proto.total_charge = 0;
      } else {
        return nullptr;
      }
    }
  } else {
    // Case strict_capacity_limit == false
    bool success = ChargeUsageMaybeEvictNonStrict<Table>(
        total_charge, capacity,
        /*need_evict_for_occupancy=*/false, state);
    if (!success) {
      // Force the issue
      usage_.fetch_add(total_charge, std::memory_order_relaxed);
    }
  }

  return StandaloneInsert<typename Table::HandleImpl>(proto);
}

template <class Table>
Status BaseClockTable::ChargeUsageMaybeEvictStrict(
    size_t total_charge, size_t capacity, bool need_evict_for_occupancy,
    typename Table::InsertState& state) {
  if (total_charge > capacity) {
    return Status::MemoryLimit(
        "Cache entry too large for a single cache shard: " +
        std::to_string(total_charge) + " > " + std::to_string(capacity));
  }
  // Grab any available capacity, and free up any more required.
  size_t old_usage = usage_.load(std::memory_order_relaxed);
  size_t new_usage;
  do {
    new_usage = std::min(capacity, old_usage + total_charge);
    if (new_usage == old_usage) {
      // No change needed
      break;
    }
  } while (!usage_.compare_exchange_weak(old_usage, new_usage,
                                         std::memory_order_relaxed));
  // How much do we need to evict then?
  size_t need_evict_charge = old_usage + total_charge - new_usage;
  size_t request_evict_charge = need_evict_charge;
  if (UNLIKELY(need_evict_for_occupancy) && request_evict_charge == 0) {
    // Require at least 1 eviction.
    request_evict_charge = 1;
  }
  if (request_evict_charge > 0) {
    EvictionData data;
    static_cast<Table*>(this)->Evict(request_evict_charge, state, &data);
    occupancy_.fetch_sub(data.freed_count, std::memory_order_release);
    if (LIKELY(data.freed_charge > need_evict_charge)) {
      assert(data.freed_count > 0);
      // Evicted more than enough
      usage_.fetch_sub(data.freed_charge - need_evict_charge,
                       std::memory_order_relaxed);
    } else if (data.freed_charge < need_evict_charge ||
               (UNLIKELY(need_evict_for_occupancy) && data.freed_count == 0)) {
      // Roll back to old usage minus evicted
      usage_.fetch_sub(data.freed_charge + (new_usage - old_usage),
                       std::memory_order_relaxed);
      if (data.freed_charge < need_evict_charge) {
        return Status::MemoryLimit(
            "Insert failed because unable to evict entries to stay within "
            "capacity limit.");
      } else {
        return Status::MemoryLimit(
            "Insert failed because unable to evict entries to stay within "
            "table occupancy limit.");
      }
    }
    // If we needed to evict something and we are proceeding, we must have
    // evicted something.
    assert(data.freed_count > 0);
  }
  return Status::OK();
}

template <class Table>
inline bool BaseClockTable::ChargeUsageMaybeEvictNonStrict(
    size_t total_charge, size_t capacity, bool need_evict_for_occupancy,
    typename Table::InsertState& state) {
  // For simplicity, we consider that either the cache can accept the insert
  // with no evictions, or we must evict enough to make (at least) enough
  // space. It could lead to unnecessary failures or excessive evictions in
  // some extreme cases, but allows a fast, simple protocol. If we allow a
  // race to get us over capacity, then we might never get back to capacity
  // limit if the sizes of entries allow each insertion to evict the minimum
  // charge. Thus, we should evict some extra if it's not a signifcant
  // portion of the shard capacity. This can have the side benefit of
  // involving fewer threads in eviction.
  size_t old_usage = usage_.load(std::memory_order_relaxed);
  size_t need_evict_charge;
  // NOTE: if total_charge > old_usage, there isn't yet enough to evict
  // `total_charge` amount. Even if we only try to evict `old_usage` amount,
  // there's likely something referenced and we would eat CPU looking for
  // enough to evict.
  if (old_usage + total_charge <= capacity || total_charge > old_usage) {
    // Good enough for me (might run over with a race)
    need_evict_charge = 0;
  } else {
    // Try to evict enough space, and maybe some extra
    need_evict_charge = total_charge;
    if (old_usage > capacity) {
      // Not too much to avoid thundering herd while avoiding strict
      // synchronization, such as the compare_exchange used with strict
      // capacity limit.
      need_evict_charge += std::min(capacity / 1024, total_charge) + 1;
    }
  }
  if (UNLIKELY(need_evict_for_occupancy) && need_evict_charge == 0) {
    // Special case: require at least 1 eviction if we only have to
    // deal with occupancy
    need_evict_charge = 1;
  }
  EvictionData data;
  if (need_evict_charge > 0) {
    static_cast<Table*>(this)->Evict(need_evict_charge, state, &data);
    // Deal with potential occupancy deficit
    if (UNLIKELY(need_evict_for_occupancy) && data.freed_count == 0) {
      assert(data.freed_charge == 0);
      // Can't meet occupancy requirement
      return false;
    } else {
      // Update occupancy for evictions
      occupancy_.fetch_sub(data.freed_count, std::memory_order_release);
    }
  }
  // Track new usage even if we weren't able to evict enough
  usage_.fetch_add(total_charge - data.freed_charge, std::memory_order_relaxed);
  // No underflow
  assert(usage_.load(std::memory_order_relaxed) < SIZE_MAX / 2);
  // Success
  return true;
}

void BaseClockTable::TrackAndReleaseEvictedEntry(
    ClockHandle* h, BaseClockTable::EvictionData* data) {
  data->freed_charge += h->GetTotalCharge();
  data->freed_count += 1;

  bool took_value_ownership = false;
  if (eviction_callback_) {
    // For key reconstructed from hash
    UniqueId64x2 unhashed;
    took_value_ownership = eviction_callback_(
        ClockCacheShard<FixedHyperClockTable>::ReverseHash(
            h->GetHash(), &unhashed, hash_seed_),
        reinterpret_cast<Cache::Handle*>(h),
        h->meta.load(std::memory_order_relaxed) & ClockHandle::kHitBitMask);
  }
  if (!took_value_ownership) {
    h->FreeData(allocator_);
  }
  MarkEmpty(*h);
}

template <class Table>
Status BaseClockTable::Insert(const ClockHandleBasicData& proto,
                              typename Table::HandleImpl** handle,
                              Cache::Priority priority, size_t capacity,
                              bool strict_capacity_limit) {
  using HandleImpl = typename Table::HandleImpl;
  Table& derived = static_cast<Table&>(*this);

  typename Table::InsertState state;
  derived.StartInsert(state);

  // Do we have the available occupancy? Optimistically assume we do
  // and deal with it if we don't.
  size_t old_occupancy = occupancy_.fetch_add(1, std::memory_order_acquire);
  // Whether we over-committed and need an eviction to make up for it
  bool need_evict_for_occupancy =
      !derived.GrowIfNeeded(old_occupancy + 1, state);

  // Usage/capacity handling is somewhat different depending on
  // strict_capacity_limit, but mostly pessimistic.
  bool use_standalone_insert = false;
  const size_t total_charge = proto.GetTotalCharge();
  if (strict_capacity_limit) {
    Status s = ChargeUsageMaybeEvictStrict<Table>(
        total_charge, capacity, need_evict_for_occupancy, state);
    if (!s.ok()) {
      // Revert occupancy
      occupancy_.fetch_sub(1, std::memory_order_relaxed);
      return s;
    }
  } else {
    // Case strict_capacity_limit == false
    bool success = ChargeUsageMaybeEvictNonStrict<Table>(
        total_charge, capacity, need_evict_for_occupancy, state);
    if (!success) {
      // Revert occupancy
      occupancy_.fetch_sub(1, std::memory_order_relaxed);
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry
        // inserted into cache and evicted immediately.
        proto.FreeData(allocator_);
        return Status::OK();
      } else {
        // Need to track usage of fallback standalone insert
        usage_.fetch_add(total_charge, std::memory_order_relaxed);
        use_standalone_insert = true;
      }
    }
  }

  if (!use_standalone_insert) {
    // Attempt a table insert, but abort if we find an existing entry for the
    // key. If we were to overwrite old entries, we would either
    // * Have to gain ownership over an existing entry to overwrite it, which
    // would only work if there are no outstanding (read) references and would
    // create a small gap in availability of the entry (old or new) to lookups.
    // * Have to insert into a suboptimal location (more probes) so that the
    // old entry can be kept around as well.

    uint64_t initial_countdown = GetInitialCountdown(priority);
    assert(initial_countdown > 0);

    HandleImpl* e =
        derived.DoInsert(proto, initial_countdown, handle != nullptr, state);

    if (e) {
      // Successfully inserted
      if (handle) {
        *handle = e;
      }
      return Status::OK();
    }
    // Not inserted
    // Revert occupancy
    occupancy_.fetch_sub(1, std::memory_order_relaxed);
    // Maybe fall back on standalone insert
    if (handle == nullptr) {
      // Revert usage
      usage_.fetch_sub(total_charge, std::memory_order_relaxed);
      // No underflow
      assert(usage_.load(std::memory_order_relaxed) < SIZE_MAX / 2);
      // As if unrefed entry immdiately evicted
      proto.FreeData(allocator_);
      return Status::OK();
    }

    use_standalone_insert = true;
  }

  // Run standalone insert
  assert(use_standalone_insert);

  *handle = StandaloneInsert<HandleImpl>(proto);

  // The OkOverwritten status is used to count "redundant" insertions into
  // block cache. This implementation doesn't strictly check for redundant
  // insertions, but we instead are probably interested in how many insertions
  // didn't go into the table (instead "standalone"), which could be redundant
  // Insert or some other reason (use_standalone_insert reasons above).
  return Status::OkOverwritten();
}

void BaseClockTable::Ref(ClockHandle& h) {
  // Increment acquire counter
  uint64_t old_meta = h.meta.fetch_add(ClockHandle::kAcquireIncrement,
                                       std::memory_order_acquire);

  assert((old_meta >> ClockHandle::kStateShift) &
         ClockHandle::kStateShareableBit);
  // Must have already had a reference
  assert(GetRefcount(old_meta) > 0);
  (void)old_meta;
}

#ifndef NDEBUG
void BaseClockTable::TEST_RefN(ClockHandle& h, size_t n) {
  // Increment acquire counter
  uint64_t old_meta = h.meta.fetch_add(n * ClockHandle::kAcquireIncrement,
                                       std::memory_order_acquire);

  assert((old_meta >> ClockHandle::kStateShift) &
         ClockHandle::kStateShareableBit);
  (void)old_meta;
}

void BaseClockTable::TEST_ReleaseNMinus1(ClockHandle* h, size_t n) {
  assert(n > 0);

  // Like n-1 Releases, but assumes one more will happen in the caller to take
  // care of anything like erasing an unreferenced, invisible entry.
  uint64_t old_meta = h->meta.fetch_add(
      (n - 1) * ClockHandle::kReleaseIncrement, std::memory_order_acquire);
  assert((old_meta >> ClockHandle::kStateShift) &
         ClockHandle::kStateShareableBit);
  (void)old_meta;
}
#endif

FixedHyperClockTable::FixedHyperClockTable(
    size_t capacity, bool /*strict_capacity_limit*/,
    CacheMetadataChargePolicy metadata_charge_policy,
    MemoryAllocator* allocator,
    const Cache::EvictionCallback* eviction_callback, const uint32_t* hash_seed,
    const Opts& opts)
    : BaseClockTable(metadata_charge_policy, allocator, eviction_callback,
                     hash_seed),
      length_bits_(CalcHashBits(capacity, opts.estimated_value_size,
                                metadata_charge_policy)),
      length_bits_mask_((size_t{1} << length_bits_) - 1),
      occupancy_limit_(static_cast<size_t>((uint64_t{1} << length_bits_) *
                                           kStrictLoadFactor)),
      array_(new HandleImpl[size_t{1} << length_bits_]) {
  if (metadata_charge_policy ==
      CacheMetadataChargePolicy::kFullChargeCacheMetadata) {
    usage_ += size_t{GetTableSize()} * sizeof(HandleImpl);
  }

  static_assert(sizeof(HandleImpl) == 64U,
                "Expecting size / alignment with common cache line size");
}

FixedHyperClockTable::~FixedHyperClockTable() {
  // Assumes there are no references or active operations on any slot/element
  // in the table.
  for (size_t i = 0; i < GetTableSize(); i++) {
    HandleImpl& h = array_[i];
    switch (h.meta >> ClockHandle::kStateShift) {
      case ClockHandle::kStateEmpty:
        // noop
        break;
      case ClockHandle::kStateInvisible:  // rare but possible
      case ClockHandle::kStateVisible:
        assert(GetRefcount(h.meta) == 0);
        h.FreeData(allocator_);
#ifndef NDEBUG
        Rollback(h.hashed_key, &h);
        ReclaimEntryUsage(h.GetTotalCharge());
#endif
        break;
      // otherwise
      default:
        assert(false);
        break;
    }
  }

#ifndef NDEBUG
  for (size_t i = 0; i < GetTableSize(); i++) {
    assert(array_[i].displacements.load() == 0);
  }
#endif

  assert(usage_.load() == 0 ||
         usage_.load() == size_t{GetTableSize()} * sizeof(HandleImpl));
  assert(occupancy_ == 0);
}

void FixedHyperClockTable::StartInsert(InsertState&) {}

bool FixedHyperClockTable::GrowIfNeeded(size_t new_occupancy, InsertState&) {
  return new_occupancy <= occupancy_limit_;
}

FixedHyperClockTable::HandleImpl* FixedHyperClockTable::DoInsert(
    const ClockHandleBasicData& proto, uint64_t initial_countdown,
    bool keep_ref, InsertState&) {
  bool already_matches = false;
  HandleImpl* e = FindSlot(
      proto.hashed_key,
      [&](HandleImpl* h) {
        return TryInsert(proto, *h, initial_countdown, keep_ref,
                         &already_matches);
      },
      [&](HandleImpl* h) {
        if (already_matches) {
          // Stop searching & roll back displacements
          Rollback(proto.hashed_key, h);
          return true;
        } else {
          // Keep going
          return false;
        }
      },
      [&](HandleImpl* h, bool is_last) {
        if (is_last) {
          // Search is ending. Roll back displacements
          Rollback(proto.hashed_key, h);
        } else {
          h->displacements.fetch_add(1, std::memory_order_relaxed);
        }
      });
  if (already_matches) {
    // Insertion skipped
    return nullptr;
  }
  if (e != nullptr) {
    // Successfully inserted
    return e;
  }
  // Else, no available slot found. Occupancy check should generally prevent
  // this, except it's theoretically possible for other threads to evict and
  // replace entries in the right order to hit every slot when it is populated.
  // Assuming random hashing, the chance of that should be no higher than
  // pow(kStrictLoadFactor, n) for n slots. That should be infeasible for
  // roughly n >= 256, so if this assertion fails, that suggests something is
  // going wrong.
  assert(GetTableSize() < 256);
  return nullptr;
}

FixedHyperClockTable::HandleImpl* FixedHyperClockTable::Lookup(
    const UniqueId64x2& hashed_key) {
  HandleImpl* e = FindSlot(
      hashed_key,
      [&](HandleImpl* h) {
        // Mostly branch-free version (similar performance)
        /*
        uint64_t old_meta = h->meta.fetch_add(ClockHandle::kAcquireIncrement,
                                     std::memory_order_acquire);
        bool Shareable = (old_meta >> (ClockHandle::kStateShift + 1)) & 1U;
        bool visible = (old_meta >> ClockHandle::kStateShift) & 1U;
        bool match = (h->key == key) & visible;
        h->meta.fetch_sub(static_cast<uint64_t>(Shareable & !match) <<
        ClockHandle::kAcquireCounterShift, std::memory_order_release); return
        match;
        */
        // Optimistic lookup should pay off when the table is relatively
        // sparse.
        constexpr bool kOptimisticLookup = true;
        uint64_t old_meta;
        if (!kOptimisticLookup) {
          old_meta = h->meta.load(std::memory_order_acquire);
          if ((old_meta >> ClockHandle::kStateShift) !=
              ClockHandle::kStateVisible) {
            return false;
          }
        }
        // (Optimistically) increment acquire counter
        old_meta = h->meta.fetch_add(ClockHandle::kAcquireIncrement,
                                     std::memory_order_acquire);
        // Check if it's an entry visible to lookups
        if ((old_meta >> ClockHandle::kStateShift) ==
            ClockHandle::kStateVisible) {
          // Acquired a read reference
          if (h->hashed_key == hashed_key) {
            // Match
            // Update the hit bit
            if (eviction_callback_) {
              h->meta.fetch_or(uint64_t{1} << ClockHandle::kHitBitShift,
                               std::memory_order_relaxed);
            }
            return true;
          } else {
            // Mismatch. Pretend we never took the reference
            Unref(*h);
          }
        } else if (UNLIKELY((old_meta >> ClockHandle::kStateShift) ==
                            ClockHandle::kStateInvisible)) {
          // Pretend we never took the reference
          Unref(*h);
        } else {
          // For other states, incrementing the acquire counter has no effect
          // so we don't need to undo it. Furthermore, we cannot safely undo
          // it because we did not acquire a read reference to lock the
          // entry in a Shareable state.
        }
        return false;
      },
      [&](HandleImpl* h) {
        return h->displacements.load(std::memory_order_relaxed) == 0;
      },
      [&](HandleImpl* /*h*/, bool /*is_last*/) {});

  return e;
}

bool FixedHyperClockTable::Release(HandleImpl* h, bool useful,
                                   bool erase_if_last_ref) {
  // In contrast with LRUCache's Release, this function won't delete the handle
  // when the cache is above capacity and the reference is the last one. Space
  // is only freed up by EvictFromClock (called by Insert when space is needed)
  // and Erase. We do this to avoid an extra atomic read of the variable usage_.

  uint64_t old_meta;
  if (useful) {
    // Increment release counter to indicate was used
    old_meta = h->meta.fetch_add(ClockHandle::kReleaseIncrement,
                                 std::memory_order_release);
  } else {
    // Decrement acquire counter to pretend it never happened
    old_meta = h->meta.fetch_sub(ClockHandle::kAcquireIncrement,
                                 std::memory_order_release);
  }

  assert((old_meta >> ClockHandle::kStateShift) &
         ClockHandle::kStateShareableBit);
  // No underflow
  assert(((old_meta >> ClockHandle::kAcquireCounterShift) &
          ClockHandle::kCounterMask) !=
         ((old_meta >> ClockHandle::kReleaseCounterShift) &
          ClockHandle::kCounterMask));

  if (erase_if_last_ref || UNLIKELY(old_meta >> ClockHandle::kStateShift ==
                                    ClockHandle::kStateInvisible)) {
    // Update for last fetch_add op
    if (useful) {
      old_meta += ClockHandle::kReleaseIncrement;
    } else {
      old_meta -= ClockHandle::kAcquireIncrement;
    }
    // Take ownership if no refs
    do {
      if (GetRefcount(old_meta) != 0) {
        // Not last ref at some point in time during this Release call
        // Correct for possible (but rare) overflow
        CorrectNearOverflow(old_meta, h->meta);
        return false;
      }
      if ((old_meta & (uint64_t{ClockHandle::kStateShareableBit}
                       << ClockHandle::kStateShift)) == 0) {
        // Someone else took ownership
        return false;
      }
      // Note that there's a small chance that we release, another thread
      // replaces this entry with another, reaches zero refs, and then we end
      // up erasing that other entry. That's an acceptable risk / imprecision.
    } while (!h->meta.compare_exchange_weak(
        old_meta,
        uint64_t{ClockHandle::kStateConstruction} << ClockHandle::kStateShift,
        std::memory_order_acquire));
    // Took ownership
    size_t total_charge = h->GetTotalCharge();
    if (UNLIKELY(h->IsStandalone())) {
      h->FreeData(allocator_);
      // Delete standalone handle
      delete h;
      standalone_usage_.fetch_sub(total_charge, std::memory_order_relaxed);
      usage_.fetch_sub(total_charge, std::memory_order_relaxed);
    } else {
      Rollback(h->hashed_key, h);
      FreeDataMarkEmpty(*h, allocator_);
      ReclaimEntryUsage(total_charge);
    }
    return true;
  } else {
    // Correct for possible (but rare) overflow
    CorrectNearOverflow(old_meta, h->meta);
    return false;
  }
}

#ifndef NDEBUG
void FixedHyperClockTable::TEST_ReleaseN(HandleImpl* h, size_t n) {
  if (n > 0) {
    // Do n-1 simple releases first
    TEST_ReleaseNMinus1(h, n);

    // Then the last release might be more involved
    Release(h, /*useful*/ true, /*erase_if_last_ref*/ false);
  }
}
#endif

void FixedHyperClockTable::Erase(const UniqueId64x2& hashed_key) {
  (void)FindSlot(
      hashed_key,
      [&](HandleImpl* h) {
        // Could be multiple entries in rare cases. Erase them all.
        // Optimistically increment acquire counter
        uint64_t old_meta = h->meta.fetch_add(ClockHandle::kAcquireIncrement,
                                              std::memory_order_acquire);
        // Check if it's an entry visible to lookups
        if ((old_meta >> ClockHandle::kStateShift) ==
            ClockHandle::kStateVisible) {
          // Acquired a read reference
          if (h->hashed_key == hashed_key) {
            // Match. Set invisible.
            old_meta =
                h->meta.fetch_and(~(uint64_t{ClockHandle::kStateVisibleBit}
                                    << ClockHandle::kStateShift),
                                  std::memory_order_acq_rel);
            // Apply update to local copy
            old_meta &= ~(uint64_t{ClockHandle::kStateVisibleBit}
                          << ClockHandle::kStateShift);
            for (;;) {
              uint64_t refcount = GetRefcount(old_meta);
              assert(refcount > 0);
              if (refcount > 1) {
                // Not last ref at some point in time during this Erase call
                // Pretend we never took the reference
                Unref(*h);
                break;
              } else if (h->meta.compare_exchange_weak(
                             old_meta,
                             uint64_t{ClockHandle::kStateConstruction}
                                 << ClockHandle::kStateShift,
                             std::memory_order_acq_rel)) {
                // Took ownership
                assert(hashed_key == h->hashed_key);
                size_t total_charge = h->GetTotalCharge();
                FreeDataMarkEmpty(*h, allocator_);
                ReclaimEntryUsage(total_charge);
                // We already have a copy of hashed_key in this case, so OK to
                // delay Rollback until after releasing the entry
                Rollback(hashed_key, h);
                break;
              }
            }
          } else {
            // Mismatch. Pretend we never took the reference
            Unref(*h);
          }
        } else if (UNLIKELY((old_meta >> ClockHandle::kStateShift) ==
                            ClockHandle::kStateInvisible)) {
          // Pretend we never took the reference
          Unref(*h);
        } else {
          // For other states, incrementing the acquire counter has no effect
          // so we don't need to undo it.
        }
        return false;
      },
      [&](HandleImpl* h) {
        return h->displacements.load(std::memory_order_relaxed) == 0;
      },
      [&](HandleImpl* /*h*/, bool /*is_last*/) {});
}

void FixedHyperClockTable::EraseUnRefEntries() {
  for (size_t i = 0; i <= this->length_bits_mask_; i++) {
    HandleImpl& h = array_[i];

    uint64_t old_meta = h.meta.load(std::memory_order_relaxed);
    if (old_meta & (uint64_t{ClockHandle::kStateShareableBit}
                    << ClockHandle::kStateShift) &&
        GetRefcount(old_meta) == 0 &&
        h.meta.compare_exchange_strong(old_meta,
                                       uint64_t{ClockHandle::kStateConstruction}
                                           << ClockHandle::kStateShift,
                                       std::memory_order_acquire)) {
      // Took ownership
      size_t total_charge = h.GetTotalCharge();
      Rollback(h.hashed_key, &h);
      FreeDataMarkEmpty(h, allocator_);
      ReclaimEntryUsage(total_charge);
    }
  }
}

template <typename MatchFn, typename AbortFn, typename UpdateFn>
inline FixedHyperClockTable::HandleImpl* FixedHyperClockTable::FindSlot(
    const UniqueId64x2& hashed_key, const MatchFn& match_fn,
    const AbortFn& abort_fn, const UpdateFn& update_fn) {
  // NOTE: upper 32 bits of hashed_key[0] is used for sharding
  //
  // We use double-hashing probing. Every probe in the sequence is a
  // pseudorandom integer, computed as a linear function of two random hashes,
  // which we call base and increment. Specifically, the i-th probe is base + i
  // * increment modulo the table size.
  size_t base = static_cast<size_t>(hashed_key[1]);
  // We use an odd increment, which is relatively prime with the power-of-two
  // table size. This implies that we cycle back to the first probe only
  // after probing every slot exactly once.
  // TODO: we could also reconsider linear probing, though locality benefits
  // are limited because each slot is a full cache line
  size_t increment = static_cast<size_t>(hashed_key[0]) | 1U;
  size_t first = ModTableSize(base);
  size_t current = first;
  bool is_last;
  do {
    HandleImpl* h = &array_[current];
    if (match_fn(h)) {
      return h;
    }
    if (abort_fn(h)) {
      return nullptr;
    }
    current = ModTableSize(current + increment);
    is_last = current == first;
    update_fn(h, is_last);
  } while (!is_last);
  // We looped back.
  return nullptr;
}

inline void FixedHyperClockTable::Rollback(const UniqueId64x2& hashed_key,
                                           const HandleImpl* h) {
  size_t current = ModTableSize(hashed_key[1]);
  size_t increment = static_cast<size_t>(hashed_key[0]) | 1U;
  while (&array_[current] != h) {
    array_[current].displacements.fetch_sub(1, std::memory_order_relaxed);
    current = ModTableSize(current + increment);
  }
}

inline void FixedHyperClockTable::ReclaimEntryUsage(size_t total_charge) {
  auto old_occupancy = occupancy_.fetch_sub(1U, std::memory_order_release);
  (void)old_occupancy;
  // No underflow
  assert(old_occupancy > 0);
  auto old_usage = usage_.fetch_sub(total_charge, std::memory_order_relaxed);
  (void)old_usage;
  // No underflow
  assert(old_usage >= total_charge);
}

inline void FixedHyperClockTable::Evict(size_t requested_charge, InsertState&,
                                        EvictionData* data) {
  // precondition
  assert(requested_charge > 0);

  // TODO: make a tuning parameter?
  constexpr size_t step_size = 4;

  // First (concurrent) increment clock pointer
  uint64_t old_clock_pointer =
      clock_pointer_.fetch_add(step_size, std::memory_order_relaxed);

  // Cap the eviction effort at this thread (along with those operating in
  // parallel) circling through the whole structure kMaxCountdown times.
  // In other words, this eviction run must find something/anything that is
  // unreferenced at start of and during the eviction run that isn't reclaimed
  // by a concurrent eviction run.
  uint64_t max_clock_pointer =
      old_clock_pointer + (ClockHandle::kMaxCountdown << length_bits_);

  for (;;) {
    for (size_t i = 0; i < step_size; i++) {
      HandleImpl& h = array_[ModTableSize(Lower32of64(old_clock_pointer + i))];
      bool evicting = ClockUpdate(h);
      if (evicting) {
        Rollback(h.hashed_key, &h);
        TrackAndReleaseEvictedEntry(&h, data);
      }
    }

    // Loop exit condition
    if (data->freed_charge >= requested_charge) {
      return;
    }
    if (old_clock_pointer >= max_clock_pointer) {
      return;
    }

    // Advance clock pointer (concurrently)
    old_clock_pointer =
        clock_pointer_.fetch_add(step_size, std::memory_order_relaxed);
  }
}

template <class Table>
ClockCacheShard<Table>::ClockCacheShard(
    size_t capacity, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy,
    MemoryAllocator* allocator,
    const Cache::EvictionCallback* eviction_callback, const uint32_t* hash_seed,
    const typename Table::Opts& opts)
    : CacheShardBase(metadata_charge_policy),
      table_(capacity, strict_capacity_limit, metadata_charge_policy, allocator,
             eviction_callback, hash_seed, opts),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit) {
  // Initial charge metadata should not exceed capacity
  assert(table_.GetUsage() <= capacity_ || capacity_ < sizeof(HandleImpl));
}

template <class Table>
void ClockCacheShard<Table>::EraseUnRefEntries() {
  table_.EraseUnRefEntries();
}

template <class Table>
void ClockCacheShard<Table>::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {
  // The state will be a simple index into the table. Even with a dynamic
  // hyper clock cache, entries will generally stay in their existing
  // slots, so we don't need to be aware of the high-level organization
  // that makes lookup efficient.
  size_t length = table_.GetTableSize();

  assert(average_entries_per_lock > 0);

  size_t index_begin = *state;
  size_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end.
    index_end = length;
    *state = SIZE_MAX;
  } else {
    *state = index_end;
  }

  auto hash_seed = table_.GetHashSeed();
  ConstApplyToEntriesRange(
      [callback, hash_seed](const HandleImpl& h) {
        UniqueId64x2 unhashed;
        callback(ReverseHash(h.hashed_key, &unhashed, hash_seed), h.value,
                 h.GetTotalCharge(), h.helper);
      },
      table_.HandlePtr(index_begin), table_.HandlePtr(index_end), false);
}

int FixedHyperClockTable::CalcHashBits(
    size_t capacity, size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  double average_slot_charge = estimated_value_size * kLoadFactor;
  if (metadata_charge_policy == kFullChargeCacheMetadata) {
    average_slot_charge += sizeof(HandleImpl);
  }
  assert(average_slot_charge > 0.0);
  uint64_t num_slots =
      static_cast<uint64_t>(capacity / average_slot_charge + 0.999999);

  int hash_bits = FloorLog2((num_slots << 1) - 1);
  if (metadata_charge_policy == kFullChargeCacheMetadata) {
    // For very small estimated value sizes, it's possible to overshoot
    while (hash_bits > 0 &&
           uint64_t{sizeof(HandleImpl)} << hash_bits > capacity) {
      hash_bits--;
    }
  }
  return hash_bits;
}

template <class Table>
void ClockCacheShard<Table>::SetCapacity(size_t capacity) {
  capacity_.store(capacity, std::memory_order_relaxed);
  // next Insert will take care of any necessary evictions
}

template <class Table>
void ClockCacheShard<Table>::SetStrictCapacityLimit(
    bool strict_capacity_limit) {
  strict_capacity_limit_.store(strict_capacity_limit,
                               std::memory_order_relaxed);
  // next Insert will take care of any necessary evictions
}

template <class Table>
Status ClockCacheShard<Table>::Insert(const Slice& key,
                                      const UniqueId64x2& hashed_key,
                                      Cache::ObjectPtr value,
                                      const Cache::CacheItemHelper* helper,
                                      size_t charge, HandleImpl** handle,
                                      Cache::Priority priority) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return Status::NotSupported("ClockCache only supports key size " +
                                std::to_string(kCacheKeySize) + "B");
  }
  ClockHandleBasicData proto;
  proto.hashed_key = hashed_key;
  proto.value = value;
  proto.helper = helper;
  proto.total_charge = charge;
  return table_.template Insert<Table>(
      proto, handle, priority, capacity_.load(std::memory_order_relaxed),
      strict_capacity_limit_.load(std::memory_order_relaxed));
}

template <class Table>
typename Table::HandleImpl* ClockCacheShard<Table>::CreateStandalone(
    const Slice& key, const UniqueId64x2& hashed_key, Cache::ObjectPtr obj,
    const Cache::CacheItemHelper* helper, size_t charge, bool allow_uncharged) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return nullptr;
  }
  ClockHandleBasicData proto;
  proto.hashed_key = hashed_key;
  proto.value = obj;
  proto.helper = helper;
  proto.total_charge = charge;
  return table_.template CreateStandalone<Table>(
      proto, capacity_.load(std::memory_order_relaxed),
      strict_capacity_limit_.load(std::memory_order_relaxed), allow_uncharged);
}

template <class Table>
typename ClockCacheShard<Table>::HandleImpl* ClockCacheShard<Table>::Lookup(
    const Slice& key, const UniqueId64x2& hashed_key) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return nullptr;
  }
  return table_.Lookup(hashed_key);
}

template <class Table>
bool ClockCacheShard<Table>::Ref(HandleImpl* h) {
  if (h == nullptr) {
    return false;
  }
  table_.Ref(*h);
  return true;
}

template <class Table>
bool ClockCacheShard<Table>::Release(HandleImpl* handle, bool useful,
                                     bool erase_if_last_ref) {
  if (handle == nullptr) {
    return false;
  }
  return table_.Release(handle, useful, erase_if_last_ref);
}

#ifndef NDEBUG
template <class Table>
void ClockCacheShard<Table>::TEST_RefN(HandleImpl* h, size_t n) {
  table_.TEST_RefN(*h, n);
}

template <class Table>
void ClockCacheShard<Table>::TEST_ReleaseN(HandleImpl* h, size_t n) {
  table_.TEST_ReleaseN(h, n);
}
#endif

template <class Table>
bool ClockCacheShard<Table>::Release(HandleImpl* handle,
                                     bool erase_if_last_ref) {
  return Release(handle, /*useful=*/true, erase_if_last_ref);
}

template <class Table>
void ClockCacheShard<Table>::Erase(const Slice& key,
                                   const UniqueId64x2& hashed_key) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return;
  }
  table_.Erase(hashed_key);
}

template <class Table>
size_t ClockCacheShard<Table>::GetUsage() const {
  return table_.GetUsage();
}

template <class Table>
size_t ClockCacheShard<Table>::GetStandaloneUsage() const {
  return table_.GetStandaloneUsage();
}

template <class Table>
size_t ClockCacheShard<Table>::GetCapacity() const {
  return capacity_;
}

template <class Table>
size_t ClockCacheShard<Table>::GetPinnedUsage() const {
  // Computes the pinned usage by scanning the whole hash table. This
  // is slow, but avoids keeping an exact counter on the clock usage,
  // i.e., the number of not externally referenced elements.
  // Why avoid this counter? Because Lookup removes elements from the clock
  // list, so it would need to update the pinned usage every time,
  // which creates additional synchronization costs.
  size_t table_pinned_usage = 0;
  const bool charge_metadata =
      metadata_charge_policy_ == kFullChargeCacheMetadata;
  ConstApplyToEntriesRange(
      [&table_pinned_usage, charge_metadata](const HandleImpl& h) {
        uint64_t meta = h.meta.load(std::memory_order_relaxed);
        uint64_t refcount = GetRefcount(meta);
        // Holding one ref for ConstApplyToEntriesRange
        assert(refcount > 0);
        if (refcount > 1) {
          table_pinned_usage += h.GetTotalCharge();
          if (charge_metadata) {
            table_pinned_usage += sizeof(HandleImpl);
          }
        }
      },
      table_.HandlePtr(0), table_.HandlePtr(table_.GetTableSize()), true);

  return table_pinned_usage + table_.GetStandaloneUsage();
}

template <class Table>
size_t ClockCacheShard<Table>::GetOccupancyCount() const {
  return table_.GetOccupancy();
}

template <class Table>
size_t ClockCacheShard<Table>::GetOccupancyLimit() const {
  return table_.GetOccupancyLimit();
}

template <class Table>
size_t ClockCacheShard<Table>::GetTableAddressCount() const {
  return table_.GetTableSize();
}

// Explicit instantiation
template class ClockCacheShard<FixedHyperClockTable>;
template class ClockCacheShard<AutoHyperClockTable>;

template <class Table>
BaseHyperClockCache<Table>::BaseHyperClockCache(
    const HyperClockCacheOptions& opts)
    : ShardedCache<ClockCacheShard<Table>>(opts) {
  // TODO: should not need to go through two levels of pointer indirection to
  // get to table entries
  size_t per_shard = this->GetPerShardCapacity();
  MemoryAllocator* alloc = this->memory_allocator();
  this->InitShards([&](Shard* cs) {
    typename Table::Opts table_opts{opts};
    new (cs) Shard(per_shard, opts.strict_capacity_limit,
                   opts.metadata_charge_policy, alloc,
                   &this->eviction_callback_, &this->hash_seed_, table_opts);
  });
}

template <class Table>
Cache::ObjectPtr BaseHyperClockCache<Table>::Value(Handle* handle) {
  return reinterpret_cast<const typename Table::HandleImpl*>(handle)->value;
}

template <class Table>
size_t BaseHyperClockCache<Table>::GetCharge(Handle* handle) const {
  return reinterpret_cast<const typename Table::HandleImpl*>(handle)
      ->GetTotalCharge();
}

template <class Table>
const Cache::CacheItemHelper* BaseHyperClockCache<Table>::GetCacheItemHelper(
    Handle* handle) const {
  auto h = reinterpret_cast<const typename Table::HandleImpl*>(handle);
  return h->helper;
}

namespace {

// For each cache shard, estimate what the table load factor would be if
// cache filled to capacity with average entries. This is considered
// indicative of a potential problem if the shard is essentially operating
// "at limit", which we define as high actual usage (>80% of capacity)
// or actual occupancy very close to limit (>95% of limit).
// Also, for each shard compute the recommended estimated_entry_charge,
// and keep the minimum one for use as overall recommendation.
void AddShardEvaluation(const FixedHyperClockCache::Shard& shard,
                        std::vector<double>& predicted_load_factors,
                        size_t& min_recommendation) {
  size_t usage = shard.GetUsage() - shard.GetStandaloneUsage();
  size_t capacity = shard.GetCapacity();
  double usage_ratio = 1.0 * usage / capacity;

  size_t occupancy = shard.GetOccupancyCount();
  size_t occ_limit = shard.GetOccupancyLimit();
  double occ_ratio = 1.0 * occupancy / occ_limit;
  if (usage == 0 || occupancy == 0 || (usage_ratio < 0.8 && occ_ratio < 0.95)) {
    // Skip as described above
    return;
  }

  // If filled to capacity, what would the occupancy ratio be?
  double ratio = occ_ratio / usage_ratio;
  // Given max load factor, what that load factor be?
  double lf = ratio * FixedHyperClockTable::kStrictLoadFactor;
  predicted_load_factors.push_back(lf);

  // Update min_recommendation also
  size_t recommendation = usage / occupancy;
  min_recommendation = std::min(min_recommendation, recommendation);
}

bool IsSlotOccupied(const ClockHandle& h) {
  return (h.meta.load(std::memory_order_relaxed) >> ClockHandle::kStateShift) !=
         0;
}
}  // namespace

// NOTE: GCC might warn about subobject linkage if this is in anon namespace
template <size_t N = 500>
class LoadVarianceStats {
 public:
  std::string Report() const {
    return "Overall " + PercentStr(positive_count_, samples_) + " (" +
           std::to_string(positive_count_) + "/" + std::to_string(samples_) +
           "), Min/Max/Window = " + PercentStr(min_, N) + "/" +
           PercentStr(max_, N) + "/" + std::to_string(N) +
           ", MaxRun{Pos/Neg} = " + std::to_string(max_pos_run_) + "/" +
           std::to_string(max_neg_run_) + "\n";
  }

  void Add(bool positive) {
    recent_[samples_ % N] = positive;
    if (positive) {
      ++positive_count_;
      ++cur_pos_run_;
      max_pos_run_ = std::max(max_pos_run_, cur_pos_run_);
      cur_neg_run_ = 0;
    } else {
      ++cur_neg_run_;
      max_neg_run_ = std::max(max_neg_run_, cur_neg_run_);
      cur_pos_run_ = 0;
    }
    ++samples_;
    if (samples_ >= N) {
      size_t count_set = recent_.count();
      max_ = std::max(max_, count_set);
      min_ = std::min(min_, count_set);
    }
  }

 private:
  size_t max_ = 0;
  size_t min_ = N;
  size_t positive_count_ = 0;
  size_t samples_ = 0;
  size_t max_pos_run_ = 0;
  size_t cur_pos_run_ = 0;
  size_t max_neg_run_ = 0;
  size_t cur_neg_run_ = 0;
  std::bitset<N> recent_;

  static std::string PercentStr(size_t a, size_t b) {
    return std::to_string(uint64_t{100} * a / b) + "%";
  }
};

template <class Table>
void BaseHyperClockCache<Table>::ReportProblems(
    const std::shared_ptr<Logger>& info_log) const {
  if (info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    LoadVarianceStats slot_stats;
    this->ForEachShard([&](const BaseHyperClockCache<Table>::Shard* shard) {
      size_t count = shard->GetTableAddressCount();
      for (size_t i = 0; i < count; ++i) {
        slot_stats.Add(IsSlotOccupied(*shard->GetTable().HandlePtr(i)));
      }
    });
    ROCKS_LOG_AT_LEVEL(info_log, InfoLogLevel::DEBUG_LEVEL,
                       "Slot occupancy stats: %s", slot_stats.Report().c_str());
  }
}

void FixedHyperClockCache::ReportProblems(
    const std::shared_ptr<Logger>& info_log) const {
  BaseHyperClockCache::ReportProblems(info_log);

  uint32_t shard_count = GetNumShards();
  std::vector<double> predicted_load_factors;
  size_t min_recommendation = SIZE_MAX;
  ForEachShard([&](const FixedHyperClockCache::Shard* shard) {
    AddShardEvaluation(*shard, predicted_load_factors, min_recommendation);
  });

  if (predicted_load_factors.empty()) {
    // None operating "at limit" -> nothing to report
    return;
  }
  std::sort(predicted_load_factors.begin(), predicted_load_factors.end());

  // First, if the average load factor is within spec, we aren't going to
  // complain about a few shards being out of spec.
  // NOTE: this is only the average among cache shards operating "at limit,"
  // which should be representative of what we care about. It it normal, even
  // desirable, for a cache to operate "at limit" so this should not create
  // selection bias. See AddShardEvaluation().
  // TODO: Consider detecting cases where decreasing the number of shards
  // would be good, e.g. serious imbalance among shards.
  double average_load_factor =
      std::accumulate(predicted_load_factors.begin(),
                      predicted_load_factors.end(), 0.0) /
      shard_count;

  constexpr double kLowSpecLoadFactor = FixedHyperClockTable::kLoadFactor / 2;
  constexpr double kMidSpecLoadFactor =
      FixedHyperClockTable::kLoadFactor / 1.414;
  if (average_load_factor > FixedHyperClockTable::kLoadFactor) {
    // Out of spec => Consider reporting load factor too high
    // Estimate effective overall capacity loss due to enforcing occupancy limit
    double lost_portion = 0.0;
    int over_count = 0;
    for (double lf : predicted_load_factors) {
      if (lf > FixedHyperClockTable::kStrictLoadFactor) {
        ++over_count;
        lost_portion +=
            (lf - FixedHyperClockTable::kStrictLoadFactor) / lf / shard_count;
      }
    }
    // >= 20% loss -> error
    // >= 10% loss -> consistent warning
    // >= 1% loss -> intermittent warning
    InfoLogLevel level = InfoLogLevel::INFO_LEVEL;
    bool report = true;
    if (lost_portion > 0.2) {
      level = InfoLogLevel::ERROR_LEVEL;
    } else if (lost_portion > 0.1) {
      level = InfoLogLevel::WARN_LEVEL;
    } else if (lost_portion > 0.01) {
      int report_percent = static_cast<int>(lost_portion * 100.0);
      if (Random::GetTLSInstance()->PercentTrue(report_percent)) {
        level = InfoLogLevel::WARN_LEVEL;
      }
    } else {
      // don't report
      report = false;
    }
    if (report) {
      ROCKS_LOG_AT_LEVEL(
          info_log, level,
          "FixedHyperClockCache@%p unable to use estimated %.1f%% capacity "
          "because of full occupancy in %d/%u cache shards "
          "(estimated_entry_charge too high). "
          "Recommend estimated_entry_charge=%zu",
          this, lost_portion * 100.0, over_count, (unsigned)shard_count,
          min_recommendation);
    }
  } else if (average_load_factor < kLowSpecLoadFactor) {
    // Out of spec => Consider reporting load factor too low
    // But cautiously because low is not as big of a problem.

    // Only report if highest occupancy shard is also below
    // spec and only if average is substantially out of spec
    if (predicted_load_factors.back() < kLowSpecLoadFactor &&
        average_load_factor < kLowSpecLoadFactor / 1.414) {
      InfoLogLevel level = InfoLogLevel::INFO_LEVEL;
      if (average_load_factor < kLowSpecLoadFactor / 2) {
        level = InfoLogLevel::WARN_LEVEL;
      }
      ROCKS_LOG_AT_LEVEL(
          info_log, level,
          "FixedHyperClockCache@%p table has low occupancy at full capacity. "
          "Higher estimated_entry_charge (about %.1fx) would likely improve "
          "performance. Recommend estimated_entry_charge=%zu",
          this, kMidSpecLoadFactor / average_load_factor, min_recommendation);
    }
  }
}

}  // namespace clock_cache

// DEPRECATED (see public API)
std::shared_ptr<Cache> NewClockCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy) {
  return NewLRUCache(capacity, num_shard_bits, strict_capacity_limit,
                     /* high_pri_pool_ratio */ 0.5, nullptr,
                     kDefaultToAdaptiveMutex, metadata_charge_policy,
                     /* low_pri_pool_ratio */ 0.0);
}

std::shared_ptr<Cache> HyperClockCacheOptions::MakeSharedCache() const {
  // For sanitized options
  HyperClockCacheOptions opts = *this;
  if (opts.num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  if (opts.num_shard_bits < 0) {
    // Use larger shard size to reduce risk of large entries clustering
    // or skewing individual shards.
    constexpr size_t min_shard_size = 32U * 1024U * 1024U;
    opts.num_shard_bits =
        GetDefaultCacheShardBits(opts.capacity, min_shard_size);
  }
  std::shared_ptr<Cache> cache;
  if (opts.estimated_entry_charge == 0) {
    // BEGIN placeholder logic to be removed
    // This is sufficient to get the placeholder Auto working in unit tests
    // much like the Fixed version.
    opts.estimated_entry_charge = opts.min_avg_entry_charge;
    // END placeholder logic to be removed
    cache = std::make_shared<clock_cache::AutoHyperClockCache>(opts);
  } else {
    cache = std::make_shared<clock_cache::FixedHyperClockCache>(opts);
  }
  if (opts.secondary_cache) {
    cache = std::make_shared<CacheWithSecondaryAdapter>(cache,
                                                        opts.secondary_cache);
  }
  return cache;
}

}  // namespace ROCKSDB_NAMESPACE
