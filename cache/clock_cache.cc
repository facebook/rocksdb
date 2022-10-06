//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/clock_cache.h"

#include <cassert>
#include <functional>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace hyper_clock_cache {

inline uint64_t GetRefcount(uint64_t meta) {
  return ((meta >> ClockHandle::kAcquireCounterShift) -
          (meta >> ClockHandle::kReleaseCounterShift)) &
         ClockHandle::kCounterMask;
}

static_assert(sizeof(ClockHandle) == 64U,
              "Expecting size / alignment with common cache line size");

ClockHandleTable::ClockHandleTable(int hash_bits, bool initial_charge_metadata)
    : length_bits_(hash_bits),
      length_bits_mask_(Lower32of64((uint64_t{1} << length_bits_) - 1)),
      occupancy_limit_(static_cast<uint32_t>((uint64_t{1} << length_bits_) *
                                             kStrictLoadFactor)),
      array_(new ClockHandle[size_t{1} << length_bits_]) {
  assert(hash_bits <= 32);  // FIXME: ensure no overlap with sharding bits
  if (initial_charge_metadata) {
    usage_ += size_t{GetTableSize()} * sizeof(ClockHandle);
  }
}

ClockHandleTable::~ClockHandleTable() {
  // Assumes there are no references or active operations on any slot/element
  // in the table.
  for (uint32_t i = 0; i < GetTableSize(); i++) {
    ClockHandle& h = array_[i];
    switch (h.meta >> ClockHandle::kStateShift) {
      case ClockHandle::kStateEmpty:
        // noop
        break;
      case ClockHandle::kStateInvisible:  // rare but possible
      case ClockHandle::kStateVisible:
        assert(GetRefcount(h.meta) == 0);
        h.FreeData();
#ifndef NDEBUG
        Rollback(h.hash, &h);
        usage_.fetch_sub(h.total_charge, std::memory_order_relaxed);
        occupancy_.fetch_sub(1U, std::memory_order_relaxed);
#endif
        break;
      // otherwise
      default:
        assert(false);
        break;
    }
  }

#ifndef NDEBUG
  for (uint32_t i = 0; i < GetTableSize(); i++) {
    assert(array_[i].displacements.load() == 0);
  }
#endif

  assert(usage_.load() == 0 ||
         usage_.load() == size_t{GetTableSize()} * sizeof(ClockHandle));
  assert(occupancy_ == 0);
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

Status ClockHandleTable::Insert(const ClockHandleMoreData& proto,
                                ClockHandle** handle, Cache::Priority priority,
                                size_t capacity, bool strict_capacity_limit) {
  // Do we have the available occupancy? Optimistically assume we do
  // and deal with it if we don't.
  uint32_t old_occupancy = occupancy_.fetch_add(1, std::memory_order_acquire);
  auto revert_occupancy_fn = [&]() {
    occupancy_.fetch_sub(1, std::memory_order_relaxed);
  };
  // Whether we over-committed and need an eviction to make up for it
  bool need_evict_for_occupancy = old_occupancy >= occupancy_limit_;

  // Usage/capacity handling is somewhat different depending on
  // strict_capacity_limit, but mostly pessimistic.
  bool use_detached_insert = false;
  const size_t total_charge = proto.total_charge;
  if (strict_capacity_limit) {
    if (total_charge > capacity) {
      assert(!use_detached_insert);
      revert_occupancy_fn();
      return Status::MemoryLimit(
          "Cache entry too large for a single cache shard: " +
          std::to_string(total_charge) + " > " + std::to_string(capacity));
    }
    // Grab any available capacity, and free up any more required.
    size_t old_usage = usage_.load(std::memory_order_relaxed);
    size_t new_usage;
    if (LIKELY(old_usage != capacity)) {
      do {
        new_usage = std::min(capacity, old_usage + total_charge);
      } while (!usage_.compare_exchange_weak(old_usage, new_usage,
                                             std::memory_order_relaxed));
    } else {
      new_usage = old_usage;
    }
    // How much do we need to evict then?
    size_t need_evict_charge = old_usage + total_charge - new_usage;
    size_t request_evict_charge = need_evict_charge;
    if (UNLIKELY(need_evict_for_occupancy) && request_evict_charge == 0) {
      // Require at least 1 eviction.
      request_evict_charge = 1;
    }
    if (request_evict_charge > 0) {
      size_t evicted_charge = 0;
      uint32_t evicted_count = 0;
      Evict(request_evict_charge, &evicted_charge, &evicted_count);
      occupancy_.fetch_sub(evicted_count, std::memory_order_release);
      if (LIKELY(evicted_charge > need_evict_charge)) {
        assert(evicted_count > 0);
        // Evicted more than enough
        usage_.fetch_sub(evicted_charge - need_evict_charge,
                         std::memory_order_relaxed);
      } else if (evicted_charge < need_evict_charge ||
                 (UNLIKELY(need_evict_for_occupancy) && evicted_count == 0)) {
        // Roll back to old usage minus evicted
        usage_.fetch_sub(evicted_charge + (new_usage - old_usage),
                         std::memory_order_relaxed);
        assert(!use_detached_insert);
        revert_occupancy_fn();
        if (evicted_charge < need_evict_charge) {
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
      assert(evicted_count > 0);
    }
  } else {
    // Case strict_capacity_limit == false

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
        // synchronization
        need_evict_charge += std::min(capacity / 1024, total_charge) + 1;
      }
    }
    if (UNLIKELY(need_evict_for_occupancy) && need_evict_charge == 0) {
      // Special case: require at least 1 eviction if we only have to
      // deal with occupancy
      need_evict_charge = 1;
    }
    size_t evicted_charge = 0;
    uint32_t evicted_count = 0;
    if (need_evict_charge > 0) {
      Evict(need_evict_charge, &evicted_charge, &evicted_count);
      // Deal with potential occupancy deficit
      if (UNLIKELY(need_evict_for_occupancy) && evicted_count == 0) {
        assert(evicted_charge == 0);
        revert_occupancy_fn();
        if (handle == nullptr) {
          // Don't insert the entry but still return ok, as if the entry
          // inserted into cache and evicted immediately.
          proto.FreeData();
          return Status::OK();
        } else {
          use_detached_insert = true;
        }
      } else {
        // Update occupancy for evictions
        occupancy_.fetch_sub(evicted_count, std::memory_order_release);
      }
    }
    // Track new usage even if we weren't able to evict enough
    usage_.fetch_add(total_charge - evicted_charge, std::memory_order_relaxed);
    // No underflow
    assert(usage_.load(std::memory_order_relaxed) < SIZE_MAX / 2);
  }
  auto revert_usage_fn = [&]() {
    usage_.fetch_sub(total_charge, std::memory_order_relaxed);
    // No underflow
    assert(usage_.load(std::memory_order_relaxed) < SIZE_MAX / 2);
  };

  if (!use_detached_insert) {
    // Attempt a table insert, but abort if we find an existing entry for the
    // key. If we were to overwrite old entries, we would either
    // * Have to gain ownership over an existing entry to overwrite it, which
    // would only work if there are no outstanding (read) references and would
    // create a small gap in availability of the entry (old or new) to lookups.
    // * Have to insert into a suboptimal location (more probes) so that the
    // old entry can be kept around as well.

    // Set initial clock data from priority
    // TODO: configuration parameters for priority handling and clock cycle
    // count?
    uint64_t initial_countdown;
    switch (priority) {
      case Cache::Priority::HIGH:
        initial_countdown = ClockHandle::kHighCountdown;
        break;
      default:
        assert(false);
        FALLTHROUGH_INTENDED;
      case Cache::Priority::LOW:
        initial_countdown = ClockHandle::kLowCountdown;
        break;
      case Cache::Priority::BOTTOM:
        initial_countdown = ClockHandle::kBottomCountdown;
        break;
    }
    assert(initial_countdown > 0);

    uint32_t probe = 0;
    ClockHandle* e = FindSlot(
        proto.hash,
        [&](ClockHandle* h) {
          // Optimistically transition the slot from "empty" to
          // "under construction" (no effect on other states)
          uint64_t old_meta =
              h->meta.fetch_or(uint64_t{ClockHandle::kStateOccupiedBit}
                                   << ClockHandle::kStateShift,
                               std::memory_order_acq_rel);
          uint64_t old_state = old_meta >> ClockHandle::kStateShift;

          if (old_state == ClockHandle::kStateEmpty) {
            // We've started inserting into an available slot, and taken
            // ownership Save data fields
            ClockHandleMoreData* h_alias = h;
            *h_alias = proto;

            // Transition from "under construction" state to "visible" state
            uint64_t new_meta = uint64_t{ClockHandle::kStateVisible}
                                << ClockHandle::kStateShift;

            // Maybe with an outstanding reference
            new_meta |= initial_countdown << ClockHandle::kAcquireCounterShift;
            new_meta |= (initial_countdown - (handle != nullptr))
                        << ClockHandle::kReleaseCounterShift;

#ifndef NDEBUG
            // Save the state transition, with assertion
            old_meta = h->meta.exchange(new_meta, std::memory_order_release);
            assert(old_meta >> ClockHandle::kStateShift ==
                   ClockHandle::kStateConstruction);
#else
            // Save the state transition
            h->meta.store(new_meta, std::memory_order_release);
#endif
            return true;
          } else if (old_state != ClockHandle::kStateVisible) {
            // Slot not usable / touchable now
            return false;
          }
          // Existing, visible entry, which might be a match.
          // But first, we need to acquire a ref to read it. In fact, number of
          // refs for initial countdown, so that we boost the clock state if
          // this is a match.
          old_meta = h->meta.fetch_add(
              ClockHandle::kAcquireIncrement * initial_countdown,
              std::memory_order_acq_rel);
          // Like Lookup
          if ((old_meta >> ClockHandle::kStateShift) ==
              ClockHandle::kStateVisible) {
            // Acquired a read reference
            if (h->key == proto.key) {
              // Match. Release in a way that boosts the clock state
              old_meta = h->meta.fetch_add(
                  ClockHandle::kReleaseIncrement * initial_countdown,
                  std::memory_order_acq_rel);
              // Correct for possible (but rare) overflow
              CorrectNearOverflow(old_meta, h->meta);
              // Insert detached instead (only if return handle needed)
              use_detached_insert = true;
              return true;
            } else {
              // Mismatch. Pretend we never took the reference
              old_meta = h->meta.fetch_sub(
                  ClockHandle::kAcquireIncrement * initial_countdown,
                  std::memory_order_acq_rel);
            }
          } else if (UNLIKELY((old_meta >> ClockHandle::kStateShift) ==
                              ClockHandle::kStateInvisible)) {
            // Pretend we never took the reference
            // WART: there's a tiny chance we release last ref to invisible
            // entry here. If that happens, we let eviction take care of it.
            old_meta = h->meta.fetch_sub(
                ClockHandle::kAcquireIncrement * initial_countdown,
                std::memory_order_acq_rel);
          } else {
            // For other states, incrementing the acquire counter has no effect
            // so we don't need to undo it.
            // Slot not usable / touchable now.
          }
          (void)old_meta;
          return false;
        },
        [&](ClockHandle* /*h*/) { return false; },
        [&](ClockHandle* h) {
          h->displacements.fetch_add(1, std::memory_order_relaxed);
        },
        probe);
    if (e == nullptr) {
      // Occupancy check and never abort FindSlot above should generally
      // prevent this, except it's theoretically possible for other threads
      // to evict and replace entries in the right order to hit every slot
      // when it is populated. Assuming random hashing, the chance of that
      // should be no higher than pow(kStrictLoadFactor, n) for n slots.
      // That should be infeasible for roughly n >= 256, so if this assertion
      // fails, that suggests something is going wrong.
      assert(GetTableSize() < 256);
      use_detached_insert = true;
    }
    if (!use_detached_insert) {
      // Successfully inserted
      if (handle) {
        *handle = e;
      }
      return Status::OK();
    }
    // Roll back table insertion
    Rollback(proto.hash, e);
    revert_occupancy_fn();
    // Maybe fall back on detached insert
    if (handle == nullptr) {
      revert_usage_fn();
      // As if unrefed entry immdiately evicted
      proto.FreeData();
      return Status::OK();
    }
  }

  // Run detached insert
  assert(use_detached_insert);

  ClockHandle* h = new ClockHandle();
  ClockHandleMoreData* h_alias = h;
  *h_alias = proto;
  h->detached = true;
  // Single reference (detached entries only created if returning a refed
  // Handle back to user)
  uint64_t meta = uint64_t{ClockHandle::kStateInvisible}
                  << ClockHandle::kStateShift;
  meta |= uint64_t{1} << ClockHandle::kAcquireCounterShift;
  h->meta.store(meta, std::memory_order_release);
  // Keep track of usage
  detached_usage_.fetch_add(total_charge, std::memory_order_relaxed);

  *handle = h;
  // The OkOverwritten status is used to count "redundant" insertions into
  // block cache. This implementation doesn't strictly check for redundant
  // insertions, but we instead are probably interested in how many insertions
  // didn't go into the table (instead "detached"), which could be redundant
  // Insert or some other reason (use_detached_insert reasons above).
  return Status::OkOverwritten();
}

ClockHandle* ClockHandleTable::Lookup(const CacheKeyBytes& key, uint32_t hash) {
  uint32_t probe = 0;
  ClockHandle* e = FindSlot(
      hash,
      [&](ClockHandle* h) {
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
          if (h->key == key) {
            // Match
            return true;
          } else {
            // Mismatch. Pretend we never took the reference
            old_meta = h->meta.fetch_sub(ClockHandle::kAcquireIncrement,
                                         std::memory_order_release);
          }
        } else if (UNLIKELY((old_meta >> ClockHandle::kStateShift) ==
                            ClockHandle::kStateInvisible)) {
          // Pretend we never took the reference
          // WART: there's a tiny chance we release last ref to invisible
          // entry here. If that happens, we let eviction take care of it.
          old_meta = h->meta.fetch_sub(ClockHandle::kAcquireIncrement,
                                       std::memory_order_release);
        } else {
          // For other states, incrementing the acquire counter has no effect
          // so we don't need to undo it. Furthermore, we cannot safely undo
          // it because we did not acquire a read reference to lock the
          // entry in a Shareable state.
        }
        (void)old_meta;
        return false;
      },
      [&](ClockHandle* h) {
        return h->displacements.load(std::memory_order_relaxed) == 0;
      },
      [&](ClockHandle* /*h*/) {}, probe);

  return e;
}

bool ClockHandleTable::Release(ClockHandle* h, bool useful,
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
    // TODO? Delay freeing?
    h->FreeData();
    size_t total_charge = h->total_charge;
    if (UNLIKELY(h->detached)) {
      // Delete detached handle
      delete h;
      detached_usage_.fetch_sub(total_charge, std::memory_order_relaxed);
    } else {
      uint32_t hash = h->hash;
#ifndef NDEBUG
      // Mark slot as empty, with assertion
      old_meta = h->meta.exchange(0, std::memory_order_release);
      assert(old_meta >> ClockHandle::kStateShift ==
             ClockHandle::kStateConstruction);
#else
      // Mark slot as empty
      h->meta.store(0, std::memory_order_release);
#endif
      occupancy_.fetch_sub(1U, std::memory_order_release);
      Rollback(hash, h);
    }
    usage_.fetch_sub(total_charge, std::memory_order_relaxed);
    assert(usage_.load(std::memory_order_relaxed) < SIZE_MAX / 2);
    return true;
  } else {
    // Correct for possible (but rare) overflow
    CorrectNearOverflow(old_meta, h->meta);
    return false;
  }
}

void ClockHandleTable::Ref(ClockHandle& h) {
  // Increment acquire counter
  uint64_t old_meta = h.meta.fetch_add(ClockHandle::kAcquireIncrement,
                                       std::memory_order_acquire);

  assert((old_meta >> ClockHandle::kStateShift) &
         ClockHandle::kStateShareableBit);
  // Must have already had a reference
  assert(GetRefcount(old_meta) > 0);
  (void)old_meta;
}

void ClockHandleTable::TEST_RefN(ClockHandle& h, size_t n) {
  // Increment acquire counter
  uint64_t old_meta = h.meta.fetch_add(n * ClockHandle::kAcquireIncrement,
                                       std::memory_order_acquire);

  assert((old_meta >> ClockHandle::kStateShift) &
         ClockHandle::kStateShareableBit);
  (void)old_meta;
}

void ClockHandleTable::TEST_ReleaseN(ClockHandle* h, size_t n) {
  if (n > 0) {
    // Split into n - 1 and 1 steps.
    uint64_t old_meta = h->meta.fetch_add(
        (n - 1) * ClockHandle::kReleaseIncrement, std::memory_order_acquire);
    assert((old_meta >> ClockHandle::kStateShift) &
           ClockHandle::kStateShareableBit);
    (void)old_meta;

    Release(h, /*useful*/ true, /*erase_if_last_ref*/ false);
  }
}

void ClockHandleTable::Erase(const CacheKeyBytes& key, uint32_t hash) {
  uint32_t probe = 0;
  (void)FindSlot(
      hash,
      [&](ClockHandle* h) {
        // Could be multiple entries in rare cases. Erase them all.
        // Optimistically increment acquire counter
        uint64_t old_meta = h->meta.fetch_add(ClockHandle::kAcquireIncrement,
                                              std::memory_order_acquire);
        // Check if it's an entry visible to lookups
        if ((old_meta >> ClockHandle::kStateShift) ==
            ClockHandle::kStateVisible) {
          // Acquired a read reference
          if (h->key == key) {
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
                h->meta.fetch_sub(ClockHandle::kAcquireIncrement,
                                  std::memory_order_release);
                break;
              } else if (h->meta.compare_exchange_weak(
                             old_meta,
                             uint64_t{ClockHandle::kStateConstruction}
                                 << ClockHandle::kStateShift,
                             std::memory_order_acq_rel)) {
                // Took ownership
                assert(hash == h->hash);
                // TODO? Delay freeing?
                h->FreeData();
                usage_.fetch_sub(h->total_charge, std::memory_order_relaxed);
                assert(usage_.load(std::memory_order_relaxed) < SIZE_MAX / 2);
#ifndef NDEBUG
                // Mark slot as empty, with assertion
                old_meta = h->meta.exchange(0, std::memory_order_release);
                assert(old_meta >> ClockHandle::kStateShift ==
                       ClockHandle::kStateConstruction);
#else
                // Mark slot as empty
                h->meta.store(0, std::memory_order_release);
#endif
                occupancy_.fetch_sub(1U, std::memory_order_release);
                Rollback(hash, h);
                break;
              }
            }
          } else {
            // Mismatch. Pretend we never took the reference
            h->meta.fetch_sub(ClockHandle::kAcquireIncrement,
                              std::memory_order_release);
          }
        } else if (UNLIKELY((old_meta >> ClockHandle::kStateShift) ==
                            ClockHandle::kStateInvisible)) {
          // Pretend we never took the reference
          // WART: there's a tiny chance we release last ref to invisible
          // entry here. If that happens, we let eviction take care of it.
          h->meta.fetch_sub(ClockHandle::kAcquireIncrement,
                            std::memory_order_release);
        } else {
          // For other states, incrementing the acquire counter has no effect
          // so we don't need to undo it.
        }
        return false;
      },
      [&](ClockHandle* h) {
        return h->displacements.load(std::memory_order_relaxed) == 0;
      },
      [&](ClockHandle* /*h*/) {}, probe);
}

void ClockHandleTable::ConstApplyToEntriesRange(
    std::function<void(const ClockHandle&)> func, uint32_t index_begin,
    uint32_t index_end, bool apply_if_will_be_deleted) const {
  uint64_t check_state_mask = ClockHandle::kStateShareableBit;
  if (!apply_if_will_be_deleted) {
    check_state_mask |= ClockHandle::kStateVisibleBit;
  }

  for (uint32_t i = index_begin; i < index_end; i++) {
    ClockHandle& h = array_[i];

    // Note: to avoid using compare_exchange, we have to be extra careful.
    uint64_t old_meta = h.meta.load(std::memory_order_relaxed);
    // Check if it's an entry visible to lookups
    if ((old_meta >> ClockHandle::kStateShift) & check_state_mask) {
      // Increment acquire counter. Note: it's possible that the entry has
      // completely changed since we loaded old_meta, but incrementing acquire
      // count is always safe. (Similar to optimistic Lookup here.)
      old_meta = h.meta.fetch_add(ClockHandle::kAcquireIncrement,
                                  std::memory_order_acquire);
      // Check whether we actually acquired a reference.
      if ((old_meta >> ClockHandle::kStateShift) &
          ClockHandle::kStateShareableBit) {
        // Apply func if appropriate
        if ((old_meta >> ClockHandle::kStateShift) & check_state_mask) {
          func(h);
        }
        // Pretend we never took the reference
        h.meta.fetch_sub(ClockHandle::kAcquireIncrement,
                         std::memory_order_release);
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

void ClockHandleTable::EraseUnRefEntries() {
  for (uint32_t i = 0; i <= this->length_bits_mask_; i++) {
    ClockHandle& h = array_[i];

    uint64_t old_meta = h.meta.load(std::memory_order_relaxed);
    if (old_meta & (uint64_t{ClockHandle::kStateShareableBit}
                    << ClockHandle::kStateShift) &&
        GetRefcount(old_meta) == 0 &&
        h.meta.compare_exchange_strong(old_meta,
                                       uint64_t{ClockHandle::kStateConstruction}
                                           << ClockHandle::kStateShift,
                                       std::memory_order_acquire)) {
      // Took ownership
      uint32_t hash = h.hash;
      h.FreeData();
      usage_.fetch_sub(h.total_charge, std::memory_order_relaxed);
#ifndef NDEBUG
      // Mark slot as empty, with assertion
      old_meta = h.meta.exchange(0, std::memory_order_release);
      assert(old_meta >> ClockHandle::kStateShift ==
             ClockHandle::kStateConstruction);
#else
      // Mark slot as empty
      h.meta.store(0, std::memory_order_release);
#endif
      occupancy_.fetch_sub(1U, std::memory_order_release);
      Rollback(hash, &h);
    }
  }
}

namespace {
inline uint32_t Remix1(uint32_t hash) {
  return Lower32of64((uint64_t{hash} * 0xbc9f1d35) >> 29);
}

inline uint32_t Remix2(uint32_t hash) {
  return Lower32of64((uint64_t{hash} * 0x7a2bb9d5) >> 29);
}
}  // namespace

ClockHandle* ClockHandleTable::FindSlot(
    uint32_t hash, std::function<bool(ClockHandle*)> match_fn,
    std::function<bool(ClockHandle*)> abort_fn,
    std::function<void(ClockHandle*)> update_fn, uint32_t& probe) {
  // We use double-hashing probing. Every probe in the sequence is a
  // pseudorandom integer, computed as a linear function of two random hashes,
  // which we call base and increment. Specifically, the i-th probe is base + i
  // * increment modulo the table size.
  uint32_t base = ModTableSize(Remix1(hash));
  // We use an odd increment, which is relatively prime with the power-of-two
  // table size. This implies that we cycle back to the first probe only
  // after probing every slot exactly once.
  // TODO: we could also reconsider linear probing, though locality benefits
  // are limited because each slot is a full cache line
  uint32_t increment = Remix2(hash) | 1U;
  uint32_t current = ModTableSize(base + probe * increment);
  while (probe <= length_bits_mask_) {
    ClockHandle* h = &array_[current];
    if (match_fn(h)) {
      probe++;
      return h;
    }
    if (abort_fn(h)) {
      return nullptr;
    }
    probe++;
    update_fn(h);
    current = ModTableSize(current + increment);
  }
  // We looped back.
  return nullptr;
}

void ClockHandleTable::Rollback(uint32_t hash, const ClockHandle* h) {
  uint32_t current = ModTableSize(Remix1(hash));
  uint32_t increment = Remix2(hash) | 1U;
  for (uint32_t i = 0; &array_[current] != h; i++) {
    array_[current].displacements.fetch_sub(1, std::memory_order_relaxed);
    current = ModTableSize(current + increment);
  }
}

void ClockHandleTable::Evict(size_t requested_charge, size_t* freed_charge,
                             uint32_t* freed_count) {
  // precondition
  assert(requested_charge > 0);

  // TODO: make a tuning parameter?
  constexpr uint32_t step_size = 4;

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
    for (uint32_t i = 0; i < step_size; i++) {
      ClockHandle& h = array_[ModTableSize(Lower32of64(old_clock_pointer + i))];
      uint64_t meta = h.meta.load(std::memory_order_relaxed);

      uint64_t acquire_count = (meta >> ClockHandle::kAcquireCounterShift) &
                               ClockHandle::kCounterMask;
      uint64_t release_count = (meta >> ClockHandle::kReleaseCounterShift) &
                               ClockHandle::kCounterMask;
      if (acquire_count != release_count) {
        // Only clock update entries with no outstanding refs
        continue;
      }
      if (!((meta >> ClockHandle::kStateShift) &
            ClockHandle::kStateShareableBit)) {
        // Only clock update Shareable entries
        continue;
      }
      if ((meta >> ClockHandle::kStateShift == ClockHandle::kStateVisible) &&
          acquire_count > 0) {
        // Decrement clock
        uint64_t new_count = std::min(acquire_count - 1,
                                      uint64_t{ClockHandle::kMaxCountdown} - 1);
        // Compare-exchange in the decremented clock info, but
        // not aggressively
        uint64_t new_meta =
            (uint64_t{ClockHandle::kStateVisible} << ClockHandle::kStateShift) |
            (new_count << ClockHandle::kReleaseCounterShift) |
            (new_count << ClockHandle::kAcquireCounterShift);
        h.meta.compare_exchange_strong(meta, new_meta,
                                       std::memory_order_relaxed);
        continue;
      }
      // Otherwise, remove entry (either unreferenced invisible or
      // unreferenced and expired visible). Compare-exchange failing probably
      // indicates the entry was used, so skip it in that case.
      if (h.meta.compare_exchange_strong(
              meta,
              uint64_t{ClockHandle::kStateConstruction}
                  << ClockHandle::kStateShift,
              std::memory_order_acquire)) {
        // Took ownership
        uint32_t hash = h.hash;
        // TODO? Delay freeing?
        h.FreeData();
        *freed_charge += h.total_charge;
#ifndef NDEBUG
        // Mark slot as empty, with assertion
        meta = h.meta.exchange(0, std::memory_order_release);
        assert(meta >> ClockHandle::kStateShift ==
               ClockHandle::kStateConstruction);
#else
        // Mark slot as empty
        h.meta.store(0, std::memory_order_release);
#endif
        *freed_count += 1;
        Rollback(hash, &h);
      }
    }

    // Loop exit condition
    if (*freed_charge >= requested_charge) {
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

ClockCacheShard::ClockCacheShard(
    size_t capacity, size_t estimated_value_size, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy)
    : CacheShard(metadata_charge_policy),
      table_(
          CalcHashBits(capacity, estimated_value_size, metadata_charge_policy),
          /*initial_charge_metadata*/ metadata_charge_policy ==
              kFullChargeCacheMetadata),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit) {
  // Initial charge metadata should not exceed capacity
  assert(table_.GetUsage() <= capacity_ || capacity_ < sizeof(ClockHandle));
}

void ClockCacheShard::EraseUnRefEntries() { table_.EraseUnRefEntries(); }

void ClockCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, void* value, size_t charge,
                             DeleterFn deleter)>& callback,
    uint32_t average_entries_per_lock, uint32_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  uint32_t length_bits = table_.GetLengthBits();
  uint32_t length = table_.GetTableSize();

  assert(average_entries_per_lock > 0);
  // Assuming we are called with same average_entries_per_lock repeatedly,
  // this simplifies some logic (index_end will not overflow).
  assert(average_entries_per_lock < length || *state == 0);

  uint32_t index_begin = *state >> (32 - length_bits);
  uint32_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end.
    index_end = length;
    *state = UINT32_MAX;
  } else {
    *state = index_end << (32 - length_bits);
  }

  table_.ConstApplyToEntriesRange(
      [callback](const ClockHandle& h) {
        callback(h.KeySlice(), h.value, h.total_charge, h.deleter);
      },
      index_begin, index_end, false);
}

int ClockCacheShard::CalcHashBits(
    size_t capacity, size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  double average_slot_charge = estimated_value_size * kLoadFactor;
  if (metadata_charge_policy == kFullChargeCacheMetadata) {
    average_slot_charge += sizeof(ClockHandle);
  }
  assert(average_slot_charge > 0.0);
  uint64_t num_slots =
      static_cast<uint64_t>(capacity / average_slot_charge + 0.999999);

  int hash_bits = std::min(FloorLog2((num_slots << 1) - 1), 32);
  if (metadata_charge_policy == kFullChargeCacheMetadata) {
    // For very small estimated value sizes, it's possible to overshoot
    while (hash_bits > 0 &&
           uint64_t{sizeof(ClockHandle)} << hash_bits > capacity) {
      hash_bits--;
    }
  }
  return hash_bits;
}

void ClockCacheShard::SetCapacity(size_t capacity) {
  capacity_.store(capacity, std::memory_order_relaxed);
  // next Insert will take care of any necessary evictions
}

void ClockCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  strict_capacity_limit_.store(strict_capacity_limit,
                               std::memory_order_relaxed);
  // next Insert will take care of any necessary evictions
}

Status ClockCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                               size_t charge, Cache::DeleterFn deleter,
                               Cache::Handle** handle,
                               Cache::Priority priority) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return Status::NotSupported("ClockCache only supports key size " +
                                std::to_string(kCacheKeySize) + "B");
  }
  ClockHandleMoreData proto;
  proto.key = *reinterpret_cast<const CacheKeyBytes*>(key.data());
  proto.hash = hash;
  proto.value = value;
  proto.deleter = deleter;
  proto.total_charge = charge;
  Status s =
      table_.Insert(proto, reinterpret_cast<ClockHandle**>(handle), priority,
                    capacity_.load(std::memory_order_relaxed),
                    strict_capacity_limit_.load(std::memory_order_relaxed));
  return s;
}

Cache::Handle* ClockCacheShard::Lookup(const Slice& key, uint32_t hash) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return nullptr;
  }
  auto key_bytes = reinterpret_cast<const CacheKeyBytes*>(key.data());
  return reinterpret_cast<Cache::Handle*>(table_.Lookup(*key_bytes, hash));
}

bool ClockCacheShard::Ref(Cache::Handle* h) {
  if (h == nullptr) {
    return false;
  }
  table_.Ref(*reinterpret_cast<ClockHandle*>(h));
  return true;
}

bool ClockCacheShard::Release(Cache::Handle* handle, bool useful,
                              bool erase_if_last_ref) {
  if (handle == nullptr) {
    return false;
  }
  return table_.Release(reinterpret_cast<ClockHandle*>(handle), useful,
                        erase_if_last_ref);
}

void ClockCacheShard::TEST_RefN(Cache::Handle* h, size_t n) {
  table_.TEST_RefN(*reinterpret_cast<ClockHandle*>(h), n);
}

void ClockCacheShard::TEST_ReleaseN(Cache::Handle* h, size_t n) {
  table_.TEST_ReleaseN(reinterpret_cast<ClockHandle*>(h), n);
}

bool ClockCacheShard::Release(Cache::Handle* handle, bool erase_if_last_ref) {
  return Release(handle, /*useful=*/true, erase_if_last_ref);
}

void ClockCacheShard::Erase(const Slice& key, uint32_t hash) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return;
  }
  auto key_bytes = reinterpret_cast<const CacheKeyBytes*>(key.data());
  table_.Erase(*key_bytes, hash);
}

size_t ClockCacheShard::GetUsage() const { return table_.GetUsage(); }

size_t ClockCacheShard::GetPinnedUsage() const {
  // Computes the pinned usage by scanning the whole hash table. This
  // is slow, but avoids keeping an exact counter on the clock usage,
  // i.e., the number of not externally referenced elements.
  // Why avoid this counter? Because Lookup removes elements from the clock
  // list, so it would need to update the pinned usage every time,
  // which creates additional synchronization costs.
  size_t table_pinned_usage = 0;
  const bool charge_metadata =
      metadata_charge_policy_ == kFullChargeCacheMetadata;
  table_.ConstApplyToEntriesRange(
      [&table_pinned_usage, charge_metadata](const ClockHandle& h) {
        uint64_t meta = h.meta.load(std::memory_order_relaxed);
        uint64_t refcount = GetRefcount(meta);
        // Holding one ref for ConstApplyToEntriesRange
        assert(refcount > 0);
        if (refcount > 1) {
          table_pinned_usage += h.total_charge;
          if (charge_metadata) {
            table_pinned_usage += sizeof(ClockHandle);
          }
        }
      },
      0, table_.GetTableSize(), true);

  return table_pinned_usage + table_.GetDetachedUsage();
}

size_t ClockCacheShard::GetOccupancyCount() const {
  return table_.GetOccupancy();
}

size_t ClockCacheShard::GetTableAddressCount() const {
  return table_.GetTableSize();
}

HyperClockCache::HyperClockCache(
    size_t capacity, size_t estimated_value_size, int num_shard_bits,
    bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit),
      num_shards_(1 << num_shard_bits) {
  assert(estimated_value_size > 0 ||
         metadata_charge_policy != kDontChargeCacheMetadata);
  // TODO: should not need to go through two levels of pointer indirection to
  // get to table entries
  shards_ = reinterpret_cast<ClockCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(ClockCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        ClockCacheShard(per_shard, estimated_value_size, strict_capacity_limit,
                        metadata_charge_policy);
  }
}

HyperClockCache::~HyperClockCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~ClockCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* HyperClockCache::GetShard(uint32_t shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* HyperClockCache::GetShard(uint32_t shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* HyperClockCache::Value(Handle* handle) {
  return reinterpret_cast<const ClockHandle*>(handle)->value;
}

size_t HyperClockCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const ClockHandle*>(handle)->total_charge;
}

Cache::DeleterFn HyperClockCache::GetDeleter(Handle* handle) const {
  auto h = reinterpret_cast<const ClockHandle*>(handle);
  return h->deleter;
}

uint32_t HyperClockCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const ClockHandle*>(handle)->hash;
}

void HyperClockCache::DisownData() {
  // Leak data only if that won't generate an ASAN/valgrind warning.
  if (!kMustFreeHeapAllocations) {
    shards_ = nullptr;
    num_shards_ = 0;
  }
}

}  // namespace hyper_clock_cache

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
  auto my_num_shard_bits = num_shard_bits;
  if (my_num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  if (my_num_shard_bits < 0) {
    // Use larger shard size to reduce risk of large entries clustering
    // or skewing individual shards.
    constexpr size_t min_shard_size = 32U * 1024U * 1024U;
    my_num_shard_bits = GetDefaultCacheShardBits(capacity, min_shard_size);
  }
  return std::make_shared<hyper_clock_cache::HyperClockCache>(
      capacity, estimated_entry_charge, my_num_shard_bits,
      strict_capacity_limit, metadata_charge_policy);
}

}  // namespace ROCKSDB_NAMESPACE
