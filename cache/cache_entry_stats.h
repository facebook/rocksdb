//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "port/lang.h"
#include "rocksdb/cache.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/coding_lean.h"

namespace ROCKSDB_NAMESPACE {

// A generic helper object for gathering stats about cache entries by
// iterating over them with ApplyToAllEntries. This class essentially
// solves the problem of slowing down a Cache with too many stats
// collectors that could be sharing stat results, such as from multiple
// column families or multiple DBs sharing a Cache. We employ a few
// mitigations:
// * Only one collector for a particular kind of Stats is alive
// for each Cache. This is guaranteed using the Cache itself to hold
// the collector.
// * A mutex ensures only one thread is gathering stats for this
// collector.
// * The most recent gathered stats are saved and simply copied to
// satisfy requests within a time window (default: 3 minutes) of
// completion of the most recent stat gathering.
//
// Template parameter Stats must be copyable and trivially constructable,
// as well as...
// concept Stats {
//   // Notification before applying callback to all entries
//   void BeginCollection(Cache*, SystemClock*, uint64_t start_time_micros);
//   // Get the callback to apply to all entries. `callback`
//   // type must be compatible with Cache::ApplyToAllEntries
//   callback GetEntryCallback();
//   // Notification after applying callback to all entries
//   void EndCollection(Cache*, SystemClock*, uint64_t end_time_micros);
//   // Notification that a collection was skipped because of
//   // sufficiently recent saved results.
//   void SkippedCollection();
// }
template <class Stats>
class CacheEntryStatsCollector {
 public:
  // Gather and save stats if saved stats are too old. (Use GetStats() to
  // read saved stats.)
  //
  // Maximum allowed age for a "hit" on saved results is determined by the
  // two interval parameters. Both set to 0 forces a re-scan. For example
  // with min_interval_seconds=300 and min_interval_factor=100, if the last
  // scan took 10s, we would only rescan ("miss") if the age in seconds of
  // the saved results is > max(300, 100*10).
  // Justification: scans can vary wildly in duration, e.g. from 0.02 sec
  // to as much as 20 seconds, so we want to be able to cap the absolute
  // and relative frequency of scans.
  void CollectStats(int min_interval_seconds, int min_interval_factor) {
    // Waits for any pending reader or writer (collector)
    std::lock_guard<std::mutex> lock(working_mutex_);

    uint64_t max_age_micros =
        static_cast<uint64_t>(std::max(min_interval_seconds, 0)) * 1000000U;

    if (last_end_time_micros_ > last_start_time_micros_ &&
        min_interval_factor > 0) {
      max_age_micros = std::max(
          max_age_micros, min_interval_factor * (last_end_time_micros_ -
                                                 last_start_time_micros_));
    }

    uint64_t start_time_micros = clock_->NowMicros();
    if ((start_time_micros - last_end_time_micros_) > max_age_micros) {
      last_start_time_micros_ = start_time_micros;
      working_stats_.BeginCollection(cache_, clock_, start_time_micros);

      cache_->ApplyToAllEntries(working_stats_.GetEntryCallback(), {});
      TEST_SYNC_POINT_CALLBACK(
          "CacheEntryStatsCollector::GetStats:AfterApplyToAllEntries", nullptr);

      uint64_t end_time_micros = clock_->NowMicros();
      last_end_time_micros_ = end_time_micros;
      working_stats_.EndCollection(cache_, clock_, end_time_micros);
    } else {
      working_stats_.SkippedCollection();
    }

    // Save so that we don't need to wait for an outstanding collection in
    // order to make of copy of the last saved stats
    std::lock_guard<std::mutex> lock2(saved_mutex_);
    saved_stats_ = working_stats_;
  }

  // Gets saved stats, regardless of age
  void GetStats(Stats *stats) {
    std::lock_guard<std::mutex> lock(saved_mutex_);
    *stats = saved_stats_;
  }

  Cache *GetCache() const { return cache_; }

  // Gets or creates a shared instance of CacheEntryStatsCollector in the
  // cache itself, and saves into `ptr`. This shared_ptr will hold the
  // entry in cache until all refs are destroyed.
  static Status GetShared(Cache *cache, SystemClock *clock,
                          std::shared_ptr<CacheEntryStatsCollector> *ptr) {
    const Slice &cache_key = GetCacheKey();

    Cache::Handle *h = cache->Lookup(cache_key);
    if (h == nullptr) {
      // Not yet in cache, but Cache doesn't provide a built-in way to
      // avoid racing insert. So we double-check under a shared mutex,
      // inspired by TableCache.
      STATIC_AVOID_DESTRUCTION(std::mutex, static_mutex);
      std::lock_guard<std::mutex> lock(static_mutex);

      h = cache->Lookup(cache_key);
      if (h == nullptr) {
        auto new_ptr = new CacheEntryStatsCollector(cache, clock);
        // TODO: non-zero charge causes some tests that count block cache
        // usage to go flaky. Fix the problem somehow so we can use an
        // accurate charge.
        size_t charge = 0;
        Status s = cache->Insert(cache_key, new_ptr, charge, Deleter, &h,
                                 Cache::Priority::HIGH);
        if (!s.ok()) {
          assert(h == nullptr);
          delete new_ptr;
          return s;
        }
      }
    }
    // If we reach here, shared entry is in cache with handle `h`.
    assert(cache->GetDeleter(h) == Deleter);

    // Build an aliasing shared_ptr that keeps `ptr` in cache while there
    // are references.
    *ptr = MakeSharedCacheHandleGuard<CacheEntryStatsCollector>(cache, h);
    return Status::OK();
  }

 private:
  explicit CacheEntryStatsCollector(Cache *cache, SystemClock *clock)
      : saved_stats_(),
        working_stats_(),
        last_start_time_micros_(0),
        last_end_time_micros_(/*pessimistic*/ 10000000),
        cache_(cache),
        clock_(clock) {}

  static void Deleter(const Slice &, void *value) {
    delete static_cast<CacheEntryStatsCollector *>(value);
  }

  static const Slice &GetCacheKey() {
    // For each template instantiation
    static CacheKey ckey = CacheKey::CreateUniqueForProcessLifetime();
    static Slice ckey_slice = ckey.AsSlice();
    return ckey_slice;
  }

  std::mutex saved_mutex_;
  Stats saved_stats_;

  std::mutex working_mutex_;
  Stats working_stats_;
  uint64_t last_start_time_micros_;
  uint64_t last_end_time_micros_;

  Cache *const cache_;
  SystemClock *const clock_;
};

}  // namespace ROCKSDB_NAMESPACE
