//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "cache/cache_reservation_manager.h"
#include "rocksdb/secondary_cache.h"

namespace ROCKSDB_NAMESPACE {

class CacheWithSecondaryAdapter : public CacheWrapper {
 public:
  explicit CacheWithSecondaryAdapter(
      std::shared_ptr<Cache> target,
      std::shared_ptr<SecondaryCache> secondary_cache,
      bool distribute_cache_res = false);

  ~CacheWithSecondaryAdapter() override;

  Status Insert(const Slice& key, ObjectPtr value,
                const CacheItemHelper* helper, size_t charge,
                Handle** handle = nullptr,
                Priority priority = Priority::LOW) override;

  Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                 CreateContext* create_context,
                 Priority priority = Priority::LOW,
                 Statistics* stats = nullptr) override;

  using Cache::Release;
  bool Release(Handle* handle, bool erase_if_last_ref = false) override;

  ObjectPtr Value(Handle* handle) override;

  void StartAsyncLookup(AsyncLookupHandle& async_handle) override;

  void WaitAll(AsyncLookupHandle* async_handles, size_t count) override;

  std::string GetPrintableOptions() const override;

  const char* Name() const override;

  Cache* TEST_GetCache() { return target_.get(); }

  SecondaryCache* TEST_GetSecondaryCache() { return secondary_cache_.get(); }

 private:
  bool EvictionHandler(const Slice& key, Handle* handle);

  void StartAsyncLookupOnMySecondary(AsyncLookupHandle& async_handle);

  Handle* Promote(
      std::unique_ptr<SecondaryCacheResultHandle>&& secondary_handle,
      const Slice& key, const CacheItemHelper* helper, Priority priority,
      Statistics* stats, bool found_dummy_entry, bool kept_in_sec_cache);

  bool ProcessDummyResult(Cache::Handle** handle, bool erase);

  void CleanupCacheObject(ObjectPtr obj, const CacheItemHelper* helper);

  std::shared_ptr<SecondaryCache> secondary_cache_;
  // Whether to proportionally distribute cache memory reservations, i.e
  // placeholder entries with null value and a non-zero charge, across
  // the primary and secondary caches.
  bool distribute_cache_res_;
  // A cache reservation manager to keep track of secondary cache memory
  // usage by reserving equivalent capacity against the primary cache
  std::shared_ptr<ConcurrentCacheReservationManager> pri_cache_res_;
  // Fraction of a cache memory reservation to be assigned to the secondary
  // cache
  double sec_cache_res_ratio_;
};

}  // namespace ROCKSDB_NAMESPACE
