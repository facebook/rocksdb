//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/secondary_cache.h"

namespace ROCKSDB_NAMESPACE {

class CacheWithSecondaryAdapter : public CacheWrapper {
 public:
  explicit CacheWithSecondaryAdapter(
      std::shared_ptr<Cache> target,
      std::shared_ptr<SecondaryCache> secondary_cache);

  ~CacheWithSecondaryAdapter() override;

  Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                 CreateContext* create_context,
                 Priority priority = Priority::LOW,
                 Statistics* stats = nullptr) override;

  ObjectPtr Value(Handle* handle) override;

  void StartAsyncLookup(AsyncLookupHandle& async_handle) override;

  void WaitAll(AsyncLookupHandle* async_handles, size_t count) override;

  std::string GetPrintableOptions() const override;

  const char* Name() const override;

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
};

}  // namespace ROCKSDB_NAMESPACE
