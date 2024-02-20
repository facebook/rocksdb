//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "port/port.h"
#include "rocksdb/advanced_cache.h"

namespace ROCKSDB_NAMESPACE {

class ConcurrentCacheReservationManager;

// A cache interface which wraps around another cache and takes care of
// reserving space in block cache towards a single global memory limit, and
// forwards all the calls to the underlying cache.
class ChargedCache : public CacheWrapper {
 public:
  ChargedCache(std::shared_ptr<Cache> cache,
               std::shared_ptr<Cache> block_cache);

  Status Insert(
      const Slice& key, ObjectPtr obj, const CacheItemHelper* helper,
      size_t charge, Handle** handle = nullptr,
      Priority priority = Priority::LOW, const Slice& compressed_val = Slice(),
      CompressionType type = CompressionType::kNoCompression) override;

  Cache::Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                        CreateContext* create_context,
                        Priority priority = Priority::LOW,
                        Statistics* stats = nullptr) override;

  void WaitAll(AsyncLookupHandle* async_handles, size_t count) override;

  bool Release(Cache::Handle* handle, bool useful,
               bool erase_if_last_ref = false) override;
  bool Release(Cache::Handle* handle, bool erase_if_last_ref = false) override;

  void Erase(const Slice& key) override;
  void EraseUnRefEntries() override;

  static const char* kClassName() { return "ChargedCache"; }
  const char* Name() const override { return kClassName(); }

  void SetCapacity(size_t capacity) override;

  inline Cache* GetCache() const { return target_.get(); }

  inline ConcurrentCacheReservationManager* TEST_GetCacheReservationManager()
      const {
    return cache_res_mgr_.get();
  }

 private:
  std::shared_ptr<ConcurrentCacheReservationManager> cache_res_mgr_;
};

}  // namespace ROCKSDB_NAMESPACE
