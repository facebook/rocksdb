//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "port/port.h"
#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {

class ConcurrentCacheReservationManager;

// A cache interface which wraps around another cache and takes care of
// reserving space in block cache towards a single global memory limit, and
// forwards all the calls to the underlying cache.
class ChargedCache : public Cache {
 public:
  ChargedCache(std::shared_ptr<Cache> cache,
               std::shared_ptr<Cache> block_cache);
  ~ChargedCache() override = default;

  Status Insert(const Slice& key, ObjectPtr obj, const CacheItemHelper* helper,
                size_t charge, Handle** handle = nullptr,
                Priority priority = Priority::LOW) override;

  Cache::Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                        CreateContext* create_context,
                        Priority priority = Priority::LOW, bool wait = true,
                        Statistics* stats = nullptr) override;

  bool Release(Cache::Handle* handle, bool useful,
               bool erase_if_last_ref = false) override;
  bool Release(Cache::Handle* handle, bool erase_if_last_ref = false) override;

  void Erase(const Slice& key) override;
  void EraseUnRefEntries() override;

  static const char* kClassName() { return "ChargedCache"; }
  const char* Name() const override { return kClassName(); }

  uint64_t NewId() override { return cache_->NewId(); }

  void SetCapacity(size_t capacity) override;

  void SetStrictCapacityLimit(bool strict_capacity_limit) override {
    cache_->SetStrictCapacityLimit(strict_capacity_limit);
  }

  bool HasStrictCapacityLimit() const override {
    return cache_->HasStrictCapacityLimit();
  }

  ObjectPtr Value(Cache::Handle* handle) override {
    return cache_->Value(handle);
  }

  bool IsReady(Cache::Handle* handle) override {
    return cache_->IsReady(handle);
  }

  void Wait(Cache::Handle* handle) override { cache_->Wait(handle); }

  void WaitAll(std::vector<Handle*>& handles) override {
    cache_->WaitAll(handles);
  }

  bool Ref(Cache::Handle* handle) override { return cache_->Ref(handle); }

  size_t GetCapacity() const override { return cache_->GetCapacity(); }

  size_t GetUsage() const override { return cache_->GetUsage(); }

  size_t GetUsage(Cache::Handle* handle) const override {
    return cache_->GetUsage(handle);
  }

  size_t GetPinnedUsage() const override { return cache_->GetPinnedUsage(); }

  size_t GetCharge(Cache::Handle* handle) const override {
    return cache_->GetCharge(handle);
  }

  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override {
    return cache_->GetCacheItemHelper(handle);
  }

  void ApplyToAllEntries(
      const std::function<void(const Slice& key, ObjectPtr value, size_t charge,
                               const CacheItemHelper* helper)>& callback,
      const Cache::ApplyToAllEntriesOptions& opts) override {
    cache_->ApplyToAllEntries(callback, opts);
  }

  std::string GetPrintableOptions() const override {
    return cache_->GetPrintableOptions();
  }

  void DisownData() override { return cache_->DisownData(); }

  inline Cache* GetCache() const { return cache_.get(); }

  inline ConcurrentCacheReservationManager* TEST_GetCacheReservationManager()
      const {
    return cache_res_mgr_.get();
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<ConcurrentCacheReservationManager> cache_res_mgr_;
};

}  // namespace ROCKSDB_NAMESPACE
