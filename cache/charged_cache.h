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

// Generic cache interface which shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are sharded by the highest num_shard_bits bits of hash value.
class ChargedCache : public Cache {
 public:
  ChargedCache(std::shared_ptr<Cache> cache,
               std::shared_ptr<Cache> block_cache);
  virtual ~ChargedCache() = default;

  virtual Status Insert(const Slice& key, void* value, size_t charge,
                        DeleterFn deleter, Handle** handle,
                        Priority priority) override;
  virtual Status Insert(const Slice& key, void* value,
                        const CacheItemHelper* helper, size_t charge,
                        Handle** handle = nullptr,
                        Priority priority = Priority::LOW) override;

  virtual Cache::Handle* Lookup(const Slice& key, Statistics* stats) override;
  virtual Cache::Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                                const CreateCallback& create_cb,
                                Priority priority, bool wait,
                                Statistics* stats = nullptr) override;

  virtual bool Release(Cache::Handle* handle, bool useful,
                       bool erase_if_last_ref = false) override;
  virtual bool Release(Cache::Handle* handle,
                       bool erase_if_last_ref = false) override;

  virtual void Erase(const Slice& key) override;
  virtual void EraseUnRefEntries() override;

  static const char* kClassName() { return "ChargedCache"; }
  inline virtual const char* Name() const override { return kClassName(); }

  inline virtual uint64_t NewId() override { return cache_->NewId(); }

  inline virtual void SetCapacity(size_t capacity) override {
    cache_->SetCapacity(capacity);
  }

  inline virtual void SetStrictCapacityLimit(
      bool strict_capacity_limit) override {
    cache_->SetStrictCapacityLimit(strict_capacity_limit);
  }

  inline virtual bool HasStrictCapacityLimit() const override {
    return cache_->HasStrictCapacityLimit();
  }

  inline virtual void* Value(Cache::Handle* handle) override {
    return cache_->Value(handle);
  }

  inline virtual bool IsReady(Cache::Handle* handle) override {
    return cache_->IsReady(handle);
  }

  inline virtual void Wait(Cache::Handle* handle) override {
    cache_->Wait(handle);
  }

  inline virtual bool Ref(Cache::Handle* handle) override {
    return cache_->Ref(handle);
  }

  inline virtual size_t GetCapacity() const override {
    return cache_->GetCapacity();
  }

  inline virtual size_t GetUsage() const override { return cache_->GetUsage(); }

  inline virtual size_t GetUsage(Cache::Handle* handle) const override {
    return cache_->GetUsage(handle);
  }

  inline virtual size_t GetPinnedUsage() const override {
    return cache_->GetPinnedUsage();
  }

  inline virtual size_t GetCharge(Cache::Handle* handle) const override {
    return cache_->GetCharge(handle);
  }

  inline virtual Cache::DeleterFn GetDeleter(
      Cache::Handle* handle) const override {
    return cache_->GetDeleter(handle);
  }

  inline virtual void ApplyToAllEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               Cache::DeleterFn deleter)>& callback,
      const Cache::ApplyToAllEntriesOptions& opts) override {
    cache_->ApplyToAllEntries(callback, opts);
  }

  inline virtual std::string GetPrintableOptions() const override {
    return cache_->GetPrintableOptions();
  }

  inline Cache* GetCache() const { return cache_.get(); }

  inline ConcurrentCacheReservationManager* GetCacheReservationManager() const {
    return cache_res_mgr_.get();
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<ConcurrentCacheReservationManager> cache_res_mgr_;
};

}  // namespace ROCKSDB_NAMESPACE
