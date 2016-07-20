//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/utilities/sim_cache.h"
#include <atomic>
#include "port/port.h"

namespace rocksdb {

namespace {
// SimCacheImpl definition
class SimCacheImpl : public SimCache {
 public:
  // capacity for real cache (ShardedLRUCache)
  // test_capacity for key only cache
  SimCacheImpl(std::shared_ptr<Cache> cache, size_t sim_capacity,
               int num_shard_bits)
      : cache_(cache),
        key_only_cache_(NewLRUCache(sim_capacity, num_shard_bits)),
        lookup_times_(0),
        hit_times_(0) {}

  virtual ~SimCacheImpl() {}
  virtual void SetCapacity(size_t capacity) override {
    cache_->SetCapacity(capacity);
  }

  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override {
    cache_->SetStrictCapacityLimit(strict_capacity_limit);
  }

  virtual Status Insert(const Slice& key, void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Handle** handle) override {
    // The handle and value passed in are for real cache, so we pass nullptr
    // to key_only_cache_ for both instead. Also, the deleter function pointer
    // will be called by user to perform some external operation which should
    // be applied only once. Thus key_only_cache accepts an empty function.
    // *Lambda function without capture can be assgined to a function pointer
    Handle* h = key_only_cache_->Lookup(key);
    if (h == nullptr) {
      key_only_cache_->Insert(key, nullptr, charge,
                              [](const Slice& k, void* v) {}, nullptr);
    } else {
      key_only_cache_->Release(h);
    }
    return cache_->Insert(key, value, charge, deleter, handle);
  }

  virtual Handle* Lookup(const Slice& key) override {
    inc_lookup_counter();
    Handle* h = key_only_cache_->Lookup(key);
    if (h != nullptr) {
      key_only_cache_->Release(h);
      inc_hit_counter();
    }
    return cache_->Lookup(key);
  }

  virtual void Release(Handle* handle) override { cache_->Release(handle); }

  virtual void Erase(const Slice& key) override {
    cache_->Erase(key);
    key_only_cache_->Erase(key);
  }

  virtual void* Value(Handle* handle) override { return cache_->Value(handle); }

  virtual uint64_t NewId() override { return cache_->NewId(); }

  virtual size_t GetCapacity() const override { return cache_->GetCapacity(); }

  virtual bool HasStrictCapacityLimit() const override {
    return cache_->HasStrictCapacityLimit();
  }

  virtual size_t GetUsage() const override { return cache_->GetUsage(); }

  virtual size_t GetUsage(Handle* handle) const override {
    return cache_->GetUsage(handle);
  }

  virtual size_t GetPinnedUsage() const override {
    return cache_->GetPinnedUsage();
  }

  virtual void DisownData() override {
    cache_->DisownData();
    key_only_cache_->DisownData();
  }

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override {
    // only apply to _cache since key_only_cache doesn't hold value
    cache_->ApplyToAllCacheEntries(callback, thread_safe);
  }

  virtual void EraseUnRefEntries() override {
    cache_->EraseUnRefEntries();
    key_only_cache_->EraseUnRefEntries();
  }

  virtual size_t GetSimCapacity() const override {
    return key_only_cache_->GetCapacity();
  }
  virtual size_t GetSimUsage() const override {
    return key_only_cache_->GetUsage();
  }
  virtual void SetSimCapacity(size_t capacity) override {
    key_only_cache_->SetCapacity(capacity);
  }

  virtual uint64_t get_lookup_counter() const override {
    return lookup_times_.load(std::memory_order_relaxed);
  }

  virtual uint64_t get_hit_counter() const override {
    return hit_times_.load(std::memory_order_relaxed);
  }

  virtual double get_hit_rate() const override {
    return get_hit_counter() * 1.0f / get_lookup_counter();
  }
  virtual void reset_counter() override {
    lookup_times_.store(0, std::memory_order_relaxed);
    hit_times_.store(0, std::memory_order_relaxed);
  }

  virtual std::string ToString() const override {
    std::string res;
    res.append("SimCache LOOKUPs: " + std::to_string(get_lookup_counter()) +
               "\n");
    res.append("SimCache HITs:    " + std::to_string(get_hit_counter()) + "\n");
    char buff[100];
    snprintf(buff, sizeof(buff), "SimCache HITRATE: %.2f%%\n",
             get_hit_rate() * 100);
    res.append(buff);
    return res;
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> key_only_cache_;
  std::atomic<uint64_t> lookup_times_;
  std::atomic<uint64_t> hit_times_;
  void inc_lookup_counter() {
    lookup_times_.fetch_add(1, std::memory_order_relaxed);
  }
  void inc_hit_counter() { hit_times_.fetch_add(1, std::memory_order_relaxed); }
};

}  // end anonymous namespace

// For instrumentation purpose, use NewSimCache instead
std::shared_ptr<SimCache> NewSimCache(std::shared_ptr<Cache> cache,
                                      size_t sim_capacity, int num_shard_bits) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  return std::make_shared<SimCacheImpl>(cache, sim_capacity, num_shard_bits);
}

}  // end namespace rocksdb
