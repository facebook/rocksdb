//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/utilities/sim_cache.h"
#include <atomic>
#include "port/port.h"
#include "util/statistics.h"

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
        miss_times_(0),
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
                        Handle** handle, Priority priority) override {
    // The handle and value passed in are for real cache, so we pass nullptr
    // to key_only_cache_ for both instead. Also, the deleter function pointer
    // will be called by user to perform some external operation which should
    // be applied only once. Thus key_only_cache accepts an empty function.
    // *Lambda function without capture can be assgined to a function pointer
    Handle* h = key_only_cache_->Lookup(key);
    if (h == nullptr) {
      key_only_cache_->Insert(key, nullptr, charge,
                              [](const Slice& k, void* v) {}, nullptr,
                              priority);
    } else {
      key_only_cache_->Release(h);
    }
    return cache_->Insert(key, value, charge, deleter, handle, priority);
  }

  virtual Handle* Lookup(const Slice& key, Statistics* stats) override {
    Handle* h = key_only_cache_->Lookup(key);
    if (h != nullptr) {
      key_only_cache_->Release(h);
      inc_hit_counter();
      RecordTick(stats, SIM_BLOCK_CACHE_HIT);
    } else {
      inc_miss_counter();
      RecordTick(stats, SIM_BLOCK_CACHE_MISS);
    }
    return cache_->Lookup(key, stats);
  }

  virtual bool Ref(Handle* handle) override { return cache_->Ref(handle); }

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

  virtual uint64_t get_miss_counter() const override {
    return miss_times_.load(std::memory_order_relaxed);
  }

  virtual uint64_t get_hit_counter() const override {
    return hit_times_.load(std::memory_order_relaxed);
  }

  virtual void reset_counter() override {
    miss_times_.store(0, std::memory_order_relaxed);
    hit_times_.store(0, std::memory_order_relaxed);
    SetTickerCount(stats_, SIM_BLOCK_CACHE_HIT, 0);
    SetTickerCount(stats_, SIM_BLOCK_CACHE_MISS, 0);
  }

  virtual std::string ToString() const override {
    std::string res;
    res.append("SimCache MISSes: " + std::to_string(get_miss_counter()) + "\n");
    res.append("SimCache HITs:    " + std::to_string(get_hit_counter()) + "\n");
    char buff[350];
    auto lookups = get_miss_counter() + get_hit_counter();
    snprintf(buff, sizeof(buff), "SimCache HITRATE: %.2f%%\n",
             (lookups == 0 ? 0 : get_hit_counter() * 100.0f / lookups));
    res.append(buff);
    return res;
  }

  virtual std::string GetPrintableOptions() const override {
    std::string ret;
    ret.reserve(20000);
    ret.append("    cache_options:\n");
    ret.append(cache_->GetPrintableOptions());
    ret.append("    sim_cache_options:\n");
    ret.append(key_only_cache_->GetPrintableOptions());
    return ret;
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> key_only_cache_;
  std::atomic<uint64_t> miss_times_;
  std::atomic<uint64_t> hit_times_;
  Statistics* stats_;
  void inc_miss_counter() {
    miss_times_.fetch_add(1, std::memory_order_relaxed);
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
