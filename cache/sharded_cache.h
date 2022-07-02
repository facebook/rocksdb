//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <string>
#include <algorithm>
#include <cstdint>
#include <memory>

#include "util/math.h"
#include "util/mutexlock.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "util/hash.h"


namespace ROCKSDB_NAMESPACE {

// Single cache shard interface.
template<typename T>
class CacheShard {
 public:
  CacheShard() = default;
  virtual ~CacheShard() = default;

  using DeleterFn = Cache::DeleterFn;
  virtual Status Insert(const Slice& key, T hash, void* value,
                        size_t charge, DeleterFn deleter,
                        Cache::Handle** handle, Cache::Priority priority) = 0;
  virtual Status Insert(const Slice& key, T hash, void* value,
                        const Cache::CacheItemHelper* helper, size_t charge,
                        Cache::Handle** handle, Cache::Priority priority) = 0;
  virtual Cache::Handle* Lookup(const Slice& key, T hash) = 0;
  virtual Cache::Handle* Lookup(const Slice& key, T hash,
                                const Cache::CacheItemHelper* helper,
                                const Cache::CreateCallback& create_cb,
                                Cache::Priority priority, bool wait,
                                Statistics* stats) = 0;
  virtual bool Release(Cache::Handle* handle, bool useful,
                       bool erase_if_last_ref) = 0;
  virtual bool IsReady(Cache::Handle* handle) = 0;
  virtual void Wait(Cache::Handle* handle) = 0;
  virtual bool Ref(Cache::Handle* handle) = 0;
  virtual bool Release(Cache::Handle* handle, bool erase_if_last_ref) = 0;
  virtual void Erase(const Slice& key, T hash) = 0;
  virtual void SetCapacity(size_t capacity) = 0;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;
  virtual size_t GetUsage() const = 0;
  virtual size_t GetPinnedUsage() const = 0;
  // Handles iterating over roughly `average_entries_per_lock` entries, using
  // `state` to somehow record where it last ended up. Caller initially uses
  // *state == 0 and implementation sets *state = UINT32_MAX to indicate
  // completion.
  virtual void ApplyToSomeEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      uint32_t average_entries_per_lock, uint32_t* state) = 0;
  virtual void EraseUnRefEntries() = 0;
  virtual std::string GetPrintableOptions() const { return ""; }
  void set_metadata_charge_policy(
      CacheMetadataChargePolicy metadata_charge_policy) {
    metadata_charge_policy_ = metadata_charge_policy;
  }

 protected:
  CacheMetadataChargePolicy metadata_charge_policy_ = kDontChargeCacheMetadata;
};

// Generic cache interface which shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are sharded by the highest num_shard_bits bits of hash value.
// There are three template parameters:
// - T: The type of the hashes.
// - Hasher: Implements inline static T hash(const Slice& s). This
//    function computes a hash, provided a key.
// - ShardExtractor: Implements
//    inline static uint32_t extract(T hash, uint32_t shard_mask).
//    This function computes the index of a shard, given a hash and the shard
//    mask.
template<typename T, typename Hasher, typename ShardExtractor>
class ShardedCache : public Cache {
 public:
  ShardedCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
               std::shared_ptr<MemoryAllocator> memory_allocator = nullptr) :
      Cache(std::move(memory_allocator)),
      shard_mask_((uint32_t{1} << num_shard_bits) - 1),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      last_id_(1) {}

  virtual ~ShardedCache() = default;
  virtual CacheShard<T>* GetShard(uint32_t shard) = 0;
  virtual const CacheShard<T>* GetShard(uint32_t shard) const = 0;

  virtual uint32_t GetHash(Handle* handle) const = 0;

  virtual void SetCapacity(size_t capacity) override {
    uint32_t num_shards = GetNumShards();
    const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
    MutexLock l(&capacity_mutex_);
    for (uint32_t s = 0; s < num_shards; s++) {
      GetShard(s)->SetCapacity(per_shard);
    }
    capacity_ = capacity;
  }

  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override {
    uint32_t num_shards = GetNumShards();
    MutexLock l(&capacity_mutex_);
    for (uint32_t s = 0; s < num_shards; s++) {
      GetShard(s)->SetStrictCapacityLimit(strict_capacity_limit);
    }
    strict_capacity_limit_ = strict_capacity_limit;
  }

  virtual Status Insert(const Slice& key, void* value, size_t charge,
                        DeleterFn deleter, Handle** handle,
                        Priority priority) override {
    T hash = Hash(key);
    return GetShard(Shard(hash))
        ->Insert(key, hash, value, charge, deleter, handle, priority);
  }

  virtual Status Insert(const Slice& key, void* value,
                        const CacheItemHelper* helper, size_t charge,
                        Handle** handle = nullptr,
                        Priority priority = Priority::LOW) override {
    T hash = Hash(key);
    if (!helper) {
      return Status::InvalidArgument();
    }
    return GetShard(Shard(hash))
        ->Insert(key, hash, value, helper, charge, handle, priority);
  }

  virtual Handle* Lookup(const Slice& key, Statistics* /* stats */) override {
    T hash = Hash(key);
    return GetShard(Shard(hash))->Lookup(key, hash);
  }

  virtual Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                         const CreateCallback& create_cb, Priority priority,
                         bool wait, Statistics* stats = nullptr) override {
    T hash = Hash(key);
    return GetShard(Shard(hash))
        ->Lookup(key, hash, helper, create_cb, priority, wait, stats);
  }

  virtual bool Release(Handle* handle, bool useful,
                       bool erase_if_last_ref = false) override {
    T hash = GetHash(handle);
    return GetShard(Shard(hash))->Release(handle, useful, erase_if_last_ref);
  }

  virtual bool Release(Handle* handle, bool erase_if_last_ref = false) override {
    T hash = GetHash(handle);
    return GetShard(Shard(hash))->Release(handle, erase_if_last_ref);
  }

  virtual bool IsReady(Handle* handle) override {
    T hash = GetHash(handle);
    return GetShard(Shard(hash))->IsReady(handle);
  }

  virtual void Wait(Handle* handle) override {
    T hash = GetHash(handle);
    GetShard(Shard(hash))->Wait(handle);
  }

  virtual bool Ref(Handle* handle) override {
    T hash = GetHash(handle);
    return GetShard(Shard(hash))->Ref(handle);
  }

  virtual void Erase(const Slice& key) override {
    T hash = Hash(key);
    GetShard(Shard(hash))->Erase(key, hash);
  }

  virtual uint64_t NewId() override {
    return last_id_.fetch_add(1, std::memory_order_relaxed);
  }

  virtual size_t GetCapacity() const override  {
    MutexLock l(&capacity_mutex_);
    return capacity_;
  }

  virtual bool HasStrictCapacityLimit() const override {
    MutexLock l(&capacity_mutex_);
    return strict_capacity_limit_;
  }

  virtual size_t GetUsage() const override {
    // We will not lock the cache when getting the usage from shards.
    uint32_t num_shards = GetNumShards();
    size_t usage = 0;
    for (uint32_t s = 0; s < num_shards; s++) {
      usage += GetShard(s)->GetUsage();
    }
    return usage;
  }

  virtual size_t GetUsage(Handle* handle) const override {
    return GetCharge(handle);
  }

  virtual size_t GetPinnedUsage() const override {
    // We will not lock the cache when getting the usage from shards.
    uint32_t num_shards = GetNumShards();
    size_t usage = 0;
    for (uint32_t s = 0; s < num_shards; s++) {
      usage += GetShard(s)->GetPinnedUsage();
    }
    return usage;
  }

  virtual void ApplyToAllEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      const ApplyToAllEntriesOptions& opts) override {
    uint32_t num_shards = GetNumShards();
    // Iterate over part of each shard, rotating between shards, to
    // minimize impact on latency of concurrent operations.
    std::unique_ptr<uint32_t[]> states(new uint32_t[num_shards]{});

    uint32_t aepl_in_32 = static_cast<uint32_t>(
        std::min(size_t{UINT32_MAX}, opts.average_entries_per_lock));
    aepl_in_32 = std::min(aepl_in_32, uint32_t{1});

    bool remaining_work;
    do {
      remaining_work = false;
      for (uint32_t s = 0; s < num_shards; s++) {
        if (states[s] != UINT32_MAX) {
          GetShard(s)->ApplyToSomeEntries(callback, aepl_in_32, &states[s]);
          remaining_work |= states[s] != UINT32_MAX;
        }
      }
    } while (remaining_work);
  }

  virtual void EraseUnRefEntries() override {
    uint32_t num_shards = GetNumShards();
    for (uint32_t s = 0; s < num_shards; s++) {
      GetShard(s)->EraseUnRefEntries();
    }
  }

  virtual std::string GetPrintableOptions() const override {
    std::string ret;
    ret.reserve(20000);
    const int kBufferSize = 200;
    char buffer[kBufferSize];
    {
      MutexLock l(&capacity_mutex_);
      snprintf(buffer, kBufferSize, "    capacity : %" ROCKSDB_PRIszt "\n",
              capacity_);
      ret.append(buffer);
      snprintf(buffer, kBufferSize, "    num_shard_bits : %d\n",
              GetNumShardBits());
      ret.append(buffer);
      snprintf(buffer, kBufferSize, "    strict_capacity_limit : %d\n",
              strict_capacity_limit_);
      ret.append(buffer);
    }
    snprintf(buffer, kBufferSize, "    memory_allocator : %s\n",
            memory_allocator() ? memory_allocator()->Name() : "None");
    ret.append(buffer);
    ret.append(GetShard(0)->GetPrintableOptions());
    return ret;
  }

  int GetNumShardBits() const { return BitsSetToOne(shard_mask_); }
  uint32_t GetNumShards() const { return shard_mask_ + 1; }

  static int GetDefaultCacheShardBits(size_t capacity) {
    int num_shard_bits = 0;
    size_t min_shard_size = 512L * 1024L;  // Every shard is at least 512KB.
    size_t num_shards = capacity / min_shard_size;
    while (num_shards >>= 1) {
      if (++num_shard_bits >= 6) {
        // No more than 6.
        return num_shard_bits;
      }
    }
    return num_shard_bits;
  }

 protected:
  inline T Hash(const Slice& key) { return Hasher::hash(key); }
  inline uint32_t Shard(T hash) { return ShardExtractor::extract(hash, shard_mask_); }

 private:
  const uint32_t shard_mask_;
  mutable port::Mutex capacity_mutex_;
  size_t capacity_;
  bool strict_capacity_limit_;
  std::atomic<uint64_t> last_id_;
};

class Hasher32 {
 public:
  inline static uint32_t hash(const Slice& s) {
    return Lower32of64(GetSliceNPHash64(s));
  }
};

class ShardExtractor32 {
 public:
  inline static uint32_t extract(uint32_t hash, uint32_t shard_mask) {
     return hash & shard_mask;
  }
};

template class ShardedCache<uint32_t, Hasher32, ShardExtractor32>;

using ShardedCache32 = ShardedCache<uint32_t, Hasher32, ShardExtractor32>;

using CacheShard32 = CacheShard<uint32_t>;

}  // namespace ROCKSDB_NAMESPACE
