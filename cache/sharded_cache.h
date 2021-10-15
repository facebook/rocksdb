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

#include "port/port.h"
#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {

// Single cache shard interface.
class CacheShard {
 public:
  CacheShard() = default;
  virtual ~CacheShard() = default;

  using DeleterFn = Cache::DeleterFn;
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge, DeleterFn deleter,
                        Cache::Handle** handle, Cache::Priority priority) = 0;
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        const Cache::CacheItemHelper* helper, size_t charge,
                        Cache::Handle** handle, Cache::Priority priority) = 0;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) = 0;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash,
                                const Cache::CacheItemHelper* helper,
                                const Cache::CreateCallback& create_cb,
                                Cache::Priority priority, bool wait,
                                Statistics* stats) = 0;
  virtual bool Release(Cache::Handle* handle, bool useful,
                       bool force_erase) = 0;
  virtual bool IsReady(Cache::Handle* handle) = 0;
  virtual void Wait(Cache::Handle* handle) = 0;
  virtual bool Ref(Cache::Handle* handle) = 0;
  virtual bool Release(Cache::Handle* handle, bool force_erase) = 0;
  virtual void Erase(const Slice& key, uint32_t hash) = 0;
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
class ShardedCache : public Cache {
 public:
  ShardedCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
               std::shared_ptr<MemoryAllocator> memory_allocator = nullptr);
  virtual ~ShardedCache() = default;
  virtual CacheShard* GetShard(uint32_t shard) = 0;
  virtual const CacheShard* GetShard(uint32_t shard) const = 0;

  virtual uint32_t GetHash(Handle* handle) const = 0;

  virtual void SetCapacity(size_t capacity) override;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  virtual Status Insert(const Slice& key, void* value, size_t charge,
                        DeleterFn deleter, Handle** handle,
                        Priority priority) override;
  virtual Status Insert(const Slice& key, void* value,
                        const CacheItemHelper* helper, size_t chargge,
                        Handle** handle = nullptr,
                        Priority priority = Priority::LOW) override;
  virtual Handle* Lookup(const Slice& key, Statistics* stats) override;
  virtual Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                         const CreateCallback& create_cb, Priority priority,
                         bool wait, Statistics* stats = nullptr) override;
  virtual bool Release(Handle* handle, bool useful,
                       bool force_erase = false) override;
  virtual bool IsReady(Handle* handle) override;
  virtual void Wait(Handle* handle) override;
  virtual bool Ref(Handle* handle) override;
  virtual bool Release(Handle* handle, bool force_erase = false) override;
  virtual void Erase(const Slice& key) override;
  virtual uint64_t NewId() override;
  virtual size_t GetCapacity() const override;
  virtual bool HasStrictCapacityLimit() const override;
  virtual size_t GetUsage() const override;
  virtual size_t GetUsage(Handle* handle) const override;
  virtual size_t GetPinnedUsage() const override;
  virtual void ApplyToAllEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      const ApplyToAllEntriesOptions& opts) override;
  virtual void EraseUnRefEntries() override;
  virtual std::string GetPrintableOptions() const override;

  int GetNumShardBits() const;
  uint32_t GetNumShards() const;

 protected:
  inline uint32_t Shard(uint32_t hash) { return hash & shard_mask_; }

 private:
  const uint32_t shard_mask_;
  mutable port::Mutex capacity_mutex_;
  size_t capacity_;
  bool strict_capacity_limit_;
  std::atomic<uint64_t> last_id_;
};

extern int GetDefaultCacheShardBits(size_t capacity);

}  // namespace ROCKSDB_NAMESPACE
