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
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {
struct ConfigOptions;

// Single cache shard interface.
class CacheShard {
 public:
  CacheShard() = default;
  virtual ~CacheShard() = default;

  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Cache::Handle** handle, Cache::Priority priority) = 0;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) = 0;
  virtual bool Ref(Cache::Handle* handle) = 0;
  virtual bool Release(Cache::Handle* handle, bool force_erase = false) = 0;
  virtual void Erase(const Slice& key, uint32_t hash) = 0;
  virtual void SetCapacity(size_t capacity) = 0;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;
  virtual size_t GetUsage() const = 0;
  virtual size_t GetPinnedUsage() const = 0;
  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) = 0;
  virtual void EraseUnRefEntries() = 0;
  virtual std::string GetPrintableOptions() const { return ""; }
  void set_metadata_charge_policy(
      CacheMetadataChargePolicy metadata_charge_policy) {
    metadata_charge_policy_ = metadata_charge_policy;
  }

 protected:
  CacheMetadataChargePolicy metadata_charge_policy_ = kDontChargeCacheMetadata;
};

struct ShardedCacheOptions {
  // Capacity of the cache.
  size_t capacity = 0;

  // Cache is sharded into 2^num_shard_bits shards,
  // by hash of key. Refer to NewLRUCache for further
  // information.
  int num_shard_bits = -1;

  // If strict_capacity_limit is set,
  // insert to the cache will fail when cache is full.
  bool strict_capacity_limit = false;

  CacheMetadataChargePolicy metadata_charge_policy =
      kDefaultCacheMetadataChargePolicy;

  ShardedCacheOptions() {}
  ShardedCacheOptions(size_t _capacity, int _num_shard_bits,
                      bool _strict_capacity_limit,
                      CacheMetadataChargePolicy _metadata_charge_policy =
                          kDefaultCacheMetadataChargePolicy)
      : capacity(_capacity),
        num_shard_bits(_num_shard_bits),
        strict_capacity_limit(_strict_capacity_limit),
        metadata_charge_policy(_metadata_charge_policy) {}
};

// Generic cache interface which shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are sharded by the highest num_shard_bits bits of hash value.
class ShardedCache : public Cache {
 protected:
  ShardedCache(
      size_t capacity, int num_shard_bits, bool strict_capacity_limit,
      CacheMetadataChargePolicy metadata_charge_policy =
          kDontChargeCacheMetadata,
      const std::shared_ptr<MemoryAllocator>& memory_allocator = nullptr);

 public:
  virtual ~ShardedCache() = default;
  virtual CacheShard* GetShard(int shard) = 0;
  virtual const CacheShard* GetShard(int shard) const = 0;
  virtual void* Value(Handle* handle) override = 0;
  virtual size_t GetCharge(Handle* handle) const override = 0;

  virtual uint32_t GetHash(Handle* handle) const = 0;
  virtual void DisownData() override = 0;

  virtual void SetCapacity(size_t capacity) override;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  virtual Status Insert(const Slice& key, void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Handle** handle, Priority priority) override;
  virtual Handle* Lookup(const Slice& key, Statistics* stats) override;
  virtual bool Ref(Handle* handle) override;
  virtual bool Release(Handle* handle, bool force_erase = false) override;
  virtual void Erase(const Slice& key) override;
  virtual uint64_t NewId() override;
  virtual size_t GetCapacity() const override;
  virtual bool HasStrictCapacityLimit() const override;
  virtual size_t GetUsage() const override;
  virtual size_t GetUsage(Handle* handle) const override;
  virtual size_t GetPinnedUsage() const override;
  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;
  virtual void EraseUnRefEntries() override;
  virtual std::string GetPrintableOptions() const override;

  int GetNumShardBits() const { return options_.num_shard_bits; }
  Status PrepareOptions(const ConfigOptions& opts) override;

 protected:
  int GetNumShards() const { return 1 << options_.num_shard_bits; }
  ShardedCacheOptions options_;

 private:
  static inline uint32_t HashSlice(const Slice& s) {
    return static_cast<uint32_t>(GetSliceNPHash64(s));
  }

  uint32_t Shard(uint32_t hash) {
    // Note, hash >> 32 yields hash in gcc, not the zero we expect!
    return (options_.num_shard_bits > 0)
               ? (hash >> (32 - options_.num_shard_bits))
               : 0;
  }

  mutable port::Mutex capacity_mutex_;
  std::atomic<uint64_t> last_id_;
};

extern int GetDefaultCacheShardBits(size_t capacity);

}  // namespace ROCKSDB_NAMESPACE
