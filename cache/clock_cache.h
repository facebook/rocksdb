// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "cache/sharded_cache.h"
#include "rocksdb/cache.h"

#if defined(TBB) && !defined(ROCKSDB_LITE)
#define SUPPORT_CLOCK_CACHE
#endif  // TBB
namespace ROCKSDB_NAMESPACE {
class ClockCacheShard;

struct ClockCacheOptions {
  ClockCacheOptions() {}
  ClockCacheOptions(
      size_t _capacity, int _num_shard_bits, bool _strict_capacity_limit,
      CacheMetadataChargePolicy _metadata_charge_policy =
          kDefaultCacheMetadataChargePolicy,
      const std::shared_ptr<MemoryAllocator>& _memory_allocator = nullptr)
      : capacity(_capacity),
        num_shard_bits(_num_shard_bits),
        strict_capacity_limit(_strict_capacity_limit),
        memory_allocator(_memory_allocator),
        metadata_charge_policy(_metadata_charge_policy) {}
  static const char* kName() { return "ClockCacheOptions"; }

  // Capacity of the cache.
  size_t capacity = 0;

  // Cache is sharded into 2^num_shard_bits shards,
  // by hash of key. Refer to NewClockCache for further
  // information.
  int num_shard_bits = -1;

  // insert to the cache will fail when cache is full.
  bool strict_capacity_limit = false;

  // If non-nullptr will use this allocator instead of system allocator when
  // allocating memory for cache blocks. Call this method before you start using
  // the cache!
  //
  // Caveat: when the cache is used as block cache, the memory allocator is
  // ignored when dealing with compression libraries that allocate memory
  // internally (currently only XPRESS).
  std::shared_ptr<MemoryAllocator> memory_allocator;

  CacheMetadataChargePolicy metadata_charge_policy =
      kDefaultCacheMetadataChargePolicy;
};

class ClockCache final : public ShardedCache {
 private:
  explicit ClockCache(const ClockCacheOptions& options);

 public:
  static bool IsClockCacheSupported();
  static Status CreateClockCache(const ClockCacheOptions& options,
                                 std::unique_ptr<ClockCache>* cache);
  static Status CreateClockCache(std::unique_ptr<ClockCache>* cache);

  ~ClockCache() override;

  static const char* kClassName() { return "ClockCache"; }
  const char* Name() const override { return kClassName(); }
  Status PrepareOptions(const ConfigOptions& options) override;
  bool IsMutable() const override;
  virtual std::string GetPrintableOptions() const override;

  virtual void SetCapacity(size_t capacity) override;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;
  virtual size_t GetCapacity() const override;
  virtual bool HasStrictCapacityLimit() const override;

  CacheShard* GetShard(uint32_t shard) override;
  const CacheShard* GetShard(uint32_t shard) const override;
  void* Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  uint32_t GetHash(Handle* handle) const override;
  DeleterFn GetDeleter(Handle* handle) const override;

  void DisownData() override;

  void WaitAll(std::vector<Handle*>& /*handles*/) override {}
  MemoryAllocator* memory_allocator() const override {
    return options_.memory_allocator.get();
  }

 private:
  ClockCacheOptions options_;
  ClockCacheShard* shards_;
};

}  // namespace ROCKSDB_NAMESPACE
