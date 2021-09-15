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

class ClockCache final : public ShardedCache {
 private:
  CacheOptions options_;
  explicit ClockCache(const CacheOptions& options);

 public:
  static bool IsClockCacheSupported();
  static Status CreateClockCache(const CacheOptions& options,
                                 std::unique_ptr<ClockCache>* cache);
  static Status CreateClockCache(std::unique_ptr<ClockCache>* cache);

  ~ClockCache() override;

  static const char* kClassName() { return "ClockCache"; }
  const char* Name() const override { return kClassName(); }

  Status PrepareOptions(const ConfigOptions& config_options) override;

  CacheShard* GetShard(uint32_t shard) override;
  const CacheShard* GetShard(uint32_t shard) const override;
  void* Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  uint32_t GetHash(Handle* handle) const override;
  DeleterFn GetDeleter(Handle* handle) const override;

  void DisownData() override {
#ifndef MUST_FREE_HEAP_ALLOCATIONS
    shards_ = nullptr;
#endif
  }

  void WaitAll(std::vector<Handle*>& /*handles*/) override {}

 private:
  ClockCacheShard* shards_;
};

}  // namespace ROCKSDB_NAMESPACE
