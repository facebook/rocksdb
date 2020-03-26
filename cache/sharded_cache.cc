//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/sharded_cache.h"

#include <algorithm>
#include <string>

#include "util/math.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

ShardedCache::ShardedCache(size_t capacity, int num_shard_bits,
                           bool strict_capacity_limit,
                           std::shared_ptr<MemoryAllocator> allocator)
    : Cache(std::move(allocator)),
      num_shard_bits_(num_shard_bits),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      last_id_(1) {}

void ShardedCache::SetCapacity(size_t capacity) {
  int num_shards = 1 << num_shard_bits_;
  const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetCapacity(per_shard);
  }
  capacity_ = capacity;
}

void ShardedCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  int num_shards = 1 << num_shard_bits_;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetStrictCapacityLimit(strict_capacity_limit);
  }
  strict_capacity_limit_ = strict_capacity_limit;
}

Status ShardedCache::Insert(const Slice& key, void* value, size_t charge,
                            void (*deleter)(const Slice& key, void* value),
                            Handle** handle, Priority priority) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))
      ->Insert(key, hash, value, charge, deleter, handle, priority);
}

Cache::Handle* ShardedCache::Lookup(const Slice& key, Statistics* /*stats*/) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->Lookup(key, hash);
}

bool ShardedCache::Ref(Handle* handle) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Ref(handle);
}

bool ShardedCache::Release(Handle* handle, bool force_erase) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Release(handle, force_erase);
}

void ShardedCache::Erase(const Slice& key) {
  uint32_t hash = HashSlice(key);
  GetShard(Shard(hash))->Erase(key, hash);
}

uint64_t ShardedCache::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
}

size_t ShardedCache::GetCapacity() const {
  MutexLock l(&capacity_mutex_);
  return capacity_;
}

bool ShardedCache::HasStrictCapacityLimit() const {
  MutexLock l(&capacity_mutex_);
  return strict_capacity_limit_;
}

size_t ShardedCache::GetUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetUsage();
  }
  return usage;
}

size_t ShardedCache::GetUsage(Handle* handle) const {
  return GetCharge(handle);
}

size_t ShardedCache::GetPinnedUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetPinnedUsage();
  }
  return usage;
}

void ShardedCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                          bool thread_safe) {
  int num_shards = 1 << num_shard_bits_;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->ApplyToAllCacheEntries(callback, thread_safe);
  }
}

void ShardedCache::EraseUnRefEntries() {
  int num_shards = 1 << num_shard_bits_;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->EraseUnRefEntries();
  }
}

std::string ShardedCache::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&capacity_mutex_);
    snprintf(buffer, kBufferSize, "    capacity : %" ROCKSDB_PRIszt "\n",
             capacity_);
    ret.append(buffer);
    snprintf(buffer, kBufferSize, "    num_shard_bits : %d\n", num_shard_bits_);
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
int GetDefaultCacheShardBits(size_t capacity) {
  // The number of shards should not exceed roughly sqrt(cache entries)/2
  // to minimize clustering effects and metadata overhead, but higher
  // number of shards (1024 shards or more) can improve latency due to less
  // lock contention.
  // Estimate one cache entry per 4KiB (2^12 bytes).
  // In base-2 logs, /2 for sqrt, -1 for /2.
  int shard_bits_for_clustering =
      std::max((FloorLog2(capacity) - 12) / 2 - 1, int{0});
  // But also use 512KiB as minimum shard size.
  int shard_bits_for_minimum_size =
      FloorLog2(std::max(size_t{1}, capacity / (size_t{512} * 1024U)));

  // For example,
  // 0 shard bits (1 shard) for < 1MiB block cache
  // 1 shard bits (2 shards) for 1MiB block cache
  // 4 shard bits (16 shards) for 8MiB block cache (long time defaults)
  // 10 shard bits (1024 shards) for 16GiB block cache.}
  return std::min(shard_bits_for_minimum_size, shard_bits_for_clustering);
}

}  // namespace ROCKSDB_NAMESPACE
