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
#include <cstdint>
#include <memory>

#include "util/hash.h"
#include "util/math.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

inline uint32_t HashSlice(const Slice& s) {
  return Lower32of64(GetSliceNPHash64(s));
}

}  // namespace

ShardedCache::ShardedCache(size_t capacity, int num_shard_bits,
                           bool strict_capacity_limit,
                           std::shared_ptr<MemoryAllocator> allocator)
    : Cache(std::move(allocator)),
      shard_mask_((uint32_t{1} << num_shard_bits) - 1),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      last_id_(1) {}

void ShardedCache::SetCapacity(size_t capacity) {
  uint32_t num_shards = GetNumShards();
  const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
  MutexLock l(&capacity_mutex_);
  for (uint32_t s = 0; s < num_shards; s++) {
    GetShard(s)->SetCapacity(per_shard);
  }
  capacity_ = capacity;
}

void ShardedCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  uint32_t num_shards = GetNumShards();
  MutexLock l(&capacity_mutex_);
  for (uint32_t s = 0; s < num_shards; s++) {
    GetShard(s)->SetStrictCapacityLimit(strict_capacity_limit);
  }
  strict_capacity_limit_ = strict_capacity_limit;
}

Status ShardedCache::Insert(const Slice& key, void* value, size_t charge,
                            DeleterFn deleter, Handle** handle,
                            Priority priority) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))
      ->Insert(key, hash, value, charge, deleter, handle, priority);
}

Status ShardedCache::Insert(const Slice& key, void* value,
                            const CacheItemHelper* helper, size_t charge,
                            Handle** handle, Priority priority) {
  uint32_t hash = HashSlice(key);
  if (!helper) {
    return Status::InvalidArgument();
  }
  return GetShard(Shard(hash))
      ->Insert(key, hash, value, helper, charge, handle, priority);
}

Cache::Handle* ShardedCache::Lookup(const Slice& key, Statistics* /*stats*/) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->Lookup(key, hash);
}

Cache::Handle* ShardedCache::Lookup(const Slice& key,
                                    const CacheItemHelper* helper,
                                    const CreateCallback& create_cb,
                                    Priority priority, bool wait,
                                    Statistics* /*stats*/) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))
      ->Lookup(key, hash, helper, create_cb, priority, wait);
}

bool ShardedCache::IsReady(Handle* handle) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->IsReady(handle);
}

void ShardedCache::Wait(Handle* handle) {
  uint32_t hash = GetHash(handle);
  GetShard(Shard(hash))->Wait(handle);
}

bool ShardedCache::Ref(Handle* handle) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Ref(handle);
}

bool ShardedCache::Release(Handle* handle, bool force_erase) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Release(handle, force_erase);
}

bool ShardedCache::Release(Handle* handle, bool useful, bool force_erase) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Release(handle, useful, force_erase);
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
  uint32_t num_shards = GetNumShards();
  size_t usage = 0;
  for (uint32_t s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetUsage();
  }
  return usage;
}

size_t ShardedCache::GetUsage(Handle* handle) const {
  return GetCharge(handle);
}

size_t ShardedCache::GetPinnedUsage() const {
  // We will not lock the cache when getting the usage from shards.
  uint32_t num_shards = GetNumShards();
  size_t usage = 0;
  for (uint32_t s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetPinnedUsage();
  }
  return usage;
}

void ShardedCache::ApplyToAllEntries(
    const std::function<void(const Slice& key, void* value, size_t charge,
                             DeleterFn deleter)>& callback,
    const ApplyToAllEntriesOptions& opts) {
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

void ShardedCache::EraseUnRefEntries() {
  uint32_t num_shards = GetNumShards();
  for (uint32_t s = 0; s < num_shards; s++) {
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
int GetDefaultCacheShardBits(size_t capacity) {
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

int ShardedCache::GetNumShardBits() const { return BitsSetToOne(shard_mask_); }

uint32_t ShardedCache::GetNumShards() const { return shard_mask_ + 1; }

}  // namespace ROCKSDB_NAMESPACE
