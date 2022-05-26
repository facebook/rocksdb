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
#include <unordered_map>

#include "util/hash.h"
#include "util/math.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

inline uint32_t HashSlice(const Slice& s) {
  return Lower32of64(GetSliceNPHash64(s));
}

ShardedCache::ShardedCache() : Cache(), shard_mask_(0), last_id_(1) {}

uint32_t ShardedCache::SetNumShards(int num_shard_bits) {
  shard_mask_ = (uint32_t{1} << num_shard_bits) - 1;
  return GetNumShards();
}

Status ShardedCache::ValidateOptions(const DBOptions& db_opts,
                                     const ColumnFamilyOptions& cf_opts) const {
  if (IsMutable()) {
    return Status::InvalidArgument("Cache is not initialized");
  } else {
    return Cache::ValidateOptions(db_opts, cf_opts);
  }
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
                                    Statistics* stats) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))
      ->Lookup(key, hash, helper, create_cb, priority, wait, stats);
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

bool ShardedCache::Release(Handle* handle, bool erase_if_last_ref) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Release(handle, erase_if_last_ref);
}

bool ShardedCache::Release(Handle* handle, bool useful,
                           bool erase_if_last_ref) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Release(handle, useful, erase_if_last_ref);
}

void ShardedCache::Erase(const Slice& key) {
  uint32_t hash = HashSlice(key);
  GetShard(Shard(hash))->Erase(key, hash);
}

uint64_t ShardedCache::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
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
