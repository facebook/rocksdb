//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/sharded_cache.h"

#include <string>

#include "rocksdb/utilities/options_type.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, CacheMetadataChargePolicy>
    metadata_charge_policy_map = {
        {"dont_charge", CacheMetadataChargePolicy::kDontChargeCacheMetadata},
        {"full_charge", CacheMetadataChargePolicy::kFullChargeCacheMetadata},
};

static std::unordered_map<std::string, OptionTypeInfo>
    sharded_cache_options_type_info = {
#ifndef ROCKSDB_LITE
        {"capacity",
         {offsetof(struct ShardedCacheOptions, capacity), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"num_shard_bits",
         {offsetof(struct ShardedCacheOptions, num_shard_bits),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"strict_capacity_limit",
         {offsetof(struct ShardedCacheOptions, strict_capacity_limit),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"metadata_charge_policy",
         OptionTypeInfo::Enum(
             offsetof(struct ShardedCacheOptions, metadata_charge_policy),
             &metadata_charge_policy_map)},
#endif  // ROCKSDB_LITE
};

ShardedCache::ShardedCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy,
    const std::shared_ptr<MemoryAllocator>& memory_allocator)
    : Cache(memory_allocator), last_id_(1) {
  options_.capacity = capacity;
  options_.num_shard_bits = num_shard_bits;
  options_.strict_capacity_limit = strict_capacity_limit;
  options_.metadata_charge_policy = metadata_charge_policy;
  RegisterOptions("ShardedCacheOptions", &options_,
                  &sharded_cache_options_type_info);
}

void ShardedCache::SetCapacity(size_t capacity) {
  int num_shards = 1 << options_.num_shard_bits;
  const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetCapacity(per_shard);
  }
  options_.capacity = capacity;
}

void ShardedCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  int num_shards = 1 << options_.num_shard_bits;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetStrictCapacityLimit(strict_capacity_limit);
  }
  options_.strict_capacity_limit = strict_capacity_limit;
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
  return options_.capacity;
}

bool ShardedCache::HasStrictCapacityLimit() const {
  MutexLock l(&capacity_mutex_);
  return options_.strict_capacity_limit;
}

size_t ShardedCache::GetUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << options_.num_shard_bits;
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
  int num_shards = 1 << options_.num_shard_bits;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetPinnedUsage();
  }
  return usage;
}

void ShardedCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                          bool thread_safe) {
  int num_shards = 1 << options_.num_shard_bits;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->ApplyToAllCacheEntries(callback, thread_safe);
  }
}

void ShardedCache::EraseUnRefEntries() {
  int num_shards = 1 << options_.num_shard_bits;
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
             options_.capacity);
    ret.append(buffer);
    snprintf(buffer, kBufferSize, "    num_shard_bits : %d\n",
             options_.num_shard_bits);
    ret.append(buffer);
    snprintf(buffer, kBufferSize, "    strict_capacity_limit : %d\n",
             options_.strict_capacity_limit);
    ret.append(buffer);
  }
  snprintf(buffer, kBufferSize, "    memory_allocator : %s\n",
           memory_allocator() ? memory_allocator()->Name() : "None");
  ret.append(buffer);
  ret.append(GetShard(0)->GetPrintableOptions());
  return ret;
}

Status ShardedCache::PrepareOptions(const ConfigOptions& opts) {
  if (options_.num_shard_bits >= 20) {
    return Status::InvalidArgument(
        "The cache cannot be sharded into too pieces");
  } else if (options_.num_shard_bits < 0) {
    options_.num_shard_bits = GetDefaultCacheShardBits(options_.capacity);
  }
  return Cache::PrepareOptions(opts);
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

}  // namespace ROCKSDB_NAMESPACE
