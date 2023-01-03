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

ShardedCacheBase::ShardedCacheBase(size_t capacity, int num_shard_bits,
                                   bool strict_capacity_limit,
                                   std::shared_ptr<MemoryAllocator> allocator)
    : Cache(std::move(allocator)),
      last_id_(1),
      shard_mask_((uint32_t{1} << num_shard_bits) - 1),
      strict_capacity_limit_(strict_capacity_limit),
      capacity_(capacity) {}

size_t ShardedCacheBase::ComputePerShardCapacity(size_t capacity) const {
  uint32_t num_shards = GetNumShards();
  return (capacity + (num_shards - 1)) / num_shards;
}

size_t ShardedCacheBase::GetPerShardCapacity() const {
  return ComputePerShardCapacity(GetCapacity());
}

uint64_t ShardedCacheBase::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
}

size_t ShardedCacheBase::GetCapacity() const {
  MutexLock l(&config_mutex_);
  return capacity_;
}

bool ShardedCacheBase::HasStrictCapacityLimit() const {
  MutexLock l(&config_mutex_);
  return strict_capacity_limit_;
}

size_t ShardedCacheBase::GetUsage(Handle* handle) const {
  return GetCharge(handle);
}

std::string ShardedCacheBase::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&config_mutex_);
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
  AppendPrintableOptions(ret);
  return ret;
}

int GetDefaultCacheShardBits(size_t capacity, size_t min_shard_size) {
  int num_shard_bits = 0;
  size_t num_shards = capacity / min_shard_size;
  while (num_shards >>= 1) {
    if (++num_shard_bits >= 6) {
      // No more than 6.
      return num_shard_bits;
    }
  }
  return num_shard_bits;
}

int ShardedCacheBase::GetNumShardBits() const {
  return BitsSetToOne(shard_mask_);
}

uint32_t ShardedCacheBase::GetNumShards() const { return shard_mask_ + 1; }

}  // namespace ROCKSDB_NAMESPACE
