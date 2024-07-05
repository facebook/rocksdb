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

#include "env/unique_id_gen.h"
#include "rocksdb/env.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace {
// The generated seeds must fit in 31 bits so that
// ShardedCacheOptions::hash_seed can be set to it explicitly, for
// diagnostic/debugging purposes.
constexpr uint32_t kSeedMask = 0x7fffffff;
uint32_t DetermineSeed(int32_t hash_seed_option) {
  if (hash_seed_option >= 0) {
    // User-specified exact seed
    return static_cast<uint32_t>(hash_seed_option);
  }
  static SemiStructuredUniqueIdGen gen;
  if (hash_seed_option == ShardedCacheOptions::kHostHashSeed) {
    std::string hostname;
    Status s = Env::Default()->GetHostNameString(&hostname);
    if (s.ok()) {
      return GetSliceHash(hostname) & kSeedMask;
    } else {
      // Fall back on something stable within the process.
      return BitwiseAnd(gen.GetBaseUpper(), kSeedMask);
    }
  } else {
    // for kQuasiRandomHashSeed and fallback
    uint32_t val = gen.GenerateNext<uint32_t>() & kSeedMask;
    // Perform some 31-bit bijective transformations so that we get
    // quasirandom, not just incrementing. (An incrementing seed from a
    // random starting point would be fine, but hard to describe in a name.)
    // See https://en.wikipedia.org/wiki/Quasirandom and using a murmur-like
    // transformation here for our bijection in the lower 31 bits.
    // See https://en.wikipedia.org/wiki/MurmurHash
    val *= /*31-bit prime*/ 1150630961;
    val ^= (val & kSeedMask) >> 17;
    val *= /*31-bit prime*/ 1320603883;
    return val & kSeedMask;
  }
}
}  // namespace

ShardedCacheBase::ShardedCacheBase(const ShardedCacheOptions& opts)
    : Cache(opts.memory_allocator),
      last_id_(1),
      shard_mask_((uint32_t{1} << opts.num_shard_bits) - 1),
      hash_seed_(DetermineSeed(opts.hash_seed)),
      strict_capacity_limit_(opts.strict_capacity_limit),
      capacity_(opts.capacity) {}

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

Status ShardedCacheBase::GetSecondaryCacheCapacity(size_t& size) const {
  size = 0;
  return Status::OK();
}

Status ShardedCacheBase::GetSecondaryCachePinnedUsage(size_t& size) const {
  size = 0;
  return Status::OK();
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
