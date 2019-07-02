//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <set>
#include "trace_replay/block_cache_tracer.h"

namespace rocksdb {

// A cache configuration provided by user.
struct CacheConfiguration {
  std::string cache_name;  // LRU.
  uint32_t num_shard_bits;
  uint64_t ghost_cache_capacity;  // ghost cache capacity in bytes.
  std::vector<uint64_t>
      cache_capacities;  // simulate cache capacities in bytes.

  bool operator=(const CacheConfiguration& o) const {
    return cache_name == o.cache_name && num_shard_bits == o.num_shard_bits &&
           ghost_cache_capacity == o.ghost_cache_capacity;
  }
  bool operator<(const CacheConfiguration& o) const {
    return cache_name < o.cache_name ||
           (cache_name == o.cache_name && num_shard_bits < o.num_shard_bits) ||
           (cache_name == o.cache_name && num_shard_bits == o.num_shard_bits &&
            ghost_cache_capacity < o.ghost_cache_capacity);
  }
};

// A ghost cache admits an entry on its second access.
class GhostCache {
 public:
  GhostCache(std::shared_ptr<Cache> sim_cache);
  ~GhostCache() = default;
  // No copy and move.
  GhostCache(const GhostCache&) = delete;
  GhostCache& operator=(const GhostCache&) = delete;
  GhostCache(GhostCache&&) = delete;
  GhostCache& operator=(GhostCache&&) = delete;

  // Returns true if the lookup_key is in the ghost cache.
  // Returns false otherwise.
  bool Admit(const Slice& lookup_key);

 private:
  std::shared_ptr<Cache> sim_cache_;
};

// A cache simulator that runs against a block cache trace.
class CacheSimulator {
 public:
  CacheSimulator(std::unique_ptr<GhostCache>&& ghost_cache,
                 std::shared_ptr<Cache> sim_cache);
  virtual ~CacheSimulator() = default;
  // No copy and move.
  CacheSimulator(const CacheSimulator&) = delete;
  CacheSimulator& operator=(const CacheSimulator&) = delete;
  CacheSimulator(CacheSimulator&&) = delete;
  CacheSimulator& operator=(CacheSimulator&&) = delete;

  virtual void Access(const BlockCacheTraceRecord& access);
  void reset_counter() {
    num_misses_ = 0;
    num_accesses_ = 0;
    user_accesses_ = 0;
    user_misses_ = 0;
  }
  double miss_ratio() {
    if (num_accesses_ == 0) {
      return -1;
    }
    return static_cast<double>(num_misses_ * 100.0 / num_accesses_);
  }
  uint64_t total_accesses() { return num_accesses_; }

  double user_miss_ratio() {
    if (user_accesses_ == 0) {
      return -1;
    }
    return static_cast<double>(user_misses_ * 100.0 / user_accesses_);
  }
  uint64_t user_accesses() { return user_accesses_; }

 protected:
  void UpdateMetrics(bool is_user_access, bool is_cache_miss);

  std::unique_ptr<GhostCache> ghost_cache_;
  std::shared_ptr<Cache> sim_cache_;
  uint64_t num_accesses_ = 0;
  uint64_t num_misses_ = 0;
  uint64_t user_accesses_ = 0;
  uint64_t user_misses_ = 0;
};

// A prioritized cache simulator that runs against a block cache trace.
// It inserts missing index/filter/uncompression-dictionary blocks with high
// priority in the cache.
class PrioritizedCacheSimulator : public CacheSimulator {
 public:
  PrioritizedCacheSimulator(std::unique_ptr<GhostCache>&& ghost_cache,
                            std::shared_ptr<Cache> sim_cache)
      : CacheSimulator(std::move(ghost_cache), sim_cache) {}
  void Access(const BlockCacheTraceRecord& access) override;

 protected:
  // Access the key-value pair and returns true upon a cache miss.
  bool AccessKeyValue(const BlockCacheTraceRecord& access, const Slice& key,
                      uint64_t value_size);
};

// A block cache simulator that reports miss ratio curves given a set of cache
// configurations.
class BlockCacheTraceSimulator {
 public:
  // warmup_seconds: The number of seconds to warmup simulated caches. The
  // hit/miss counters are reset after the warmup completes.
  BlockCacheTraceSimulator(
      uint64_t warmup_seconds, uint32_t downsample_ratio,
      const std::vector<CacheConfiguration>& cache_configurations);
  ~BlockCacheTraceSimulator() = default;
  // No copy and move.
  BlockCacheTraceSimulator(const BlockCacheTraceSimulator&) = delete;
  BlockCacheTraceSimulator& operator=(const BlockCacheTraceSimulator&) = delete;
  BlockCacheTraceSimulator(BlockCacheTraceSimulator&&) = delete;
  BlockCacheTraceSimulator& operator=(BlockCacheTraceSimulator&&) = delete;

  Status InitializeCaches();

  void Access(const BlockCacheTraceRecord& access);

  const std::map<CacheConfiguration,
                 std::vector<std::shared_ptr<CacheSimulator>>>&
  sim_caches() const {
    return sim_caches_;
  }

 private:
  const uint64_t warmup_seconds_;
  const uint32_t downsample_ratio_;
  const std::vector<CacheConfiguration> cache_configurations_;

  bool warmup_complete_ = false;
  std::map<CacheConfiguration, std::vector<std::shared_ptr<CacheSimulator>>>
      sim_caches_;
  uint64_t trace_start_time_ = 0;
};

}  // namespace rocksdb
