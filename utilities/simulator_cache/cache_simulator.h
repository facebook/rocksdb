//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/utilities/sim_cache.h"
#include "trace_replay/block_cache_tracer.h"

namespace rocksdb {

// A cache configuration provided by user.
struct CacheConfiguration {
  std::string cache_name;  // LRU.
  uint32_t num_shard_bits;
  std::vector<uint64_t>
      cache_capacities;  // simulate cache capacities in bytes.

  bool operator=(const CacheConfiguration& o) const {
    return cache_name == o.cache_name && num_shard_bits == o.num_shard_bits;
  }
  bool operator<(const CacheConfiguration& o) const {
    return cache_name < o.cache_name ||
           (cache_name == o.cache_name && num_shard_bits < o.num_shard_bits);
  }
};

// A cache simulator that runs against a block cache trace.
class CacheSimulator {
 public:
  CacheSimulator(std::shared_ptr<SimCache> sim_cache);
  virtual ~CacheSimulator() = default;
  // No copy and move.
  CacheSimulator(const CacheSimulator&) = delete;
  CacheSimulator& operator=(const CacheSimulator&) = delete;
  CacheSimulator(CacheSimulator&&) = delete;
  CacheSimulator& operator=(CacheSimulator&&) = delete;

  virtual void Access(const BlockCacheTraceRecord& access);
  void reset_counter() { sim_cache_->reset_counter(); }
  double miss_ratio();
  uint64_t total_accesses();

 protected:
  std::shared_ptr<SimCache> sim_cache_;
};

// A prioritized cache simulator that runs against a block cache trace.
// It inserts missing index/filter/uncompression-dictionary blocks with high
// priority in the cache.
class PrioritizedCacheSimulator : public CacheSimulator {
 public:
  PrioritizedCacheSimulator(std::shared_ptr<SimCache> sim_cache)
      : CacheSimulator(sim_cache) {}
  void Access(const BlockCacheTraceRecord& access) override;
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
