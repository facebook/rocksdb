//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"

namespace rocksdb {
CacheSimulator::CacheSimulator(std::shared_ptr<SimCache> sim_cache)
    : sim_cache_(sim_cache) {}

void CacheSimulator::Access(const BlockCacheTraceRecord& access) {
  auto handle = sim_cache_->Lookup(access.block_key);
  if (handle == nullptr && !access.no_insert) {
    sim_cache_->Insert(access.block_key, /*value=*/nullptr, access.block_size,
                       /*deleter=*/nullptr, /*handle=*/nullptr);
  }
}

void PrioritizedCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  auto handle = sim_cache_->Lookup(access.block_key);
  if (handle == nullptr && !access.no_insert) {
    Cache::Priority priority = Cache::Priority::LOW;
    if (access.block_type == TraceType::kBlockTraceFilterBlock ||
        access.block_type == TraceType::kBlockTraceIndexBlock ||
        access.block_type == TraceType::kBlockTraceUncompressionDictBlock) {
      priority = Cache::Priority::HIGH;
    }
    sim_cache_->Insert(access.block_key, /*value=*/nullptr, access.block_size,
                       /*deleter=*/nullptr, /*handle=*/nullptr, priority);
  }
}

double CacheSimulator::miss_ratio() {
  uint64_t hits = sim_cache_->get_hit_counter();
  uint64_t misses = sim_cache_->get_miss_counter();
  uint64_t total_accesses = hits + misses;
  return static_cast<double>(misses * 100.0 / total_accesses);
}

uint64_t CacheSimulator::total_accesses() {
  return sim_cache_->get_hit_counter() + sim_cache_->get_miss_counter();
}

BlockCacheTraceSimulator::BlockCacheTraceSimulator(
    uint64_t warmup_seconds, uint32_t downsample_ratio,
    const std::vector<CacheConfiguration>& cache_configurations)
    : warmup_seconds_(warmup_seconds),
      downsample_ratio_(downsample_ratio),
      cache_configurations_(cache_configurations) {}

Status BlockCacheTraceSimulator::InitializeCaches() {
  for (auto const& config : cache_configurations_) {
    for (auto cache_capacity : config.cache_capacities) {
      // Scale down the cache capacity since the trace contains accesses on
      // 1/'downsample_ratio' blocks.
      uint64_t simulate_cache_capacity = cache_capacity / downsample_ratio_;
      std::shared_ptr<CacheSimulator> sim_cache;
      if (config.cache_name == "lru") {
        sim_cache = std::make_shared<CacheSimulator>(NewSimCache(
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0),
            /*real_cache=*/nullptr, config.num_shard_bits));
      } else if (config.cache_name == "lru_priority") {
        sim_cache = std::make_shared<PrioritizedCacheSimulator>(NewSimCache(
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5),
            /*real_cache=*/nullptr, config.num_shard_bits));
      } else {
        // Not supported.
        return Status::InvalidArgument("Unknown cache name " +
                                       config.cache_name);
      }
      sim_caches_[config].push_back(sim_cache);
    }
  }
  return Status::OK();
}

void BlockCacheTraceSimulator::Access(const BlockCacheTraceRecord& access) {
  if (trace_start_time_ == 0) {
    trace_start_time_ = access.access_timestamp;
  }
  // access.access_timestamp is in microseconds.
  if (!warmup_complete_ &&
      trace_start_time_ + warmup_seconds_ * kMicrosInSecond <=
          access.access_timestamp) {
    for (auto& config_caches : sim_caches_) {
      for (auto& sim_cache : config_caches.second) {
        sim_cache->reset_counter();
      }
    }
    warmup_complete_ = true;
  }
  for (auto& config_caches : sim_caches_) {
    for (auto& sim_cache : config_caches.second) {
      sim_cache->Access(access);
    }
  }
}

}  // namespace rocksdb
