//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"

namespace rocksdb {

GhostCache::GhostCache(std::shared_ptr<Cache> sim_cache)
    : sim_cache_(sim_cache) {}

bool GhostCache::Admit(const Slice& lookup_key) {
  auto handle = sim_cache_->Lookup(lookup_key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    return true;
  }
  sim_cache_->Insert(lookup_key, /*value=*/nullptr, lookup_key.size(),
                     /*deleter=*/nullptr, /*handle=*/nullptr);
  return false;
}

CacheSimulator::CacheSimulator(std::unique_ptr<GhostCache>&& ghost_cache,
                               std::shared_ptr<Cache> sim_cache)
    : ghost_cache_(std::move(ghost_cache)), sim_cache_(sim_cache) {}

void CacheSimulator::Access(const BlockCacheTraceRecord& access) {
  bool admit = true;
  const bool is_user_access =
      BlockCacheTraceHelper::IsUserAccess(access.caller);
  bool is_cache_miss = true;
  if (ghost_cache_ && access.no_insert == Boolean::kFalse) {
    admit = ghost_cache_->Admit(access.block_key);
  }
  auto handle = sim_cache_->Lookup(access.block_key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    is_cache_miss = false;
  } else {
    if (access.no_insert == Boolean::kFalse && admit) {
      sim_cache_->Insert(access.block_key, /*value=*/nullptr, access.block_size,
                         /*deleter=*/nullptr, /*handle=*/nullptr);
    }
  }
  UpdateMetrics(is_user_access, is_cache_miss);
}

void CacheSimulator::UpdateMetrics(bool is_user_access, bool is_cache_miss) {
  num_accesses_ += 1;
  if (is_cache_miss) {
    num_misses_ += 1;
  }
  if (is_user_access) {
    user_accesses_ += 1;
    if (is_cache_miss) {
      user_misses_ += 1;
    }
  }
}

bool PrioritizedCacheSimulator::AccessKeyValue(
    const BlockCacheTraceRecord& access, const Slice& key,
    uint64_t value_size) {
  bool admit = true;
  if (ghost_cache_ && access.no_insert == Boolean::kFalse) {
    admit = ghost_cache_->Admit(key);
  }
  const bool is_user_access =
      BlockCacheTraceHelper::IsUserAccess(access.caller);
  bool is_cache_miss = true;
  auto handle = sim_cache_->Lookup(key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    is_cache_miss = false;
  } else if (access.no_insert == Boolean::kFalse && admit && value_size != 0) {
    Cache::Priority priority = Cache::Priority::LOW;
    if (access.block_type == TraceType::kBlockTraceFilterBlock ||
        access.block_type == TraceType::kBlockTraceIndexBlock ||
        access.block_type == TraceType::kBlockTraceUncompressionDictBlock) {
      priority = Cache::Priority::HIGH;
    }
    sim_cache_->Insert(key, /*value=*/nullptr, value_size,
                       /*deleter=*/nullptr, /*handle=*/nullptr, priority);
  }
  UpdateMetrics(is_user_access, is_cache_miss);
  return is_cache_miss;
}

void PrioritizedCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  AccessKeyValue(access, access.block_key, access.block_size);
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
      std::unique_ptr<GhostCache> ghost_cache;
      if (config.cache_name.find("ghost") != std::string::npos) {
        ghost_cache.reset(new GhostCache(
            NewLRUCache(config.ghost_cache_capacity, /*num_shard_bits=*/1,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0)));
      }
      if (config.cache_name == "lru") {
        sim_cache = std::make_shared<CacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0));
      } else if (config.cache_name == "lru_priority") {
        sim_cache = std::make_shared<PrioritizedCacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5));
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
