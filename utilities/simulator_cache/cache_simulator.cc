//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"
#include "db/dbformat.h"

namespace rocksdb {

namespace {
const std::string kGhostCachePrefix = "ghost_";
}

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

Cache::Priority PrioritizedCacheSimulator::ComputeBlockPriority(
    const BlockCacheTraceRecord& access) const {
  if (access.block_type == TraceType::kBlockTraceFilterBlock ||
      access.block_type == TraceType::kBlockTraceIndexBlock ||
      access.block_type == TraceType::kBlockTraceUncompressionDictBlock) {
    return Cache::Priority::HIGH;
  }
  return Cache::Priority::LOW;
}

void PrioritizedCacheSimulator::AccessKVPair(
    const Slice& key, uint64_t value_size, Cache::Priority priority,
    bool no_insert, bool is_user_access, bool* is_cache_miss, bool* admitted,
    bool update_metrics) {
  assert(is_cache_miss);
  assert(admitted);
  *is_cache_miss = true;
  *admitted = true;
  if (ghost_cache_ && !no_insert) {
    *admitted = ghost_cache_->Admit(key);
  }
  auto handle = sim_cache_->Lookup(key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    *is_cache_miss = false;
  } else if (!no_insert && *admitted && value_size > 0) {
    sim_cache_->Insert(key, /*value=*/nullptr, value_size,
                       /*deleter=*/nullptr, /*handle=*/nullptr, priority);
  }
  if (update_metrics) {
    UpdateMetrics(is_user_access, *is_cache_miss);
  }
}

void PrioritizedCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  bool is_cache_miss = true;
  bool admitted = true;
  AccessKVPair(access.block_key, access.block_size,
               ComputeBlockPriority(access), access.no_insert,
               BlockCacheTraceHelper::IsUserAccess(access.caller),
               &is_cache_miss, &admitted, /*update_metrics=*/true);
}

std::string HybridRowBlockCacheSimulator::ComputeRowKey(
    const BlockCacheTraceRecord& access) {
  assert(access.get_id != BlockCacheTraceHelper::kReservedGetId);
  Slice key;
  if (access.referenced_key_exist_in_block == Boolean::kTrue) {
    key = ExtractUserKey(access.referenced_key);
  } else {
    key = access.referenced_key;
  }
  return std::to_string(access.sst_fd_number) + "_" + key.ToString();
}

void HybridRowBlockCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  bool is_cache_miss = true;
  bool admitted = true;
  if (access.get_id != BlockCacheTraceHelper::kReservedGetId) {
    // This is a Get/MultiGet request.
    const std::string& row_key = ComputeRowKey(access);
    if (getid_getkeys_map_[access.get_id].find(row_key) ==
        getid_getkeys_map_[access.get_id].end()) {
      // This is the first time that this key is accessed. Look up the key-value
      // pair first. Do not update the miss/accesses metrics here since it will
      // be updated later.
      AccessKVPair(row_key, access.referenced_data_size, Cache::Priority::HIGH,
                   /*no_insert=*/false,
                   /*is_user_access=*/true, &is_cache_miss, &admitted,
                   /*update_metrics=*/false);
      InsertResult result = InsertResult::NO_INSERT;
      if (admitted && access.referenced_data_size > 0) {
        result = InsertResult::INSERTED;
      } else if (admitted) {
        result = InsertResult::ADMITTED;
      }
      getid_getkeys_map_[access.get_id][row_key] =
          std::make_pair(is_cache_miss, result);
    }
    std::pair<bool, InsertResult> miss_inserted =
        getid_getkeys_map_[access.get_id][row_key];
    if (!miss_inserted.first) {
      // This is a cache hit. Skip future accesses to its index/filter/data
      // blocks. These block lookups are unnecessary if we observe a hit for the
      // referenced key-value pair already. Thus, we treat these lookups as
      // hits. This is also to ensure the total number of accesses are the same
      // when comparing to other policies.
      UpdateMetrics(/*is_user_access=*/true, /*is_cache_miss=*/false);
      return;
    }
    // The key-value pair observes a cache miss. We need to access its
    // index/filter/data blocks.
    AccessKVPair(
        access.block_key, access.block_type, ComputeBlockPriority(access),
        /*no_insert=*/!insert_blocks_upon_row_kvpair_miss_ || access.no_insert,
        /*is_user_access=*/true, &is_cache_miss, &admitted,
        /*update_metrics=*/true);
    if (access.referenced_data_size > 0 &&
        miss_inserted.second == InsertResult::ADMITTED) {
      sim_cache_->Insert(
          row_key, /*value=*/nullptr, access.referenced_data_size,
          /*deleter=*/nullptr, /*handle=*/nullptr, Cache::Priority::HIGH);
      getid_getkeys_map_[access.get_id][row_key] =
          std::make_pair(true, InsertResult::INSERTED);
    }
    return;
  }
  AccessKVPair(access.block_key, access.block_size,
               ComputeBlockPriority(access), access.no_insert,
               BlockCacheTraceHelper::IsUserAccess(access.caller),
               &is_cache_miss, &admitted, /*update_metrics=*/true);
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
      std::string cache_name = config.cache_name;
      if (cache_name.find(kGhostCachePrefix) != std::string::npos) {
        ghost_cache.reset(new GhostCache(
            NewLRUCache(config.ghost_cache_capacity, /*num_shard_bits=*/1,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0)));
        cache_name = cache_name.substr(kGhostCachePrefix.size());
      }
      if (cache_name == "lru") {
        sim_cache = std::make_shared<CacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0));
      } else if (cache_name == "lru_priority") {
        sim_cache = std::make_shared<PrioritizedCacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5));
      } else if (cache_name == "lru_hybrid") {
        sim_cache = std::make_shared<HybridRowBlockCacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5),
            /*insert_blocks_upon_row_kvpair_miss=*/true);
      } else if (cache_name == "lru_hybrid_no_insert_on_row_miss") {
        sim_cache = std::make_shared<HybridRowBlockCacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5),
            /*insert_blocks_upon_row_kvpair_miss=*/false);
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
