//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"
#include <algorithm>
#include "db/dbformat.h"

namespace rocksdb {

namespace {
const std::string kGhostCachePrefix = "ghost_";
inline uint32_t HashSlice(const Slice& s) {
  return static_cast<uint32_t>(GetSliceNPHash64(s));
}
}  // namespace

const uint32_t LeCaR::kSampleSize = 16;
const double LeCaR::kLearningRate = 0.45;
const std::vector<LeCaR::Policy> LeCaR::kPolicies = {
    LeCaR::Policy::LRU, LeCaR::Policy::LFU, LeCaR::Policy::MRU};

LeCaR::LeCaR(size_t cache_capacity,
             const std::unordered_map<Policy, double, EnumClassHash>&
                 policy_init_regret_weights,
             std::shared_ptr<MemoryAllocator> allocator)
    : Cache(std::move(allocator)),
      cache_capacity_(cache_capacity),
      discount_rate_(0.005) {
  for (auto const& policy_init_weight : policy_init_regret_weights) {
    policy_states_[policy_init_weight.first].regret_weight =
        policy_init_weight.second;
  }
  ComputeRewardWeights();
}

LeCaR::Policy LeCaR::DecidePolicy() {
  LeCaR::Policy policy = LeCaR::Policy::LRU;
  double sum_rewards = 0.0;
  double r = ((double)rand() / (RAND_MAX));
  assert(r <= 1 && r >= 0);
  for (auto const& policy_state : policy_states_) {
    sum_rewards += policy_state.second.reward_weight;
    if (sum_rewards >= r) {
      policy = policy_state.first;
      break;
    }
  }
  assert(sum_rewards <= 1.0);
  current_policy_ = policy;
  return policy;
}

void LeCaR::Erase(const Slice& key) {
  uint32_t hash = HashSlice(key);
  LRUHandle* e = cache_.Remove(key, hash);
  if (e) {
    e->Free();
  }
}

Status LeCaR::Insert(const Slice& key, void* value, size_t /*charge*/,
                     void (*)(const Slice& key, void* value),
                     Handle** /*handle*/, Priority /*priority*/) {
  Erase(key);
  assert(value);
  LRUHandle* e = reinterpret_cast<LRUHandle*>(value);
  LeCaRHandle* handle = reinterpret_cast<LeCaRHandle*>(e->value);
  assert(handle->number_of_hits == 0);
  assert(handle->value_size > 0);
  if (handle->value_size > cache_capacity_) {
    e->Free();
    return Status::OK();
  }
  // Update weight.
  std::string key_str = key.ToString();
  UpdateWeight(key_str, handle->last_access_time);
  Policy action = DecidePolicy();
  while (usage_ + handle->value_size > cache_capacity_) {
    Evict(action, handle);
  }
  cache_.Insert(e);
  usage_ += handle->value_size;
  return Status::OK();
}

void LeCaR::NormalizeRegretWeights() {
  double weight_sum = 0.0;
  double cumulated_weights = 0;
  uint32_t index = 0;
  for (auto& policy_state : policy_states_) {
    weight_sum += policy_state.second.regret_weight;
  }
  for (auto& policy_state : policy_states_) {
    if (index == policy_states_.size() - 1) {
      policy_state.second.regret_weight = 1 - cumulated_weights;
      break;
    }
    policy_state.second.regret_weight =
        policy_state.second.regret_weight / weight_sum;
    cumulated_weights += policy_state.second.regret_weight;
    index++;
  }
}

void LeCaR::ComputeRewardWeights() {
  double weight_sum = 0.0;
  double cumulated_weights = 0;
  uint32_t index = 0;
  for (auto& policy_state : policy_states_) {
    policy_state.second.reward_weight = 1.0 - policy_state.second.regret_weight;
    weight_sum += policy_state.second.reward_weight;
  }
  for (auto& policy_state : policy_states_) {
    if (index == policy_states_.size() - 1) {
      policy_state.second.reward_weight = 1 - cumulated_weights;
      break;
    }
    policy_state.second.reward_weight =
        policy_state.second.reward_weight / weight_sum;
    cumulated_weights += policy_state.second.reward_weight;
    index++;
  }
}

void LeCaR::UpdateWeight(const std::string& key, uint64_t now) {
  for (auto& policy_state : policy_states_) {
    if (policy_state.second.evicted_keys.find(key) !=
        policy_state.second.evicted_keys.end()) {
      // The missing key is evicted by this policy. Increase the regret weight
      // for this policy.
      const LeCaRHandle handle = policy_state.second.evicted_keys[key];
      const double t = static_cast<double>(now - handle.last_access_time);
      const double regret = std::pow(discount_rate_, t);
      policy_state.second.regret_weight *= std::exp(kLearningRate * regret);
      // Remove this key since it will be brought into the cache.
      assert(policy_state.second.evicted_keys.erase(key));
    }
  }
  // Normalize the weights.
  NormalizeRegretWeights();
  // Calculate the rewards=1-regret.
  ComputeRewardWeights();
}

Cache::Handle* LeCaR::Lookup(const Slice& key, uint64_t now) {
  uint32_t hash = HashSlice(key);
  LRUHandle* e = cache_.Lookup(key, hash);
  if (e == nullptr) {
    return nullptr;
  }
  LeCaRHandle* handle = reinterpret_cast<LeCaRHandle*>(e->value);
  handle->number_of_hits += 1;
  handle->last_access_time = now;
  return reinterpret_cast<Cache::Handle*>(e);
}

LRUHandle* LeCaR::NewLRUHandle(const Slice& key, LeCaRHandle* handle) {
  assert(handle);
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      new char[sizeof(LRUHandle) - 1 + key.size()]);
  e->value = handle;
  e->deleter = [](const Slice&, void* value) {
    delete reinterpret_cast<LeCaRHandle*>(value);
  };
  e->charge = handle->value_size;
  e->key_length = key.size();
  e->flags = 0;
  e->hash = HashSlice(key);
  e->refs = 0;
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  memcpy(e->key_data, key.data(), key.size());
  return e;
}

void LeCaR::Evict(Policy policy, LeCaRHandle* handle) {
  // Random sample.
  uint32_t sample_index = 0;
  std::vector<LRUHandle*> random_samples = cache_.RandomSample(kSampleSize);
  for (auto sample : random_samples) {
    samples_[sample->key().ToString()] =
        reinterpret_cast<LeCaRHandle*>(sample->value);
  }
  assert(samples_.size() <= kSampleSize * 2);
  std::vector<std::pair<std::string, LeCaRHandle*>> sample_set(samples_.size());
  for (auto const& sample : samples_) {
    sample_set[sample_index] = std::make_pair(sample.first, sample.second);
    sample_index++;
  }
  switch (policy) {
    case Policy::LRU:
      std::sort(sample_set.begin(), sample_set.end(),
                std::bind(&LeCaR::LRUComparator, this, std::placeholders::_1,
                          std::placeholders::_2));
      break;
    case Policy::LFU:
      std::sort(sample_set.begin(), sample_set.end(),
                std::bind(&LeCaR::LFUComparator, this, std::placeholders::_1,
                          std::placeholders::_2));
      break;
    case Policy::MRU:
      std::sort(sample_set.begin(), sample_set.end(),
                std::bind(&LeCaR::MRUComparator, this, std::placeholders::_1,
                          std::placeholders::_2));
      break;
    default:
      assert(false);
      break;
  }
  uint32_t evict_index = 0;
  const size_t num_keys_in_cache = cache_.size();
  while (usage_ + handle->value_size > cache_capacity_ &&
         evict_index < sample_set.size()) {
    std::pair<std::string, LeCaRHandle*> candidate = sample_set[evict_index];
    LRUHandle* e = cache_.Remove(candidate.first, HashSlice(candidate.first));
    assert(samples_.erase(candidate.first));
    usage_ -= candidate.second->value_size;
    // Add to the evicted keys of this policy.
    while (policy_states_[policy].evicted_keys.size() >= num_keys_in_cache &&
           num_keys_in_cache > 0) {
      policy_states_[policy].evicted_keys.erase(
          policy_states_[policy].evicted_keys.begin());
    }
    policy_states_[policy].evicted_keys[candidate.first] = *(candidate.second);
    e->Free();
    evict_index++;
  }
  // Maintain kSampleSize samples.
  if (samples_.size() > kSampleSize) {
    uint32_t evict_index_from_last =
        static_cast<uint32_t>(sample_set.size() - 1);
    for (; evict_index_from_last >= evict_index; evict_index_from_last--) {
      assert(samples_.erase(sample_set[evict_index_from_last].first));
    }
  }
  assert(samples_.size() <= kSampleSize);
}

bool CacheSimulatorDelegate::Lookup(const Slice& key,
                                    const BlockCacheTraceRecord& access) {
  auto lecar_cache = std::dynamic_pointer_cast<LeCaR>(sim_cache_);
  if (lecar_cache) {
    return lecar_cache->Lookup(key, access.access_timestamp) != nullptr;
  }
  auto handle = sim_cache_->Lookup(key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    return true;
  }
  return false;
}

Status CacheSimulatorDelegate::Insert(const Slice& key, uint64_t value_size,
                                      const BlockCacheTraceRecord& access,
                                      Cache::Priority priority) {
  auto lecar_cache = std::dynamic_pointer_cast<LeCaR>(sim_cache_);
  if (lecar_cache) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = value_size;
    value->last_access_time = access.access_timestamp / kMicrosInSecond;
    LRUHandle* handle = lecar_cache->NewLRUHandle(key, value);
    return lecar_cache->Insert(key, handle, value_size, /*deleter=*/nullptr);
  }
  return sim_cache_->Insert(key, /*value=*/nullptr, value_size,
                            /*deleter=*/nullptr, /*handle=*/nullptr, priority);
}

GhostCache::GhostCache(std::shared_ptr<Cache> sim_cache)
    : sim_cache_(new CacheSimulatorDelegate(sim_cache)) {}

bool GhostCache::Admit(const Slice& lookup_key,
                       const BlockCacheTraceRecord& access) {
  bool hit = sim_cache_->Lookup(lookup_key, access);
  if (hit) {
    return true;
  }
  sim_cache_->Insert(lookup_key, lookup_key.size(), access,
                     Cache::Priority::LOW);
  return false;
}

CacheSimulator::CacheSimulator(std::unique_ptr<GhostCache>&& ghost_cache,
                               std::shared_ptr<Cache> sim_cache)
    : ghost_cache_(std::move(ghost_cache)),
      sim_cache_(new CacheSimulatorDelegate(sim_cache)) {}

void CacheSimulator::Access(const BlockCacheTraceRecord& access) {
  bool admit = true;
  const bool is_user_access =
      BlockCacheTraceHelper::IsUserAccess(access.caller);
  bool is_cache_miss = true;
  if (ghost_cache_ && access.no_insert == Boolean::kFalse) {
    admit = ghost_cache_->Admit(access.block_key, access);
  }
  bool hit = sim_cache_->Lookup(access.block_key, access);
  if (hit) {
    is_cache_miss = false;
  } else {
    if (access.no_insert == Boolean::kFalse && admit && access.block_size > 0) {
      sim_cache_->Insert(access.block_key, access.block_size, access,
                         Cache::Priority::LOW);
    }
  }
  miss_ratio_stats_.UpdateMetrics(access.access_timestamp, is_user_access,
                                  is_cache_miss);
}

void MissRatioStats::UpdateMetrics(uint64_t timestamp_in_ms,
                                   bool is_user_access, bool is_cache_miss) {
  uint64_t timestamp_in_seconds = timestamp_in_ms / kMicrosInSecond;
  num_accesses_timeline_[timestamp_in_seconds] += 1;
  num_accesses_ += 1;
  if (num_misses_timeline_.find(timestamp_in_seconds) ==
      num_misses_timeline_.end()) {
    num_misses_timeline_[timestamp_in_seconds] = 0;
  }
  if (is_cache_miss) {
    num_misses_ += 1;
    num_misses_timeline_[timestamp_in_seconds] += 1;
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
    const BlockCacheTraceRecord& access, bool no_insert, bool is_user_access,
    bool* is_cache_miss, bool* admitted, bool update_metrics) {
  assert(is_cache_miss);
  assert(admitted);
  *is_cache_miss = true;
  *admitted = true;
  if (ghost_cache_ && !no_insert) {
    *admitted = ghost_cache_->Admit(key, access);
  }
  bool hit = sim_cache_->Lookup(key, access);
  if (hit) {
    *is_cache_miss = false;
  } else if (!no_insert && *admitted && value_size > 0) {
    sim_cache_->Insert(key, value_size, access, priority);
  }
  if (update_metrics) {
    miss_ratio_stats_.UpdateMetrics(access.access_timestamp, is_user_access,
                                    *is_cache_miss);
  }
}

void PrioritizedCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  bool is_cache_miss = true;
  bool admitted = true;
  AccessKVPair(access.block_key, access.block_size,
               ComputeBlockPriority(access), access, access.no_insert,
               BlockCacheTraceHelper::IsUserAccess(access.caller),
               &is_cache_miss, &admitted, /*update_metrics=*/true);
}

void HybridRowBlockCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  // TODO (haoyu): We only support Get for now. We need to extend the tracing
  // for MultiGet, i.e., non-data block accesses must log all keys in a
  // MultiGet.
  bool is_cache_miss = false;
  bool admitted = false;
  if (access.caller == TableReaderCaller::kUserGet &&
      access.get_id != BlockCacheTraceHelper::kReservedGetId) {
    // This is a Get/MultiGet request.
    const std::string& row_key = BlockCacheTraceHelper::ComputeRowKey(access);
    if (getid_getkeys_map_[access.get_id].find(row_key) ==
        getid_getkeys_map_[access.get_id].end()) {
      // This is the first time that this key is accessed. Look up the key-value
      // pair first. Do not update the miss/accesses metrics here since it will
      // be updated later.
      AccessKVPair(row_key, access.referenced_data_size, Cache::Priority::HIGH,
                   access,
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
      miss_ratio_stats_.UpdateMetrics(access.access_timestamp,
                                      /*is_user_access=*/true,
                                      /*is_cache_miss=*/false);
      return;
    }
    // The key-value pair observes a cache miss. We need to access its
    // index/filter/data blocks.
    AccessKVPair(
        access.block_key, access.block_type, ComputeBlockPriority(access),
        access,
        /*no_insert=*/!insert_blocks_upon_row_kvpair_miss_ || access.no_insert,
        /*is_user_access=*/true, &is_cache_miss, &admitted,
        /*update_metrics=*/true);
    if (access.referenced_data_size > 0 &&
        miss_inserted.second == InsertResult::ADMITTED) {
      sim_cache_->Insert(row_key, access.referenced_data_size, access,
                         Cache::Priority::HIGH);
      getid_getkeys_map_[access.get_id][row_key] =
          std::make_pair(true, InsertResult::INSERTED);
    }
    return;
  }
  AccessKVPair(access.block_key, access.block_size,
               ComputeBlockPriority(access), access, access.no_insert,
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
      } else if (cache_name == "lecar") {
        std::unordered_map<LeCaR::Policy, double, LeCaR::EnumClassHash>
            policy_regret_weights;
        policy_regret_weights[LeCaR::Policy::LRU] = 0.32;
        policy_regret_weights[LeCaR::Policy::LFU] = 0.34;
        policy_regret_weights[LeCaR::Policy::MRU] = 0.34;
        sim_cache = std::make_shared<CacheSimulator>(
            std::move(ghost_cache),
            std::make_shared<LeCaR>(simulate_cache_capacity,
                                    policy_regret_weights));
      } else if (cache_name == "lecar_hybrid") {
        std::unordered_map<LeCaR::Policy, double, LeCaR::EnumClassHash>
            policy_regret_weights;
        policy_regret_weights[LeCaR::Policy::LRU] = 0.32;
        policy_regret_weights[LeCaR::Policy::LFU] = 0.34;
        policy_regret_weights[LeCaR::Policy::MRU] = 0.34;
        sim_cache = std::make_shared<HybridRowBlockCacheSimulator>(
            std::move(ghost_cache),
            std::make_shared<LeCaR>(simulate_cache_capacity,
                                    policy_regret_weights),
            /*insert_blocks_upon_row_kvpair_miss=*/true);
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
