//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <unordered_map>

#include "cache/lru_cache.h"
#include "trace_replay/block_cache_tracer.h"

namespace rocksdb {

// A cache configuration provided by user.
struct CacheConfiguration {
  std::string cache_name;  // LRU.
  uint32_t num_shard_bits;
  uint64_t ghost_cache_capacity;  // ghost cache capacity in bytes.
  std::vector<uint64_t>
      cache_capacities;  // simulate cache capacities in bytes.

  bool operator==(const CacheConfiguration& o) const {
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

class MissRatioStats {
 public:
  void reset_counter() {
    num_misses_ = 0;
    num_accesses_ = 0;
    user_accesses_ = 0;
    user_misses_ = 0;
  }
  double miss_ratio() const {
    if (num_accesses_ == 0) {
      return -1;
    }
    return static_cast<double>(num_misses_ * 100.0 / num_accesses_);
  }
  uint64_t total_accesses() const { return num_accesses_; }

  const std::map<uint64_t, uint64_t>& num_accesses_timeline() const {
    return num_accesses_timeline_;
  }

  const std::map<uint64_t, uint64_t>& num_misses_timeline() const {
    return num_misses_timeline_;
  }

  double user_miss_ratio() const {
    if (user_accesses_ == 0) {
      return -1;
    }
    return static_cast<double>(user_misses_ * 100.0 / user_accesses_);
  }
  uint64_t user_accesses() const { return user_accesses_; }

  void UpdateMetrics(uint64_t timestamp_in_ms, bool is_user_access,
                     bool is_cache_miss);

 private:
  uint64_t num_accesses_ = 0;
  uint64_t num_misses_ = 0;
  uint64_t user_accesses_ = 0;
  uint64_t user_misses_ = 0;

  std::map<uint64_t, uint64_t> num_accesses_timeline_;
  std::map<uint64_t, uint64_t> num_misses_timeline_;
};

// An implementation of LeCaR [1]. It supports three policies: LRU, MRU, and
// LFU.
//
// Reinforcement learning: Each policy is associated with a reward_weight.
// The sum of reward_weight is 1. A policy maintains a set of keys that was
// evicted by this policy. Upon a cache miss, we penalize a policy if it evicted
// the key. Then, we adjust the reward_weights of all policies accordingly.
//
// Eviction: When the cache is full, it selects the policy based on their
// reward_weight for eviction. A policy is more likely to be selected if it has
// a higher reward_weight. Then, we randomly sample a few entries and evict keys
// using the selected policy until the cache has sufficient space for the new
// key-value pair.
//
// [1]. Vietri, Giuseppe, et al. "Driving Cache Replacement with ML-based
// LeCaR." 10th {USENIX} Workshop on Hot Topics in Storage and File Systems
// (HotStorage 18). 2018.
class LeCaR : public Cache {
 public:
  struct LeCaRHandle {
    size_t value_size = 0;
    uint64_t last_access_time = 0;
    uint64_t number_of_hits = 0;
  };

  enum Policy : size_t { LRU = 0, LFU = 1, MRU = 2 };

  struct EnumClassHash {
    template <typename T>
    std::size_t operator()(T t) const {
      return static_cast<std::size_t>(t);
    }
  };

  struct PolicyState {
    std::unordered_map<std::string, LeCaRHandle> evicted_keys;
    double reward_weight = 0;
    double regret_weight = 0;
  };

  LeCaR(size_t cache_capacity,
        const std::unordered_map<Policy, double, EnumClassHash>&
            policy_init_regret_weights,
        std::shared_ptr<MemoryAllocator> allocator = nullptr);

  // The type of the Cache
  const char* Name() const override { return "LeCaR"; }

  Status Insert(const Slice& key, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value),
                Handle** handle = nullptr,
                Priority priority = Priority::LOW) override;

  Handle* Lookup(const Slice& key, uint64_t now);

  void Erase(const Slice& key) override;

  Policy current_policy() { return current_policy_; }

  LRUHandle* NewLRUHandle(const Slice& key, LeCaRHandle* handle);

  std::unordered_map<Policy, PolicyState, EnumClassHash>& Test_policy_states() {
    return policy_states_;
  }

  Handle* Lookup(const Slice& /*key*/,
                 Statistics* /*stats*/ = nullptr) override {
    return nullptr;
  }
  bool Ref(Handle* /*handle*/) override { return false; };
  bool Release(Handle* /*handle*/, bool /*force_erase*/ = false) override {
    return false;
  };
  void* Value(Handle* /*handle*/) override { return nullptr; }
  uint64_t NewId() override { return 0; }
  void SetCapacity(size_t /*capacity*/) override {}
  void SetStrictCapacityLimit(bool /*strict_capacity_limit*/) override {}
  bool HasStrictCapacityLimit() const override { return false; }
  size_t GetCapacity() const override { return cache_capacity_; }
  size_t GetUsage() const override { return usage_; }
  size_t GetUsage(Handle* /*handle*/) const override { return 0; }
  size_t GetPinnedUsage() const override { return 0; };
  size_t GetCharge(Handle* /*handle*/) const override { return 0; }
  void ApplyToAllCacheEntries(void (*)(void*, size_t),
                              bool /*thread_safe*/) override {}
  void EraseUnRefEntries() override{};

  static const uint32_t kSampleSize;
  static const double kLearningRate;
  static const std::vector<LeCaR::Policy> kPolicies;

 private:
  Policy DecidePolicy();

  void Evict(Policy policy, LeCaRHandle* handle);

  bool LRUComparator(const std::pair<std::string, LeCaRHandle*>& a,
                     const std::pair<std::string, LeCaRHandle*>& b) {
    assert(a.second != nullptr);
    assert(b.second != nullptr);
    return a.second->last_access_time < b.second->last_access_time;
  };

  bool MRUComparator(const std::pair<std::string, LeCaRHandle*>& a,
                     const std::pair<std::string, LeCaRHandle*>& b) {
    assert(a.second != nullptr);
    assert(b.second != nullptr);
    return a.second->last_access_time > b.second->last_access_time;
  };

  bool LFUComparator(const std::pair<std::string, LeCaRHandle*>& a,
                     const std::pair<std::string, LeCaRHandle*>& b) {
    assert(a.second != nullptr);
    assert(b.second != nullptr);
    return a.second->number_of_hits < b.second->number_of_hits;
  };

  void UpdateWeight(const std::string& key, uint64_t now);
  void NormalizeRegretWeights();
  void ComputeRewardWeights();

  const size_t cache_capacity_;
  const double discount_rate_;
  size_t usage_ = 0;
  Policy current_policy_ = Policy::LRU;
  LRUHandleTable cache_;
  std::unordered_map<std::string, LeCaRHandle*> samples_;
  std::unordered_map<Policy, PolicyState, EnumClassHash> policy_states_;
};

// It delegates lookup/insert calls to LeCaR or Cache.
// This class is needed as LeCaR requires additional information of the access
// for reinforcement learning.
class CacheSimulatorDelegate {
 public:
  CacheSimulatorDelegate(std::shared_ptr<Cache> sim_cache)
      : sim_cache_(sim_cache) {}
  bool Lookup(const Slice& key, const BlockCacheTraceRecord& access);

  Status Insert(const Slice& key, uint64_t value_size,
                const BlockCacheTraceRecord& access, Cache::Priority priority);

 private:
  std::shared_ptr<Cache> sim_cache_;
};

// A ghost cache admits an entry on its second access.
class GhostCache {
 public:
  explicit GhostCache(std::shared_ptr<Cache> sim_cache);
  ~GhostCache() = default;
  // No copy and move.
  GhostCache(const GhostCache&) = delete;
  GhostCache& operator=(const GhostCache&) = delete;
  GhostCache(GhostCache&&) = delete;
  GhostCache& operator=(GhostCache&&) = delete;

  // Returns true if the lookup_key is in the ghost cache.
  // Returns false otherwise.
  bool Admit(const Slice& lookup_key, const BlockCacheTraceRecord& access);

 private:
  std::unique_ptr<CacheSimulatorDelegate> sim_cache_;
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

  void reset_counter() { miss_ratio_stats_.reset_counter(); }

  const MissRatioStats& miss_ratio_stats() const { return miss_ratio_stats_; }

 protected:
  MissRatioStats miss_ratio_stats_;
  std::unique_ptr<GhostCache> ghost_cache_;
  std::unique_ptr<CacheSimulatorDelegate> sim_cache_;
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
  void AccessKVPair(const Slice& key, uint64_t value_size,
                    Cache::Priority priority,
                    const BlockCacheTraceRecord& access, bool no_insert,
                    bool is_user_access, bool* is_cache_miss, bool* admitted,
                    bool update_metrics);

  Cache::Priority ComputeBlockPriority(
      const BlockCacheTraceRecord& access) const;
};

// A hybrid row and block cache simulator. It looks up/inserts key-value pairs
// referenced by Get/MultiGet requests, and not their accessed index/filter/data
// blocks.
//
// Upon a Get/MultiGet request, it looks up the referenced key first.
// If it observes a cache hit, future block accesses on this key-value pair is
// skipped since the request is served already. Otherwise, it continues to look
// up/insert its index/filter/data blocks. It also inserts the referenced
// key-value pair in the cache for future lookups.
class HybridRowBlockCacheSimulator : public PrioritizedCacheSimulator {
 public:
  HybridRowBlockCacheSimulator(std::unique_ptr<GhostCache>&& ghost_cache,
                               std::shared_ptr<Cache> sim_cache,
                               bool insert_blocks_upon_row_kvpair_miss)
      : PrioritizedCacheSimulator(std::move(ghost_cache), sim_cache),
        insert_blocks_upon_row_kvpair_miss_(
            insert_blocks_upon_row_kvpair_miss) {}
  void Access(const BlockCacheTraceRecord& access) override;

 private:
  enum InsertResult : char {
    INSERTED,
    ADMITTED,
    NO_INSERT,
  };

  // A map stores get_id to a map of row keys. For each row key, it stores a
  // boolean and an enum. The first bool is true when we observe a miss upon the
  // first time we encounter the row key. The second arg is INSERTED when the
  // kv-pair has been inserted into the cache, ADMITTED if it should be inserted
  // but haven't been, NO_INSERT if it should not be inserted.
  //
  // A kv-pair is in ADMITTED state when we encounter this kv-pair but do not
  // know its size. This may happen if the first access on the referenced key is
  // an index/filter block.
  std::map<uint64_t, std::map<std::string, std::pair<bool, InsertResult>>>
      getid_getkeys_map_;
  bool insert_blocks_upon_row_kvpair_miss_;
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
