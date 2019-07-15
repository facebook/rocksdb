//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"

#include <cstdlib>
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace rocksdb {
namespace {
const std::string kBlockKeyPrefix = "test-block-";
const std::string kRefKeyPrefix = "test-get-";
const uint64_t kGetId = 1;
const uint64_t kGetBlockId = 100;
const uint64_t kCompactionBlockId = 1000;
const uint64_t kCacheSize = 1024 * 1024 * 1024;
const uint64_t kGhostCacheSize = 1024 * 1024;
}  // namespace

class CacheSimulatorTest : public testing::Test {
 public:
  const size_t kNumBlocks = 5;
  const size_t kValueSize = 1000;

  CacheSimulatorTest() { env_ = rocksdb::Env::Default(); }

  BlockCacheTraceRecord GenerateGetRecord(uint64_t getid) {
    BlockCacheTraceRecord record;
    record.block_type = TraceType::kBlockTraceDataBlock;
    record.block_size = 4096;
    record.block_key = kBlockKeyPrefix + std::to_string(kGetBlockId);
    record.access_timestamp = env_->NowMicros();
    record.cf_id = 0;
    record.cf_name = "test";
    record.caller = TableReaderCaller::kUserGet;
    record.level = 6;
    record.sst_fd_number = kGetBlockId;
    record.get_id = getid;
    record.is_cache_hit = Boolean::kFalse;
    record.no_insert = Boolean::kFalse;
    record.referenced_key =
        kRefKeyPrefix + std::to_string(kGetId) + std::string(8, 'c');
    record.referenced_key_exist_in_block = Boolean::kTrue;
    record.referenced_data_size = 100;
    record.num_keys_in_block = 300;
    return record;
  }

  BlockCacheTraceRecord GenerateCompactionRecord() {
    BlockCacheTraceRecord record;
    record.block_type = TraceType::kBlockTraceDataBlock;
    record.block_size = 4096;
    record.block_key = kBlockKeyPrefix + std::to_string(kCompactionBlockId);
    record.access_timestamp = env_->NowMicros();
    record.cf_id = 0;
    record.cf_name = "test";
    record.caller = TableReaderCaller::kCompaction;
    record.level = 6;
    record.sst_fd_number = kCompactionBlockId;
    record.is_cache_hit = Boolean::kFalse;
    record.no_insert = Boolean::kTrue;
    return record;
  }

  Env* env_;
};

TEST_F(CacheSimulatorTest, GhostCache) {
  const std::string key1 = "test1";
  const std::string key2 = "test2";
  BlockCacheTraceRecord dummy_record;
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  EXPECT_FALSE(ghost_cache->Admit(key1, dummy_record));
  EXPECT_TRUE(ghost_cache->Admit(key1, dummy_record));
  EXPECT_TRUE(ghost_cache->Admit(key1, dummy_record));
  EXPECT_FALSE(ghost_cache->Admit(key2, dummy_record));
  EXPECT_TRUE(ghost_cache->Admit(key2, dummy_record));
}

TEST_F(CacheSimulatorTest, CacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  const BlockCacheTraceRecord& compaction_access = GenerateCompactionRecord();
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<CacheSimulator> cache_simulator(
      new CacheSimulator(nullptr, sim_cache));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().user_miss_ratio());

  cache_simulator->Access(compaction_access);
  cache_simulator->Access(compaction_access);
  ASSERT_EQ(4, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(75, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().user_miss_ratio());

  cache_simulator->reset_counter();
  ASSERT_EQ(0, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(-1, cache_simulator->miss_ratio_stats().miss_ratio());
  auto handle = sim_cache->Lookup(access.block_key);
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  handle = sim_cache->Lookup(compaction_access.block_key);
  ASSERT_EQ(nullptr, handle);
}

TEST_F(CacheSimulatorTest, GhostCacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  std::unique_ptr<CacheSimulator> cache_simulator(new CacheSimulator(
      std::move(ghost_cache),
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  // Both of them will be miss since we have a ghost cache.
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
}

TEST_F(CacheSimulatorTest, PrioritizedCacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<PrioritizedCacheSimulator> cache_simulator(
      new PrioritizedCacheSimulator(nullptr, sim_cache));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().miss_ratio());

  auto handle = sim_cache->Lookup(access.block_key);
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
}

TEST_F(CacheSimulatorTest, GhostPrioritizedCacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  std::unique_ptr<PrioritizedCacheSimulator> cache_simulator(
      new PrioritizedCacheSimulator(
          std::move(ghost_cache),
          NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                      /*strict_capacity_limit=*/false,
                      /*high_pri_pool_ratio=*/0)));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  // Both of them will be miss since we have a ghost cache.
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
}

TEST_F(CacheSimulatorTest, LeCaRLRU) {
  uint64_t now = 1;
  std::unordered_map<LeCaR::Policy, double, LeCaR::EnumClassHash>
      policy_regret_weights;
  policy_regret_weights[LeCaR::Policy::LRU] = 0.0;
  std::unique_ptr<LeCaR> sim_cache(
      new LeCaR(LeCaR::kSampleSize, policy_regret_weights));
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = 1;
    value->last_access_time = ++now;
    std::string key = "k" + std::to_string(i);
    ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, value), 1,
                                nullptr));
    ASSERT_EQ(LeCaR::Policy::LRU, sim_cache->current_policy());
    ASSERT_EQ(i + 1, sim_cache->GetUsage());
  }

  // Check if all inserted keys exist.
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        sim_cache->Lookup("k" + std::to_string(i), ++now));
    ASSERT_NE(nullptr, e);
    LeCaR::LeCaRHandle* handle =
        reinterpret_cast<LeCaR::LeCaRHandle*>(e->value);
    ASSERT_NE(nullptr, handle);
    ASSERT_EQ(now, handle->last_access_time);
    ASSERT_EQ(1, handle->number_of_hits);
    ASSERT_EQ(LeCaR::kSampleSize, sim_cache->GetUsage());
  }

  // Insert new key-value pairs should evict k0, k1, k2....
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = 1;
    value->last_access_time = ++now;
    std::string key = "k" + std::to_string(i + LeCaR::kSampleSize);
    ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, value), 1,
                                nullptr));
    ASSERT_EQ(LeCaR::Policy::LRU, sim_cache->current_policy());
    ASSERT_EQ(LeCaR::kSampleSize, sim_cache->GetUsage());
    ASSERT_EQ(nullptr, sim_cache->Lookup("k" + std::to_string(i), ++now));
  }

  // Check if all inserted keys exist.
  auto& policy_states = sim_cache->Test_policy_states();
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        sim_cache->Lookup("k" + std::to_string(i + LeCaR::kSampleSize), ++now));
    ASSERT_NE(nullptr, e);
    LeCaR::LeCaRHandle* handle =
        reinterpret_cast<LeCaR::LeCaRHandle*>(e->value);
    ASSERT_NE(nullptr, handle);
    ASSERT_EQ(now, handle->last_access_time);
    ASSERT_EQ(1, handle->number_of_hits);
    ASSERT_NE(policy_states[LeCaR::Policy::LRU].evicted_keys.end(),
              policy_states[LeCaR::Policy::LRU].evicted_keys.find(
                  "k" + std::to_string(i)));
  }
  ASSERT_EQ(LeCaR::kSampleSize,
            policy_states[LeCaR::Policy::LRU].evicted_keys.size());
  ASSERT_EQ(0, policy_states[LeCaR::Policy::LFU].evicted_keys.size());
  ASSERT_EQ(0, policy_states[LeCaR::Policy::MRU].evicted_keys.size());
}

TEST_F(CacheSimulatorTest, LeCaRMRU) {
  uint64_t now = port::kMaxUint64;
  std::unordered_map<LeCaR::Policy, double, LeCaR::EnumClassHash>
      policy_regret_weights;
  policy_regret_weights[LeCaR::Policy::MRU] = 0.0;
  std::unique_ptr<LeCaR> sim_cache(
      new LeCaR(LeCaR::kSampleSize, policy_regret_weights));
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = 1;
    value->last_access_time = --now;
    std::string key = "k" + std::to_string(i);
    ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, value), 1,
                                nullptr));
    ASSERT_EQ(LeCaR::Policy::MRU, sim_cache->current_policy());
  }

  // Check if all inserted keys exist.
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        sim_cache->Lookup("k" + std::to_string(i), --now));
    ASSERT_NE(nullptr, e);
    LeCaR::LeCaRHandle* handle =
        reinterpret_cast<LeCaR::LeCaRHandle*>(e->value);
    ASSERT_NE(nullptr, handle);
    ASSERT_EQ(now, handle->last_access_time);
    ASSERT_EQ(1, handle->number_of_hits);
  }

  // Insert new key-value pairs should evict k0, k1, k2....
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = 1;
    value->last_access_time = --now;
    std::string key = "k" + std::to_string(i + LeCaR::kSampleSize);
    ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, value), 1,
                                nullptr));
    ASSERT_EQ(LeCaR::Policy::MRU, sim_cache->current_policy());
    ASSERT_EQ(nullptr, sim_cache->Lookup("k" + std::to_string(i), --now));
  }

  // Check if all inserted keys exist.
  auto& policy_states = sim_cache->Test_policy_states();
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        sim_cache->Lookup("k" + std::to_string(i + LeCaR::kSampleSize), --now));
    ASSERT_NE(nullptr, e);
    LeCaR::LeCaRHandle* handle =
        reinterpret_cast<LeCaR::LeCaRHandle*>(e->value);
    ASSERT_NE(nullptr, handle);
    ASSERT_EQ(now, handle->last_access_time);
    ASSERT_EQ(1, handle->number_of_hits);
    ASSERT_NE(policy_states[LeCaR::Policy::MRU].evicted_keys.end(),
              policy_states[LeCaR::Policy::MRU].evicted_keys.find(
                  "k" + std::to_string(i)));
  }
  ASSERT_EQ(LeCaR::kSampleSize,
            policy_states[LeCaR::Policy::MRU].evicted_keys.size());
  ASSERT_EQ(0, policy_states[LeCaR::Policy::LFU].evicted_keys.size());
  ASSERT_EQ(0, policy_states[LeCaR::Policy::LRU].evicted_keys.size());
}

TEST_F(CacheSimulatorTest, LeCaRLFU) {
  uint64_t now = 0;
  std::unordered_map<LeCaR::Policy, double, LeCaR::EnumClassHash>
      policy_regret_weights;
  policy_regret_weights[LeCaR::Policy::LFU] = 0.0;
  std::unique_ptr<LeCaR> sim_cache(
      new LeCaR(LeCaR::kSampleSize, policy_regret_weights));
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = 1;
    value->last_access_time = ++now;
    std::string key = "k" + std::to_string(i);
    ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, value), 1,
                                nullptr));
    ASSERT_EQ(LeCaR::Policy::LFU, sim_cache->current_policy());

    for (uint32_t j = 0; j < i; j++) {
      LRUHandle* e =
          reinterpret_cast<LRUHandle*>(sim_cache->Lookup(key, ++now));
      ASSERT_NE(nullptr, e);
      LeCaR::LeCaRHandle* handle =
          reinterpret_cast<LeCaR::LeCaRHandle*>(e->value);
      ASSERT_NE(nullptr, handle);
      ASSERT_EQ(now, handle->last_access_time);
      ASSERT_EQ(j + 1, handle->number_of_hits);
    }
  }

  // Insert new key-value pairs should evict k0, k1, k2....
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = 1;
    value->last_access_time = ++now;
    std::string key = "k" + std::to_string(i + LeCaR::kSampleSize);
    ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, value), 1,
                                nullptr));
    // Give enough hits so it will evict the keys added previously.
    for (uint32_t j = 0; j <= LeCaR::kSampleSize; j++) {
      LRUHandle* e =
          reinterpret_cast<LRUHandle*>(sim_cache->Lookup(key, ++now));
      ASSERT_NE(nullptr, e);
      LeCaR::LeCaRHandle* handle =
          reinterpret_cast<LeCaR::LeCaRHandle*>(e->value);
      ASSERT_NE(nullptr, handle);
      ASSERT_EQ(now, handle->last_access_time);
      ASSERT_EQ(j + 1, handle->number_of_hits);
    }
    ASSERT_EQ(LeCaR::Policy::LFU, sim_cache->current_policy());
    ASSERT_EQ(nullptr, sim_cache->Lookup("k" + std::to_string(i), ++now));
  }

  // Check if all inserted keys exist.
  auto& policy_states = sim_cache->Test_policy_states();
  for (uint32_t i = 0; i < LeCaR::kSampleSize; i++) {
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        sim_cache->Lookup("k" + std::to_string(i + LeCaR::kSampleSize), ++now));
    ASSERT_NE(nullptr, e);
    LeCaR::LeCaRHandle* handle =
        reinterpret_cast<LeCaR::LeCaRHandle*>(e->value);
    ASSERT_NE(nullptr, handle);
    ASSERT_EQ(now, handle->last_access_time);
    ASSERT_EQ(LeCaR::kSampleSize + 2, handle->number_of_hits);
    ASSERT_NE(policy_states[LeCaR::Policy::LFU].evicted_keys.end(),
              policy_states[LeCaR::Policy::LFU].evicted_keys.find(
                  "k" + std::to_string(i)));
  }
  ASSERT_EQ(LeCaR::kSampleSize,
            policy_states[LeCaR::Policy::LFU].evicted_keys.size());
  ASSERT_EQ(0, policy_states[LeCaR::Policy::MRU].evicted_keys.size());
  ASSERT_EQ(0, policy_states[LeCaR::Policy::LRU].evicted_keys.size());
}

TEST_F(CacheSimulatorTest, LeCaRMix) {
  uint64_t now = 1;
  std::unordered_map<LeCaR::Policy, double, LeCaR::EnumClassHash>
      policy_regret_weights;
  policy_regret_weights[LeCaR::Policy::LRU] = 0.32;
  policy_regret_weights[LeCaR::Policy::LFU] = 0.34;
  policy_regret_weights[LeCaR::Policy::MRU] = 0.34;
  std::unique_ptr<LeCaR> sim_cache(new LeCaR(1024, policy_regret_weights));

  auto& policy_states = sim_cache->Test_policy_states();
  ASSERT_DOUBLE_EQ(0.32, policy_states[LeCaR::Policy::LRU].regret_weight);
  ASSERT_DOUBLE_EQ(0.34, policy_states[LeCaR::Policy::LFU].regret_weight);
  ASSERT_DOUBLE_EQ(0.34, policy_states[LeCaR::Policy::MRU].regret_weight);
  ASSERT_DOUBLE_EQ(0.34, policy_states[LeCaR::Policy::LRU].reward_weight);
  ASSERT_DOUBLE_EQ(0.33, policy_states[LeCaR::Policy::LFU].reward_weight);
  ASSERT_DOUBLE_EQ(0.33, policy_states[LeCaR::Policy::MRU].reward_weight);
  // Perform a bunch of insertions and lookups.
  for (uint32_t k = 1; k < 10000; k++) {
    LeCaR::LeCaRHandle* value = new LeCaR::LeCaRHandle;
    value->value_size = (k % 1024) + 1;
    value->last_access_time = ++now;
    std::string key = "k" + std::to_string(k);
    ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, value),
                                value->value_size, nullptr));
  }
  uint32_t misses = 0;
  uint32_t accesses = 0;
  for (uint32_t k = 1; k < 10000; k++) {
    if (!sim_cache->Lookup("k" + std::to_string(k), now++)) {
      misses++;
    }
    accesses++;
  }
  ASSERT_LT(misses, accesses);
  LeCaR::LeCaRHandle* large_value = new LeCaR::LeCaRHandle;
  large_value->value_size = 1024;
  large_value->last_access_time = ++now;
  std::string key = "k-1";
  ASSERT_OK(sim_cache->Insert(key, sim_cache->NewLRUHandle(key, large_value),
                              1024, nullptr));
  ASSERT_NE(nullptr, sim_cache->Lookup("k-1", now++));
}

TEST_F(CacheSimulatorTest, HybridRowBlockCacheSimulator) {
  uint64_t block_id = 100;
  BlockCacheTraceRecord first_get = GenerateGetRecord(kGetId);
  first_get.get_from_user_specified_snapshot = Boolean::kTrue;
  BlockCacheTraceRecord second_get = GenerateGetRecord(kGetId + 1);
  second_get.referenced_data_size = 0;
  second_get.referenced_key_exist_in_block = Boolean::kFalse;
  second_get.get_from_user_specified_snapshot = Boolean::kTrue;
  BlockCacheTraceRecord third_get = GenerateGetRecord(kGetId + 2);
  third_get.referenced_data_size = 0;
  third_get.referenced_key_exist_in_block = Boolean::kFalse;
  third_get.referenced_key = kRefKeyPrefix + "third_get";
  // We didn't find the referenced key in the third get.
  third_get.referenced_key_exist_in_block = Boolean::kFalse;
  third_get.referenced_data_size = 0;
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<HybridRowBlockCacheSimulator> cache_simulator(
      new HybridRowBlockCacheSimulator(
          nullptr, sim_cache, /*insert_blocks_row_kvpair_misses=*/true));
  // The first get request accesses 10 blocks. We should only report 10 accesses
  // and 100% miss.
  for (uint32_t i = 0; i < 10; i++) {
    first_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(first_get);
    block_id++;
  }

  ASSERT_EQ(10, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(10, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().user_miss_ratio());
  auto handle = sim_cache->Lookup(
      std::to_string(first_get.sst_fd_number) + "_" +
      ExtractUserKey(first_get.referenced_key).ToString() + "_" +
      std::to_string(1 + GetInternalKeySeqno(first_get.referenced_key)));
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  for (uint32_t i = 100; i < block_id; i++) {
    handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
    ASSERT_NE(nullptr, handle);
    sim_cache->Release(handle);
  }

  // The second get request accesses the same key. We should report 15
  // access and 66% miss, 10 misses with 15 accesses.
  // We do not consider these 5 block lookups as misses since the row hits the
  // cache.
  for (uint32_t i = 0; i < 5; i++) {
    second_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(second_get);
    block_id++;
  }
  ASSERT_EQ(15, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(66, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().miss_ratio()));
  ASSERT_EQ(15, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(66, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().user_miss_ratio()));
  handle = sim_cache->Lookup(
      std::to_string(second_get.sst_fd_number) + "_" +
      ExtractUserKey(second_get.referenced_key).ToString() + "_" +
      std::to_string(1 + GetInternalKeySeqno(second_get.referenced_key)));
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  for (uint32_t i = 100; i < block_id; i++) {
    handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
    if (i < 110) {
      ASSERT_NE(nullptr, handle) << i;
      sim_cache->Release(handle);
    } else {
      ASSERT_EQ(nullptr, handle) << i;
    }
  }

  // The third get on a different key and does not have a size.
  // This key should not be inserted into the cache.
  for (uint32_t i = 0; i < 5; i++) {
    third_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(third_get);
    block_id++;
  }
  ASSERT_EQ(20, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(75, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().miss_ratio()));
  ASSERT_EQ(20, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(75, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().user_miss_ratio()));
  // Assert that the third key is not inserted into the cache.
  handle = sim_cache->Lookup(std::to_string(third_get.sst_fd_number) + "_" +
                             third_get.referenced_key);
  ASSERT_EQ(nullptr, handle);
  for (uint32_t i = 100; i < block_id; i++) {
    if (i < 110 || i >= 115) {
      handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
      ASSERT_NE(nullptr, handle) << i;
      sim_cache->Release(handle);
    } else {
      handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
      ASSERT_EQ(nullptr, handle) << i;
    }
  }
}

TEST_F(CacheSimulatorTest, HybridRowBlockNoInsertCacheSimulator) {
  uint64_t block_id = 100;
  BlockCacheTraceRecord first_get = GenerateGetRecord(kGetId);
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<HybridRowBlockCacheSimulator> cache_simulator(
      new HybridRowBlockCacheSimulator(
          nullptr, sim_cache, /*insert_blocks_row_kvpair_misses=*/false));
  for (uint32_t i = 0; i < 9; i++) {
    first_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(first_get);
    block_id++;
  }
  auto handle = sim_cache->Lookup(
      std::to_string(first_get.sst_fd_number) + "_" +
      ExtractUserKey(first_get.referenced_key).ToString() + "_0");
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  // All blocks are missing from the cache since insert_blocks_row_kvpair_misses
  // is set to false.
  for (uint32_t i = 100; i < block_id; i++) {
    handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
    ASSERT_EQ(nullptr, handle);
  }
}

TEST_F(CacheSimulatorTest, GhostHybridRowBlockCacheSimulator) {
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  const BlockCacheTraceRecord& first_get = GenerateGetRecord(kGetId);
  const BlockCacheTraceRecord& second_get = GenerateGetRecord(kGetId + 1);
  const BlockCacheTraceRecord& third_get = GenerateGetRecord(kGetId + 2);
  std::unique_ptr<HybridRowBlockCacheSimulator> cache_simulator(
      new HybridRowBlockCacheSimulator(
          std::move(ghost_cache),
          NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                      /*strict_capacity_limit=*/false,
                      /*high_pri_pool_ratio=*/0),
          /*insert_blocks_row_kvpair_misses=*/false));
  // Two get requests access the same key.
  cache_simulator->Access(first_get);
  cache_simulator->Access(second_get);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().user_miss_ratio());
  // We insert the key-value pair upon the second get request. A third get
  // request should observe a hit.
  for (uint32_t i = 0; i < 10; i++) {
    cache_simulator->Access(third_get);
  }
  ASSERT_EQ(12, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(16, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().miss_ratio()));
  ASSERT_EQ(12, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(16, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().user_miss_ratio()));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
