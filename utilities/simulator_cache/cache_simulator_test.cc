//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"

#include <cstdlib>
#include "db/db_test_util.h"

namespace rocksdb {
namespace {
const std::string kBlockKeyPrefix = "test-block-";
const std::string kRefKeyPrefix = "test-get-";
const uint64_t kGetId = 1;
const uint64_t kGetBlockId = 100;
const uint64_t kCompactionBlockId = 1000;
const uint64_t kCacheSize = 1024 * 1024;
const uint64_t kGhostCacheSize = 1024;
}  // namespace

class CacheSimulatorTest : public DBTestBase {
 public:
  const size_t kNumBlocks = 5;
  const size_t kValueSize = 1000;

  CacheSimulatorTest() : DBTestBase("/cache_simulator_test") {}

<<<<<<< HEAD
  BlockCacheTraceRecord GenerateGetRecord(uint64_t getid) {
=======
  BlockCacheTraceRecord GenerateAccessRecord(uint32_t key_id, bool no_insert) {
>>>>>>> Add more tests
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
<<<<<<< HEAD
    record.no_insert = Boolean::kFalse;
    record.referenced_key = kRefKeyPrefix + std::to_string(kGetBlockId);
=======
    record.no_insert = no_insert ? Boolean::kTrue : Boolean::kFalse;
    record.referenced_key = kRefKeyPrefix + std::to_string(key_id);
>>>>>>> Add more tests
    record.referenced_key_exist_in_block = Boolean::kTrue;
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
};

TEST_F(CacheSimulatorTest, GhostCache) {
  const std::string key1 = "test1";
  const std::string key2 = "test2";
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  EXPECT_FALSE(ghost_cache->Admit(key1));
  EXPECT_TRUE(ghost_cache->Admit(key1));
  EXPECT_TRUE(ghost_cache->Admit(key1));
  EXPECT_FALSE(ghost_cache->Admit(key2));
  EXPECT_TRUE(ghost_cache->Admit(key2));
}

TEST_F(CacheSimulatorTest, CacheSimulator) {
<<<<<<< HEAD
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  const BlockCacheTraceRecord& compaction_access = GenerateCompactionRecord();
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<CacheSimulator> cache_simulator(
      new CacheSimulator(nullptr, sim_cache));
=======
  const BlockCacheTraceRecord& access =
      GenerateAccessRecord(/*key_id=*/0, /*no_insert=*/false);
  const BlockCacheTraceRecord& access_no_insert =
      GenerateAccessRecord(/*key_id=*/1, /*no_insert=*/true);
  std::unique_ptr<CacheSimulator> cache_simulator(new CacheSimulator(
      nullptr, NewLRUCache(/*capacity=*/cache_size, /*num_shard_bits=*/1,
                           /*strict_capacity_limit=*/false,
                           /*high_pri_pool_ratio=*/0)));
>>>>>>> Add more tests
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->total_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio());
<<<<<<< HEAD
  ASSERT_EQ(2, cache_simulator->user_accesses());
  ASSERT_EQ(50, cache_simulator->user_miss_ratio());

  cache_simulator->Access(compaction_access);
  cache_simulator->Access(compaction_access);
  ASSERT_EQ(4, cache_simulator->total_accesses());
  ASSERT_EQ(75, cache_simulator->miss_ratio());
  ASSERT_EQ(2, cache_simulator->user_accesses());
  ASSERT_EQ(50, cache_simulator->user_miss_ratio());
=======
  cache_simulator->Access(access_no_insert);
  cache_simulator->Access(access_no_insert);
  ASSERT_EQ(4, cache_simulator->total_accesses());
  ASSERT_EQ(75, cache_simulator->miss_ratio());
>>>>>>> Add more tests

  cache_simulator->reset_counter();
  ASSERT_EQ(0, cache_simulator->total_accesses());
  ASSERT_EQ(-1, cache_simulator->miss_ratio());
  auto handle = sim_cache->Lookup(access.block_key);
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  handle = sim_cache->Lookup(compaction_access.block_key);
  ASSERT_EQ(nullptr, handle);
}

TEST_F(CacheSimulatorTest, GhostCacheSimulator) {
<<<<<<< HEAD
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
=======
  const BlockCacheTraceRecord& access =
      GenerateAccessRecord(/*key_id=*/0, /*no_insert=*/false);
>>>>>>> Add more tests
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
  ASSERT_EQ(2, cache_simulator->total_accesses());
  // Both of them will be miss since we have a ghost cache.
  ASSERT_EQ(100, cache_simulator->miss_ratio());
}

TEST_F(CacheSimulatorTest, PrioritizedCacheSimulator) {
<<<<<<< HEAD
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
=======
  const BlockCacheTraceRecord& access =
      GenerateAccessRecord(/*key_id=*/0, /*no_insert=*/false);
  const BlockCacheTraceRecord& access_no_insert =
      GenerateAccessRecord(/*key_id=*/1, /*no_insert=*/true);
>>>>>>> Add more tests
  std::unique_ptr<PrioritizedCacheSimulator> cache_simulator(
      new PrioritizedCacheSimulator(nullptr, sim_cache));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->total_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio());
<<<<<<< HEAD

  auto handle = sim_cache->Lookup(access.block_key);
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
}

TEST_F(CacheSimulatorTest, GhostPrioritizedCacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
=======
  cache_simulator->Access(access_no_insert);
  cache_simulator->Access(access_no_insert);
  ASSERT_EQ(4, cache_simulator->total_accesses());
  ASSERT_EQ(75, cache_simulator->miss_ratio());
}

TEST_F(CacheSimulatorTest, GhostPrioritizedCacheSimulator) {
  const BlockCacheTraceRecord& access =
      GenerateAccessRecord(/*key_id=*/0, /*no_insert=*/false);
>>>>>>> Add more tests
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
  ASSERT_EQ(2, cache_simulator->total_accesses());
  // Both of them will be miss since we have a ghost cache.
  ASSERT_EQ(100, cache_simulator->miss_ratio());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
