//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#include "rocksdb/utilities/sim_cache.h"
#include <cstdlib>
#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace rocksdb {

class SimCacheTest : public DBTestBase {
 private:
  size_t miss_count_ = 0;
  size_t hit_count_ = 0;
  size_t insert_count_ = 0;
  size_t failure_count_ = 0;

 public:
  const size_t kNumBlocks = 5;
  const size_t kValueSize = 1000;

  SimCacheTest() : DBTestBase("/sim_cache_test") {}

  BlockBasedTableOptions GetTableOptions() {
    BlockBasedTableOptions table_options;
    // Set a small enough block size so that each key-value get its own block.
    table_options.block_size = 1;
    return table_options;
  }

  Options GetOptions(const BlockBasedTableOptions& table_options) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    // options.compression = kNoCompression;
    options.statistics = rocksdb::CreateDBStatistics();
    options.table_factory.reset(new BlockBasedTableFactory(table_options));
    return options;
  }

  void InitTable(const Options& options) {
    std::string value(kValueSize, 'a');
    for (size_t i = 0; i < kNumBlocks * 2; i++) {
      ASSERT_OK(Put(ToString(i), value.c_str()));
    }
  }

  void RecordCacheCounters(const Options& options) {
    miss_count_ = TestGetTickerCount(options, BLOCK_CACHE_MISS);
    hit_count_ = TestGetTickerCount(options, BLOCK_CACHE_HIT);
    insert_count_ = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    failure_count_ = TestGetTickerCount(options, BLOCK_CACHE_ADD_FAILURES);
  }

  void CheckCacheCounters(const Options& options, size_t expected_misses,
                          size_t expected_hits, size_t expected_inserts,
                          size_t expected_failures) {
    size_t new_miss_count = TestGetTickerCount(options, BLOCK_CACHE_MISS);
    size_t new_hit_count = TestGetTickerCount(options, BLOCK_CACHE_HIT);
    size_t new_insert_count = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    size_t new_failure_count =
        TestGetTickerCount(options, BLOCK_CACHE_ADD_FAILURES);
    ASSERT_EQ(miss_count_ + expected_misses, new_miss_count);
    ASSERT_EQ(hit_count_ + expected_hits, new_hit_count);
    ASSERT_EQ(insert_count_ + expected_inserts, new_insert_count);
    ASSERT_EQ(failure_count_ + expected_failures, new_failure_count);
    miss_count_ = new_miss_count;
    hit_count_ = new_hit_count;
    insert_count_ = new_insert_count;
    failure_count_ = new_failure_count;
  }
};

TEST_F(SimCacheTest, SimCache) {
  ReadOptions read_options;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);
  std::shared_ptr<SimCache> simCache =
      NewSimCache(NewLRUCache(0, 0, false), 20000, 0);
  table_options.block_cache = simCache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks);
  Iterator* iter = nullptr;

  // Load blocks into cache.
  for (size_t i = 0; i < kNumBlocks; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(ToString(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 1, 0, 1, 0);
    iterators[i].reset(iter);
  }
  ASSERT_EQ(kNumBlocks,
            simCache->get_hit_counter() + simCache->get_miss_counter());
  ASSERT_EQ(0, simCache->get_hit_counter());
  size_t usage = simCache->GetUsage();
  ASSERT_LT(0, usage);
  ASSERT_EQ(usage, simCache->GetSimUsage());
  simCache->SetCapacity(usage);
  ASSERT_EQ(usage, simCache->GetPinnedUsage());

  // Test with strict capacity limit.
  simCache->SetStrictCapacityLimit(true);
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(kNumBlocks * 2 - 1));
  ASSERT_TRUE(iter->status().IsIncomplete());
  CheckCacheCounters(options, 1, 0, 0, 1);
  delete iter;
  iter = nullptr;

  // Release iterators and access cache again.
  for (size_t i = 0; i < kNumBlocks; i++) {
    iterators[i].reset();
    CheckCacheCounters(options, 0, 0, 0, 0);
  }
  // Add kNumBlocks again
  for (size_t i = 0; i < kNumBlocks; i++) {
    std::unique_ptr<Iterator> it(db_->NewIterator(read_options));
    it->Seek(ToString(i));
    ASSERT_OK(it->status());
    CheckCacheCounters(options, 0, 1, 0, 0);
  }
  ASSERT_EQ(5, simCache->get_hit_counter());
  for (size_t i = kNumBlocks; i < kNumBlocks * 2; i++) {
    std::unique_ptr<Iterator> it(db_->NewIterator(read_options));
    it->Seek(ToString(i));
    ASSERT_OK(it->status());
    CheckCacheCounters(options, 1, 0, 1, 0);
  }
  ASSERT_EQ(0, simCache->GetPinnedUsage());
  ASSERT_EQ(3 * kNumBlocks + 1,
            simCache->get_hit_counter() + simCache->get_miss_counter());
  ASSERT_EQ(6, simCache->get_hit_counter());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
