//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cstdlib>
#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace rocksdb {

static uint64_t TestGetTickerCount(const Options& options,
                                   Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}

class DBBlockCacheTest : public DBTestBase {
 private:
  size_t miss_count_ = 0;
  size_t hit_count_ = 0;
  size_t insert_count_ = 0;
  size_t failure_count_ = 0;
  size_t compressed_miss_count_ = 0;
  size_t compressed_hit_count_ = 0;
  size_t compressed_insert_count_ = 0;
  size_t compressed_failure_count_ = 0;

 public:
  const size_t kNumBlocks = 10;
  const size_t kValueSize = 100;

  DBBlockCacheTest() : DBTestBase("/db_block_cache_test") {}

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
    for (size_t i = 0; i < kNumBlocks; i++) {
      ASSERT_OK(Put(ToString(i), value.c_str()));
    }
  }

  void RecordCacheCounters(const Options& options) {
    miss_count_ = TestGetTickerCount(options, BLOCK_CACHE_MISS);
    hit_count_ = TestGetTickerCount(options, BLOCK_CACHE_HIT);
    insert_count_ = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    failure_count_ = TestGetTickerCount(options, BLOCK_CACHE_ADD_FAILURES);
    compressed_miss_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS);
    compressed_hit_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT);
    compressed_insert_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD);
    compressed_failure_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
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

  void CheckCompressedCacheCounters(const Options& options,
                                    size_t expected_misses,
                                    size_t expected_hits,
                                    size_t expected_inserts,
                                    size_t expected_failures) {
    size_t new_miss_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS);
    size_t new_hit_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT);
    size_t new_insert_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD);
    size_t new_failure_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
    ASSERT_EQ(compressed_miss_count_ + expected_misses, new_miss_count);
    ASSERT_EQ(compressed_hit_count_ + expected_hits, new_hit_count);
    ASSERT_EQ(compressed_insert_count_ + expected_inserts, new_insert_count);
    ASSERT_EQ(compressed_failure_count_ + expected_failures, new_failure_count);
    compressed_miss_count_ = new_miss_count;
    compressed_hit_count_ = new_hit_count;
    compressed_insert_count_ = new_insert_count;
    compressed_failure_count_ = new_failure_count;
  }
};

TEST_F(DBBlockCacheTest, TestWithoutCompressedBlockCache) {
  ReadOptions read_options;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  std::shared_ptr<Cache> cache = NewLRUCache(0, 0, false);
  table_options.block_cache = cache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks - 1);
  Iterator* iter = nullptr;

  // Load blocks into cache.
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(ToString(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 1, 0, 1, 0);
    iterators[i].reset(iter);
  }
  size_t usage = cache->GetUsage();
  ASSERT_LT(0, usage);
  cache->SetCapacity(usage);
  ASSERT_EQ(usage, cache->GetPinnedUsage());

  // Test with strict capacity limit.
  cache->SetStrictCapacityLimit(true);
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(kNumBlocks - 1));
  ASSERT_TRUE(iter->status().IsIncomplete());
  CheckCacheCounters(options, 1, 0, 0, 1);
  delete iter;
  iter = nullptr;

  // Release interators and access cache again.
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iterators[i].reset();
    CheckCacheCounters(options, 0, 0, 0, 0);
  }
  ASSERT_EQ(0, cache->GetPinnedUsage());
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(ToString(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 0, 1, 0, 0);
    iterators[i].reset(iter);
  }
}

#ifdef SNAPPY
TEST_F(DBBlockCacheTest, TestWithCompressedBlockCache) {
  ReadOptions read_options;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  options.compression = CompressionType::kSnappyCompression;
  InitTable(options);

  std::shared_ptr<Cache> cache = NewLRUCache(0, 0, false);
  std::shared_ptr<Cache> compressed_cache = NewLRUCache(0, 0, false);
  table_options.block_cache = cache;
  table_options.block_cache_compressed = compressed_cache;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks - 1);
  Iterator* iter = nullptr;

  // Load blocks into cache.
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(ToString(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 1, 0, 1, 0);
    CheckCompressedCacheCounters(options, 1, 0, 1, 0);
    iterators[i].reset(iter);
  }
  size_t usage = cache->GetUsage();
  ASSERT_LT(0, usage);
  ASSERT_EQ(usage, cache->GetPinnedUsage());
  size_t compressed_usage = compressed_cache->GetUsage();
  ASSERT_LT(0, compressed_usage);
  // Compressed block cache cannot be pinned.
  ASSERT_EQ(0, compressed_cache->GetPinnedUsage());

  // Set strict capacity limit flag. Now block will only load into compressed
  // block cache.
  cache->SetCapacity(usage);
  cache->SetStrictCapacityLimit(true);
  ASSERT_EQ(usage, cache->GetPinnedUsage());
  // compressed_cache->SetCapacity(compressed_usage);
  compressed_cache->SetCapacity(0);
  // compressed_cache->SetStrictCapacityLimit(true);
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(kNumBlocks - 1));
  ASSERT_TRUE(iter->status().IsIncomplete());
  CheckCacheCounters(options, 1, 0, 0, 1);
  CheckCompressedCacheCounters(options, 1, 0, 1, 0);
  delete iter;
  iter = nullptr;

  // Clear strict capacity limit flag. This time we shall hit compressed block
  // cache.
  cache->SetStrictCapacityLimit(false);
  iter = db_->NewIterator(read_options);
  iter->Seek(ToString(kNumBlocks - 1));
  ASSERT_OK(iter->status());
  CheckCacheCounters(options, 1, 0, 1, 0);
  CheckCompressedCacheCounters(options, 0, 1, 0, 0);
  delete iter;
  iter = nullptr;
}
#endif

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
