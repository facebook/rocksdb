//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cstdlib>
#include <functional>
#include <memory>
#include <unordered_set>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "cache/lru_cache.h"
#include "cache/typed_cache.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "env/unique_id_gen.h"
#include "port/stack_trace.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/unique_id_impl.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/random.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class DBBlockCacheTest : public DBTestBase {
 private:
  size_t miss_count_ = 0;
  size_t hit_count_ = 0;
  size_t insert_count_ = 0;
  size_t failure_count_ = 0;
  size_t compression_dict_miss_count_ = 0;
  size_t compression_dict_hit_count_ = 0;
  size_t compression_dict_insert_count_ = 0;
  size_t compressed_miss_count_ = 0;
  size_t compressed_hit_count_ = 0;
  size_t compressed_insert_count_ = 0;
  size_t compressed_failure_count_ = 0;

 public:
  const size_t kNumBlocks = 10;
  const size_t kValueSize = 100;

  DBBlockCacheTest()
      : DBTestBase("db_block_cache_test", /*env_do_fsync=*/true) {}

  BlockBasedTableOptions GetTableOptions() {
    BlockBasedTableOptions table_options;
    // Set a small enough block size so that each key-value get its own block.
    table_options.block_size = 1;
    return table_options;
  }

  Options GetOptions(const BlockBasedTableOptions& table_options) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.avoid_flush_during_recovery = false;
    // options.compression = kNoCompression;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    return options;
  }

  void InitTable(const Options& /*options*/) {
    std::string value(kValueSize, 'a');
    for (size_t i = 0; i < kNumBlocks; i++) {
      ASSERT_OK(Put(std::to_string(i), value.c_str()));
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

  void RecordCacheCountersForCompressionDict(const Options& options) {
    compression_dict_miss_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_MISS);
    compression_dict_hit_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_HIT);
    compression_dict_insert_count_ =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_ADD);
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

  void CheckCacheCountersForCompressionDict(
      const Options& options, size_t expected_compression_dict_misses,
      size_t expected_compression_dict_hits,
      size_t expected_compression_dict_inserts) {
    size_t new_compression_dict_miss_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_MISS);
    size_t new_compression_dict_hit_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_HIT);
    size_t new_compression_dict_insert_count =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_ADD);
    ASSERT_EQ(compression_dict_miss_count_ + expected_compression_dict_misses,
              new_compression_dict_miss_count);
    ASSERT_EQ(compression_dict_hit_count_ + expected_compression_dict_hits,
              new_compression_dict_hit_count);
    ASSERT_EQ(
        compression_dict_insert_count_ + expected_compression_dict_inserts,
        new_compression_dict_insert_count);
    compression_dict_miss_count_ = new_compression_dict_miss_count;
    compression_dict_hit_count_ = new_compression_dict_hit_count;
    compression_dict_insert_count_ = new_compression_dict_insert_count;
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

#ifndef ROCKSDB_LITE
  const std::array<size_t, kNumCacheEntryRoles> GetCacheEntryRoleCountsBg() {
    // Verify in cache entry role stats
    std::array<size_t, kNumCacheEntryRoles> cache_entry_role_counts;
    std::map<std::string, std::string> values;
    EXPECT_TRUE(db_->GetMapProperty(DB::Properties::kFastBlockCacheEntryStats,
                                    &values));
    for (size_t i = 0; i < kNumCacheEntryRoles; ++i) {
      auto role = static_cast<CacheEntryRole>(i);
      cache_entry_role_counts[i] =
          ParseSizeT(values[BlockCacheEntryStatsMapKeys::EntryCount(role)]);
    }
    return cache_entry_role_counts;
  }
#endif  // ROCKSDB_LITE
};

TEST_F(DBBlockCacheTest, IteratorBlockCacheUsage) {
  ReadOptions read_options;
  read_options.fill_cache = false;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  LRUCacheOptions co;
  co.capacity = 0;
  co.num_shard_bits = 0;
  co.strict_capacity_limit = false;
  // Needed not to count entry stats collector
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks - 1);
  Iterator* iter = nullptr;

  ASSERT_EQ(0, cache->GetUsage());
  iter = db_->NewIterator(read_options);
  iter->Seek(std::to_string(0));
  ASSERT_LT(0, cache->GetUsage());
  delete iter;
  iter = nullptr;
  ASSERT_EQ(0, cache->GetUsage());
}

TEST_F(DBBlockCacheTest, TestWithoutCompressedBlockCache) {
  ReadOptions read_options;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  LRUCacheOptions co;
  co.capacity = 0;
  co.num_shard_bits = 0;
  co.strict_capacity_limit = false;
  // Needed not to count entry stats collector
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  std::vector<std::unique_ptr<Iterator>> iterators(kNumBlocks - 1);
  Iterator* iter = nullptr;

  // Load blocks into cache.
  for (size_t i = 0; i + 1 < kNumBlocks; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(std::to_string(i));
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
  iter->Seek(std::to_string(kNumBlocks - 1));
  ASSERT_TRUE(iter->status().IsMemoryLimit());
  CheckCacheCounters(options, 1, 0, 0, 1);
  delete iter;
  iter = nullptr;

  // Release iterators and access cache again.
  for (size_t i = 0; i + 1 < kNumBlocks; i++) {
    iterators[i].reset();
    CheckCacheCounters(options, 0, 0, 0, 0);
  }
  ASSERT_EQ(0, cache->GetPinnedUsage());
  for (size_t i = 0; i + 1 < kNumBlocks; i++) {
    iter = db_->NewIterator(read_options);
    iter->Seek(std::to_string(i));
    ASSERT_OK(iter->status());
    CheckCacheCounters(options, 0, 1, 0, 0);
    iterators[i].reset(iter);
  }
}

#ifdef SNAPPY
TEST_F(DBBlockCacheTest, TestWithCompressedBlockCache) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  table_options.block_cache_compressed = nullptr;
  table_options.block_size = 1;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  table_options.cache_index_and_filter_blocks = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.compression = CompressionType::kSnappyCompression;

  DestroyAndReopen(options);

  std::string value(kValueSize, 'a');
  for (size_t i = 0; i < kNumBlocks; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Flush());
  }

  ReadOptions read_options;
  std::shared_ptr<Cache> compressed_cache = NewLRUCache(1 << 25, 0, false);
  LRUCacheOptions co;
  co.capacity = 0;
  co.num_shard_bits = 0;
  co.strict_capacity_limit = false;
  // Needed not to count entry stats collector
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  table_options.block_cache = cache;
  table_options.no_block_cache = false;
  table_options.block_cache_compressed = compressed_cache;
  table_options.max_auto_readahead_size = 0;
  table_options.cache_index_and_filter_blocks = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  RecordCacheCounters(options);

  // Load blocks into cache.
  for (size_t i = 0; i < kNumBlocks - 1; i++) {
    ASSERT_EQ(value, Get(std::to_string(i)));
    CheckCacheCounters(options, 1, 0, 1, 0);
    CheckCompressedCacheCounters(options, 1, 0, 1, 0);
  }

  size_t usage = cache->GetUsage();
  ASSERT_EQ(0, usage);
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

  // Load last key block.
  ASSERT_EQ(
      "Operation aborted: Memory limit reached: Insert failed due to LRU cache "
      "being full.",
      Get(std::to_string(kNumBlocks - 1)));
  // Failure will also record the miss counter.
  CheckCacheCounters(options, 1, 0, 0, 1);
  CheckCompressedCacheCounters(options, 1, 0, 1, 0);

  // Clear strict capacity limit flag. This time we shall hit compressed block
  // cache and load into block cache.
  cache->SetStrictCapacityLimit(false);
  // Load last key block.
  ASSERT_EQ(value, Get(std::to_string(kNumBlocks - 1)));
  CheckCacheCounters(options, 1, 0, 1, 0);
  CheckCompressedCacheCounters(options, 0, 1, 0, 0);
}

namespace {
class PersistentCacheFromCache : public PersistentCache {
 public:
  PersistentCacheFromCache(std::shared_ptr<Cache> cache, bool read_only)
      : cache_(cache), read_only_(read_only) {}

  Status Insert(const Slice& key, const char* data,
                const size_t size) override {
    if (read_only_) {
      return Status::NotSupported();
    }
    std::unique_ptr<char[]> copy{new char[size]};
    std::copy_n(data, size, copy.get());
    Status s = cache_.Insert(key, copy.get(), size);
    if (s.ok()) {
      copy.release();
    }
    return s;
  }

  Status Lookup(const Slice& key, std::unique_ptr<char[]>* data,
                size_t* size) override {
    auto handle = cache_.Lookup(key);
    if (handle) {
      char* ptr = cache_.Value(handle);
      *size = cache_.get()->GetCharge(handle);
      data->reset(new char[*size]);
      std::copy_n(ptr, *size, data->get());
      cache_.Release(handle);
      return Status::OK();
    } else {
      return Status::NotFound();
    }
  }

  bool IsCompressed() override { return false; }

  StatsType Stats() override { return StatsType(); }

  std::string GetPrintableOptions() const override { return ""; }

  uint64_t NewId() override { return cache_.get()->NewId(); }

 private:
  BasicTypedSharedCacheInterface<char[], CacheEntryRole::kMisc> cache_;
  bool read_only_;
};

class ReadOnlyCacheWrapper : public CacheWrapper {
  using CacheWrapper::CacheWrapper;

  using Cache::Insert;
  Status Insert(const Slice& /*key*/, Cache::ObjectPtr /*value*/,
                const CacheItemHelper* /*helper*/, size_t /*charge*/,
                Handle** /*handle*/, Priority /*priority*/) override {
    return Status::NotSupported();
  }
};

}  // anonymous namespace

TEST_F(DBBlockCacheTest, TestWithSameCompressed) {
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  std::shared_ptr<Cache> rw_cache{NewLRUCache(1000000)};
  std::shared_ptr<PersistentCacheFromCache> rw_pcache{
      new PersistentCacheFromCache(rw_cache, /*read_only*/ false)};
  // Exercise some obscure behavior with read-only wrappers
  std::shared_ptr<Cache> ro_cache{new ReadOnlyCacheWrapper(rw_cache)};
  std::shared_ptr<PersistentCacheFromCache> ro_pcache{
      new PersistentCacheFromCache(rw_cache, /*read_only*/ true)};

  // Simple same pointer
  table_options.block_cache = rw_cache;
  table_options.block_cache_compressed = rw_cache;
  table_options.persistent_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(TryReopen(options).ToString(),
            "Invalid argument: block_cache same as block_cache_compressed not "
            "currently supported, and would be bad for performance anyway");

  // Other cases
  table_options.block_cache = ro_cache;
  table_options.block_cache_compressed = rw_cache;
  table_options.persistent_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(TryReopen(options).ToString(),
            "Invalid argument: block_cache and block_cache_compressed share "
            "the same key space, which is not supported");

  table_options.block_cache = rw_cache;
  table_options.block_cache_compressed = ro_cache;
  table_options.persistent_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(TryReopen(options).ToString(),
            "Invalid argument: block_cache_compressed and block_cache share "
            "the same key space, which is not supported");

  table_options.block_cache = ro_cache;
  table_options.block_cache_compressed.reset();
  table_options.persistent_cache = rw_pcache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(TryReopen(options).ToString(),
            "Invalid argument: block_cache and persistent_cache share the same "
            "key space, which is not supported");

  table_options.block_cache = rw_cache;
  table_options.block_cache_compressed.reset();
  table_options.persistent_cache = ro_pcache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(TryReopen(options).ToString(),
            "Invalid argument: persistent_cache and block_cache share the same "
            "key space, which is not supported");

  table_options.block_cache.reset();
  table_options.no_block_cache = true;
  table_options.block_cache_compressed = ro_cache;
  table_options.persistent_cache = rw_pcache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(TryReopen(options).ToString(),
            "Invalid argument: block_cache_compressed and persistent_cache "
            "share the same key space, which is not supported");

  table_options.block_cache.reset();
  table_options.no_block_cache = true;
  table_options.block_cache_compressed = rw_cache;
  table_options.persistent_cache = ro_pcache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(TryReopen(options).ToString(),
            "Invalid argument: persistent_cache and block_cache_compressed "
            "share the same key space, which is not supported");
}
#endif  // SNAPPY

#ifndef ROCKSDB_LITE

// Make sure that when options.block_cache is set, after a new table is
// created its index/filter blocks are added to block cache.
TEST_F(DBBlockCacheTest, IndexAndFilterBlocksOfNewTableAddedToCache) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));

  // index/filter blocks added to block cache right after table creation.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, /* only index/filter were added */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));
  uint64_t int_num;
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);

  // Make sure filter block is in cache.
  std::string value;
  ReadOptions ropt;
  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);

  // Miss count should remain the same.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Make sure index block is in cache.
  auto index_block_hit = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);
  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(index_block_hit + 1,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(index_block_hit + 2,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
}

// With fill_cache = false, fills up the cache, then iterates over the entire
// db, verify dummy entries inserted in `BlockBasedTable::NewDataBlockIterator`
// does not cause heap-use-after-free errors in COMPILE_WITH_ASAN=1 runs
TEST_F(DBBlockCacheTest, FillCacheAndIterateDB) {
  ReadOptions read_options;
  read_options.fill_cache = false;
  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  InitTable(options);

  std::shared_ptr<Cache> cache = NewLRUCache(10, 0, true);
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key3", "val3"));
  ASSERT_OK(Put("key4", "val4"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key5", "val5"));
  ASSERT_OK(Put("key6", "val6"));
  ASSERT_OK(Flush());

  Iterator* iter = nullptr;

  iter = db_->NewIterator(read_options);
  iter->Seek(std::to_string(0));
  while (iter->Valid()) {
    iter->Next();
  }
  delete iter;
  iter = nullptr;
}

TEST_F(DBBlockCacheTest, IndexAndFilterBlocksStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  LRUCacheOptions co;
  // 500 bytes are enough to hold the first two blocks
  co.capacity = 500;
  co.num_shard_bits = 0;
  co.strict_capacity_limit = false;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  table_options.block_cache = cache;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "longer_key", "val"));
  // Create a new table
  ASSERT_OK(Flush(1));
  size_t index_bytes_insert =
      TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_INSERT);
  size_t filter_bytes_insert =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_INSERT);
  ASSERT_GT(index_bytes_insert, 0);
  ASSERT_GT(filter_bytes_insert, 0);
  ASSERT_EQ(cache->GetUsage(), index_bytes_insert + filter_bytes_insert);
  // set the cache capacity to the current usage
  cache->SetCapacity(index_bytes_insert + filter_bytes_insert);
  // The index and filter eviction statistics were broken by the refactoring
  // that moved the readers out of the block cache. Disabling these until we can
  // bring the stats back.
  // ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_EVICT), 0);
  // ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_EVICT), 0);
  // Note that the second key needs to be no longer than the first one.
  // Otherwise the second index block may not fit in cache.
  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table
  ASSERT_OK(Flush(1));
  // cache evicted old index and block entries
  ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_INSERT),
            index_bytes_insert);
  ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_INSERT),
            filter_bytes_insert);
  // The index and filter eviction statistics were broken by the refactoring
  // that moved the readers out of the block cache. Disabling these until we can
  // bring the stats back.
  // ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_INDEX_BYTES_EVICT),
  //           index_bytes_insert);
  // ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_FILTER_BYTES_EVICT),
  //           filter_bytes_insert);
}

#if (defined OS_LINUX || defined OS_WIN)
TEST_F(DBBlockCacheTest, WarmCacheWithDataBlocksDuringFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(1 << 25, 0, false);
  table_options.cache_index_and_filter_blocks = false;
  table_options.prepopulate_block_cache =
      BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  std::string value(kValueSize, 'a');
  for (size_t i = 1; i <= kNumBlocks; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Flush());
    ASSERT_EQ(i, options.statistics->getTickerCount(BLOCK_CACHE_DATA_ADD));
    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(0, options.statistics->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_EQ(i, options.statistics->getTickerCount(BLOCK_CACHE_DATA_HIT));
  }
  // Verify compaction not counted
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(kNumBlocks,
            options.statistics->getTickerCount(BLOCK_CACHE_DATA_ADD));
}

// This test cache data, index and filter blocks during flush.
class DBBlockCacheTest1 : public DBTestBase,
                          public ::testing::WithParamInterface<uint32_t> {
 public:
  const size_t kNumBlocks = 10;
  const size_t kValueSize = 100;
  DBBlockCacheTest1() : DBTestBase("db_block_cache_test1", true) {}
};

INSTANTIATE_TEST_CASE_P(DBBlockCacheTest1, DBBlockCacheTest1,
                        ::testing::Values(1, 2));

TEST_P(DBBlockCacheTest1, WarmCacheWithBlocksDuringFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(1 << 25, 0, false);

  uint32_t filter_type = GetParam();
  switch (filter_type) {
    case 1:  // partition_filter
      table_options.partition_filters = true;
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      table_options.filter_policy.reset(NewBloomFilterPolicy(10));
      break;
    case 2:  // full filter
      table_options.filter_policy.reset(NewBloomFilterPolicy(10));
      break;
    default:
      assert(false);
  }

  table_options.cache_index_and_filter_blocks = true;
  table_options.prepopulate_block_cache =
      BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  std::string value(kValueSize, 'a');
  for (size_t i = 1; i <= kNumBlocks; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Flush());
    ASSERT_EQ(i, options.statistics->getTickerCount(BLOCK_CACHE_DATA_ADD));
    if (filter_type == 1) {
      ASSERT_EQ(2 * i,
                options.statistics->getTickerCount(BLOCK_CACHE_INDEX_ADD));
      ASSERT_EQ(2 * i,
                options.statistics->getTickerCount(BLOCK_CACHE_FILTER_ADD));
    } else {
      ASSERT_EQ(i, options.statistics->getTickerCount(BLOCK_CACHE_INDEX_ADD));
      ASSERT_EQ(i, options.statistics->getTickerCount(BLOCK_CACHE_FILTER_ADD));
    }
    ASSERT_EQ(value, Get(std::to_string(i)));

    ASSERT_EQ(0, options.statistics->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_EQ(i, options.statistics->getTickerCount(BLOCK_CACHE_DATA_HIT));

    ASSERT_EQ(0, options.statistics->getTickerCount(BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(i * 3, options.statistics->getTickerCount(BLOCK_CACHE_INDEX_HIT));
    if (filter_type == 1) {
      ASSERT_EQ(i * 3,
                options.statistics->getTickerCount(BLOCK_CACHE_FILTER_HIT));
    } else {
      ASSERT_EQ(i * 2,
                options.statistics->getTickerCount(BLOCK_CACHE_FILTER_HIT));
    }
    ASSERT_EQ(0, options.statistics->getTickerCount(BLOCK_CACHE_FILTER_MISS));
  }

  // Verify compaction not counted
  CompactRangeOptions cro;
  // Ensure files are rewritten, not just trivially moved.
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));
  EXPECT_EQ(kNumBlocks,
            options.statistics->getTickerCount(BLOCK_CACHE_DATA_ADD));
  // Index and filter blocks are automatically warmed when the new table file
  // is automatically opened at the end of compaction. This is not easily
  // disabled so results in the new index and filter blocks being warmed.
  if (filter_type == 1) {
    EXPECT_EQ(2 * (1 + kNumBlocks),
              options.statistics->getTickerCount(BLOCK_CACHE_INDEX_ADD));
    EXPECT_EQ(2 * (1 + kNumBlocks),
              options.statistics->getTickerCount(BLOCK_CACHE_FILTER_ADD));
  } else {
    EXPECT_EQ(1 + kNumBlocks,
              options.statistics->getTickerCount(BLOCK_CACHE_INDEX_ADD));
    EXPECT_EQ(1 + kNumBlocks,
              options.statistics->getTickerCount(BLOCK_CACHE_FILTER_ADD));
  }
}

TEST_F(DBBlockCacheTest, DynamicallyWarmCacheDuringFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(1 << 25, 0, false);
  table_options.cache_index_and_filter_blocks = false;
  table_options.prepopulate_block_cache =
      BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  std::string value(kValueSize, 'a');

  for (size_t i = 1; i <= 5; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Flush());
    ASSERT_EQ(1,
              options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(0,
              options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_ADD));
    ASSERT_EQ(
        0, options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_EQ(1,
              options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_HIT));
  }

  ASSERT_OK(dbfull()->SetOptions(
      {{"block_based_table_factory", "{prepopulate_block_cache=kDisable;}"}}));

  for (size_t i = 6; i <= kNumBlocks; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Flush());
    ASSERT_EQ(0,
              options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(1,
              options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_ADD));
    ASSERT_EQ(
        1, options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_EQ(0,
              options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_HIT));
  }
}
#endif

namespace {

// A mock cache wraps LRUCache, and record how many entries have been
// inserted for each priority.
class MockCache : public LRUCache {
 public:
  static uint32_t high_pri_insert_count;
  static uint32_t low_pri_insert_count;

  MockCache()
      : LRUCache((size_t)1 << 25 /*capacity*/, 0 /*num_shard_bits*/,
                 false /*strict_capacity_limit*/, 0.0 /*high_pri_pool_ratio*/,
                 0.0 /*low_pri_pool_ratio*/) {}

  using ShardedCache::Insert;

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                Handle** handle, Priority priority) override {
    if (priority == Priority::LOW) {
      low_pri_insert_count++;
    } else {
      high_pri_insert_count++;
    }
    return LRUCache::Insert(key, value, helper, charge, handle, priority);
  }
};

uint32_t MockCache::high_pri_insert_count = 0;
uint32_t MockCache::low_pri_insert_count = 0;

}  // anonymous namespace

TEST_F(DBBlockCacheTest, IndexAndFilterBlocksCachePriority) {
  for (auto priority : {Cache::Priority::LOW, Cache::Priority::HIGH}) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.block_cache.reset(new MockCache());
    table_options.filter_policy.reset(NewBloomFilterPolicy(20));
    table_options.cache_index_and_filter_blocks_with_high_priority =
        priority == Cache::Priority::HIGH ? true : false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    MockCache::high_pri_insert_count = 0;
    MockCache::low_pri_insert_count = 0;

    // Create a new table.
    ASSERT_OK(Put("foo", "value"));
    ASSERT_OK(Put("bar", "value"));
    ASSERT_OK(Flush());
    ASSERT_EQ(1, NumTableFilesAtLevel(0));

    // index/filter blocks added to block cache right after table creation.
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(2, /* only index/filter were added */
              TestGetTickerCount(options, BLOCK_CACHE_ADD));
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));
    if (priority == Cache::Priority::LOW) {
      ASSERT_EQ(0u, MockCache::high_pri_insert_count);
      ASSERT_EQ(2u, MockCache::low_pri_insert_count);
    } else {
      ASSERT_EQ(2u, MockCache::high_pri_insert_count);
      ASSERT_EQ(0u, MockCache::low_pri_insert_count);
    }

    // Access data block.
    ASSERT_EQ("value", Get("foo"));

    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(3, /*adding data block*/
              TestGetTickerCount(options, BLOCK_CACHE_ADD));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));

    // Data block should be inserted with low priority.
    if (priority == Cache::Priority::LOW) {
      ASSERT_EQ(0u, MockCache::high_pri_insert_count);
      ASSERT_EQ(3u, MockCache::low_pri_insert_count);
    } else {
      ASSERT_EQ(2u, MockCache::high_pri_insert_count);
      ASSERT_EQ(1u, MockCache::low_pri_insert_count);
    }
  }
}

namespace {

// An LRUCache wrapper that can falsely report "not found" on Lookup.
// This allows us to manipulate BlockBasedTableReader into thinking
// another thread inserted the data in between Lookup and Insert,
// while mostly preserving the LRUCache interface/behavior.
class LookupLiarCache : public CacheWrapper {
  int nth_lookup_not_found_ = 0;

 public:
  explicit LookupLiarCache(std::shared_ptr<Cache> target)
      : CacheWrapper(std::move(target)) {}

  using Cache::Lookup;
  Handle* Lookup(const Slice& key, const CacheItemHelper* helper = nullptr,
                 CreateContext* create_context = nullptr,
                 Priority priority = Priority::LOW, bool wait = true,
                 Statistics* stats = nullptr) override {
    if (nth_lookup_not_found_ == 1) {
      nth_lookup_not_found_ = 0;
      return nullptr;
    }
    if (nth_lookup_not_found_ > 1) {
      --nth_lookup_not_found_;
    }
    return CacheWrapper::Lookup(key, helper, create_context, priority, wait,
                                stats);
  }

  // 1 == next lookup, 2 == after next, etc.
  void SetNthLookupNotFound(int n) { nth_lookup_not_found_ = n; }
};

}  // anonymous namespace

TEST_F(DBBlockCacheTest, AddRedundantStats) {
  const size_t capacity = size_t{1} << 25;
  const int num_shard_bits = 0;  // 1 shard
  int iterations_tested = 0;
  for (std::shared_ptr<Cache> base_cache :
       {NewLRUCache(capacity, num_shard_bits),
        HyperClockCacheOptions(
            capacity,
            BlockBasedTableOptions().block_size /*estimated_value_size*/,
            num_shard_bits)
            .MakeSharedCache()}) {
    if (!base_cache) {
      // Skip clock cache when not supported
      continue;
    }
    ++iterations_tested;
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    std::shared_ptr<LookupLiarCache> cache =
        std::make_shared<LookupLiarCache>(base_cache);

    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.block_cache = cache;
    table_options.filter_policy.reset(NewBloomFilterPolicy(50));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    // Create a new table.
    ASSERT_OK(Put("foo", "value"));
    ASSERT_OK(Put("bar", "value"));
    ASSERT_OK(Flush());
    ASSERT_EQ(1, NumTableFilesAtLevel(0));

    // Normal access filter+index+data.
    ASSERT_EQ("value", Get("foo"));

    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD));
    // --------
    ASSERT_EQ(3, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD_REDUNDANT));
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD_REDUNDANT));
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD_REDUNDANT));
    // --------
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_ADD_REDUNDANT));

    // Againt access filter+index+data, but force redundant load+insert on index
    cache->SetNthLookupNotFound(2);
    ASSERT_EQ("value", Get("bar"));

    ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD));
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD));
    // --------
    ASSERT_EQ(4, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD_REDUNDANT));
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD_REDUNDANT));
    ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD_REDUNDANT));
    // --------
    ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_ADD_REDUNDANT));

    // Access just filter (with high probability), and force redundant
    // load+insert
    cache->SetNthLookupNotFound(1);
    ASSERT_EQ("NOT_FOUND", Get("this key was not added"));

    EXPECT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD));
    EXPECT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD));
    EXPECT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD));
    // --------
    EXPECT_EQ(5, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    EXPECT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD_REDUNDANT));
    EXPECT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD_REDUNDANT));
    EXPECT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD_REDUNDANT));
    // --------
    EXPECT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_ADD_REDUNDANT));

    // Access just data, forcing redundant load+insert
    ReadOptions read_options;
    std::unique_ptr<Iterator> iter{db_->NewIterator(read_options)};
    cache->SetNthLookupNotFound(1);
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "bar");

    EXPECT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD));
    EXPECT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD));
    EXPECT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD));
    // --------
    EXPECT_EQ(6, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    EXPECT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_ADD_REDUNDANT));
    EXPECT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_ADD_REDUNDANT));
    EXPECT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_DATA_ADD_REDUNDANT));
    // --------
    EXPECT_EQ(3, TestGetTickerCount(options, BLOCK_CACHE_ADD_REDUNDANT));
  }
  EXPECT_GE(iterations_tested, 1);
}

TEST_F(DBBlockCacheTest, ParanoidFileChecks) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.level0_file_num_compaction_trigger = 2;
  options.paranoid_file_checks = true;
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = false;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "1_key", "val"));
  ASSERT_OK(Put(1, "9_key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));
  ASSERT_EQ(1, /* read and cache data block */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));

  ASSERT_OK(Put(1, "1_key2", "val2"));
  ASSERT_OK(Put(1, "9_key2", "val2"));
  // Create a new SST file. This will further trigger a compaction
  // and generate another file.
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(3, /* Totally 3 files created up to now */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));

  // After disabling options.paranoid_file_checks. NO further block
  // is added after generating a new file.
  ASSERT_OK(
      dbfull()->SetOptions(handles_[1], {{"paranoid_file_checks", "false"}}));

  ASSERT_OK(Put(1, "1_key3", "val3"));
  ASSERT_OK(Put(1, "9_key3", "val3"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "1_key4", "val4"));
  ASSERT_OK(Put(1, "9_key4", "val4"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(3, /* Totally 3 files created up to now */
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
}

TEST_F(DBBlockCacheTest, CompressedCache) {
  if (!Snappy_Supported()) {
    return;
  }
  int num_iter = 80;

  // Run this test three iterations.
  // Iteration 1: only a uncompressed block cache
  // Iteration 2: only a compressed block cache
  // Iteration 3: both block cache and compressed cache
  // Iteration 4: both block cache and compressed cache, but DB is not
  // compressed
  for (int iter = 0; iter < 4; iter++) {
    Options options = CurrentOptions();
    options.write_buffer_size = 64 * 1024;  // small write buffer
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    BlockBasedTableOptions table_options;
    switch (iter) {
      case 0:
        // only uncompressed block cache
        table_options.block_cache = NewLRUCache(8 * 1024);
        table_options.block_cache_compressed = nullptr;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 1:
        // no block cache, only compressed cache
        table_options.no_block_cache = true;
        table_options.block_cache = nullptr;
        table_options.block_cache_compressed = NewLRUCache(8 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 2:
        // both compressed and uncompressed block cache
        table_options.block_cache = NewLRUCache(1024);
        table_options.block_cache_compressed = NewLRUCache(8 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 3:
        // both block cache and compressed cache, but DB is not compressed
        // also, make block cache sizes bigger, to trigger block cache hits
        table_options.block_cache = NewLRUCache(1024 * 1024);
        table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        options.compression = kNoCompression;
        break;
      default:
        FAIL();
    }
    CreateAndReopenWithCF({"pikachu"}, options);
    // default column family doesn't have block cache
    Options no_block_cache_opts;
    no_block_cache_opts.statistics = options.statistics;
    no_block_cache_opts = CurrentOptions(no_block_cache_opts);
    BlockBasedTableOptions table_options_no_bc;
    table_options_no_bc.no_block_cache = true;
    no_block_cache_opts.table_factory.reset(
        NewBlockBasedTableFactory(table_options_no_bc));
    ReopenWithColumnFamilies(
        {"default", "pikachu"},
        std::vector<Options>({no_block_cache_opts, options}));

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    std::vector<std::string> values;
    std::string str;
    for (int i = 0; i < num_iter; i++) {
      if (i % 4 == 0) {  // high compression ratio
        str = rnd.RandomString(1000);
      }
      values.push_back(str);
      ASSERT_OK(Put(1, Key(i), values[i]));
    }

    // flush all data from memtable so that reads are from block cache
    ASSERT_OK(Flush(1));

    for (int i = 0; i < num_iter; i++) {
      ASSERT_EQ(Get(1, Key(i)), values[i]);
    }

    // check that we triggered the appropriate code paths in the cache
    switch (iter) {
      case 0:
        // only uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 1:
        // no block cache, only compressed cache
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 2:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 3:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_HIT), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        // compressed doesn't have any hits since blocks are not compressed on
        // storage
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT), 0);
        break;
      default:
        FAIL();
    }

    options.create_if_missing = true;
    DestroyAndReopen(options);
  }
}

TEST_F(DBBlockCacheTest, CacheCompressionDict) {
  const int kNumFiles = 4;
  const int kNumEntriesPerFile = 128;
  const int kNumBytesPerEntry = 1024;

  // Try all the available libraries that support dictionary compression
  std::vector<CompressionType> compression_types;
  if (Zlib_Supported()) {
    compression_types.push_back(kZlibCompression);
  }
  if (LZ4_Supported()) {
    compression_types.push_back(kLZ4Compression);
    compression_types.push_back(kLZ4HCCompression);
  }
  if (ZSTD_Supported()) {
    compression_types.push_back(kZSTD);
  } else if (ZSTDNotFinal_Supported()) {
    compression_types.push_back(kZSTDNotFinalCompression);
  }
  Random rnd(301);
  for (auto compression_type : compression_types) {
    Options options = CurrentOptions();
    options.bottommost_compression = compression_type;
    options.bottommost_compression_opts.max_dict_bytes = 4096;
    options.bottommost_compression_opts.enabled = true;
    options.create_if_missing = true;
    options.num_levels = 2;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.target_file_size_base = kNumEntriesPerFile * kNumBytesPerEntry;
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.block_cache.reset(new MockCache());
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    RecordCacheCountersForCompressionDict(options);

    for (int i = 0; i < kNumFiles; ++i) {
      ASSERT_EQ(i, NumTableFilesAtLevel(0, 0));
      for (int j = 0; j < kNumEntriesPerFile; ++j) {
        std::string value = rnd.RandomString(kNumBytesPerEntry);
        ASSERT_OK(Put(Key(j * kNumFiles + i), value.c_str()));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(1));

    // Compression dictionary blocks are preloaded.
    CheckCacheCountersForCompressionDict(
        options, kNumFiles /* expected_compression_dict_misses */,
        0 /* expected_compression_dict_hits */,
        kNumFiles /* expected_compression_dict_inserts */);

    // Seek to a key in a file. It should cause the SST's dictionary meta-block
    // to be read.
    RecordCacheCounters(options);
    RecordCacheCountersForCompressionDict(options);
    ReadOptions read_options;
    ASSERT_NE("NOT_FOUND", Get(Key(kNumFiles * kNumEntriesPerFile - 1)));
    // Two block hits: index and dictionary since they are prefetched
    // One block missed/added: data block
    CheckCacheCounters(options, 1 /* expected_misses */, 2 /* expected_hits */,
                       1 /* expected_inserts */, 0 /* expected_failures */);
    CheckCacheCountersForCompressionDict(
        options, 0 /* expected_compression_dict_misses */,
        1 /* expected_compression_dict_hits */,
        0 /* expected_compression_dict_inserts */);
  }
}

static void ClearCache(Cache* cache) {
  std::deque<std::string> keys;
  Cache::ApplyToAllEntriesOptions opts;
  auto callback = [&](const Slice& key, Cache::ObjectPtr, size_t /*charge*/,
                      const Cache::CacheItemHelper* helper) {
    if (helper && helper->role == CacheEntryRole::kMisc) {
      // Keep the stats collector
      return;
    }
    keys.push_back(key.ToString());
  };
  cache->ApplyToAllEntries(callback, opts);
  for (auto& k : keys) {
    cache->Erase(k);
  }
}

TEST_F(DBBlockCacheTest, CacheEntryRoleStats) {
  const size_t capacity = size_t{1} << 25;
  int iterations_tested = 0;
  for (bool partition : {false, true}) {
    for (std::shared_ptr<Cache> cache :
         {NewLRUCache(capacity),
          HyperClockCacheOptions(
              capacity,
              BlockBasedTableOptions().block_size /*estimated_value_size*/)
              .MakeSharedCache()}) {
      ++iterations_tested;

      Options options = CurrentOptions();
      SetTimeElapseOnlySleepOnReopen(&options);
      options.create_if_missing = true;
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.max_open_files = 13;
      options.table_cache_numshardbits = 0;
      // If this wakes up, it could interfere with test
      options.stats_dump_period_sec = 0;

      BlockBasedTableOptions table_options;
      table_options.block_cache = cache;
      table_options.cache_index_and_filter_blocks = true;
      table_options.filter_policy.reset(NewBloomFilterPolicy(50));
      if (partition) {
        table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
        table_options.partition_filters = true;
      }
      table_options.metadata_cache_options.top_level_index_pinning =
          PinningTier::kNone;
      table_options.metadata_cache_options.partition_pinning =
          PinningTier::kNone;
      table_options.metadata_cache_options.unpartitioned_pinning =
          PinningTier::kNone;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      DestroyAndReopen(options);

      // Create a new table.
      ASSERT_OK(Put("foo", "value"));
      ASSERT_OK(Put("bar", "value"));
      ASSERT_OK(Flush());

      ASSERT_OK(Put("zfoo", "value"));
      ASSERT_OK(Put("zbar", "value"));
      ASSERT_OK(Flush());

      ASSERT_EQ(2, NumTableFilesAtLevel(0));

      // Fresh cache
      ClearCache(cache.get());

      std::array<size_t, kNumCacheEntryRoles> expected{};
      // For CacheEntryStatsCollector
      expected[static_cast<size_t>(CacheEntryRole::kMisc)] = 1;
      EXPECT_EQ(expected, GetCacheEntryRoleCountsBg());

      std::array<size_t, kNumCacheEntryRoles> prev_expected = expected;

      // First access only filters
      ASSERT_EQ("NOT_FOUND", Get("different from any key added"));
      expected[static_cast<size_t>(CacheEntryRole::kFilterBlock)] += 2;
      if (partition) {
        expected[static_cast<size_t>(CacheEntryRole::kFilterMetaBlock)] += 2;
      }
      // Within some time window, we will get cached entry stats
      EXPECT_EQ(prev_expected, GetCacheEntryRoleCountsBg());
      // Not enough to force a miss
      env_->MockSleepForSeconds(45);
      EXPECT_EQ(prev_expected, GetCacheEntryRoleCountsBg());
      // Enough to force a miss
      env_->MockSleepForSeconds(601);
      EXPECT_EQ(expected, GetCacheEntryRoleCountsBg());

      // Now access index and data block
      ASSERT_EQ("value", Get("foo"));
      expected[static_cast<size_t>(CacheEntryRole::kIndexBlock)]++;
      if (partition) {
        // top-level
        expected[static_cast<size_t>(CacheEntryRole::kIndexBlock)]++;
      }
      expected[static_cast<size_t>(CacheEntryRole::kDataBlock)]++;
      // Enough to force a miss
      env_->MockSleepForSeconds(601);
      // But inject a simulated long scan so that we need a longer
      // interval to force a miss next time.
      SyncPoint::GetInstance()->SetCallBack(
          "CacheEntryStatsCollector::GetStats:AfterApplyToAllEntries",
          [this](void*) {
            // To spend no more than 0.2% of time scanning, we would need
            // interval of at least 10000s
            env_->MockSleepForSeconds(20);
          });
      SyncPoint::GetInstance()->EnableProcessing();
      EXPECT_EQ(expected, GetCacheEntryRoleCountsBg());
      prev_expected = expected;
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();

      // The same for other file
      ASSERT_EQ("value", Get("zfoo"));
      expected[static_cast<size_t>(CacheEntryRole::kIndexBlock)]++;
      if (partition) {
        // top-level
        expected[static_cast<size_t>(CacheEntryRole::kIndexBlock)]++;
      }
      expected[static_cast<size_t>(CacheEntryRole::kDataBlock)]++;
      // Because of the simulated long scan, this is not enough to force
      // a miss
      env_->MockSleepForSeconds(601);
      EXPECT_EQ(prev_expected, GetCacheEntryRoleCountsBg());
      // But this is enough
      env_->MockSleepForSeconds(10000);
      EXPECT_EQ(expected, GetCacheEntryRoleCountsBg());
      prev_expected = expected;

      // Also check the GetProperty interface
      std::map<std::string, std::string> values;
      ASSERT_TRUE(
          db_->GetMapProperty(DB::Properties::kBlockCacheEntryStats, &values));

      for (size_t i = 0; i < kNumCacheEntryRoles; ++i) {
        auto role = static_cast<CacheEntryRole>(i);
        EXPECT_EQ(std::to_string(expected[i]),
                  values[BlockCacheEntryStatsMapKeys::EntryCount(role)]);
      }

      // Add one for kWriteBuffer
      {
        WriteBufferManager wbm(size_t{1} << 20, cache);
        wbm.ReserveMem(1024);
        expected[static_cast<size_t>(CacheEntryRole::kWriteBuffer)]++;
        // Now we check that the GetProperty interface is more agressive about
        // re-scanning stats, but not totally aggressive.
        // Within some time window, we will get cached entry stats
        env_->MockSleepForSeconds(1);
        EXPECT_EQ(std::to_string(prev_expected[static_cast<size_t>(
                      CacheEntryRole::kWriteBuffer)]),
                  values[BlockCacheEntryStatsMapKeys::EntryCount(
                      CacheEntryRole::kWriteBuffer)]);
        // Not enough for a "background" miss but enough for a "foreground" miss
        env_->MockSleepForSeconds(45);

        ASSERT_TRUE(db_->GetMapProperty(DB::Properties::kBlockCacheEntryStats,
                                        &values));
        EXPECT_EQ(
            std::to_string(
                expected[static_cast<size_t>(CacheEntryRole::kWriteBuffer)]),
            values[BlockCacheEntryStatsMapKeys::EntryCount(
                CacheEntryRole::kWriteBuffer)]);
      }
      prev_expected = expected;

      // With collector pinned in cache, we should be able to hit
      // even if the cache is full
      ClearCache(cache.get());
      Cache::Handle* h = nullptr;
      if (strcmp(cache->Name(), "LRUCache") == 0) {
        ASSERT_OK(cache->Insert("Fill-it-up", nullptr, &kNoopCacheItemHelper,
                                capacity + 1, &h, Cache::Priority::HIGH));
      } else {
        // For ClockCache we use a 16-byte key.
        ASSERT_OK(cache->Insert("Fill-it-up-xxxxx", nullptr,
                                &kNoopCacheItemHelper, capacity + 1, &h,
                                Cache::Priority::HIGH));
      }
      ASSERT_GT(cache->GetUsage(), cache->GetCapacity());
      expected = {};
      // For CacheEntryStatsCollector
      expected[static_cast<size_t>(CacheEntryRole::kMisc)] = 1;
      // For Fill-it-up
      expected[static_cast<size_t>(CacheEntryRole::kMisc)]++;
      // Still able to hit on saved stats
      EXPECT_EQ(prev_expected, GetCacheEntryRoleCountsBg());
      // Enough to force a miss
      env_->MockSleepForSeconds(1000);
      EXPECT_EQ(expected, GetCacheEntryRoleCountsBg());

      cache->Release(h);

      // Now we test that the DB mutex is not held during scans, for the ways
      // we know how to (possibly) trigger them. Without a better good way to
      // check this, we simply inject an acquire & release of the DB mutex
      // deep in the stat collection code. If we were already holding the
      // mutex, that is UB that would at least be found by TSAN.
      int scan_count = 0;
      SyncPoint::GetInstance()->SetCallBack(
          "CacheEntryStatsCollector::GetStats:AfterApplyToAllEntries",
          [this, &scan_count](void*) {
            dbfull()->TEST_LockMutex();
            dbfull()->TEST_UnlockMutex();
            ++scan_count;
          });
      SyncPoint::GetInstance()->EnableProcessing();

      // Different things that might trigger a scan, with mock sleeps to
      // force a miss.
      env_->MockSleepForSeconds(10000);
      dbfull()->DumpStats();
      ASSERT_EQ(scan_count, 1);

      env_->MockSleepForSeconds(60);
      ASSERT_TRUE(db_->GetMapProperty(DB::Properties::kFastBlockCacheEntryStats,
                                      &values));
      ASSERT_EQ(scan_count, 1);
      ASSERT_TRUE(
          db_->GetMapProperty(DB::Properties::kBlockCacheEntryStats, &values));
      ASSERT_EQ(scan_count, 2);

      env_->MockSleepForSeconds(10000);
      ASSERT_TRUE(db_->GetMapProperty(DB::Properties::kFastBlockCacheEntryStats,
                                      &values));
      ASSERT_EQ(scan_count, 3);

      env_->MockSleepForSeconds(60);
      std::string value_str;
      ASSERT_TRUE(db_->GetProperty(DB::Properties::kFastBlockCacheEntryStats,
                                   &value_str));
      ASSERT_EQ(scan_count, 3);
      ASSERT_TRUE(
          db_->GetProperty(DB::Properties::kBlockCacheEntryStats, &value_str));
      ASSERT_EQ(scan_count, 4);

      env_->MockSleepForSeconds(10000);
      ASSERT_TRUE(db_->GetProperty(DB::Properties::kFastBlockCacheEntryStats,
                                   &value_str));
      ASSERT_EQ(scan_count, 5);

      ASSERT_TRUE(db_->GetProperty(DB::Properties::kCFStats, &value_str));
      // To match historical speed, querying this property no longer triggers
      // a scan, even if results are old. But periodic dump stats should keep
      // things reasonably updated.
      ASSERT_EQ(scan_count, /*unchanged*/ 5);

      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
    }
    EXPECT_GE(iterations_tested, 1);
  }
}

namespace {

void DummyFillCache(Cache& cache, size_t entry_size,
                    std::vector<CacheHandleGuard<void>>& handles) {
  // fprintf(stderr, "Entry size: %zu\n", entry_size);
  handles.clear();
  cache.EraseUnRefEntries();
  void* fake_value = &cache;
  size_t capacity = cache.GetCapacity();
  OffsetableCacheKey ck{"abc", "abc", 42};
  for (size_t my_usage = 0; my_usage < capacity;) {
    size_t charge = std::min(entry_size, capacity - my_usage);
    Cache::Handle* handle;
    Status st = cache.Insert(ck.WithOffset(my_usage).AsSlice(), fake_value,
                             &kNoopCacheItemHelper, charge, &handle);
    ASSERT_OK(st);
    handles.emplace_back(&cache, handle);
    my_usage += charge;
  }
}

class CountingLogger : public Logger {
 public:
  ~CountingLogger() override {}
  using Logger::Logv;
  void Logv(const InfoLogLevel log_level, const char* format,
            va_list /*ap*/) override {
    if (std::strstr(format, "HyperClockCache") == nullptr) {
      // Not a match
      return;
    }
    // static StderrLogger debug;
    // debug.Logv(log_level, format, ap);
    if (log_level == InfoLogLevel::INFO_LEVEL) {
      ++info_count_;
    } else if (log_level == InfoLogLevel::WARN_LEVEL) {
      ++warn_count_;
    } else if (log_level == InfoLogLevel::ERROR_LEVEL) {
      ++error_count_;
    }
  }

  std::array<int, 3> PopCounts() {
    std::array<int, 3> rv{{info_count_, warn_count_, error_count_}};
    info_count_ = warn_count_ = error_count_ = 0;
    return rv;
  }

 private:
  int info_count_{};
  int warn_count_{};
  int error_count_{};
};

}  // namespace

TEST_F(DBBlockCacheTest, HyperClockCacheReportProblems) {
  size_t capacity = 1024 * 1024;
  size_t value_size_est = 8 * 1024;
  HyperClockCacheOptions hcc_opts{capacity, value_size_est};
  hcc_opts.num_shard_bits = 2;  // 4 shards
  hcc_opts.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = hcc_opts.MakeSharedCache();
  std::shared_ptr<CountingLogger> logger = std::make_shared<CountingLogger>();

  auto table_options = GetTableOptions();
  auto options = GetOptions(table_options);
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.info_log = logger;
  // Going to sample more directly
  options.stats_dump_period_sec = 0;
  Reopen(options);

  std::vector<CacheHandleGuard<void>> handles;

  // Clear anything from DB startup
  logger->PopCounts();

  // Fill cache based on expected size and check that when we
  // don't report anything relevant in periodic stats dump
  DummyFillCache(*cache, value_size_est, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{0, 0, 0}}));

  // Same, within reasonable bounds
  DummyFillCache(*cache, value_size_est - value_size_est / 4, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{0, 0, 0}}));

  DummyFillCache(*cache, value_size_est + value_size_est / 3, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{0, 0, 0}}));

  // Estimate too high (value size too low) eventually reports ERROR
  DummyFillCache(*cache, value_size_est / 2, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{0, 1, 0}}));

  DummyFillCache(*cache, value_size_est / 3, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{0, 0, 1}}));

  // Estimate too low (value size too high) starts with INFO
  // and is only WARNING in the worst case
  DummyFillCache(*cache, value_size_est * 2, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{1, 0, 0}}));

  DummyFillCache(*cache, value_size_est * 3, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{0, 1, 0}}));

  DummyFillCache(*cache, value_size_est * 20, handles);
  dbfull()->DumpStats();
  EXPECT_EQ(logger->PopCounts(), (std::array<int, 3>{{0, 1, 0}}));
}

#endif  // ROCKSDB_LITE

class DBBlockCacheKeyTest
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  DBBlockCacheKeyTest()
      : DBTestBase("db_block_cache_test", /*env_do_fsync=*/false) {}

  void SetUp() override {
    use_compressed_cache_ = std::get<0>(GetParam());
    exclude_file_numbers_ = std::get<1>(GetParam());
  }

  bool use_compressed_cache_;
  bool exclude_file_numbers_;
};

// Disable LinkFile so that we can physically copy a DB using Checkpoint.
// Disable file GetUniqueId to enable stable cache keys.
class StableCacheKeyTestFS : public FaultInjectionTestFS {
 public:
  explicit StableCacheKeyTestFS(const std::shared_ptr<FileSystem>& base)
      : FaultInjectionTestFS(base) {
    SetFailGetUniqueId(true);
  }

  virtual ~StableCacheKeyTestFS() override {}

  IOStatus LinkFile(const std::string&, const std::string&, const IOOptions&,
                    IODebugContext*) override {
    return IOStatus::NotSupported("Disabled");
  }
};

TEST_P(DBBlockCacheKeyTest, StableCacheKeys) {
  std::shared_ptr<StableCacheKeyTestFS> test_fs{
      new StableCacheKeyTestFS(env_->GetFileSystem())};
  std::unique_ptr<CompositeEnvWrapper> test_env{
      new CompositeEnvWrapper(env_, test_fs)};

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.env = test_env.get();

  // Corrupting the table properties corrupts the unique id.
  // Ignore the unique id recorded in the manifest.
  options.verify_sst_unique_id_in_manifest = false;

  BlockBasedTableOptions table_options;

  int key_count = 0;
  uint64_t expected_stat = 0;

  std::function<void()> verify_stats;
  if (use_compressed_cache_) {
    if (!Snappy_Supported()) {
      ROCKSDB_GTEST_SKIP("Compressed cache test requires snappy support");
      return;
    }
    options.compression = CompressionType::kSnappyCompression;
    table_options.no_block_cache = true;
    table_options.block_cache_compressed = NewLRUCache(1 << 25, 0, false);
    verify_stats = [&options, &expected_stat] {
      // One for ordinary SST file and one for external SST file
      ASSERT_EQ(expected_stat,
                options.statistics->getTickerCount(BLOCK_CACHE_COMPRESSED_ADD));
    };
  } else {
    table_options.cache_index_and_filter_blocks = true;
    table_options.block_cache = NewLRUCache(1 << 25, 0, false);
    verify_stats = [&options, &expected_stat] {
      ASSERT_EQ(expected_stat,
                options.statistics->getTickerCount(BLOCK_CACHE_DATA_ADD));
      ASSERT_EQ(expected_stat,
                options.statistics->getTickerCount(BLOCK_CACHE_INDEX_ADD));
      ASSERT_EQ(expected_stat,
                options.statistics->getTickerCount(BLOCK_CACHE_FILTER_ADD));
    };
  }

  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"koko"}, options);

  if (exclude_file_numbers_) {
    // Simulate something like old behavior without file numbers in properties.
    // This is a "control" side of the test that also ensures safely degraded
    // behavior on old files.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::BlockBasedTableBuilder:PreSetupBaseCacheKey",
        [&](void* arg) {
          TableProperties* props = reinterpret_cast<TableProperties*>(arg);
          props->orig_file_number = 0;
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  }

  std::function<void()> perform_gets = [&key_count, &expected_stat, this]() {
    if (exclude_file_numbers_) {
      // No cache key reuse should happen, because we can't rely on current
      // file number being stable
      expected_stat += key_count;
    } else {
      // Cache keys should be stable
      expected_stat = key_count;
    }
    for (int i = 0; i < key_count; ++i) {
      ASSERT_EQ(Get(1, Key(i)), "abc");
    }
  };

  // Ordinary SST files with same session id
  const std::string something_compressible(500U, 'x');
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Put(1, Key(key_count), "abc"));
    ASSERT_OK(Put(1, Key(key_count) + "a", something_compressible));
    ASSERT_OK(Flush(1));
    ++key_count;
  }

#ifndef ROCKSDB_LITE
  // Save an export of those ordinary SST files for later
  std::string export_files_dir = dbname_ + "/exported";
  ExportImportFilesMetaData* metadata_ptr_ = nullptr;
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->ExportColumnFamily(handles_[1], export_files_dir,
                                           &metadata_ptr_));
  ASSERT_NE(metadata_ptr_, nullptr);
  delete checkpoint;
  checkpoint = nullptr;

  // External SST files with same session id
  SstFileWriter sst_file_writer(EnvOptions(), options);
  std::vector<std::string> external;
  for (int i = 0; i < 2; ++i) {
    std::string f = dbname_ + "/external" + std::to_string(i) + ".sst";
    external.push_back(f);
    ASSERT_OK(sst_file_writer.Open(f));
    ASSERT_OK(sst_file_writer.Put(Key(key_count), "abc"));
    ASSERT_OK(
        sst_file_writer.Put(Key(key_count) + "a", something_compressible));
    ++key_count;
    ExternalSstFileInfo external_info;
    ASSERT_OK(sst_file_writer.Finish(&external_info));
    IngestExternalFileOptions ingest_opts;
    ASSERT_OK(db_->IngestExternalFile(handles_[1], {f}, ingest_opts));
  }

  if (exclude_file_numbers_) {
    // FIXME(peterd): figure out where these extra ADDs are coming from
    options.statistics->recordTick(BLOCK_CACHE_COMPRESSED_ADD,
                                   uint64_t{0} - uint64_t{2});
  }
#endif

  perform_gets();
  verify_stats();

  // Make sure we can cache hit after re-open
  ReopenWithColumnFamilies({"default", "koko"}, options);

  perform_gets();
  verify_stats();

  // Make sure we can cache hit even on a full copy of the DB. Using
  // StableCacheKeyTestFS, Checkpoint will resort to full copy not hard link.
  // (Checkpoint  not available in LITE mode to test this.)
#ifndef ROCKSDB_LITE
  auto db_copy_name = dbname_ + "-copy";
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(db_copy_name));
  delete checkpoint;

  Close();
  Destroy(options);

  // Switch to the DB copy
  SaveAndRestore<std::string> save_dbname(&dbname_, db_copy_name);
  ReopenWithColumnFamilies({"default", "koko"}, options);

  perform_gets();
  verify_stats();

  // And ensure that re-importing + ingesting the same files into a
  // different DB uses same cache keys
  DestroyAndReopen(options);

  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db_->CreateColumnFamilyWithImport(ColumnFamilyOptions(), "yoyo",
                                              ImportColumnFamilyOptions(),
                                              *metadata_ptr_, &cfh));
  ASSERT_NE(cfh, nullptr);
  delete cfh;
  cfh = nullptr;
  delete metadata_ptr_;
  metadata_ptr_ = nullptr;

  ASSERT_OK(DestroyDB(export_files_dir, options));

  ReopenWithColumnFamilies({"default", "yoyo"}, options);

  IngestExternalFileOptions ingest_opts;
  ASSERT_OK(db_->IngestExternalFile(handles_[1], {external}, ingest_opts));

  perform_gets();
  verify_stats();
#endif  // !ROCKSDB_LITE

  Close();
  Destroy(options);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

class CacheKeyTest : public testing::Test {
 public:
  CacheKey GetBaseCacheKey() {
    CacheKey rv = GetOffsetableCacheKey(0, /*min file_number*/ 1).WithOffset(0);
    // Correct for file_number_ == 1
    *reinterpret_cast<uint64_t*>(&rv) ^= ReverseBits(uint64_t{1});
    return rv;
  }
  CacheKey GetCacheKey(uint64_t session_counter, uint64_t file_number,
                       uint64_t offset) {
    OffsetableCacheKey offsetable =
        GetOffsetableCacheKey(session_counter, file_number);
    // * 4 to counteract optimization that strips lower 2 bits in encoding
    // the offset in BlockBasedTable::GetCacheKey (which we prefer to include
    // in unit tests to maximize functional coverage).
    EXPECT_GE(offset * 4, offset);  // no overflow
    return BlockBasedTable::GetCacheKey(offsetable,
                                        BlockHandle(offset * 4, /*size*/ 5));
  }

 protected:
  OffsetableCacheKey GetOffsetableCacheKey(uint64_t session_counter,
                                           uint64_t file_number) {
    // Like SemiStructuredUniqueIdGen::GenerateNext
    tp_.db_session_id = EncodeSessionId(base_session_upper_,
                                        base_session_lower_ ^ session_counter);
    tp_.db_id = std::to_string(db_id_);
    tp_.orig_file_number = file_number;
    bool is_stable;
    std::string cur_session_id = "";  // ignored
    uint64_t cur_file_number = 42;    // ignored
    OffsetableCacheKey rv;
    BlockBasedTable::SetupBaseCacheKey(&tp_, cur_session_id, cur_file_number,
                                       &rv, &is_stable);
    EXPECT_TRUE(is_stable);
    EXPECT_TRUE(!rv.IsEmpty());
    // BEGIN some assertions in relation to SST unique IDs
    std::string external_unique_id_str;
    EXPECT_OK(GetUniqueIdFromTableProperties(tp_, &external_unique_id_str));
    UniqueId64x2 sst_unique_id = {};
    EXPECT_OK(DecodeUniqueIdBytes(external_unique_id_str, &sst_unique_id));
    ExternalUniqueIdToInternal(&sst_unique_id);
    OffsetableCacheKey ock =
        OffsetableCacheKey::FromInternalUniqueId(&sst_unique_id);
    EXPECT_EQ(rv.WithOffset(0).AsSlice(), ock.WithOffset(0).AsSlice());
    EXPECT_EQ(ock.ToInternalUniqueId(), sst_unique_id);
    // END some assertions in relation to SST unique IDs
    return rv;
  }

  TableProperties tp_;
  uint64_t base_session_upper_ = 0;
  uint64_t base_session_lower_ = 0;
  uint64_t db_id_ = 0;
};

TEST_F(CacheKeyTest, DBImplSessionIdStructure) {
  // We have to generate our own session IDs for simulation purposes in other
  // tests. Here we verify that the DBImpl implementation seems to match
  // our construction here, by using lowest XORed-in bits for "session
  // counter."
  std::string session_id1 = DBImpl::GenerateDbSessionId(/*env*/ nullptr);
  std::string session_id2 = DBImpl::GenerateDbSessionId(/*env*/ nullptr);
  uint64_t upper1, upper2, lower1, lower2;
  ASSERT_OK(DecodeSessionId(session_id1, &upper1, &lower1));
  ASSERT_OK(DecodeSessionId(session_id2, &upper2, &lower2));
  // Because generated in same process
  ASSERT_EQ(upper1, upper2);
  // Unless we generate > 4 billion session IDs in this process...
  ASSERT_EQ(Upper32of64(lower1), Upper32of64(lower2));
  // But they must be different somewhere
  ASSERT_NE(Lower32of64(lower1), Lower32of64(lower2));
}

namespace {
// Deconstruct cache key, based on knowledge of implementation details.
void DeconstructNonemptyCacheKey(const CacheKey& key, uint64_t* file_num_etc64,
                                 uint64_t* offset_etc64) {
  *file_num_etc64 = *reinterpret_cast<const uint64_t*>(key.AsSlice().data());
  *offset_etc64 = *reinterpret_cast<const uint64_t*>(key.AsSlice().data() + 8);
  assert(*file_num_etc64 != 0);
  if (*offset_etc64 == 0) {
    std::swap(*file_num_etc64, *offset_etc64);
  }
  assert(*offset_etc64 != 0);
}

// Make a bit mask of 0 to 64 bits
uint64_t MakeMask64(int bits) {
  if (bits >= 64) {
    return uint64_t{0} - 1;
  } else {
    return (uint64_t{1} << bits) - 1;
  }
}

// See CacheKeyTest::Encodings
struct CacheKeyDecoder {
  // Inputs
  uint64_t base_file_num_etc64, base_offset_etc64;
  int session_counter_bits, file_number_bits, offset_bits;

  // Derived
  uint64_t session_counter_mask, file_number_mask, offset_mask;

  // Outputs
  uint64_t decoded_session_counter, decoded_file_num, decoded_offset;

  void SetBaseCacheKey(const CacheKey& base) {
    DeconstructNonemptyCacheKey(base, &base_file_num_etc64, &base_offset_etc64);
  }

  void SetRanges(int _session_counter_bits, int _file_number_bits,
                 int _offset_bits) {
    session_counter_bits = _session_counter_bits;
    session_counter_mask = MakeMask64(session_counter_bits);
    file_number_bits = _file_number_bits;
    file_number_mask = MakeMask64(file_number_bits);
    offset_bits = _offset_bits;
    offset_mask = MakeMask64(offset_bits);
  }

  void Decode(const CacheKey& key) {
    uint64_t file_num_etc64, offset_etc64;
    DeconstructNonemptyCacheKey(key, &file_num_etc64, &offset_etc64);

    // First decode session counter
    if (offset_bits + session_counter_bits <= 64) {
      // fully recoverable from offset_etc64
      decoded_session_counter =
          ReverseBits((offset_etc64 ^ base_offset_etc64)) &
          session_counter_mask;
    } else if (file_number_bits + session_counter_bits <= 64) {
      // fully recoverable from file_num_etc64
      decoded_session_counter = DownwardInvolution(
          (file_num_etc64 ^ base_file_num_etc64) & session_counter_mask);
    } else {
      // Need to combine parts from each word.
      // Piece1 will contain some correct prefix of the bottom bits of
      // session counter.
      uint64_t piece1 =
          ReverseBits((offset_etc64 ^ base_offset_etc64) & ~offset_mask);
      int piece1_bits = 64 - offset_bits;
      // Piece2 will contain involuded bits that we can combine with piece1
      // to infer rest of session counter
      int piece2_bits = std::min(64 - file_number_bits, 64 - piece1_bits);
      ASSERT_LT(piece2_bits, 64);
      uint64_t piece2_mask = MakeMask64(piece2_bits);
      uint64_t piece2 = (file_num_etc64 ^ base_file_num_etc64) & piece2_mask;

      // Cancel out the part of piece2 that we can infer from piece1
      // (DownwardInvolution distributes over xor)
      piece2 ^= DownwardInvolution(piece1) & piece2_mask;

      // Now we need to solve for the unknown original bits in higher
      // positions than piece1 provides. We use Gaussian elimination
      // because we know that a piece2_bits X piece2_bits submatrix of
      // the matrix underlying DownwardInvolution times the vector of
      // unknown original bits equals piece2.
      //
      // Build an augmented row matrix for that submatrix, built column by
      // column.
      std::array<uint64_t, 64> aug_rows{};
      for (int i = 0; i < piece2_bits; ++i) {  // over columns
        uint64_t col_i = DownwardInvolution(uint64_t{1} << piece1_bits << i);
        ASSERT_NE(col_i & 1U, 0);
        for (int j = 0; j < piece2_bits; ++j) {  // over rows
          aug_rows[j] |= (col_i & 1U) << i;
          col_i >>= 1;
        }
      }
      // Augment with right hand side
      for (int j = 0; j < piece2_bits; ++j) {  // over rows
        aug_rows[j] |= (piece2 & 1U) << piece2_bits;
        piece2 >>= 1;
      }
      // Run Gaussian elimination
      for (int i = 0; i < piece2_bits; ++i) {  // over columns
        // Find a row that can be used to cancel others
        uint64_t canceller = 0;
        // Note: Rows 0 through i-1 contain 1s in columns already eliminated
        for (int j = i; j < piece2_bits; ++j) {  // over rows
          if (aug_rows[j] & (uint64_t{1} << i)) {
            // Swap into appropriate row
            std::swap(aug_rows[i], aug_rows[j]);
            // Keep a handy copy for row reductions
            canceller = aug_rows[i];
            break;
          }
        }
        ASSERT_NE(canceller, 0);
        for (int j = 0; j < piece2_bits; ++j) {  // over rows
          if (i != j && ((aug_rows[j] >> i) & 1) != 0) {
            // Row reduction
            aug_rows[j] ^= canceller;
          }
        }
      }
      // Extract result
      decoded_session_counter = piece1;
      for (int j = 0; j < piece2_bits; ++j) {  // over rows
        ASSERT_EQ(aug_rows[j] & piece2_mask, uint64_t{1} << j);
        decoded_session_counter |= aug_rows[j] >> piece2_bits << piece1_bits
                                                              << j;
      }
    }

    decoded_offset =
        offset_etc64 ^ base_offset_etc64 ^ ReverseBits(decoded_session_counter);

    decoded_file_num = ReverseBits(file_num_etc64 ^ base_file_num_etc64 ^
                                   DownwardInvolution(decoded_session_counter));
  }
};
}  // anonymous namespace

TEST_F(CacheKeyTest, Encodings) {
  // This test primarily verifies this claim from cache_key.cc:
  // // In fact, if DB ids were not involved, we would be guaranteed unique
  // // cache keys for files generated in a single process until total bits for
  // // biggest session_id_counter, orig_file_number, and offset_in_file
  // // reach 128 bits.
  //
  // To demonstrate this, CacheKeyDecoder can reconstruct the structured inputs
  // to the cache key when provided an output cache key, the unstructured
  // inputs, and bounds on the structured inputs.
  //
  // See OffsetableCacheKey comments in cache_key.cc.

  // We are going to randomly initialize some values that *should* not affect
  // result
  Random64 r{std::random_device{}()};

  CacheKeyDecoder decoder;
  db_id_ = r.Next();
  base_session_upper_ = r.Next();
  base_session_lower_ = r.Next();
  if (base_session_lower_ == 0) {
    base_session_lower_ = 1;
  }

  decoder.SetBaseCacheKey(GetBaseCacheKey());

  // Loop over configurations and test those
  for (int session_counter_bits = 0; session_counter_bits <= 64;
       ++session_counter_bits) {
    for (int file_number_bits = 1; file_number_bits <= 64; ++file_number_bits) {
      // 62 bits max because unoptimized offset will be 64 bits in that case
      for (int offset_bits = 0; offset_bits <= 62; ++offset_bits) {
        if (session_counter_bits + file_number_bits + offset_bits > 128) {
          break;
        }

        decoder.SetRanges(session_counter_bits, file_number_bits, offset_bits);

        uint64_t session_counter = r.Next() & decoder.session_counter_mask;
        uint64_t file_number = r.Next() & decoder.file_number_mask;
        if (file_number == 0) {
          // Minimum
          file_number = 1;
        }
        uint64_t offset = r.Next() & decoder.offset_mask;
        decoder.Decode(GetCacheKey(session_counter, file_number, offset));

        EXPECT_EQ(decoder.decoded_session_counter, session_counter);
        EXPECT_EQ(decoder.decoded_file_num, file_number);
        EXPECT_EQ(decoder.decoded_offset, offset);
      }
    }
  }
}

INSTANTIATE_TEST_CASE_P(DBBlockCacheKeyTest, DBBlockCacheKeyTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

class DBBlockCachePinningTest
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<bool, PinningTier, PinningTier, PinningTier>> {
 public:
  DBBlockCachePinningTest()
      : DBTestBase("db_block_cache_test", /*env_do_fsync=*/false) {}

  void SetUp() override {
    partition_index_and_filters_ = std::get<0>(GetParam());
    top_level_index_pinning_ = std::get<1>(GetParam());
    partition_pinning_ = std::get<2>(GetParam());
    unpartitioned_pinning_ = std::get<3>(GetParam());
  }

  bool partition_index_and_filters_;
  PinningTier top_level_index_pinning_;
  PinningTier partition_pinning_;
  PinningTier unpartitioned_pinning_;
};

TEST_P(DBBlockCachePinningTest, TwoLevelDB) {
  // Creates one file in L0 and one file in L1. Both files have enough data that
  // their index and filter blocks are partitioned. The L1 file will also have
  // a compression dictionary (those are trained only during compaction), which
  // must be unpartitioned.
  const int kKeySize = 32;
  const int kBlockSize = 128;
  const int kNumBlocksPerFile = 128;
  const int kNumKeysPerFile = kBlockSize * kNumBlocksPerFile / kKeySize;

  Options options = CurrentOptions();
  // `kNoCompression` makes the unit test more portable. But it relies on the
  // current behavior of persisting/accessing dictionary even when there's no
  // (de)compression happening, which seems fairly likely to change over time.
  options.compression = kNoCompression;
  options.compression_opts.max_dict_bytes = 4 << 10;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(1 << 20 /* capacity */);
  table_options.block_size = kBlockSize;
  table_options.metadata_block_size = kBlockSize;
  table_options.cache_index_and_filter_blocks = true;
  table_options.metadata_cache_options.top_level_index_pinning =
      top_level_index_pinning_;
  table_options.metadata_cache_options.partition_pinning = partition_pinning_;
  table_options.metadata_cache_options.unpartitioned_pinning =
      unpartitioned_pinning_;
  table_options.filter_policy.reset(
      NewBloomFilterPolicy(10 /* bits_per_key */));
  if (partition_index_and_filters_) {
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    table_options.partition_filters = true;
  }
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kKeySize)));
    }
    ASSERT_OK(Flush());
    if (i == 0) {
      // Prevent trivial move so file will be rewritten with dictionary and
      // reopened with L1's pinning settings.
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
  }

  // Clear all unpinned blocks so unpinned blocks will show up as cache misses
  // when reading a key from a file.
  table_options.block_cache->EraseUnRefEntries();

  // Get base cache values
  uint64_t filter_misses = TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  uint64_t index_misses = TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  uint64_t compression_dict_misses =
      TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_MISS);

  // Read a key from the L0 file
  Get(Key(kNumKeysPerFile));
  uint64_t expected_filter_misses = filter_misses;
  uint64_t expected_index_misses = index_misses;
  uint64_t expected_compression_dict_misses = compression_dict_misses;
  if (partition_index_and_filters_) {
    if (top_level_index_pinning_ == PinningTier::kNone) {
      ++expected_filter_misses;
      ++expected_index_misses;
    }
    if (partition_pinning_ == PinningTier::kNone) {
      ++expected_filter_misses;
      ++expected_index_misses;
    }
  } else {
    if (unpartitioned_pinning_ == PinningTier::kNone) {
      ++expected_filter_misses;
      ++expected_index_misses;
    }
  }
  if (unpartitioned_pinning_ == PinningTier::kNone) {
    ++expected_compression_dict_misses;
  }
  ASSERT_EQ(expected_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(expected_index_misses,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(expected_compression_dict_misses,
            TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_MISS));

  // Clear all unpinned blocks so unpinned blocks will show up as cache misses
  // when reading a key from a file.
  table_options.block_cache->EraseUnRefEntries();

  // Read a key from the L1 file
  Get(Key(0));
  if (partition_index_and_filters_) {
    if (top_level_index_pinning_ == PinningTier::kNone ||
        top_level_index_pinning_ == PinningTier::kFlushedAndSimilar) {
      ++expected_filter_misses;
      ++expected_index_misses;
    }
    if (partition_pinning_ == PinningTier::kNone ||
        partition_pinning_ == PinningTier::kFlushedAndSimilar) {
      ++expected_filter_misses;
      ++expected_index_misses;
    }
  } else {
    if (unpartitioned_pinning_ == PinningTier::kNone ||
        unpartitioned_pinning_ == PinningTier::kFlushedAndSimilar) {
      ++expected_filter_misses;
      ++expected_index_misses;
    }
  }
  if (unpartitioned_pinning_ == PinningTier::kNone ||
      unpartitioned_pinning_ == PinningTier::kFlushedAndSimilar) {
    ++expected_compression_dict_misses;
  }
  ASSERT_EQ(expected_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(expected_index_misses,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(expected_compression_dict_misses,
            TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_MISS));
}

INSTANTIATE_TEST_CASE_P(
    DBBlockCachePinningTest, DBBlockCachePinningTest,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Values(PinningTier::kNone, PinningTier::kFlushedAndSimilar,
                          PinningTier::kAll),
        ::testing::Values(PinningTier::kNone, PinningTier::kFlushedAndSimilar,
                          PinningTier::kAll),
        ::testing::Values(PinningTier::kNone, PinningTier::kFlushedAndSimilar,
                          PinningTier::kAll)));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
