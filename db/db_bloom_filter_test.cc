//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"

namespace rocksdb {

// DB tests related to bloom filter.

class DBBloomFilterTest : public DBTestBase {
 public:
  DBBloomFilterTest() : DBTestBase("/db_bloom_filter_test") {}
};

// KeyMayExist can lead to a few false positives, but not false negatives.
// To make test deterministic, use a much larger number of bits per key-20 than
// bits in the key, so that false positives are eliminated
TEST_F(DBBloomFilterTest, KeyMayExist) {
  do {
    ReadOptions ropts;
    std::string value;
    anon::OptionsOverride options_override;
    options_override.filter_policy.reset(NewBloomFilterPolicy(20));
    Options options = CurrentOptions(options_override);
    options.statistics = rocksdb::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));

    ASSERT_OK(Put(1, "a", "b"));
    bool value_found = false;
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_TRUE(value_found);
    ASSERT_EQ("b", value);

    ASSERT_OK(Flush(1));
    value.clear();

    uint64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    uint64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(
        db_->KeyMayExist(ropts, handles_[1], "a", &value, &value_found));
    ASSERT_TRUE(!value_found);
    // assert that no new files were opened and no new blocks were
    // read into block cache.
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "a"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Flush(1));
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                true /* disallow trivial move */);

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "a", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    ASSERT_OK(Delete(1, "c"));

    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    ASSERT_TRUE(!db_->KeyMayExist(ropts, handles_[1], "c", &value));
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));

    // KeyMayExist function only checks data in block caches, which is not used
    // by plain table format.
  } while (
      ChangeOptions(kSkipPlainTable | kSkipHashIndex | kSkipFIFOCompaction));
}

// A delete is skipped for key if KeyMayExist(key) returns False
// Tests Writebatch consistency and proper delete behaviour
TEST_F(DBBloomFilterTest, FilterDeletes) {
  do {
    anon::OptionsOverride options_override;
    options_override.filter_policy.reset(NewBloomFilterPolicy(20));
    Options options = CurrentOptions(options_override);
    options.filter_deletes = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    WriteBatch batch;

    batch.Delete(handles_[1], "a");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(AllEntriesFor("a", 1), "[ ]");  // Delete skipped
    batch.Clear();

    batch.Put(handles_[1], "a", "b");
    batch.Delete(handles_[1], "a");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(Get(1, "a"), "NOT_FOUND");
    ASSERT_EQ(AllEntriesFor("a", 1), "[ DEL, b ]");  // Delete issued
    batch.Clear();

    batch.Delete(handles_[1], "c");
    batch.Put(handles_[1], "c", "d");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(Get(1, "c"), "d");
    ASSERT_EQ(AllEntriesFor("c", 1), "[ d ]");  // Delete skipped
    batch.Clear();

    ASSERT_OK(Flush(1));  // A stray Flush

    batch.Delete(handles_[1], "c");
    dbfull()->Write(WriteOptions(), &batch);
    ASSERT_EQ(AllEntriesFor("c", 1), "[ DEL, d ]");  // Delete issued
    batch.Clear();
  } while (ChangeCompactOptions());
}

TEST_F(DBBloomFilterTest, GetFilterByPrefixBloom) {
  Options options = last_options_;
  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  WriteOptions wo;
  ReadOptions ro;
  FlushOptions fo;
  fo.wait = true;
  std::string value;

  ASSERT_OK(dbfull()->Put(wo, "barbarbar", "foo"));
  ASSERT_OK(dbfull()->Put(wo, "barbarbar2", "foo2"));
  ASSERT_OK(dbfull()->Put(wo, "foofoofoo", "bar"));

  dbfull()->Flush(fo);

  ASSERT_EQ("foo", Get("barbarbar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("foo2", Get("barbarbar2"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("barbarbar3"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

  ASSERT_EQ("NOT_FOUND", Get("barfoofoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

  ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);

  ro.total_order_seek = true;
  ASSERT_TRUE(db_->Get(ro, "foobarbar", &value).IsNotFound());
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
}

TEST_F(DBBloomFilterTest, WholeKeyFilterProp) {
  Options options = last_options_;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  options.statistics = rocksdb::CreateDBStatistics();

  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  WriteOptions wo;
  ReadOptions ro;
  FlushOptions fo;
  fo.wait = true;
  std::string value;

  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  dbfull()->Flush(fo);

  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

  // Reopen with whole key filtering enabled and prefix extractor
  // NULL. Bloom filter should be off for both of whole key and
  // prefix bloom.
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.prefix_extractor.reset();
  Reopen(options);

  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  // Write DB with only full key filtering.
  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  // Reopen with both of whole key off and prefix extractor enabled.
  // Still no bloom filter should be used.
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);

  // Try to create a DB with mixed files:
  ASSERT_OK(dbfull()->Put(wo, "foobar", "foo"));
  // Needs insert some keys to make sure files are not filtered out by key
  // ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  options.prefix_extractor.reset();
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  // Try to create a DB with mixed files.
  ASSERT_OK(dbfull()->Put(wo, "barfoo", "bar"));
  // In this case needs insert some keys to make sure files are
  // not filtered out by key ranges.
  ASSERT_OK(dbfull()->Put(wo, "aaa", ""));
  ASSERT_OK(dbfull()->Put(wo, "zzz", ""));
  Flush();

  // Now we have two files:
  // File 1: An older file with prefix bloom.
  // File 2: A newer file with whole bloom filter.
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 3);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);

  // Reopen with the same setting: only whole key is used
  Reopen(options);
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 4);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 5);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 6);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);

  // Restart with both filters are allowed
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 7);
  // File 1 will has it filtered out.
  // File 2 will not, as prefix `foo` exists in the file.
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 8);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 10);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);

  // Restart with only prefix bloom is allowed.
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 11);
  ASSERT_EQ("NOT_FOUND", Get("bar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
  ASSERT_EQ("foo", Get("foobar"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
  ASSERT_EQ("bar", Get("barfoo"));
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 12);
}

TEST_F(DBBloomFilterTest, BloomFilter) {
  do {
    Options options = CurrentOptions();
    env_->count_random_reads_ = true;
    options.env = env_;
    // ChangeCompactOptions() only changes compaction style, which does not
    // trigger reset of table_factory
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    CreateAndReopenWithCF({"pikachu"}, options);

    // Populate multiple layers
    const int N = 10000;
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Compact(1, "a", "z");
    for (int i = 0; i < N; i += 100) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Flush(1);

    // Prevent auto compactions triggered by seeks
    env_->delay_sstable_sync_.store(true, std::memory_order_release);

    // Lookup present keys.  Should rarely read from small sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    int reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d present => %d reads\n", N, reads);
    ASSERT_GE(reads, N);
    ASSERT_LE(reads, N + 2 * N / 100);

    // Lookup present keys.  Should rarely read from either sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i) + ".missing"));
    }
    reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d missing => %d reads\n", N, reads);
    ASSERT_LE(reads, 3 * N / 100);

    env_->delay_sstable_sync_.store(false, std::memory_order_release);
    Close();
  } while (ChangeCompactOptions());
}

TEST_F(DBBloomFilterTest, BloomFilterRate) {
  while (ChangeFilterOptions()) {
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);

    const int maxKey = 10000;
    for (int i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    // Add a large key to make the file contain wide range
    ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
    Flush(1);

    // Check if they can be found
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);

    // Check if filter is useful
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
    }
    ASSERT_GE(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey * 0.98);
  }
}

TEST_F(DBBloomFilterTest, BloomFilterCompatibility) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Create with block based filter
  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  Flush(1);

  // Check db with full filter
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
}

TEST_F(DBBloomFilterTest, BloomFilterReverseCompatibility) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Create with full filter
  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  Flush(1);

  // Check db with block_based filter
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
}

namespace {
// A wrapped bloom over default FilterPolicy
class WrappedBloom : public FilterPolicy {
 public:
  explicit WrappedBloom(int bits_per_key)
      : filter_(NewBloomFilterPolicy(bits_per_key)), counter_(0) {}

  ~WrappedBloom() { delete filter_; }

  const char* Name() const override { return "WrappedRocksDbFilterPolicy"; }

  void CreateFilter(const rocksdb::Slice* keys, int n,
                    std::string* dst) const override {
    std::unique_ptr<rocksdb::Slice[]> user_keys(new rocksdb::Slice[n]);
    for (int i = 0; i < n; ++i) {
      user_keys[i] = convertKey(keys[i]);
    }
    return filter_->CreateFilter(user_keys.get(), n, dst);
  }

  bool KeyMayMatch(const rocksdb::Slice& key,
                   const rocksdb::Slice& filter) const override {
    counter_++;
    return filter_->KeyMayMatch(convertKey(key), filter);
  }

  uint32_t GetCounter() { return counter_; }

 private:
  const FilterPolicy* filter_;
  mutable uint32_t counter_;

  rocksdb::Slice convertKey(const rocksdb::Slice& key) const { return key; }
};
}  // namespace

TEST_F(DBBloomFilterTest, BloomFilterWrapper) {
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();

  BlockBasedTableOptions table_options;
  WrappedBloom* policy = new WrappedBloom(10);
  table_options.filter_policy.reset(policy);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  CreateAndReopenWithCF({"pikachu"}, options);

  const int maxKey = 10000;
  for (int i = 0; i < maxKey; i++) {
    ASSERT_OK(Put(1, Key(i), Key(i)));
  }
  // Add a large key to make the file contain wide range
  ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
  ASSERT_EQ(0U, policy->GetCounter());
  Flush(1);

  // Check if they can be found
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ(Key(i), Get(1, Key(i)));
  }
  ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
  ASSERT_EQ(1U * maxKey, policy->GetCounter());

  // Check if filter is useful
  for (int i = 0; i < maxKey; i++) {
    ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
  }
  ASSERT_GE(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey * 0.98);
  ASSERT_EQ(2U * maxKey, policy->GetCounter());
}

class SliceTransformLimitedDomain : public SliceTransform {
  const char* Name() const override { return "SliceTransformLimitedDomain"; }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 5);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 5 && src[0] == 'x';
  }

  bool InRange(const Slice& dst) const override {
    // prefix will be x????
    return dst.size() == 5 && dst[0] == 'x';
  }
};

TEST_F(DBBloomFilterTest, PrefixExtractorFullFilter) {
  BlockBasedTableOptions bbto;
  // Full Filter Block
  bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;

  Options options = CurrentOptions();
  options.prefix_extractor = std::make_shared<SliceTransformLimitedDomain>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  ASSERT_OK(Put("x1111_AAAA", "val1"));
  ASSERT_OK(Put("x1112_AAAA", "val2"));
  ASSERT_OK(Put("x1113_AAAA", "val3"));
  ASSERT_OK(Put("x1114_AAAA", "val4"));
  // Not in domain, wont be added to filter
  ASSERT_OK(Put("zzzzz_AAAA", "val5"));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get("x1111_AAAA"), "val1");
  ASSERT_EQ(Get("x1112_AAAA"), "val2");
  ASSERT_EQ(Get("x1113_AAAA"), "val3");
  ASSERT_EQ(Get("x1114_AAAA"), "val4");
  // Was not added to filter but rocksdb will try to read it from the filter
  ASSERT_EQ(Get("zzzzz_AAAA"), "val5");
}

TEST_F(DBBloomFilterTest, PrefixExtractorBlockFilter) {
  BlockBasedTableOptions bbto;
  // Block Filter Block
  bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

  Options options = CurrentOptions();
  options.prefix_extractor = std::make_shared<SliceTransformLimitedDomain>();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  ASSERT_OK(Put("x1113_AAAA", "val3"));
  ASSERT_OK(Put("x1114_AAAA", "val4"));
  // Not in domain, wont be added to filter
  ASSERT_OK(Put("zzzzz_AAAA", "val1"));
  ASSERT_OK(Put("zzzzz_AAAB", "val2"));
  ASSERT_OK(Put("zzzzz_AAAC", "val3"));
  ASSERT_OK(Put("zzzzz_AAAD", "val4"));

  ASSERT_OK(Flush());

  std::vector<std::string> iter_res;
  auto iter = db_->NewIterator(ReadOptions());
  // Seek to a key that was not in Domain
  for (iter->Seek("zzzzz_AAAA"); iter->Valid(); iter->Next()) {
    iter_res.emplace_back(iter->value().ToString());
  }

  std::vector<std::string> expected_res = {"val1", "val2", "val3", "val4"};
  ASSERT_EQ(iter_res, expected_res);
  delete iter;
}

#ifndef ROCKSDB_LITE
class BloomStatsTestWithParam
    : public DBBloomFilterTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  BloomStatsTestWithParam() {
    use_block_table_ = std::get<0>(GetParam());
    use_block_based_builder_ = std::get<1>(GetParam());

    options_.create_if_missing = true;
    options_.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(4));
    options_.memtable_prefix_bloom_bits = 8 * 1024;
    if (use_block_table_) {
      BlockBasedTableOptions table_options;
      table_options.hash_index_allow_collision = false;
      table_options.filter_policy.reset(
          NewBloomFilterPolicy(10, use_block_based_builder_));
      options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    } else {
      PlainTableOptions table_options;
      options_.table_factory.reset(NewPlainTableFactory(table_options));
    }

    perf_context.Reset();
    DestroyAndReopen(options_);
  }

  ~BloomStatsTestWithParam() {
    perf_context.Reset();
    Destroy(options_);
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  bool use_block_table_;
  bool use_block_based_builder_;
  Options options_;
};

// 1 Insert 2 K-V pairs into DB
// 2 Call Get() for both keys - expext memtable bloom hit stat to be 2
// 3 Call Get() for nonexisting key - expect memtable bloom miss stat to be 1
// 4 Call Flush() to create SST
// 5 Call Get() for both keys - expext SST bloom hit stat to be 2
// 6 Call Get() for nonexisting key - expect SST bloom miss stat to be 1
// Test both: block and plain SST
TEST_P(BloomStatsTestWithParam, BloomStatsTest) {
  std::string key1("AAAA");
  std::string key2("RXDB");  // not in DB
  std::string key3("ZBRA");
  std::string value1("Value1");
  std::string value3("Value3");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key3, value3, WriteOptions()));

  // check memtable bloom stats
  ASSERT_EQ(value1, Get(key1));
  ASSERT_EQ(1, perf_context.bloom_memtable_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2, perf_context.bloom_memtable_hit_count);
  ASSERT_EQ(0, perf_context.bloom_memtable_miss_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, perf_context.bloom_memtable_miss_count);
  ASSERT_EQ(2, perf_context.bloom_memtable_hit_count);

  // sanity checks
  ASSERT_EQ(0, perf_context.bloom_sst_hit_count);
  ASSERT_EQ(0, perf_context.bloom_sst_miss_count);

  Flush();

  // sanity checks
  ASSERT_EQ(0, perf_context.bloom_sst_hit_count);
  ASSERT_EQ(0, perf_context.bloom_sst_miss_count);

  // check SST bloom stats
  // NOTE: hits per get differs because of code paths differences
  // in BlockBasedTable::Get()
  int hits_per_get = use_block_table_ && !use_block_based_builder_ ? 2 : 1;
  ASSERT_EQ(value1, Get(key1));
  ASSERT_EQ(hits_per_get, perf_context.bloom_sst_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2 * hits_per_get, perf_context.bloom_sst_hit_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, perf_context.bloom_sst_miss_count);
}

// Same scenario as in BloomStatsTest but using an iterator
TEST_P(BloomStatsTestWithParam, BloomStatsTestWithIter) {
  std::string key1("AAAA");
  std::string key2("RXDB");  // not in DB
  std::string key3("ZBRA");
  std::string value1("Value1");
  std::string value3("Value3");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key3, value3, WriteOptions()));

  unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));

  // check memtable bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, perf_context.bloom_memtable_hit_count);
  ASSERT_EQ(0, perf_context.bloom_memtable_miss_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  ASSERT_EQ(2, perf_context.bloom_memtable_hit_count);
  ASSERT_EQ(0, perf_context.bloom_memtable_miss_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
  ASSERT_EQ(1, perf_context.bloom_memtable_miss_count);
  ASSERT_EQ(2, perf_context.bloom_memtable_hit_count);

  Flush();

  iter.reset(dbfull()->NewIterator(ReadOptions()));

  // Check SST bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, perf_context.bloom_sst_hit_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  ASSERT_EQ(2, perf_context.bloom_sst_hit_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
  ASSERT_EQ(1, perf_context.bloom_sst_miss_count);
  ASSERT_EQ(2, perf_context.bloom_sst_hit_count);
}

INSTANTIATE_TEST_CASE_P(BloomStatsTestWithParam, BloomStatsTestWithParam,
                        ::testing::Values(std::make_tuple(true, true),
                                          std::make_tuple(true, false),
                                          std::make_tuple(false, false)));

namespace {
void PrefixScanInit(DBBloomFilterTest* dbtest) {
  char buf[100];
  std::string keystr;
  const int small_range_sstfiles = 5;
  const int big_range_sstfiles = 5;

  // Generate 11 sst files with the following prefix ranges.
  // GROUP 0: [0,10]                              (level 1)
  // GROUP 1: [1,2], [2,3], [3,4], [4,5], [5, 6]  (level 0)
  // GROUP 2: [0,6], [0,7], [0,8], [0,9], [0,10]  (level 0)
  //
  // A seek with the previous API would do 11 random I/Os (to all the
  // files).  With the new API and a prefix filter enabled, we should
  // only do 2 random I/O, to the 2 files containing the key.

  // GROUP 0
  snprintf(buf, sizeof(buf), "%02d______:start", 0);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  snprintf(buf, sizeof(buf), "%02d______:end", 10);
  keystr = std::string(buf);
  ASSERT_OK(dbtest->Put(keystr, keystr));
  dbtest->Flush();
  dbtest->dbfull()->CompactRange(CompactRangeOptions(), nullptr,
                                 nullptr);  // move to level 1

  // GROUP 1
  for (int i = 1; i <= small_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", i);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", i + 1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    dbtest->Flush();
  }

  // GROUP 2
  for (int i = 1; i <= big_range_sstfiles; i++) {
    snprintf(buf, sizeof(buf), "%02d______:start", 0);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    snprintf(buf, sizeof(buf), "%02d______:end", small_range_sstfiles + i + 1);
    keystr = std::string(buf);
    ASSERT_OK(dbtest->Put(keystr, keystr));
    dbtest->Flush();
  }
}
}  // namespace

TEST_F(DBBloomFilterTest, PrefixScan) {
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip,
             kSkipNoPrefix);
  while (ChangeFilterOptions()) {
    int count;
    Slice prefix;
    Slice key;
    char buf[100];
    Iterator* iter;
    snprintf(buf, sizeof(buf), "03______:");
    prefix = Slice(buf, 8);
    key = Slice(buf, 9);
    ASSERT_EQ(key.difference_offset(prefix), 8);
    ASSERT_EQ(prefix.difference_offset(key), 8);
    // db configs
    env_->count_random_reads_ = true;
    Options options = CurrentOptions();
    options.env = env_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.disable_auto_compactions = true;
    options.max_background_compactions = 2;
    options.create_if_missing = true;
    options.memtable_factory.reset(NewHashSkipListRepFactory(16));

    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    table_options.whole_key_filtering = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // 11 RAND I/Os
    DestroyAndReopen(options);
    PrefixScanInit(this);
    count = 0;
    env_->random_read_counter_.Reset();
    iter = db_->NewIterator(ReadOptions());
    for (iter->Seek(prefix); iter->Valid(); iter->Next()) {
      if (!iter->key().starts_with(prefix)) {
        break;
      }
      count++;
    }
    ASSERT_OK(iter->status());
    delete iter;
    ASSERT_EQ(count, 2);
    ASSERT_EQ(env_->random_read_counter_.Read(), 2);
    Close();
  }  // end of while
  XFUNC_TEST("", "dbtest_prefix", prefix_skip1, XFuncPoint::SetSkip, 0);
}

TEST_F(DBBloomFilterTest, OptimizeFiltersForHits) {
  Options options = CurrentOptions();
  options.write_buffer_size = 64 * 1024;
  options.arena_block_size = 4 * 1024;
  options.target_file_size_base = 64 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.max_bytes_for_level_base = 256 * 1024;
  options.max_write_buffer_number = 2;
  options.max_background_compactions = 8;
  options.max_background_flushes = 8;
  options.compression = kNoCompression;
  options.compaction_style = kCompactionStyleLevel;
  options.level_compaction_dynamic_level_bytes = true;
  BlockBasedTableOptions bbto;
  bbto.cache_index_and_filter_blocks = true;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, true));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.optimize_filters_for_hits = true;
  options.statistics = rocksdb::CreateDBStatistics();
  CreateAndReopenWithCF({"mypikachu"}, options);

  int numkeys = 200000;

  // Generate randomly shuffled keys, so the updates are almost
  // random.
  std::vector<int> keys;
  keys.reserve(numkeys);
  for (int i = 0; i < numkeys; i += 2) {
    keys.push_back(i);
  }
  std::random_shuffle(std::begin(keys), std::end(keys));

  int num_inserted = 0;
  for (int key : keys) {
    ASSERT_OK(Put(1, Key(key), "val"));
    if (++num_inserted % 1000 == 0) {
      dbfull()->TEST_WaitForFlushMemTable();
      dbfull()->TEST_WaitForCompact();
    }
  }
  ASSERT_OK(Put(1, Key(0), "val"));
  ASSERT_OK(Put(1, Key(numkeys), "val"));
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForCompact();

  if (NumTableFilesAtLevel(0, 1) == 0) {
    // No Level 0 file. Create one.
    ASSERT_OK(Put(1, Key(0), "val"));
    ASSERT_OK(Put(1, Key(numkeys), "val"));
    ASSERT_OK(Flush(1));
    dbfull()->TEST_WaitForCompact();
  }

  for (int i = 1; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "NOT_FOUND");
  }

  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L0));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L1));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L2_AND_UP));

  // Now we have three sorted run, L0, L5 and L6 with most files in L6 have
  // no bloom filter. Most keys be checked bloom filters twice.
  ASSERT_GT(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 65000 * 2);
  ASSERT_LT(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 120000 * 2);

  for (int i = 0; i < numkeys; i += 2) {
    ASSERT_EQ(Get(1, Key(i)), "val");
  }

  // Part 2 (read path): rewrite last level with blooms, then verify they get
  // cached only if !optimize_filters_for_hits
  options.disable_auto_compactions = true;
  options.num_levels = 9;
  options.optimize_filters_for_hits = false;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);
  MoveFilesToLevel(7 /* level */, 1 /* column family index */);

  std::string value = Get(1, Key(0));
  uint64_t prev_cache_filter_hits =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  value = Get(1, Key(0));
  ASSERT_EQ(prev_cache_filter_hits + 1,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Now that we know the filter blocks exist in the last level files, see if
  // filter caching is skipped for this optimization
  options.optimize_filters_for_hits = true;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  value = Get(1, Key(0));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(2 /* index and data block */,
            TestGetTickerCount(options, BLOCK_CACHE_ADD));

  // Check filter block ignored for files preloaded during DB::Open()
  options.max_open_files = -1;
  options.statistics = CreateDBStatistics();
  bbto.block_cache.reset();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  uint64_t prev_cache_filter_misses =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  prev_cache_filter_hits = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  Get(1, Key(0));
  ASSERT_EQ(prev_cache_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(prev_cache_filter_hits,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));

  // Check filter block ignored for file trivially-moved to bottom level
  bbto.block_cache.reset();
  options.max_open_files = 100;  // setting > -1 makes it not preload all files
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  ASSERT_OK(Put(1, Key(numkeys + 1), "val"));
  ASSERT_OK(Flush(1));

  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* arg) { trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* arg) { non_trivial_move++; });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kSkip;
  compact_options.change_level = true;
  compact_options.target_level = 7;
  db_->CompactRange(compact_options, handles_[1], nullptr, nullptr);

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  prev_cache_filter_hits = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  prev_cache_filter_misses =
      TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  value = Get(1, Key(numkeys + 1));
  ASSERT_EQ(prev_cache_filter_hits,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(prev_cache_filter_misses,
            TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));

  // Check filter block not cached for iterator
  bbto.block_cache.reset();
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ReopenWithColumnFamilies({"default", "mypikachu"}, options);

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions(), handles_[1]));
  iter->SeekToFirst();
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(2 /* index and data block */,
            TestGetTickerCount(options, BLOCK_CACHE_ADD));
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
