//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/format.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
std::shared_ptr<const FilterPolicy> Create(double bits_per_key,
                                           const std::string& name) {
  return BloomLikeFilterPolicy::Create(name, bits_per_key);
}
const std::string kLegacyBloom = test::LegacyBloomFilterPolicy::kClassName();
const std::string kDeprecatedBlock =
    DeprecatedBlockBasedBloomFilterPolicy::kClassName();
const std::string kFastLocalBloom =
    test::FastLocalBloomFilterPolicy::kClassName();
const std::string kStandard128Ribbon =
    test::Standard128RibbonFilterPolicy::kClassName();
const std::string kAutoBloom = BloomFilterPolicy::kClassName();
const std::string kAutoRibbon = RibbonFilterPolicy::kClassName();
}  // namespace

// DB tests related to bloom filter.

class DBBloomFilterTest : public DBTestBase {
 public:
  DBBloomFilterTest()
      : DBTestBase("db_bloom_filter_test", /*env_do_fsync=*/true) {}
};

class DBBloomFilterTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<std::string, bool, uint32_t>> {
  //                             public testing::WithParamInterface<bool> {
 protected:
  std::string bfp_impl_;
  bool partition_filters_;
  uint32_t format_version_;

 public:
  DBBloomFilterTestWithParam()
      : DBTestBase("db_bloom_filter_tests", /*env_do_fsync=*/true) {}

  ~DBBloomFilterTestWithParam() override {}

  void SetUp() override {
    bfp_impl_ = std::get<0>(GetParam());
    partition_filters_ = std::get<1>(GetParam());
    format_version_ = std::get<2>(GetParam());
  }
};

class DBBloomFilterTestDefFormatVersion : public DBBloomFilterTestWithParam {};

class SliceTransformLimitedDomainGeneric : public SliceTransform {
  const char* Name() const override {
    return "SliceTransformLimitedDomainGeneric";
  }

  Slice Transform(const Slice& src) const override {
    return Slice(src.data(), 5);
  }

  bool InDomain(const Slice& src) const override {
    // prefix will be x????
    return src.size() >= 5;
  }

  bool InRange(const Slice& dst) const override {
    // prefix will be x????
    return dst.size() == 5;
  }
};

// KeyMayExist can lead to a few false positives, but not false negatives.
// To make test deterministic, use a much larger number of bits per key-20 than
// bits in the key, so that false positives are eliminated
TEST_P(DBBloomFilterTestDefFormatVersion, KeyMayExist) {
  do {
    ReadOptions ropts;
    std::string value;
    anon::OptionsOverride options_override;
    options_override.filter_policy = Create(20, bfp_impl_);
    options_override.partition_filters = partition_filters_;
    options_override.metadata_block_size = 32;
    Options options = CurrentOptions(options_override);
    if (partition_filters_) {
      auto* table_options =
          options.table_factory->GetOptions<BlockBasedTableOptions>();
      if (table_options != nullptr &&
          table_options->index_type !=
              BlockBasedTableOptions::kTwoLevelIndexSearch) {
        // In the current implementation partitioned filters depend on
        // partitioned indexes
        continue;
      }
    }
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
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
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                          true /* disallow trivial move */));

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

TEST_F(DBBloomFilterTest, GetFilterByPrefixBloomCustomPrefixExtractor) {
  for (bool partition_filters : {true, false}) {
    Options options = last_options_;
    options.prefix_extractor =
        std::make_shared<SliceTransformLimitedDomainGeneric>();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    if (partition_filters) {
      bbto.partition_filters = true;
      bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
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

    ASSERT_OK(dbfull()->Flush(fo));

    ASSERT_EQ("foo", Get("barbarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ(
        0,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
    ASSERT_EQ("foo2", Get("barbarbar2"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ(
        0,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
    ASSERT_EQ("NOT_FOUND", Get("barbarbar3"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 0);
    ASSERT_EQ(
        0,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    ASSERT_EQ("NOT_FOUND", Get("barfoofoo"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 1);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 2);
    ASSERT_EQ(
        2,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    ro.total_order_seek = true;
    // NOTE: total_order_seek no longer affects Get()
    ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 3);
    ASSERT_EQ(
        3,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    // No bloom on extractor changed
#ifndef ROCKSDB_LITE
    ASSERT_OK(db_->SetOptions({{"prefix_extractor", "capped:10"}}));
    ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 3);
    ASSERT_EQ(
        3,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
#endif  // ROCKSDB_LITE

    get_perf_context()->Reset();
  }
}

TEST_F(DBBloomFilterTest, GetFilterByPrefixBloom) {
  for (bool partition_filters : {true, false}) {
    Options options = last_options_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    if (partition_filters) {
      bbto.partition_filters = true;
      bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
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

    ASSERT_OK(dbfull()->Flush(fo));

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
    // NOTE: total_order_seek no longer affects Get()
    ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 3);
    ASSERT_EQ(
        3,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);

    // No bloom on extractor changed
#ifndef ROCKSDB_LITE
    ASSERT_OK(db_->SetOptions({{"prefix_extractor", "capped:10"}}));
    ASSERT_EQ("NOT_FOUND", Get("foobarbar"));
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_USEFUL), 3);
    ASSERT_EQ(
        3,
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
#endif  // ROCKSDB_LITE

    get_perf_context()->Reset();
  }
}

TEST_F(DBBloomFilterTest, WholeKeyFilterProp) {
  for (bool partition_filters : {true, false}) {
    Options options = last_options_;
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();

    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    bbto.whole_key_filtering = false;
    if (partition_filters) {
      bbto.partition_filters = true;
      bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
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
    ASSERT_OK(dbfull()->Flush(fo));

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
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

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
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

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
    ASSERT_OK(Flush());

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
    uint64_t bloom_filter_useful_all_levels = 0;
    for (auto& kv : (*(get_perf_context()->level_to_perf_context))) {
      if (kv.second.bloom_filter_useful > 0) {
        bloom_filter_useful_all_levels += kv.second.bloom_filter_useful;
      }
    }
    ASSERT_EQ(12, bloom_filter_useful_all_levels);
    get_perf_context()->Reset();
  }
}

TEST_P(DBBloomFilterTestWithParam, BloomFilter) {
  do {
    Options options = CurrentOptions();
    env_->count_random_reads_ = true;
    options.env = env_;
    // ChangeCompactOptions() only changes compaction style, which does not
    // trigger reset of table_factory
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy = Create(10, bfp_impl_);
    table_options.partition_filters = partition_filters_;
    if (partition_filters_) {
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    table_options.format_version = format_version_;
    if (format_version_ >= 4) {
      // value delta encoding challenged more with index interval > 1
      table_options.index_block_restart_interval = 8;
    }
    table_options.metadata_block_size = 32;
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
    ASSERT_OK(Flush(1));

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
    if (partition_filters_) {
      // Without block cache, we read an extra partition filter per each
      // level*read and a partition index per each read
      ASSERT_LE(reads, 4 * N + 2 * N / 100);
    } else {
      ASSERT_LE(reads, N + 2 * N / 100);
    }

    // Lookup present keys.  Should rarely read from either sstable.
    env_->random_read_counter_.Reset();
    for (int i = 0; i < N; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i) + ".missing"));
    }
    reads = env_->random_read_counter_.Read();
    fprintf(stderr, "%d missing => %d reads\n", N, reads);
    if (partition_filters_) {
      // With partitioned filter we read one extra filter per level per each
      // missed read.
      ASSERT_LE(reads, 2 * N + 3 * N / 100);
    } else {
      ASSERT_LE(reads, 3 * N / 100);
    }

#ifndef ROCKSDB_LITE
    // Sanity check some table properties
    std::map<std::string, std::string> props;
    ASSERT_TRUE(db_->GetMapProperty(
        handles_[1], DB::Properties::kAggregatedTableProperties, &props));
    uint64_t nkeys = N + N / 100;
    uint64_t filter_size = ParseUint64(props["filter_size"]);
    EXPECT_LE(filter_size,
              (partition_filters_ ? 12 : 11) * nkeys / /*bits / byte*/ 8);
    if (bfp_impl_ == kAutoRibbon) {
      // Sometimes using Ribbon filter which is more space-efficient
      EXPECT_GE(filter_size, 7 * nkeys / /*bits / byte*/ 8);
    } else {
      // Always Bloom
      EXPECT_GE(filter_size, 10 * nkeys / /*bits / byte*/ 8);
    }

    uint64_t num_filter_entries = ParseUint64(props["num_filter_entries"]);
    EXPECT_EQ(num_filter_entries, nkeys);
#endif  // ROCKSDB_LITE

    env_->delay_sstable_sync_.store(false, std::memory_order_release);
    Close();
  } while (ChangeCompactOptions());
}

namespace {

class AlwaysTrueBitsBuilder : public FilterBitsBuilder {
 public:
  void AddKey(const Slice&) override {}
  size_t EstimateEntriesAdded() override { return 0U; }
  Slice Finish(std::unique_ptr<const char[]>* /* buf */) override {
    // Interpreted as "always true" filter (0 probes over 1 byte of
    // payload, 5 bytes metadata)
    return Slice("\0\0\0\0\0\0", 6);
  }
  using FilterBitsBuilder::Finish;
  size_t ApproximateNumEntries(size_t) override { return SIZE_MAX; }
};

class AlwaysTrueFilterPolicy : public ReadOnlyBuiltinFilterPolicy {
 public:
  explicit AlwaysTrueFilterPolicy(bool skip) : skip_(skip) {}

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext&) const override {
    if (skip_) {
      return nullptr;
    } else {
      return new AlwaysTrueBitsBuilder();
    }
  }

 private:
  bool skip_;
};

}  // namespace

TEST_P(DBBloomFilterTestWithParam, SkipFilterOnEssentiallyZeroBpk) {
  constexpr int maxKey = 10;
  auto PutFn = [&]() {
    int i;
    // Put
    for (i = 0; i < maxKey; i++) {
      ASSERT_OK(Put(Key(i), Key(i)));
    }
    Flush();
  };
  auto GetFn = [&]() {
    int i;
    // Get OK
    for (i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(Key(i)));
    }
    // Get NotFound
    for (; i < maxKey * 2; i++) {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  };
  auto PutAndGetFn = [&]() {
    PutFn();
    GetFn();
  };
#ifndef ROCKSDB_LITE
  std::map<std::string, std::string> props;
  const auto& kAggTableProps = DB::Properties::kAggregatedTableProperties;
#endif  // ROCKSDB_LITE

  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.partition_filters = partition_filters_;
  if (partition_filters_) {
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  }
  table_options.format_version = format_version_;

  // Test 1: bits per key < 0.5 means skip filters -> no filter
  // constructed or read.
  table_options.filter_policy = Create(0.4, bfp_impl_);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify no filter access nor contruction
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE), 0);
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), 0);

#ifndef ROCKSDB_LITE
  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_EQ(props["filter_size"], "0");
#endif  // ROCKSDB_LITE

  // Test 2: use custom API to skip filters -> no filter constructed
  // or read.
  table_options.filter_policy.reset(
      new AlwaysTrueFilterPolicy(/* skip */ true));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify no filter access nor construction
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE), 0);
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), 0);

#ifndef ROCKSDB_LITE
  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_EQ(props["filter_size"], "0");
#endif  // ROCKSDB_LITE

  // Control test: using an actual filter with 100% FP rate -> the filter
  // is constructed and checked on read.
  table_options.filter_policy.reset(
      new AlwaysTrueFilterPolicy(/* skip */ false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify filter is accessed (and constructed)
  EXPECT_EQ(TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE),
            maxKey * 2);
  EXPECT_EQ(
      TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE),
      maxKey);
#ifndef ROCKSDB_LITE
  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_NE(props["filter_size"], "0");
#endif  // ROCKSDB_LITE

  // Test 3 (options test): Able to read existing filters with longstanding
  // generated options file entry `filter_policy=rocksdb.BuiltinBloomFilter`
  ASSERT_OK(FilterPolicy::CreateFromString(ConfigOptions(),
                                           "rocksdb.BuiltinBloomFilter",
                                           &table_options.filter_policy));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  GetFn();

  // Verify filter is accessed
  EXPECT_EQ(TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE),
            maxKey * 2);
  EXPECT_EQ(
      TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE),
      maxKey);

  // But new filters are not generated (configuration details unknown)
  DestroyAndReopen(options);
  PutAndGetFn();

  // Verify no filter access nor construction
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE), 0);
  EXPECT_EQ(TestGetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE), 0);

#ifndef ROCKSDB_LITE
  props.clear();
  ASSERT_TRUE(db_->GetMapProperty(kAggTableProps, &props));
  EXPECT_EQ(props["filter_size"], "0");
#endif  // ROCKSDB_LITE
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
INSTANTIATE_TEST_CASE_P(
    FormatDef, DBBloomFilterTestDefFormatVersion,
    ::testing::Values(
        std::make_tuple(kDeprecatedBlock, false, test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom, true, test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom, false, test::kDefaultFormatVersion),
        std::make_tuple(kAutoRibbon, false, test::kDefaultFormatVersion)));

INSTANTIATE_TEST_CASE_P(
    FormatDef, DBBloomFilterTestWithParam,
    ::testing::Values(
        std::make_tuple(kDeprecatedBlock, false, test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom, true, test::kDefaultFormatVersion),
        std::make_tuple(kAutoBloom, false, test::kDefaultFormatVersion),
        std::make_tuple(kAutoRibbon, false, test::kDefaultFormatVersion)));

INSTANTIATE_TEST_CASE_P(
    FormatLatest, DBBloomFilterTestWithParam,
    ::testing::Values(
        std::make_tuple(kDeprecatedBlock, false, kLatestFormatVersion),
        std::make_tuple(kAutoBloom, true, kLatestFormatVersion),
        std::make_tuple(kAutoBloom, false, kLatestFormatVersion),
        std::make_tuple(kAutoRibbon, false, kLatestFormatVersion)));
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_F(DBBloomFilterTest, BloomFilterRate) {
  while (ChangeFilterOptions()) {
    Options options = CurrentOptions();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    get_perf_context()->EnablePerLevelPerfContext();
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
    ASSERT_GE(
        (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful,
        maxKey * 0.98);
    get_perf_context()->Reset();
  }
}

TEST_F(DBBloomFilterTest, BloomFilterCompatibility) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
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

  // Check db with partitioned full filter
  table_options.partition_filters = true;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
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
  for (bool partition_filters : {true, false}) {
    Options options = CurrentOptions();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    if (partition_filters) {
      table_options.partition_filters = true;
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

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
}

/*
 * A cache wrapper that tracks peaks and increments of filter
 * construction cache reservation.
 *        p0
 *       / \   p1
 *      /   \  /\
 *     /     \/  \
 *  a /       b   \
 * peaks = {p0, p1}
 * increments = {p1-a, p2-b}
 */
class FilterConstructResPeakTrackingCache : public CacheWrapper {
 public:
  explicit FilterConstructResPeakTrackingCache(std::shared_ptr<Cache> target)
      : CacheWrapper(std::move(target)),
        cur_cache_res_(0),
        cache_res_peak_(0),
        cache_res_increment_(0),
        last_peak_tracked_(false),
        cache_res_increments_sum_(0) {}

  using Cache::Insert;
  Status Insert(const Slice& key, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value),
                Handle** handle = nullptr,
                Priority priority = Priority::LOW) override {
    Status s = target_->Insert(key, value, charge, deleter, handle, priority);
    if (deleter == kNoopDeleterForFilterConstruction) {
      if (last_peak_tracked_) {
        cache_res_peak_ = 0;
        cache_res_increment_ = 0;
        last_peak_tracked_ = false;
      }
      cur_cache_res_ += charge;
      cache_res_peak_ = std::max(cache_res_peak_, cur_cache_res_);
      cache_res_increment_ += charge;
    }
    return s;
  }

  using Cache::Release;
  bool Release(Handle* handle, bool force_erase = false) override {
    auto deleter = GetDeleter(handle);
    if (deleter == kNoopDeleterForFilterConstruction) {
      if (!last_peak_tracked_) {
        cache_res_peaks_.push_back(cache_res_peak_);
        cache_res_increments_sum_ += cache_res_increment_;
        last_peak_tracked_ = true;
      }
      cur_cache_res_ -= GetCharge(handle);
    }
    bool is_successful = target_->Release(handle, force_erase);
    return is_successful;
  }

  std::deque<std::size_t> GetReservedCachePeaks() { return cache_res_peaks_; }

  std::size_t GetReservedCacheIncrementSum() {
    return cache_res_increments_sum_;
  }

 private:
  static const Cache::DeleterFn kNoopDeleterForFilterConstruction;

  std::size_t cur_cache_res_;
  std::size_t cache_res_peak_;
  std::size_t cache_res_increment_;
  bool last_peak_tracked_;
  std::deque<std::size_t> cache_res_peaks_;
  std::size_t cache_res_increments_sum_;
};

const Cache::DeleterFn
    FilterConstructResPeakTrackingCache::kNoopDeleterForFilterConstruction =
        CacheReservationManager::TEST_GetNoopDeleterForRole<
            CacheEntryRole::kFilterConstruction>();

// To align with the type of hash entry being reserved in implementation.
using FilterConstructionReserveMemoryHash = uint64_t;

class DBFilterConstructionReserveMemoryTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<bool, std::string, bool, bool>> {
 public:
  DBFilterConstructionReserveMemoryTestWithParam()
      : DBTestBase("db_bloom_filter_tests",
                   /*env_do_fsync=*/true),
        num_key_(0),
        reserve_table_builder_memory_(std::get<0>(GetParam())),
        policy_(std::get<1>(GetParam())),
        partition_filters_(std::get<2>(GetParam())),
        detect_filter_construct_corruption_(std::get<3>(GetParam())) {
    if (!reserve_table_builder_memory_ || policy_ == kDeprecatedBlock ||
        policy_ == kLegacyBloom) {
      // For these cases, we only interested in whether filter construction
      // cache resevation happens instead of its accuracy. Therefore we don't
      // need many keys.
      num_key_ = 5;
    } else if (partition_filters_) {
      // For PartitionFilter case, since we set
      // table_options.metadata_block_size big enough such that each partition
      // trigger at least 1 dummy entry reservation each for hash entries and
      // final filter, we need a large number of keys to ensure we have at least
      // two partitions.
      num_key_ = 18 * CacheReservationManager::GetDummyEntrySize() /
                 sizeof(FilterConstructionReserveMemoryHash);
    } else if (policy_ == kFastLocalBloom) {
      // For Bloom Filter + FullFilter case, since we design the num_key_ to
      // make hash entry cache reservation be a multiple of dummy entries, the
      // correct behavior of charging final filter on top of it will trigger at
      // least another dummy entry insertion. Therefore we can assert that
      // behavior and we don't need a large number of keys to verify we
      // indeed charge the final filter for cache reservation, even though final
      // filter is a lot smaller than hash entries.
      num_key_ = 1 * CacheReservationManager::GetDummyEntrySize() /
                 sizeof(FilterConstructionReserveMemoryHash);
    } else {
      // For Ribbon Filter + FullFilter case, we need a large enough number of
      // keys so that charging final filter after releasing the hash entries
      // reservation will trigger at least another dummy entry (or equivalently
      // to saying, causing another peak in cache reservation) as banding
      // reservation might not be a multiple of dummy entry.
      num_key_ = 12 * CacheReservationManager::GetDummyEntrySize() /
                 sizeof(FilterConstructionReserveMemoryHash);
    }
  }

  BlockBasedTableOptions GetBlockBasedTableOptions() {
    BlockBasedTableOptions table_options;

    // We set cache capacity big enough to prevent cache full for convenience in
    // calculation.
    constexpr std::size_t kCacheCapacity = 100 * 1024 * 1024;

    table_options.reserve_table_builder_memory = reserve_table_builder_memory_;
    table_options.filter_policy = Create(10, policy_);
    table_options.partition_filters = partition_filters_;
    if (table_options.partition_filters) {
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      // We set table_options.metadata_block_size big enough so that each
      // partition trigger at least 1 dummy entry insertion each for hash
      // entries and final filter.
      table_options.metadata_block_size = 409000;
    }
    table_options.detect_filter_construct_corruption =
        detect_filter_construct_corruption_;

    LRUCacheOptions lo;
    lo.capacity = kCacheCapacity;
    lo.num_shard_bits = 0;  // 2^0 shard
    lo.strict_capacity_limit = true;
    cache_ = std::make_shared<FilterConstructResPeakTrackingCache>(
        (NewLRUCache(lo)));
    table_options.block_cache = cache_;

    return table_options;
  }

  std::size_t GetNumKey() { return num_key_; }

  bool ReserveTableBuilderMemory() { return reserve_table_builder_memory_; }

  std::string GetFilterPolicy() { return policy_; }

  bool PartitionFilters() { return partition_filters_; }

  std::shared_ptr<FilterConstructResPeakTrackingCache>
  GetFilterConstructResPeakTrackingCache() {
    return cache_;
  }

 private:
  std::size_t num_key_;
  bool reserve_table_builder_memory_;
  std::string policy_;
  bool partition_filters_;
  std::shared_ptr<FilterConstructResPeakTrackingCache> cache_;
  bool detect_filter_construct_corruption_;
};

INSTANTIATE_TEST_CASE_P(
    BlockBasedTableOptions, DBFilterConstructionReserveMemoryTestWithParam,
    ::testing::Values(std::make_tuple(false, kFastLocalBloom, false, false),

                      std::make_tuple(true, kFastLocalBloom, false, false),
                      std::make_tuple(true, kFastLocalBloom, false, true),
                      std::make_tuple(true, kFastLocalBloom, true, false),
                      std::make_tuple(true, kFastLocalBloom, true, true),

                      std::make_tuple(true, kStandard128Ribbon, false, false),
                      std::make_tuple(true, kStandard128Ribbon, false, true),
                      std::make_tuple(true, kStandard128Ribbon, true, false),
                      std::make_tuple(true, kStandard128Ribbon, true, true),

                      std::make_tuple(true, kDeprecatedBlock, false, false),
                      std::make_tuple(true, kLegacyBloom, false, false)));

// TODO: Speed up this test.
// The current test inserts many keys (on the scale of dummy entry size)
// in order to make small memory user (e.g, final filter, partitioned hash
// entries/filter/banding) , which is proportional to the number of
// keys, big enough so that its cache reservation triggers dummy entry insertion
// and becomes observable in the test.
//
// However, inserting that many keys slows down this test and leaves future
// developers an opportunity to speed it up.
//
// Possible approaches & challenges:
// 1. Use sync point during cache reservation of filter construction
//
// Benefit: It does not rely on triggering dummy entry insertion
// but the sync point to verify small memory user is charged correctly.
//
// Challenge: this approach is intrusive.
//
// 2. Make dummy entry size configurable and set it small in the test
//
// Benefit: It increases the precision of cache reservation and therefore
// small memory usage can still trigger insertion of dummy entry.
//
// Challenge: change CacheReservationManager related APIs and a hack
// might be needed to control the size of dummmy entry of
// CacheReservationManager used in filter construction for testing
// since CacheReservationManager is not exposed at the high level.
//
TEST_P(DBFilterConstructionReserveMemoryTestWithParam, ReserveMemory) {
  Options options = CurrentOptions();
  // We set write_buffer_size big enough so that in the case where there is
  // filter construction cache reservation, flush won't be triggered before we
  // manually trigger it for clean testing
  options.write_buffer_size = 640 << 20;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::shared_ptr<FilterConstructResPeakTrackingCache> cache =
      GetFilterConstructResPeakTrackingCache();
  options.create_if_missing = true;
  // Disable auto compaction to prevent its unexpected side effect
  // to the number of keys per partition designed by us in the test
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  int num_key = static_cast<int>(GetNumKey());
  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  ASSERT_EQ(cache->GetReservedCacheIncrementSum(), 0)
      << "Flush was triggered too early in the test case with filter "
         "construction cache reservation - please make sure no flush triggered "
         "during the key insertions above";

  ASSERT_OK(Flush());

  bool reserve_table_builder_memory = ReserveTableBuilderMemory();
  std::string policy = GetFilterPolicy();
  bool partition_filters = PartitionFilters();
  bool detect_filter_construct_corruption =
      table_options.detect_filter_construct_corruption;

  std::deque<std::size_t> filter_construction_cache_res_peaks =
      cache->GetReservedCachePeaks();
  std::size_t filter_construction_cache_res_increments_sum =
      cache->GetReservedCacheIncrementSum();

  if (!reserve_table_builder_memory) {
    EXPECT_EQ(filter_construction_cache_res_peaks.size(), 0);
    return;
  }

  if (policy == kDeprecatedBlock || policy == kLegacyBloom) {
    EXPECT_EQ(filter_construction_cache_res_peaks.size(), 0)
        << "There shouldn't be filter construction cache reservation as this "
           "feature does not support kDeprecatedBlock "
           "nor kLegacyBloom";
    return;
  }

  const std::size_t kDummyEntrySize =
      CacheReservationManager::GetDummyEntrySize();

  const std::size_t predicted_hash_entries_cache_res =
      num_key * sizeof(FilterConstructionReserveMemoryHash);
  ASSERT_EQ(predicted_hash_entries_cache_res % kDummyEntrySize, 0)
      << "It's by this test's design that predicted_hash_entries_cache_res is "
         "a multipe of dummy entry";

  const std::size_t predicted_hash_entries_cache_res_dummy_entry_num =
      predicted_hash_entries_cache_res / kDummyEntrySize;
  const std::size_t predicted_final_filter_cache_res =
      static_cast<std::size_t>(
          std::ceil(1.0 * predicted_hash_entries_cache_res_dummy_entry_num / 6 *
                    (policy == kStandard128Ribbon ? 0.7 : 1))) *
      kDummyEntrySize;
  const std::size_t predicted_banding_cache_res =
      static_cast<std::size_t>(
          std::ceil(predicted_hash_entries_cache_res_dummy_entry_num * 2.5)) *
      kDummyEntrySize;

  if (policy == kFastLocalBloom) {
    /* kFastLocalBloom + FullFilter
     *        p0
     *       /  \
     *    b /    \
     *     /      \
     *    /        \
     *  0/          \
     *  hash entries = b - 0, final filter = p0 - b
     *  p0 = hash entries + final filter
     *
     *  The test is designed in a way such that the reservation for b is a
     *  multiple of dummy entries so that reservation for (p0 - b)
     *  will trigger at least another dummy entry insertion.
     *
     * kFastLocalBloom + FullFilter +
     * detect_filter_construct_corruption
     *  The peak p0 stays the same as
     *  (kFastLocalBloom + FullFilter) but just lasts
     *  longer since we release hash entries reservation later.
     *
     * kFastLocalBloom + PartitionedFilter
     *                   p1
     *                  /  \
     *        p0     b'/    \
     *       /  \     /      \
     *    b /    \   /        \
     *     /      \ /          \
     *    /        a            \
     *  0/                       \
     *  partitioned hash entries1 = b - 0, partitioned hash entries1 = b' - a
     *  parittioned final filter1 = p0 - b, parittioned final filter2 = p1 - b'
     *
     *  (increment p0 - 0) + (increment p1 - a)
     *  = partitioned hash entries1 + partitioned hash entries2
     *  + parittioned final filter1 + parittioned final filter2
     *  = hash entries + final filter
     *
     * kFastLocalBloom + PartitionedFilter +
     * detect_filter_construct_corruption
     *  The peak p0, p1 stay the same as
     *  (kFastLocalBloom + PartitionedFilter) but just
     *  last longer since we release hash entries reservation later.
     *
     */
    if (!partition_filters) {
      EXPECT_EQ(filter_construction_cache_res_peaks.size(), 1)
          << "Filter construction cache reservation should have only 1 peak in "
             "case: kFastLocalBloom + FullFilter";
      std::size_t filter_construction_cache_res_peak =
          filter_construction_cache_res_peaks[0];
      EXPECT_GT(filter_construction_cache_res_peak,
                predicted_hash_entries_cache_res)
          << "The testing number of hash entries is designed to make hash "
             "entries cache reservation be multiples of dummy entries"
             " so the correct behavior of charging final filter on top of it"
             " should've triggered at least another dummy entry insertion";

      std::size_t predicted_filter_construction_cache_res_peak =
          predicted_hash_entries_cache_res + predicted_final_filter_cache_res;
      EXPECT_GE(filter_construction_cache_res_peak,
                predicted_filter_construction_cache_res_peak * 0.9);
      EXPECT_LE(filter_construction_cache_res_peak,
                predicted_filter_construction_cache_res_peak * 1.1);
      return;
    } else {
      EXPECT_GE(filter_construction_cache_res_peaks.size(), 2)
          << "Filter construction cache reservation should have multiple peaks "
             "in case: kFastLocalBloom + "
             "PartitionedFilter";
      std::size_t predicted_filter_construction_cache_res_increments_sum =
          predicted_hash_entries_cache_res + predicted_final_filter_cache_res;
      EXPECT_GE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 0.9);
      EXPECT_LE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 1.1);
      return;
    }
  }

  if (policy == kStandard128Ribbon) {
    /* kStandard128Ribbon + FullFilter
     *        p0
     *       /  \  p1
     *      /    \/\
     *   b /     b' \
     *    /          \
     *  0/            \
     *  hash entries = b - 0, banding = p0 - b, final filter = p1 - b'
     *  p0 = hash entries + banding
     *
     *  The test is designed in a way such that the reservation for (p1 - b')
     *  will trigger at least another dummy entry insertion
     *  (or equivelantly to saying, creating another peak).
     *
     * kStandard128Ribbon + FullFilter +
     * detect_filter_construct_corruption
     *
     *         new p0
     *          /  \
     *         /    \
     *     pre p0    \
     *       /        \
     *      /          \
     *   b /            \
     *    /              \
     *  0/                \
     *  hash entries = b - 0, banding = pre p0 - b,
     *  final filter = new p0 - pre p0
     *  new p0 =  hash entries + banding + final filter
     *
     *  The previous p0 will no longer be a peak since under
     *  detect_filter_construct_corruption == true, we do not release hash
     *  entries reserveration (like p0 - b' previously) until after final filter
     *  creation and post-verification
     *
     * kStandard128Ribbon + PartitionedFilter
     *                     p3
     *        p0           /\  p4
     *       /  \ p1      /  \ /\
     *      /    \/\  b''/    a' \
     *   b /     b' \   /         \
     *    /          \ /           \
     *  0/            a             \
     *  partitioned hash entries1 = b - 0, partitioned hash entries2 = b'' - a
     *  partitioned banding1 = p0 - b, partitioned banding2 = p3 - b''
     *  parittioned final filter1 = p1 - b',parittioned final filter2 = p4 - a'
     *
     *  (increment p0 - 0) + (increment p1 - b')
     *  + (increment p3 - a) + (increment p4 - a')
     *  = partitioned hash entries1 + partitioned hash entries2
     *  + parittioned banding1 + parittioned banding2
     *  + parittioned final filter1 + parittioned final filter2
     *  = hash entries + banding + final filter
     *
     * kStandard128Ribbon + PartitionedFilter +
     * detect_filter_construct_corruption
     *
     *                          new p3
     *                          /    \
     *                        pre p3  \
     *        new p0          /        \
     *         /  \          /          \
     *      pre p0 \        /            \
     *       /      \    b'/              \
     *      /        \    /                \
     *   b /          \  /                  \
     *    /            \a                    \
     *  0/                                    \
     *  partitioned hash entries1 = b - 0, partitioned hash entries2 = b' - a
     *  partitioned banding1 = pre p0 - b, partitioned banding2 = pre p3 - b'
     *  parittioned final filter1 = new p0 - pre p0,
     *  parittioned final filter2 = new p3 - pre p3
     *
     *  The previous p0 and p3 will no longer be a peak since under
     *  detect_filter_construct_corruption == true, we do not release hash
     *  entries reserveration (like p0 - b', p3 - a' previously) until after
     *  parittioned final filter creation and post-verification
     *
     *  However, increments sum stay the same as shown below:
     *    (increment new p0 - 0) + (increment new p3 - a)
     *  = partitioned hash entries1 + partitioned hash entries2
     *  + parittioned banding1 + parittioned banding2
     *  + parittioned final filter1 + parittioned final filter2
     *  = hash entries + banding + final filter
     *
     */
    if (!partition_filters) {
      ASSERT_GE(std::floor(1.0 * predicted_final_filter_cache_res /
                           CacheReservationManager::GetDummyEntrySize()),
                1)
          << "Final filter cache reservation too small for this test - please "
             "increase the number of keys";
      if (!detect_filter_construct_corruption) {
        EXPECT_EQ(filter_construction_cache_res_peaks.size(), 2)
            << "Filter construction cache reservation should have 2 peaks in "
               "case: kStandard128Ribbon + "
               "FullFilter. "
               "The second peak is resulted from charging the final filter "
               "after "
               "decreasing the hash entry reservation since the testing final "
               "filter reservation is designed to be at least 1 dummy entry "
               "size";

        std::size_t filter_construction_cache_res_peak =
            filter_construction_cache_res_peaks[0];
        std::size_t predicted_filter_construction_cache_res_peak =
            predicted_hash_entries_cache_res + predicted_banding_cache_res;
        EXPECT_GE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 0.9);
        EXPECT_LE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 1.1);
      } else {
        EXPECT_EQ(filter_construction_cache_res_peaks.size(), 1)
            << "Filter construction cache reservation should have 1 peaks in "
               "case: kStandard128Ribbon + FullFilter "
               "+ detect_filter_construct_corruption. "
               "The previous second peak now disappears since we don't "
               "decrease the hash entry reservation"
               "until after final filter reservation and post-verification";

        std::size_t filter_construction_cache_res_peak =
            filter_construction_cache_res_peaks[0];
        std::size_t predicted_filter_construction_cache_res_peak =
            predicted_hash_entries_cache_res + predicted_banding_cache_res +
            predicted_final_filter_cache_res;
        EXPECT_GE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 0.9);
        EXPECT_LE(filter_construction_cache_res_peak,
                  predicted_filter_construction_cache_res_peak * 1.1);
      }
      return;
    } else {
      if (!detect_filter_construct_corruption) {
        EXPECT_GE(filter_construction_cache_res_peaks.size(), 3)
            << "Filter construction cache reservation should have more than 3 "
               "peaks "
               "in case: kStandard128Ribbon + "
               "PartitionedFilter";
      } else {
        EXPECT_GE(filter_construction_cache_res_peaks.size(), 2)
            << "Filter construction cache reservation should have more than 2 "
               "peaks "
               "in case: kStandard128Ribbon + "
               "PartitionedFilter + detect_filter_construct_corruption";
      }
      std::size_t predicted_filter_construction_cache_res_increments_sum =
          predicted_hash_entries_cache_res + predicted_banding_cache_res +
          predicted_final_filter_cache_res;
      EXPECT_GE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 0.9);
      EXPECT_LE(filter_construction_cache_res_increments_sum,
                predicted_filter_construction_cache_res_increments_sum * 1.1);
      return;
    }
  }
}

class DBFilterConstructionCorruptionTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<bool /* detect_filter_construct_corruption */, std::string,
                     bool /* partition_filters */>> {
 public:
  DBFilterConstructionCorruptionTestWithParam()
      : DBTestBase("db_bloom_filter_tests",
                   /*env_do_fsync=*/true) {}

  BlockBasedTableOptions GetBlockBasedTableOptions() {
    BlockBasedTableOptions table_options;
    table_options.detect_filter_construct_corruption = std::get<0>(GetParam());
    table_options.filter_policy = Create(10, std::get<1>(GetParam()));
    table_options.partition_filters = std::get<2>(GetParam());
    if (table_options.partition_filters) {
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      // We set table_options.metadata_block_size small enough so we can
      // trigger filter partitioning with GetNumKey() amount of keys
      table_options.metadata_block_size = 10;
    }

    return table_options;
  }

  // Return an appropriate amount of keys for testing
  // to generate a long filter (i.e, size >= 8 + kMetadataLen)
  std::size_t GetNumKey() { return 5000; }
};

INSTANTIATE_TEST_CASE_P(
    DBFilterConstructionCorruptionTestWithParam,
    DBFilterConstructionCorruptionTestWithParam,
    ::testing::Values(std::make_tuple(false, kFastLocalBloom, false),
                      std::make_tuple(true, kFastLocalBloom, false),
                      std::make_tuple(true, kFastLocalBloom, true),
                      std::make_tuple(true, kStandard128Ribbon, false),
                      std::make_tuple(true, kStandard128Ribbon, true)));

TEST_P(DBFilterConstructionCorruptionTestWithParam, DetectCorruption) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  int num_key = static_cast<int>(GetNumKey());
  Status s;

  // Case 1: No corruption in filter construction
  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }
  s = Flush();
  EXPECT_TRUE(s.ok());

  // Case 2: Corruption of hash entries in filter construction
  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  SyncPoint::GetInstance()->SetCallBack(
      "XXPH3FilterBitsBuilder::Finish::TamperHashEntries", [&](void* arg) {
        std::deque<uint64_t>* hash_entries_to_corrupt =
            (std::deque<uint64_t>*)arg;
        assert(!hash_entries_to_corrupt->empty());
        *(hash_entries_to_corrupt->begin()) =
            *(hash_entries_to_corrupt->begin()) ^ uint64_t { 1 };
      });
  SyncPoint::GetInstance()->EnableProcessing();

  s = Flush();

  if (table_options.detect_filter_construct_corruption) {
    EXPECT_TRUE(s.IsCorruption());
    EXPECT_TRUE(
        s.ToString().find("Filter's hash entries checksum mismatched") !=
        std::string::npos);
  } else {
    EXPECT_TRUE(s.ok());
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearCallBack(
      "XXPH3FilterBitsBuilder::Finish::"
      "TamperHashEntries");

  // Case 3: Corruption of filter content in filter construction
  DestroyAndReopen(options);

  for (int i = 0; i < num_key; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
  }

  SyncPoint::GetInstance()->SetCallBack(
      "XXPH3FilterBitsBuilder::Finish::TamperFilter", [&](void* arg) {
        std::pair<std::unique_ptr<char[]>*, std::size_t>* TEST_arg_pair =
            (std::pair<std::unique_ptr<char[]>*, std::size_t>*)arg;
        std::size_t filter_size = TEST_arg_pair->second;
        // 5 is the kMetadataLen and
        assert(filter_size >= 8 + 5);
        std::unique_ptr<char[]>* filter_content_to_corrupt =
            TEST_arg_pair->first;
        std::memset(filter_content_to_corrupt->get(), '\0', 8);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  s = Flush();

  if (table_options.detect_filter_construct_corruption) {
    EXPECT_TRUE(s.IsCorruption());
    EXPECT_TRUE(s.ToString().find("Corrupted filter content") !=
                std::string::npos);
  } else {
    EXPECT_TRUE(s.ok());
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearCallBack(
      "XXPH3FilterBitsBuilder::Finish::"
      "TamperFilter");
}

namespace {
// NOTE: This class is referenced by HISTORY.md as a model for a wrapper
// FilterPolicy selecting among configurations based on context.
class LevelAndStyleCustomFilterPolicy : public FilterPolicy {
 public:
  explicit LevelAndStyleCustomFilterPolicy(int bpk_fifo, int bpk_l0_other,
                                           int bpk_otherwise)
      : policy_fifo_(NewBloomFilterPolicy(bpk_fifo)),
        policy_l0_other_(NewBloomFilterPolicy(bpk_l0_other)),
        policy_otherwise_(NewBloomFilterPolicy(bpk_otherwise)) {}

  const char* Name() const override {
    return "LevelAndStyleCustomFilterPolicy";
  }

  // OK to use built-in policy name because we are deferring to a
  // built-in builder. We aren't changing the serialized format.
  const char* CompatibilityName() const override {
    return policy_fifo_->CompatibilityName();
  }

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext& context) const override {
    if (context.compaction_style == kCompactionStyleFIFO) {
      return policy_fifo_->GetBuilderWithContext(context);
    } else if (context.level_at_creation == 0) {
      return policy_l0_other_->GetBuilderWithContext(context);
    } else {
      return policy_otherwise_->GetBuilderWithContext(context);
    }
  }

  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
    // OK to defer to any of them; they all can parse built-in filters
    // from any settings.
    return policy_fifo_->GetFilterBitsReader(contents);
  }

 private:
  const std::unique_ptr<const FilterPolicy> policy_fifo_;
  const std::unique_ptr<const FilterPolicy> policy_l0_other_;
  const std::unique_ptr<const FilterPolicy> policy_otherwise_;
};

static std::map<TableFileCreationReason, std::string>
    table_file_creation_reason_to_string{
        {TableFileCreationReason::kCompaction, "kCompaction"},
        {TableFileCreationReason::kFlush, "kFlush"},
        {TableFileCreationReason::kMisc, "kMisc"},
        {TableFileCreationReason::kRecovery, "kRecovery"},
    };

class TestingContextCustomFilterPolicy
    : public LevelAndStyleCustomFilterPolicy {
 public:
  explicit TestingContextCustomFilterPolicy(int bpk_fifo, int bpk_l0_other,
                                            int bpk_otherwise)
      : LevelAndStyleCustomFilterPolicy(bpk_fifo, bpk_l0_other, bpk_otherwise) {
  }

  FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext& context) const override {
    test_report_ += "cf=";
    test_report_ += context.column_family_name;
    test_report_ += ",s=";
    test_report_ +=
        OptionsHelper::compaction_style_to_string[context.compaction_style];
    test_report_ += ",n=";
    test_report_ += ROCKSDB_NAMESPACE::ToString(context.num_levels);
    test_report_ += ",l=";
    test_report_ += ROCKSDB_NAMESPACE::ToString(context.level_at_creation);
    test_report_ += ",b=";
    test_report_ += ROCKSDB_NAMESPACE::ToString(int{context.is_bottommost});
    test_report_ += ",r=";
    test_report_ += table_file_creation_reason_to_string[context.reason];
    test_report_ += "\n";

    return LevelAndStyleCustomFilterPolicy::GetBuilderWithContext(context);
  }

  std::string DumpTestReport() {
    std::string rv;
    std::swap(rv, test_report_);
    return rv;
  }

 private:
  mutable std::string test_report_;
};
}  // namespace

TEST_F(DBBloomFilterTest, ContextCustomFilterPolicy) {
  auto policy = std::make_shared<TestingContextCustomFilterPolicy>(15, 8, 5);
  Options options;
  for (bool fifo : {true, false}) {
    options = CurrentOptions();
    options.max_open_files = fifo ? -1 : options.max_open_files;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.compaction_style =
        fifo ? kCompactionStyleFIFO : kCompactionStyleLevel;

    BlockBasedTableOptions table_options;
    table_options.filter_policy = policy;
    table_options.format_version = 5;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    TryReopen(options);
    CreateAndReopenWithCF({fifo ? "abe" : "bob"}, options);

    const int maxKey = 10000;
    for (int i = 0; i < maxKey / 2; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    // Add a large key to make the file contain wide range
    ASSERT_OK(Put(1, Key(maxKey + 55555), Key(maxKey + 55555)));
    Flush(1);
    EXPECT_EQ(policy->DumpTestReport(),
              fifo ? "cf=abe,s=kCompactionStyleFIFO,n=1,l=0,b=0,r=kFlush\n"
                   : "cf=bob,s=kCompactionStyleLevel,n=7,l=0,b=0,r=kFlush\n");

    for (int i = maxKey / 2; i < maxKey; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i)));
    }
    Flush(1);
    EXPECT_EQ(policy->DumpTestReport(),
              fifo ? "cf=abe,s=kCompactionStyleFIFO,n=1,l=0,b=0,r=kFlush\n"
                   : "cf=bob,s=kCompactionStyleLevel,n=7,l=0,b=0,r=kFlush\n");

    // Check that they can be found
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ(Key(i), Get(1, Key(i)));
    }
    // Since we have two tables / two filters, we might have Bloom checks on
    // our queries, but no more than one "useful" per query on a found key.
    EXPECT_LE(TestGetAndResetTickerCount(options, BLOOM_FILTER_USEFUL), maxKey);

    // Check that we have two filters, each about
    // fifo: 0.12% FP rate (15 bits per key)
    // level: 2.3% FP rate (8 bits per key)
    for (int i = 0; i < maxKey; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
    }
    {
      auto useful_count =
          TestGetAndResetTickerCount(options, BLOOM_FILTER_USEFUL);
      EXPECT_GE(useful_count, maxKey * 2 * (fifo ? 0.9980 : 0.975));
      EXPECT_LE(useful_count, maxKey * 2 * (fifo ? 0.9995 : 0.98));
    }

    if (!fifo) {  // FIFO only has L0
      // Full compaction
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                  nullptr));
      EXPECT_EQ(policy->DumpTestReport(),
                "cf=bob,s=kCompactionStyleLevel,n=7,l=1,b=1,r=kCompaction\n");

      // Check that we now have one filter, about 9.2% FP rate (5 bits per key)
      for (int i = 0; i < maxKey; i++) {
        ASSERT_EQ("NOT_FOUND", Get(1, Key(i + 33333)));
      }
      {
        auto useful_count =
            TestGetAndResetTickerCount(options, BLOOM_FILTER_USEFUL);
        EXPECT_GE(useful_count, maxKey * 0.90);
        EXPECT_LE(useful_count, maxKey * 0.91);
      }
    } else {
#ifndef ROCKSDB_LITE
      // Also try external SST file
      {
        std::string file_path = dbname_ + "/external.sst";
        SstFileWriter sst_file_writer(EnvOptions(), options, handles_[1]);
        ASSERT_OK(sst_file_writer.Open(file_path));
        ASSERT_OK(sst_file_writer.Put("key", "value"));
        ASSERT_OK(sst_file_writer.Finish());
      }
      // Note: kCompactionStyleLevel is default, ignored if num_levels == -1
      EXPECT_EQ(policy->DumpTestReport(),
                "cf=abe,s=kCompactionStyleLevel,n=-1,l=-1,b=0,r=kMisc\n");
#endif
    }

    // Destroy
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
    handles_[1] = nullptr;
  }
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
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10, false));
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
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10, true));

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

TEST_F(DBBloomFilterTest, MemtableWholeKeyBloomFilter) {
  // regression test for #2743. the range delete tombstones in memtable should
  // be added even when Get() skips searching due to its prefix bloom filter
  const int kMemtableSize = 1 << 20;              // 1MB
  const int kMemtablePrefixFilterSize = 1 << 13;  // 8KB
  const int kPrefixLen = 4;
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio =
      static_cast<double>(kMemtablePrefixFilterSize) / kMemtableSize;
  options.prefix_extractor.reset(
      ROCKSDB_NAMESPACE::NewFixedPrefixTransform(kPrefixLen));
  options.write_buffer_size = kMemtableSize;
  options.memtable_whole_key_filtering = false;
  Reopen(options);
  std::string key1("AAAABBBB");
  std::string key2("AAAACCCC");  // not in DB
  std::string key3("AAAADDDD");
  std::string key4("AAAAEEEE");
  std::string value1("Value1");
  std::string value3("Value3");
  std::string value4("Value4");

  ASSERT_OK(Put(key1, value1, WriteOptions()));

  // check memtable bloom stats
  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);
  // same prefix, bloom filter false positive
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);

  // enable whole key bloom filter
  options.memtable_whole_key_filtering = true;
  Reopen(options);
  // check memtable bloom stats
  ASSERT_OK(Put(key3, value3, WriteOptions()));
  ASSERT_EQ("NOT_FOUND", Get(key2));
  // whole key bloom filter kicks in and determines it's a miss
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);

  // verify whole key filtering does not depend on prefix_extractor
  options.prefix_extractor.reset();
  Reopen(options);
  // check memtable bloom stats
  ASSERT_OK(Put(key4, value4, WriteOptions()));
  ASSERT_EQ("NOT_FOUND", Get(key2));
  // whole key bloom filter kicks in and determines it's a miss
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
}

TEST_F(DBBloomFilterTest, MemtableWholeKeyBloomFilterMultiGet) {
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio = 0.015;
  options.memtable_whole_key_filtering = true;
  Reopen(options);
  std::string key1("AA");
  std::string key2("BB");
  std::string key3("CC");
  std::string key4("DD");
  std::string key_not("EE");
  std::string value1("Value1");
  std::string value2("Value2");
  std::string value3("Value3");
  std::string value4("Value4");

  ASSERT_OK(Put(key1, value1, WriteOptions()));
  ASSERT_OK(Put(key2, value2, WriteOptions()));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(key3, value3, WriteOptions()));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(key4, value4, WriteOptions()));

  // Delete key2 and key3
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "BA", "CZ"));

  // Read without snapshot
  auto results = MultiGet({key_not, key1, key2, key3, key4});
  ASSERT_EQ(results[0], "NOT_FOUND");
  ASSERT_EQ(results[1], value1);
  ASSERT_EQ(results[2], "NOT_FOUND");
  ASSERT_EQ(results[3], "NOT_FOUND");
  ASSERT_EQ(results[4], value4);

  // Also check Get
  ASSERT_EQ(Get(key1), value1);
  ASSERT_EQ(Get(key2), "NOT_FOUND");
  ASSERT_EQ(Get(key3), "NOT_FOUND");
  ASSERT_EQ(Get(key4), value4);

  // Read with snapshot
  results = MultiGet({key_not, key1, key2, key3, key4}, snapshot);
  ASSERT_EQ(results[0], "NOT_FOUND");
  ASSERT_EQ(results[1], value1);
  ASSERT_EQ(results[2], value2);
  ASSERT_EQ(results[3], value3);
  ASSERT_EQ(results[4], "NOT_FOUND");

  // Also check Get
  ASSERT_EQ(Get(key1, snapshot), value1);
  ASSERT_EQ(Get(key2, snapshot), value2);
  ASSERT_EQ(Get(key3, snapshot), value3);
  ASSERT_EQ(Get(key4, snapshot), "NOT_FOUND");

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBloomFilterTest, MemtablePrefixBloomOutOfDomain) {
  constexpr size_t kPrefixSize = 8;
  const std::string kKey = "key";
  assert(kKey.size() < kPrefixSize);
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(kPrefixSize));
  options.memtable_prefix_bloom_size_ratio = 0.25;
  Reopen(options);
  ASSERT_OK(Put(kKey, "v"));
  ASSERT_EQ("v", Get(kKey));
  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));
  iter->Seek(kKey);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kKey, iter->key());
  iter->SeekForPrev(kKey);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kKey, iter->key());
}

class DBBloomFilterTestVaryPrefixAndFormatVer
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, uint32_t>> {
 protected:
  bool use_prefix_;
  uint32_t format_version_;

 public:
  DBBloomFilterTestVaryPrefixAndFormatVer()
      : DBTestBase("db_bloom_filter_tests", /*env_do_fsync=*/true) {}

  ~DBBloomFilterTestVaryPrefixAndFormatVer() override {}

  void SetUp() override {
    use_prefix_ = std::get<0>(GetParam());
    format_version_ = std::get<1>(GetParam());
  }

  static std::string UKey(uint32_t i) { return Key(static_cast<int>(i)); }
};

TEST_P(DBBloomFilterTestVaryPrefixAndFormatVer, PartitionedMultiGet) {
  Options options = CurrentOptions();
  if (use_prefix_) {
    // Entire key from UKey()
    options.prefix_extractor.reset(NewCappedPrefixTransform(9));
  }
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(20));
  bbto.partition_filters = true;
  bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  bbto.whole_key_filtering = !use_prefix_;
  if (use_prefix_) {  // (not related to prefix, just alternating between)
    // Make sure code appropriately deals with metadata block size setting
    // that is "too small" (smaller than minimum size for filter builder)
    bbto.metadata_block_size = 63;
  } else {
    // Make sure the test will work even on platforms with large minimum
    // filter size, due to large cache line size.
    // (Largest cache line size + 10+% overhead.)
    bbto.metadata_block_size = 290;
  }
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);
  ReadOptions ropts;

  constexpr uint32_t N = 12000;
  // Add N/2 evens
  for (uint32_t i = 0; i < N; i += 2) {
    ASSERT_OK(Put(UKey(i), UKey(i)));
  }
  ASSERT_OK(Flush());
#ifndef ROCKSDB_LITE
  ASSERT_EQ(TotalTableFiles(), 1);
#endif

  constexpr uint32_t Q = 29;
  // MultiGet In
  std::array<std::string, Q> keys;
  std::array<Slice, Q> key_slices;
  std::array<ColumnFamilyHandle*, Q> column_families;
  // MultiGet Out
  std::array<Status, Q> statuses;
  std::array<PinnableSlice, Q> values;

  TestGetAndResetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  TestGetAndResetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  TestGetAndResetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL);
  TestGetAndResetTickerCount(options, BLOOM_FILTER_USEFUL);
  TestGetAndResetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED);
  TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_POSITIVE);
  TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE);

  // Check that initial clump of keys only loads one partition filter from
  // block cache.
  // And that spread out keys load many partition filters.
  // In both cases, mix present vs. not present keys.
  for (uint32_t stride : {uint32_t{1}, (N / Q) | 1}) {
    for (uint32_t i = 0; i < Q; ++i) {
      keys[i] = UKey(i * stride);
      key_slices[i] = Slice(keys[i]);
      column_families[i] = db_->DefaultColumnFamily();
      statuses[i] = Status();
      values[i] = PinnableSlice();
    }

    db_->MultiGet(ropts, Q, &column_families[0], &key_slices[0], &values[0],
                  /*timestamps=*/nullptr, &statuses[0], true);

    // Confirm correct status results
    uint32_t number_not_found = 0;
    for (uint32_t i = 0; i < Q; ++i) {
      if ((i * stride % 2) == 0) {
        ASSERT_OK(statuses[i]);
      } else {
        ASSERT_TRUE(statuses[i].IsNotFound());
        ++number_not_found;
      }
    }

    // Confirm correct Bloom stats (no FPs)
    uint64_t filter_useful = TestGetAndResetTickerCount(
        options,
        use_prefix_ ? BLOOM_FILTER_PREFIX_USEFUL : BLOOM_FILTER_USEFUL);
    uint64_t filter_checked =
        TestGetAndResetTickerCount(options, use_prefix_
                                                ? BLOOM_FILTER_PREFIX_CHECKED
                                                : BLOOM_FILTER_FULL_POSITIVE) +
        (use_prefix_ ? 0 : filter_useful);
    EXPECT_EQ(filter_useful, number_not_found);
    EXPECT_EQ(filter_checked, Q);
    if (!use_prefix_) {
      EXPECT_EQ(
          TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE),
          Q - number_not_found);
    }

    // Confirm no duplicate loading same filter partition
    uint64_t filter_accesses =
        TestGetAndResetTickerCount(options, BLOCK_CACHE_FILTER_HIT) +
        TestGetAndResetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
    if (stride == 1) {
      EXPECT_EQ(filter_accesses, 1);
    } else {
      // for large stride
      EXPECT_GE(filter_accesses, Q / 2 + 1);
    }
  }

  // Check that a clump of keys (present and not) works when spanning
  // two partitions
  int found_spanning = 0;
  for (uint32_t start = 0; start < N / 2;) {
    for (uint32_t i = 0; i < Q; ++i) {
      keys[i] = UKey(start + i);
      key_slices[i] = Slice(keys[i]);
      column_families[i] = db_->DefaultColumnFamily();
      statuses[i] = Status();
      values[i] = PinnableSlice();
    }

    db_->MultiGet(ropts, Q, &column_families[0], &key_slices[0], &values[0],
                  /*timestamps=*/nullptr, &statuses[0], true);

    // Confirm correct status results
    uint32_t number_not_found = 0;
    for (uint32_t i = 0; i < Q; ++i) {
      if (((start + i) % 2) == 0) {
        ASSERT_OK(statuses[i]);
      } else {
        ASSERT_TRUE(statuses[i].IsNotFound());
        ++number_not_found;
      }
    }

    // Confirm correct Bloom stats (might see some FPs)
    uint64_t filter_useful = TestGetAndResetTickerCount(
        options,
        use_prefix_ ? BLOOM_FILTER_PREFIX_USEFUL : BLOOM_FILTER_USEFUL);
    uint64_t filter_checked =
        TestGetAndResetTickerCount(options, use_prefix_
                                                ? BLOOM_FILTER_PREFIX_CHECKED
                                                : BLOOM_FILTER_FULL_POSITIVE) +
        (use_prefix_ ? 0 : filter_useful);
    EXPECT_GE(filter_useful, number_not_found - 2);  // possible FP
    EXPECT_EQ(filter_checked, Q);
    if (!use_prefix_) {
      EXPECT_EQ(
          TestGetAndResetTickerCount(options, BLOOM_FILTER_FULL_TRUE_POSITIVE),
          Q - number_not_found);
    }

    // Confirm no duplicate loading of same filter partition
    uint64_t filter_accesses =
        TestGetAndResetTickerCount(options, BLOCK_CACHE_FILTER_HIT) +
        TestGetAndResetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
    if (filter_accesses == 2) {
      // Spanned across partitions.
      ++found_spanning;
      if (found_spanning >= 2) {
        break;
      } else {
        // Ensure that at least once we have at least one present and
        // one non-present key on both sides of partition boundary.
        start += 2;
      }
    } else {
      EXPECT_EQ(filter_accesses, 1);
      // See explanation at "start += 2"
      start += Q - 4;
    }
  }
  EXPECT_TRUE(found_spanning >= 2);
}

INSTANTIATE_TEST_CASE_P(DBBloomFilterTestVaryPrefixAndFormatVer,
                        DBBloomFilterTestVaryPrefixAndFormatVer,
                        ::testing::Values(
                            // (use_prefix, format_version)
                            std::make_tuple(false, 2),
                            std::make_tuple(false, 3),
                            std::make_tuple(false, 4),
                            std::make_tuple(false, 5),
                            std::make_tuple(true, 2),
                            std::make_tuple(true, 3),
                            std::make_tuple(true, 4),
                            std::make_tuple(true, 5)));

#ifndef ROCKSDB_LITE
namespace {
static const std::string kPlainTable = "test_PlainTableBloom";
}  // namespace

class BloomStatsTestWithParam
    : public DBBloomFilterTest,
      public testing::WithParamInterface<std::tuple<std::string, bool>> {
 public:
  BloomStatsTestWithParam() {
    bfp_impl_ = std::get<0>(GetParam());
    partition_filters_ = std::get<1>(GetParam());

    options_.create_if_missing = true;
    options_.prefix_extractor.reset(
        ROCKSDB_NAMESPACE::NewFixedPrefixTransform(4));
    options_.memtable_prefix_bloom_size_ratio =
        8.0 * 1024.0 / static_cast<double>(options_.write_buffer_size);
    if (bfp_impl_ == kPlainTable) {
      assert(!partition_filters_);  // not supported in plain table
      PlainTableOptions table_options;
      options_.table_factory.reset(NewPlainTableFactory(table_options));
    } else {
      BlockBasedTableOptions table_options;
      table_options.hash_index_allow_collision = false;
      if (partition_filters_) {
        assert(bfp_impl_ != kDeprecatedBlock);
        table_options.partition_filters = partition_filters_;
        table_options.index_type =
            BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      }
      table_options.filter_policy = Create(10, bfp_impl_);
      options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    options_.env = env_;

    get_perf_context()->Reset();
    DestroyAndReopen(options_);
  }

  ~BloomStatsTestWithParam() override {
    get_perf_context()->Reset();
    Destroy(options_);
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  std::string bfp_impl_;
  bool partition_filters_;
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
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);

  // sanity checks
  ASSERT_EQ(0, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_sst_miss_count);

  Flush();

  // sanity checks
  ASSERT_EQ(0, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_sst_miss_count);

  // check SST bloom stats
  ASSERT_EQ(value1, Get(key1));
  ASSERT_EQ(1, get_perf_context()->bloom_sst_hit_count);
  ASSERT_EQ(value3, Get(key3));
  ASSERT_EQ(2, get_perf_context()->bloom_sst_hit_count);

  ASSERT_EQ("NOT_FOUND", Get(key2));
  ASSERT_EQ(1, get_perf_context()->bloom_sst_miss_count);
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

  std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ReadOptions()));

  // check memtable bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);
  ASSERT_EQ(0, get_perf_context()->bloom_memtable_miss_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
  ASSERT_EQ(1, get_perf_context()->bloom_memtable_miss_count);
  ASSERT_EQ(2, get_perf_context()->bloom_memtable_hit_count);

  Flush();

  iter.reset(dbfull()->NewIterator(ReadOptions()));

  // Check SST bloom stats
  iter->Seek(key1);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value1, iter->value().ToString());
  ASSERT_EQ(1, get_perf_context()->bloom_sst_hit_count);

  iter->Seek(key3);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(value3, iter->value().ToString());
  // The seek doesn't check block-based bloom filter because last index key
  // starts with the same prefix we're seeking to.
  uint64_t expected_hits = bfp_impl_ == kDeprecatedBlock ? 1 : 2;
  ASSERT_EQ(expected_hits, get_perf_context()->bloom_sst_hit_count);

  iter->Seek(key2);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
  ASSERT_EQ(1, get_perf_context()->bloom_sst_miss_count);
  ASSERT_EQ(expected_hits, get_perf_context()->bloom_sst_hit_count);
}

INSTANTIATE_TEST_CASE_P(
    BloomStatsTestWithParam, BloomStatsTestWithParam,
    ::testing::Values(std::make_tuple(kDeprecatedBlock, false),
                      std::make_tuple(kLegacyBloom, false),
                      std::make_tuple(kLegacyBloom, true),
                      std::make_tuple(kFastLocalBloom, false),
                      std::make_tuple(kFastLocalBloom, true),
                      std::make_tuple(kPlainTable, false)));

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
  ASSERT_OK(dbtest->Flush());
  ASSERT_OK(dbtest->dbfull()->CompactRange(CompactRangeOptions(), nullptr,
                                           nullptr));  // move to level 1

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
    assert(!options.unordered_write);
    // It is incompatible with allow_concurrent_memtable_write=false
    options.allow_concurrent_memtable_write = false;

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
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  get_perf_context()->Reset();
  get_perf_context()->EnablePerLevelPerfContext();
  CreateAndReopenWithCF({"mypikachu"}, options);

  int numkeys = 200000;

  // Generate randomly shuffled keys, so the updates are almost
  // random.
  std::vector<int> keys;
  keys.reserve(numkeys);
  for (int i = 0; i < numkeys; i += 2) {
    keys.push_back(i);
  }
  RandomShuffle(std::begin(keys), std::end(keys));
  int num_inserted = 0;
  for (int key : keys) {
    ASSERT_OK(Put(1, Key(key), "val"));
    if (++num_inserted % 1000 == 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  }
  ASSERT_OK(Put(1, Key(0), "val"));
  ASSERT_OK(Put(1, Key(numkeys), "val"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  if (NumTableFilesAtLevel(0, 1) == 0) {
    // No Level 0 file. Create one.
    ASSERT_OK(Put(1, Key(0), "val"));
    ASSERT_OK(Put(1, Key(numkeys), "val"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
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
  uint64_t bloom_filter_useful_all_levels = 0;
  for (auto& kv : (*(get_perf_context()->level_to_perf_context))) {
    if (kv.second.bloom_filter_useful > 0) {
      bloom_filter_useful_all_levels += kv.second.bloom_filter_useful;
    }
  }
  ASSERT_GT(bloom_filter_useful_all_levels, 65000 * 2);
  ASSERT_LT(bloom_filter_useful_all_levels, 120000 * 2);

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
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kSkip;
  compact_options.change_level = true;
  compact_options.target_level = 7;
  ASSERT_TRUE(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr)
                  .IsNotSupported());

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
  get_perf_context()->Reset();
}

int CountIter(std::unique_ptr<Iterator>& iter, const Slice& key) {
  int count = 0;
  for (iter->Seek(key); iter->Valid(); iter->Next()) {
    count++;
  }
  EXPECT_OK(iter->status());
  return count;
}

// use iterate_upper_bound to hint compatiability of existing bloom filters.
// The BF is considered compatible if 1) upper bound and seek key transform
// into the same string, or 2) the transformed seek key is of the same length
// as the upper bound and two keys are adjacent according to the comparator.
TEST_F(DBBloomFilterTest, DynamicBloomFilterUpperBound) {
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    int using_full_builder = bfp_impl != kDeprecatedBlock;
    Options options;
    options.create_if_missing = true;
    options.env = CurrentOptions().env;
    options.prefix_extractor.reset(NewCappedPrefixTransform(4));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy = Create(10, bfp_impl);
    table_options.index_shortening = BlockBasedTableOptions::
        IndexShorteningMode::kShortenSeparatorsAndSuccessor;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    ASSERT_OK(Put("abcdxxx0", "val1"));
    ASSERT_OK(Put("abcdxxx1", "val2"));
    ASSERT_OK(Put("abcdxxx2", "val3"));
    ASSERT_OK(Put("abcdxxx3", "val4"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    {
      // prefix_extractor has not changed, BF will always be read
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcd0000"), 4);
    }
    {
      Slice upper_bound("abcdzzzz");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcd0000"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:5"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.FixedPrefix.5");
    {
      // BF changed, [abcdxx00, abce) is a valid bound, will trigger BF read
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx00"), 4);
      // should check bloom filter since upper bound meets requirement
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + using_full_builder);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    {
      // [abcdxx01, abcey) is not valid bound since upper bound is too long for
      // the BF in SST (capped:4)
      Slice upper_bound("abcey");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx01"), 4);
      // should skip bloom filter since upper bound is too long
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + using_full_builder);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    {
      // [abcdxx02, abcdy) is a valid bound since the prefix is the same
      Slice upper_bound("abcdy");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abcdxx02"), 4);
      // should check bloom filter since upper bound matches transformed seek
      // key
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + using_full_builder * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    {
      // [aaaaaaaa, abce) is not a valid bound since 1) they don't share the
      // same prefix, 2) the prefixes are not consecutive
      Slice upper_bound("abce");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "aaaaaaaa"), 0);
      // should skip bloom filter since mismatch is found
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + using_full_builder * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:3"}}));
    {
      // [abc, abd) is not a valid bound since the upper bound is too short
      // for BF (capped:4)
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + using_full_builder * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:4"}}));
    {
      // set back to capped:4 and verify BF is always read
      Slice upper_bound("abd");
      ReadOptions read_options;
      read_options.prefix_same_as_start = true;
      read_options.iterate_upper_bound = &upper_bound;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "abc"), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                3 + using_full_builder * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
    }
  }
}

// Create multiple SST files each with a different prefix_extractor config,
// verify iterators can read all SST files using the latest config.
TEST_F(DBBloomFilterTest, DynamicBloomFilterMultipleSST) {
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    int using_full_builder = bfp_impl != kDeprecatedBlock;
    Options options;
    options.env = CurrentOptions().env;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.filter_policy = Create(10, bfp_impl);
    table_options.cache_index_and_filter_blocks = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    Slice upper_bound("foz90000");
    ReadOptions read_options;
    read_options.prefix_same_as_start = true;

    // first SST with fixed:1 BF
    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put("foq1", "bar1"));
    ASSERT_OK(Put("fpa", "0"));
    dbfull()->Flush(FlushOptions());
    std::unique_ptr<Iterator> iter_old(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter_old, "foo"), 4);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 1);

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    read_options.iterate_upper_bound = &upper_bound;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter, "foo"), 2);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              1 + using_full_builder);
    ASSERT_EQ(CountIter(iter, "gpk"), 0);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              1 + using_full_builder);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);

    // second SST with capped:3 BF
    ASSERT_OK(Put("foo3", "bar3"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Put("foq5", "bar5"));
    ASSERT_OK(Put("fpb", "1"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    {
      // BF is cappped:3 now
      std::unique_ptr<Iterator> iter_tmp(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_tmp, "foo"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                2 + using_full_builder * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
      ASSERT_EQ(CountIter(iter_tmp, "gpk"), 0);
      // both counters are incremented because BF is "not changed" for 1 of the
      // 2 SST files, so filter is checked once and found no match.
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                3 + using_full_builder * 2);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
    }

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:2"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.FixedPrefix.2");
    // third SST with fixed:2 BF
    ASSERT_OK(Put("foo6", "bar6"));
    ASSERT_OK(Put("foo7", "bar7"));
    ASSERT_OK(Put("foq8", "bar8"));
    ASSERT_OK(Put("fpc", "2"));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    {
      // BF is fixed:2 now
      std::unique_ptr<Iterator> iter_tmp(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_tmp, "foo"), 9);
      // the first and last BF are checked
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                4 + using_full_builder * 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 1);
      ASSERT_EQ(CountIter(iter_tmp, "gpk"), 0);
      // only last BF is checked and not found
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                5 + using_full_builder * 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 2);
    }

    // iter_old can only see the first SST, so checked plus 1
    ASSERT_EQ(CountIter(iter_old, "foo"), 4);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              6 + using_full_builder * 3);
    // iter was created after the first setoptions call so only full filter
    // will check the filter
    ASSERT_EQ(CountIter(iter, "foo"), 2);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
              6 + using_full_builder * 4);

    {
      // keys in all three SSTs are visible to iterator
      // The range of [foo, foz90000] is compatible with (fixed:1) and (fixed:2)
      // so +2 for checked counter
      std::unique_ptr<Iterator> iter_all(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_all, "foo"), 9);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                7 + using_full_builder * 5);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 2);
      ASSERT_EQ(CountIter(iter_all, "gpk"), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                8 + using_full_builder * 5);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 3);
    }
    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    {
      std::unique_ptr<Iterator> iter_all(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter_all, "foo"), 6);
      // all three SST are checked because the current options has the same as
      // the remaining SST (capped:3)
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                9 + using_full_builder * 7);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 3);
      ASSERT_EQ(CountIter(iter_all, "gpk"), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED),
                10 + using_full_builder * 7);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 4);
    }
    // TODO(Zhongyi): Maybe also need to add Get calls to test point look up?
  }
}

// Create a new column family in a running DB, change prefix_extractor
// dynamically, verify the iterator created on the new column family behaves
// as expected
TEST_F(DBBloomFilterTest, DynamicBloomFilterNewColumnFamily) {
  int iteration = 0;
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy = Create(10, bfp_impl);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    CreateAndReopenWithCF({"pikachu" + std::to_string(iteration)}, options);
    ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    // create a new CF and set prefix_extractor dynamically
    options.prefix_extractor.reset(NewCappedPrefixTransform(3));
    CreateColumnFamilies({"ramen_dojo_" + std::to_string(iteration)}, options);
    ASSERT_EQ(dbfull()->GetOptions(handles_[2]).prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    ASSERT_OK(Put(2, "foo3", "bar3"));
    ASSERT_OK(Put(2, "foo4", "bar4"));
    ASSERT_OK(Put(2, "foo5", "bar5"));
    ASSERT_OK(Put(2, "foq6", "bar6"));
    ASSERT_OK(Put(2, "fpq7", "bar7"));
    dbfull()->Flush(FlushOptions());
    {
      std::unique_ptr<Iterator> iter(
          db_->NewIterator(read_options, handles_[2]));
      ASSERT_EQ(CountIter(iter, "foo"), 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(
        dbfull()->SetOptions(handles_[2], {{"prefix_extractor", "fixed:2"}}));
    ASSERT_EQ(dbfull()->GetOptions(handles_[2]).prefix_extractor->AsString(),
              "rocksdb.FixedPrefix.2");
    {
      std::unique_ptr<Iterator> iter(
          db_->NewIterator(read_options, handles_[2]));
      ASSERT_EQ(CountIter(iter, "foo"), 4);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 0);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[2]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[2]));
    handles_[2] = nullptr;
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
    handles_[1] = nullptr;
    iteration++;
  }
}

// Verify it's possible to change prefix_extractor at runtime and iterators
// behaves as expected
TEST_F(DBBloomFilterTest, DynamicBloomFilterOptions) {
  for (const auto& bfp_impl : BloomLikeFilterPolicy::GetAllFixedImpls()) {
    Options options;
    options.env = CurrentOptions().env;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    // Enable prefix bloom for SST files
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy = Create(10, bfp_impl);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);

    ASSERT_OK(Put("foo2", "bar2"));
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put("foo1", "bar1"));
    ASSERT_OK(Put("fpa", "0"));
    dbfull()->Flush(FlushOptions());
    ASSERT_OK(Put("foo3", "bar3"));
    ASSERT_OK(Put("foo4", "bar4"));
    ASSERT_OK(Put("foo5", "bar5"));
    ASSERT_OK(Put("fpb", "1"));
    dbfull()->Flush(FlushOptions());
    ASSERT_OK(Put("foo6", "bar6"));
    ASSERT_OK(Put("foo7", "bar7"));
    ASSERT_OK(Put("foo8", "bar8"));
    ASSERT_OK(Put("fpc", "2"));
    dbfull()->Flush(FlushOptions());

    ReadOptions read_options;
    read_options.prefix_same_as_start = true;
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      ASSERT_EQ(CountIter(iter, "foo"), 12);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 3);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }
    std::unique_ptr<Iterator> iter_old(db_->NewIterator(read_options));
    ASSERT_EQ(CountIter(iter_old, "foo"), 12);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 6);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);

    ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "capped:3"}}));
    ASSERT_EQ(dbfull()->GetOptions().prefix_extractor->AsString(),
              "rocksdb.CappedPrefix.3");
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      // "fp*" should be skipped
      ASSERT_EQ(CountIter(iter, "foo"), 9);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 6);
      ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    }

    // iterator created before should not be affected and see all keys
    ASSERT_EQ(CountIter(iter_old, "foo"), 12);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 9);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 0);
    ASSERT_EQ(CountIter(iter_old, "abc"), 0);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_CHECKED), 12);
    ASSERT_EQ(TestGetTickerCount(options, BLOOM_FILTER_PREFIX_USEFUL), 3);
  }
}

TEST_F(DBBloomFilterTest, SeekForPrevWithPartitionedFilters) {
  Options options = CurrentOptions();
  constexpr size_t kNumKeys = 10000;
  static_assert(kNumKeys <= 10000, "kNumKeys have to be <= 10000");
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeys + 10));
  options.create_if_missing = true;
  constexpr size_t kPrefixLength = 4;
  options.prefix_extractor.reset(NewFixedPrefixTransform(kPrefixLength));
  options.compression = kNoCompression;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(50));
  bbto.index_shortening =
      BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
  bbto.block_size = 128;
  bbto.metadata_block_size = 128;
  bbto.partition_filters = true;
  bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  const std::string value(64, '\0');

  WriteOptions write_opts;
  write_opts.disableWAL = true;
  for (size_t i = 0; i < kNumKeys; ++i) {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << std::fixed << i;
    ASSERT_OK(db_->Put(write_opts, oss.str(), value));
  }
  ASSERT_OK(Flush());

  ReadOptions read_opts;
  // Use legacy, implicit prefix seek
  read_opts.total_order_seek = false;
  read_opts.auto_prefix_mode = false;
  std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
  for (size_t i = 0; i < kNumKeys; ++i) {
    // Seek with a key after each one added but with same prefix. One will
    // surely cross a partition boundary.
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << std::fixed << i << "a";
    it->SeekForPrev(oss.str());
    ASSERT_OK(it->status());
    ASSERT_TRUE(it->Valid());
  }
  it.reset();
}

#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
