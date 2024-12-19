//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <set>

#include "db/db_impl/db_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_bloom.h"
#include "table/plain/plain_table_factory.h"
#include "table/plain/plain_table_key_coding.h"
#include "table/plain/plain_table_reader.h"
#include "table/table_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
class PlainTableKeyDecoderTest : public testing::Test {};

TEST_F(PlainTableKeyDecoderTest, ReadNonMmap) {
  Random rnd(301);
  const uint32_t kLength = 2222;
  std::string tmp = rnd.RandomString(kLength);
  Slice contents(tmp);
  test::StringSource* string_source =
      new test::StringSource(contents, 0, false);
  std::unique_ptr<FSRandomAccessFile> holder(string_source);
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(holder), "test"));
  std::unique_ptr<PlainTableReaderFileInfo> file_info(
      new PlainTableReaderFileInfo(std::move(file_reader), EnvOptions(),
                                   kLength));

  {
    PlainTableFileReader reader(file_info.get());

    const uint32_t kReadSize = 77;
    for (uint32_t pos = 0; pos < kLength; pos += kReadSize) {
      uint32_t read_size = std::min(kLength - pos, kReadSize);
      Slice out;
      ASSERT_TRUE(reader.Read(pos, read_size, &out));
      ASSERT_EQ(0, out.compare(tmp.substr(pos, read_size)));
    }

    ASSERT_LT(uint32_t(string_source->total_reads()), kLength / kReadSize / 2);
  }

  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> reads = {
      {{600, 30}, {590, 30}, {600, 20}, {600, 40}},
      {{800, 20}, {100, 20}, {500, 20}, {1500, 20}, {100, 20}, {80, 20}},
      {{1000, 20}, {500, 20}, {1000, 50}},
      {{1000, 20}, {500, 20}, {500, 20}},
      {{1000, 20}, {500, 20}, {200, 20}, {500, 20}},
      {{1000, 20}, {500, 20}, {200, 20}, {1000, 50}},
      {{600, 500}, {610, 20}, {100, 20}},
      {{500, 100}, {490, 100}, {550, 50}},
  };

  std::vector<int> num_file_reads = {2, 6, 2, 2, 4, 3, 2, 2};

  for (size_t i = 0; i < reads.size(); i++) {
    string_source->set_total_reads(0);
    PlainTableFileReader reader(file_info.get());
    for (auto p : reads[i]) {
      Slice out;
      ASSERT_TRUE(reader.Read(p.first, p.second, &out));
      ASSERT_EQ(0, out.compare(tmp.substr(p.first, p.second)));
    }
    ASSERT_EQ(num_file_reads[i], string_source->total_reads());
  }
}

class PlainTableDBTest : public testing::Test,
                         public testing::WithParamInterface<bool> {
 protected:
 private:
  std::string dbname_;
  Env* env_;
  DB* db_;

  bool mmap_mode_;
  Options last_options_;

 public:
  PlainTableDBTest() : env_(Env::Default()) {}

  ~PlainTableDBTest() override {
    delete db_;
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  void SetUp() override {
    mmap_mode_ = GetParam();
    dbname_ = test::PerThreadDBPath("plain_table_db_test");
    EXPECT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.level_compaction_dynamic_level_bytes = false;

    PlainTableOptions plain_table_options;
    plain_table_options.user_key_len = 0;
    plain_table_options.bloom_bits_per_key = 2;
    plain_table_options.hash_table_ratio = 0.8;
    plain_table_options.index_sparseness = 3;
    plain_table_options.huge_page_tlb_size = 0;
    plain_table_options.encoding_type = kPrefix;
    plain_table_options.full_scan_mode = false;
    plain_table_options.store_index_in_file = false;

    options.table_factory.reset(NewPlainTableFactory(plain_table_options));
    options.memtable_factory.reset(NewHashLinkListRepFactory(4, 0, 3, true));

    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.allow_mmap_reads = mmap_mode_;
    options.allow_concurrent_memtable_write = false;
    options.unordered_write = false;
    return options;
  }

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_); }

  void Reopen(Options* options = nullptr) { ASSERT_OK(TryReopen(options)); }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  bool mmap_mode() const { return mmap_mode_; }

  void DestroyAndReopen(Options* options = nullptr) {
    // Destroy using last options
    Destroy(&last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(Options* options) {
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, *options));
  }

  Status PureReopen(Options* options, DB** db) {
    return DB::Open(*options, dbname_, db);
  }

  Status ReopenForReadOnly(Options* options) {
    delete db_;
    db_ = nullptr;
    return DB::OpenForReadOnly(*options, dbname_, &db_);
  }

  Status TryReopen(Options* options = nullptr) {
    delete db_;
    db_ = nullptr;
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts = CurrentOptions();
      opts.create_if_missing = true;
    }
    last_options_ = opts;

    return DB::Open(opts, dbname_, &db_);
  }

  Status Put(const Slice& k, const Slice& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  int NumTableFilesAtLevel(int level) {
    std::string property;
    EXPECT_TRUE(db_->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(level), &property));
    return atoi(property.c_str());
  }

  // Return spread of files per level
  std::string FilesPerLevel() {
    std::string result;
    size_t last_non_zero_offset = 0;
    for (int level = 0; level < db_->NumberLevels(); level++) {
      int f = NumTableFilesAtLevel(level);
      char buf[100];
      snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
      result += buf;
      if (f > 0) {
        last_non_zero_offset = result.size();
      }
    }
    result.resize(last_non_zero_offset);
    return result;
  }

  std::string IterStatus(Iterator* iter) {
    std::string result;
    if (iter->Valid()) {
      result = iter->key().ToString() + "->" + iter->value().ToString();
    } else {
      result = "(invalid)";
    }
    return result;
  }
};

TEST_P(PlainTableDBTest, Empty) {
  ASSERT_TRUE(dbfull() != nullptr);
  ASSERT_EQ("NOT_FOUND", Get("0000000000000foo"));
}

class TestPlainTableReader : public PlainTableReader {
 public:
  TestPlainTableReader(
      const EnvOptions& env_options, const InternalKeyComparator& icomparator,
      EncodingType encoding_type, uint64_t file_size, int bloom_bits_per_key,
      double hash_table_ratio, size_t index_sparseness,
      std::unique_ptr<TableProperties>&& props,
      std::unique_ptr<RandomAccessFileReader>&& file,
      const ImmutableOptions& ioptions, const SliceTransform* prefix_extractor,
      bool* expect_bloom_not_match, bool store_index_in_file,
      uint32_t column_family_id, const std::string& column_family_name)
      : PlainTableReader(ioptions, std::move(file), env_options, icomparator,
                         encoding_type, file_size, props.get(),
                         prefix_extractor),
        expect_bloom_not_match_(expect_bloom_not_match) {
    Status s = MmapDataIfNeeded();
    EXPECT_TRUE(s.ok());

    s = PopulateIndex(props.get(), bloom_bits_per_key, hash_table_ratio,
                      index_sparseness, 2 * 1024 * 1024);
    EXPECT_TRUE(s.ok());

    EXPECT_EQ(column_family_id, static_cast<uint32_t>(props->column_family_id));
    EXPECT_EQ(column_family_name, props->column_family_name);
    if (store_index_in_file) {
      auto bloom_version_ptr = props->user_collected_properties.find(
          PlainTablePropertyNames::kBloomVersion);
      EXPECT_TRUE(bloom_version_ptr != props->user_collected_properties.end());
      EXPECT_EQ(bloom_version_ptr->second, std::string("1"));
      if (ioptions.bloom_locality > 0) {
        auto num_blocks_ptr = props->user_collected_properties.find(
            PlainTablePropertyNames::kNumBloomBlocks);
        EXPECT_TRUE(num_blocks_ptr != props->user_collected_properties.end());
      }
    }
    table_properties_ = std::move(props);
  }

  ~TestPlainTableReader() override = default;

 private:
  bool MatchBloom(uint32_t hash) const override {
    bool ret = PlainTableReader::MatchBloom(hash);
    if (*expect_bloom_not_match_) {
      EXPECT_TRUE(!ret);
    } else {
      EXPECT_TRUE(ret);
    }
    return ret;
  }
  bool* expect_bloom_not_match_;
};

class TestPlainTableFactory : public PlainTableFactory {
 public:
  explicit TestPlainTableFactory(bool* expect_bloom_not_match,
                                 const PlainTableOptions& options,
                                 uint32_t column_family_id,
                                 std::string column_family_name)
      : PlainTableFactory(options),
        bloom_bits_per_key_(options.bloom_bits_per_key),
        hash_table_ratio_(options.hash_table_ratio),
        index_sparseness_(options.index_sparseness),
        store_index_in_file_(options.store_index_in_file),
        expect_bloom_not_match_(expect_bloom_not_match),
        column_family_id_(column_family_id),
        column_family_name_(std::move(column_family_name)) {}

  using PlainTableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& /*ro*/, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table,
      bool /*prefetch_index_and_filter_in_cache*/) const override {
    std::unique_ptr<TableProperties> props;
    const ReadOptions read_options;
    auto s = ReadTableProperties(file.get(), file_size, kPlainTableMagicNumber,
                                 table_reader_options.ioptions, read_options,
                                 &props);
    EXPECT_TRUE(s.ok());

    if (store_index_in_file_) {
      BlockHandle bloom_block_handle;
      s = FindMetaBlockInFile(file.get(), file_size, kPlainTableMagicNumber,
                              table_reader_options.ioptions, read_options,
                              BloomBlockBuilder::kBloomBlock,
                              &bloom_block_handle);
      EXPECT_TRUE(s.ok());

      BlockHandle index_block_handle;
      s = FindMetaBlockInFile(file.get(), file_size, kPlainTableMagicNumber,
                              table_reader_options.ioptions, read_options,
                              PlainTableIndexBuilder::kPlainTableIndexBlock,
                              &index_block_handle);
      EXPECT_TRUE(s.ok());
    }

    auto& user_props = props->user_collected_properties;
    auto encoding_type_prop =
        user_props.find(PlainTablePropertyNames::kEncodingType);
    assert(encoding_type_prop != user_props.end());
    EncodingType encoding_type = static_cast<EncodingType>(
        DecodeFixed32(encoding_type_prop->second.c_str()));

    std::unique_ptr<PlainTableReader> new_reader(new TestPlainTableReader(
        table_reader_options.env_options,
        table_reader_options.internal_comparator, encoding_type, file_size,
        bloom_bits_per_key_, hash_table_ratio_, index_sparseness_,
        std::move(props), std::move(file), table_reader_options.ioptions,
        table_reader_options.prefix_extractor.get(), expect_bloom_not_match_,
        store_index_in_file_, column_family_id_, column_family_name_));

    *table = std::move(new_reader);
    return s;
  }

 private:
  int bloom_bits_per_key_;
  double hash_table_ratio_;
  size_t index_sparseness_;
  bool store_index_in_file_;
  bool* expect_bloom_not_match_;
  const uint32_t column_family_id_;
  const std::string column_family_name_;
};

TEST_P(PlainTableDBTest, BadOptions1) {
  // Build with a prefix extractor
  ASSERT_OK(Put("1000000000000foo", "v1"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  // Bad attempt to re-open without a prefix extractor
  Options options = CurrentOptions();
  options.prefix_extractor.reset();
  ASSERT_EQ(
      "Invalid argument: Prefix extractor is missing when opening a PlainTable "
      "built using a prefix extractor",
      TryReopen(&options).ToString());

  // Bad attempt to re-open with different prefix extractor
  options.prefix_extractor.reset(NewFixedPrefixTransform(6));
  ASSERT_EQ(
      "Invalid argument: Prefix extractor given doesn't match the one used to "
      "build PlainTable",
      TryReopen(&options).ToString());

  // Correct prefix extractor
  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  Reopen(&options);
  ASSERT_EQ("v1", Get("1000000000000foo"));
}

TEST_P(PlainTableDBTest, BadOptions2) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset();
  options.create_if_missing = true;
  DestroyAndReopen(&options);
  // Build without a prefix extractor
  // (apparently works even if hash_table_ratio > 0)
  ASSERT_OK(Put("1000000000000foo", "v1"));
  // Build without a prefix extractor, this call will fail and returns the
  // status for this bad attempt.
  ASSERT_NOK(dbfull()->TEST_FlushMemTable());

  // Bad attempt to re-open with hash_table_ratio > 0 and no prefix extractor
  Status s = TryReopen(&options);
  ASSERT_EQ(
      "Not implemented: PlainTable requires a prefix extractor enable prefix "
      "hash mode.",
      s.ToString());

  // OK to open with hash_table_ratio == 0 and no prefix extractor
  PlainTableOptions plain_table_options;
  plain_table_options.hash_table_ratio = 0;
  options.table_factory.reset(NewPlainTableFactory(plain_table_options));
  Reopen(&options);
  ASSERT_EQ("v1", Get("1000000000000foo"));

  // OK to open newly with a prefix_extractor and hash table; builds index
  // in memory.
  options = CurrentOptions();
  Reopen(&options);
  ASSERT_EQ("v1", Get("1000000000000foo"));
}

TEST_P(PlainTableDBTest, Flush) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (EncodingType encoding_type : {kPlain, kPrefix}) {
      for (int bloom = -1; bloom <= 117; bloom += 117) {
        const int bloom_bits = std::max(bloom, 0);
        const bool full_scan_mode = bloom < 0;
        for (int total_order = 0; total_order <= 1; total_order++) {
          for (int store_index_in_file = 0; store_index_in_file <= 1;
               ++store_index_in_file) {
            Options options = CurrentOptions();
            options.create_if_missing = true;
            // Set only one bucket to force bucket conflict.
            // Test index interval for the same prefix to be 1, 2 and 4
            if (total_order) {
              options.prefix_extractor.reset();

              PlainTableOptions plain_table_options;
              plain_table_options.user_key_len = 0;
              plain_table_options.bloom_bits_per_key = bloom_bits;
              plain_table_options.hash_table_ratio = 0;
              plain_table_options.index_sparseness = 2;
              plain_table_options.huge_page_tlb_size = huge_page_tlb_size;
              plain_table_options.encoding_type = encoding_type;
              plain_table_options.full_scan_mode = full_scan_mode;
              plain_table_options.store_index_in_file = store_index_in_file;

              options.table_factory.reset(
                  NewPlainTableFactory(plain_table_options));
            } else {
              PlainTableOptions plain_table_options;
              plain_table_options.user_key_len = 0;
              plain_table_options.bloom_bits_per_key = bloom_bits;
              plain_table_options.hash_table_ratio = 0.75;
              plain_table_options.index_sparseness = 16;
              plain_table_options.huge_page_tlb_size = huge_page_tlb_size;
              plain_table_options.encoding_type = encoding_type;
              plain_table_options.full_scan_mode = full_scan_mode;
              plain_table_options.store_index_in_file = store_index_in_file;

              options.table_factory.reset(
                  NewPlainTableFactory(plain_table_options));
            }
            DestroyAndReopen(&options);
            uint64_t int_num;
            ASSERT_TRUE(dbfull()->GetIntProperty(
                "rocksdb.estimate-table-readers-mem", &int_num));
            ASSERT_EQ(int_num, 0U);

            ASSERT_OK(Put("1000000000000foo", "v1"));
            ASSERT_OK(Put("0000000000000bar", "v2"));
            ASSERT_OK(Put("1000000000000foo", "v3"));
            ASSERT_OK(dbfull()->TEST_FlushMemTable());

            ASSERT_TRUE(dbfull()->GetIntProperty(
                "rocksdb.estimate-table-readers-mem", &int_num));
            ASSERT_GT(int_num, 0U);

            TablePropertiesCollection ptc;
            ASSERT_OK(
                static_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc));
            ASSERT_EQ(1U, ptc.size());
            auto row = ptc.begin();
            auto tp = row->second;

            if (full_scan_mode) {
              // Does not support Get/Seek
              std::unique_ptr<Iterator> iter(
                  dbfull()->NewIterator(ReadOptions()));
              iter->SeekToFirst();
              ASSERT_TRUE(iter->Valid());
              ASSERT_EQ("0000000000000bar", iter->key().ToString());
              ASSERT_EQ("v2", iter->value().ToString());
              iter->Next();
              ASSERT_TRUE(iter->Valid());
              ASSERT_EQ("1000000000000foo", iter->key().ToString());
              ASSERT_EQ("v3", iter->value().ToString());
              iter->Next();
              ASSERT_TRUE(!iter->Valid());
              ASSERT_TRUE(iter->status().ok());
            } else {
              if (!store_index_in_file) {
                ASSERT_EQ(total_order ? "4" : "12",
                          (tp->user_collected_properties)
                              .at("plain_table_hash_table_size"));
                ASSERT_EQ("0", (tp->user_collected_properties)
                                   .at("plain_table_sub_index_size"));
              } else {
                ASSERT_EQ("0", (tp->user_collected_properties)
                                   .at("plain_table_hash_table_size"));
                ASSERT_EQ("0", (tp->user_collected_properties)
                                   .at("plain_table_sub_index_size"));
              }
              ASSERT_EQ("v3", Get("1000000000000foo"));
              ASSERT_EQ("v2", Get("0000000000000bar"));
            }
          }
        }
      }
    }
  }
}

TEST_P(PlainTableDBTest, Flush2) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (EncodingType encoding_type : {kPlain, kPrefix}) {
      for (int bloom_bits = 0; bloom_bits <= 117; bloom_bits += 117) {
        for (int total_order = 0; total_order <= 1; total_order++) {
          for (int store_index_in_file = 0; store_index_in_file <= 1;
               ++store_index_in_file) {
            if (encoding_type == kPrefix && total_order) {
              continue;
            }
            if (!bloom_bits && store_index_in_file) {
              continue;
            }
            if (total_order && store_index_in_file) {
              continue;
            }
            bool expect_bloom_not_match = false;
            Options options = CurrentOptions();
            options.create_if_missing = true;
            // Set only one bucket to force bucket conflict.
            // Test index interval for the same prefix to be 1, 2 and 4
            PlainTableOptions plain_table_options;
            if (total_order) {
              options.prefix_extractor = nullptr;
              plain_table_options.hash_table_ratio = 0;
              plain_table_options.index_sparseness = 2;
            } else {
              plain_table_options.hash_table_ratio = 0.75;
              plain_table_options.index_sparseness = 16;
            }
            plain_table_options.user_key_len = kPlainTableVariableLength;
            plain_table_options.bloom_bits_per_key = bloom_bits;
            plain_table_options.huge_page_tlb_size = huge_page_tlb_size;
            plain_table_options.encoding_type = encoding_type;
            plain_table_options.store_index_in_file = store_index_in_file;
            options.table_factory.reset(new TestPlainTableFactory(
                &expect_bloom_not_match, plain_table_options,
                0 /* column_family_id */, kDefaultColumnFamilyName));

            DestroyAndReopen(&options);
            ASSERT_OK(Put("0000000000000bar", "b"));
            ASSERT_OK(Put("1000000000000foo", "v1"));
            ASSERT_OK(dbfull()->TEST_FlushMemTable());

            ASSERT_OK(Put("1000000000000foo", "v2"));
            ASSERT_OK(dbfull()->TEST_FlushMemTable());
            ASSERT_EQ("v2", Get("1000000000000foo"));

            ASSERT_OK(Put("0000000000000eee", "v3"));
            ASSERT_OK(dbfull()->TEST_FlushMemTable());
            ASSERT_EQ("v3", Get("0000000000000eee"));

            ASSERT_OK(Delete("0000000000000bar"));
            ASSERT_OK(dbfull()->TEST_FlushMemTable());
            ASSERT_EQ("NOT_FOUND", Get("0000000000000bar"));

            ASSERT_OK(Put("0000000000000eee", "v5"));
            ASSERT_OK(Put("9000000000000eee", "v5"));
            ASSERT_OK(dbfull()->TEST_FlushMemTable());
            ASSERT_EQ("v5", Get("0000000000000eee"));

            // Test Bloom Filter
            if (bloom_bits > 0) {
              // Neither key nor value should exist.
              expect_bloom_not_match = true;
              ASSERT_EQ("NOT_FOUND", Get("5_not00000000bar"));
              // Key doesn't exist any more but prefix exists.
              if (total_order) {
                ASSERT_EQ("NOT_FOUND", Get("1000000000000not"));
                ASSERT_EQ("NOT_FOUND", Get("0000000000000not"));
              }
              expect_bloom_not_match = false;
            }
          }
        }
      }
    }
  }
}

TEST_P(PlainTableDBTest, Immortal) {
  for (EncodingType encoding_type : {kPlain, kPrefix}) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.max_open_files = -1;
    // Set only one bucket to force bucket conflict.
    // Test index interval for the same prefix to be 1, 2 and 4
    PlainTableOptions plain_table_options;
    plain_table_options.hash_table_ratio = 0.75;
    plain_table_options.index_sparseness = 16;
    plain_table_options.user_key_len = kPlainTableVariableLength;
    plain_table_options.bloom_bits_per_key = 10;
    plain_table_options.encoding_type = encoding_type;
    options.table_factory.reset(NewPlainTableFactory(plain_table_options));

    DestroyAndReopen(&options);
    ASSERT_OK(Put("0000000000000bar", "b"));
    ASSERT_OK(Put("1000000000000foo", "v1"));
    ASSERT_OK(dbfull()->TEST_FlushMemTable());

    int copied = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "GetContext::SaveValue::PinSelf", [&](void* /*arg*/) { copied++; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_EQ("b", Get("0000000000000bar"));
    ASSERT_EQ("v1", Get("1000000000000foo"));
    ASSERT_EQ(2, copied);
    copied = 0;

    Close();
    ASSERT_OK(ReopenForReadOnly(&options));

    ASSERT_EQ("b", Get("0000000000000bar"));
    ASSERT_EQ("v1", Get("1000000000000foo"));
    ASSERT_EQ("NOT_FOUND", Get("1000000000000bar"));
    if (mmap_mode()) {
      ASSERT_EQ(0, copied);
    } else {
      ASSERT_EQ(2, copied);
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_P(PlainTableDBTest, Iterator) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (EncodingType encoding_type : {kPlain, kPrefix}) {
      for (int bloom_bits = 0; bloom_bits <= 117; bloom_bits += 117) {
        for (int total_order = 0; total_order <= 1; total_order++) {
          if (encoding_type == kPrefix && total_order == 1) {
            continue;
          }
          bool expect_bloom_not_match = false;
          Options options = CurrentOptions();
          options.create_if_missing = true;
          // Set only one bucket to force bucket conflict.
          // Test index interval for the same prefix to be 1, 2 and 4
          if (total_order) {
            options.prefix_extractor = nullptr;

            PlainTableOptions plain_table_options;
            plain_table_options.user_key_len = 16;
            plain_table_options.bloom_bits_per_key = bloom_bits;
            plain_table_options.hash_table_ratio = 0;
            plain_table_options.index_sparseness = 2;
            plain_table_options.huge_page_tlb_size = huge_page_tlb_size;
            plain_table_options.encoding_type = encoding_type;

            options.table_factory.reset(new TestPlainTableFactory(
                &expect_bloom_not_match, plain_table_options,
                0 /* column_family_id */, kDefaultColumnFamilyName));
          } else {
            PlainTableOptions plain_table_options;
            plain_table_options.user_key_len = 16;
            plain_table_options.bloom_bits_per_key = bloom_bits;
            plain_table_options.hash_table_ratio = 0.75;
            plain_table_options.index_sparseness = 16;
            plain_table_options.huge_page_tlb_size = huge_page_tlb_size;
            plain_table_options.encoding_type = encoding_type;

            options.table_factory.reset(new TestPlainTableFactory(
                &expect_bloom_not_match, plain_table_options,
                0 /* column_family_id */, kDefaultColumnFamilyName));
          }
          DestroyAndReopen(&options);

          ASSERT_OK(Put("1000000000foo002", "v_2"));
          ASSERT_OK(Put("0000000000000bar", "random"));
          ASSERT_OK(Put("1000000000foo001", "v1"));
          ASSERT_OK(Put("3000000000000bar", "bar_v"));
          ASSERT_OK(Put("1000000000foo003", "v__3"));
          ASSERT_OK(Put("1000000000foo004", "v__4"));
          ASSERT_OK(Put("1000000000foo005", "v__5"));
          ASSERT_OK(Put("1000000000foo007", "v__7"));
          ASSERT_OK(Put("1000000000foo008", "v__8"));
          ASSERT_OK(dbfull()->TEST_FlushMemTable());
          ASSERT_EQ("v1", Get("1000000000foo001"));
          ASSERT_EQ("v__3", Get("1000000000foo003"));
          Iterator* iter = dbfull()->NewIterator(ReadOptions());
          iter->Seek("1000000000foo000");
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo001", iter->key().ToString());
          ASSERT_EQ("v1", iter->value().ToString());

          iter->Next();
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo002", iter->key().ToString());
          ASSERT_EQ("v_2", iter->value().ToString());

          iter->Next();
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo003", iter->key().ToString());
          ASSERT_EQ("v__3", iter->value().ToString());

          iter->Next();
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo004", iter->key().ToString());
          ASSERT_EQ("v__4", iter->value().ToString());

          iter->Seek("3000000000000bar");
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("3000000000000bar", iter->key().ToString());
          ASSERT_EQ("bar_v", iter->value().ToString());

          iter->Seek("1000000000foo000");
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo001", iter->key().ToString());
          ASSERT_EQ("v1", iter->value().ToString());

          iter->Seek("1000000000foo005");
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo005", iter->key().ToString());
          ASSERT_EQ("v__5", iter->value().ToString());

          iter->Seek("1000000000foo006");
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo007", iter->key().ToString());
          ASSERT_EQ("v__7", iter->value().ToString());

          iter->Seek("1000000000foo008");
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ("1000000000foo008", iter->key().ToString());
          ASSERT_EQ("v__8", iter->value().ToString());

          if (total_order == 0) {
            iter->Seek("1000000000foo009");
            ASSERT_TRUE(iter->Valid());
            ASSERT_EQ("3000000000000bar", iter->key().ToString());
          }

          // Test Bloom Filter
          if (bloom_bits > 0) {
            if (!total_order) {
              // Neither key nor value should exist.
              expect_bloom_not_match = true;
              iter->Seek("2not000000000bar");
              ASSERT_TRUE(!iter->Valid());
              ASSERT_EQ("NOT_FOUND", Get("2not000000000bar"));
              expect_bloom_not_match = false;
            } else {
              expect_bloom_not_match = true;
              ASSERT_EQ("NOT_FOUND", Get("2not000000000bar"));
              expect_bloom_not_match = false;
            }
          }
          ASSERT_OK(iter->status());
          delete iter;
        }
      }
    }
  }
}

namespace {
std::string NthKey(size_t n, char filler) {
  std::string rv(16, filler);
  rv[0] = n % 10;
  rv[1] = (n / 10) % 10;
  rv[2] = (n / 100) % 10;
  rv[3] = (n / 1000) % 10;
  return rv;
}
}  // anonymous namespace

TEST_P(PlainTableDBTest, BloomSchema) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  for (int bloom_locality = 0; bloom_locality <= 1; bloom_locality++) {
    options.bloom_locality = bloom_locality;
    PlainTableOptions plain_table_options;
    plain_table_options.user_key_len = 16;
    plain_table_options.bloom_bits_per_key = 3;  // high FP rate for test
    plain_table_options.hash_table_ratio = 0.75;
    plain_table_options.index_sparseness = 16;
    plain_table_options.huge_page_tlb_size = 0;
    plain_table_options.encoding_type = kPlain;

    bool expect_bloom_not_match = false;
    options.table_factory.reset(new TestPlainTableFactory(
        &expect_bloom_not_match, plain_table_options, 0 /* column_family_id */,
        kDefaultColumnFamilyName));
    DestroyAndReopen(&options);

    for (unsigned i = 0; i < 2345; ++i) {
      ASSERT_OK(Put(NthKey(i, 'y'), "added"));
    }
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_EQ("added", Get(NthKey(42, 'y')));

    for (unsigned i = 0; i < 32; ++i) {
      // Known pattern of Bloom filter false positives can detect schema change
      // with high probability. Known FPs stuffed into bits:
      uint32_t pattern;
      if (!bloom_locality) {
        pattern = 1785868347UL;
      } else if (CACHE_LINE_SIZE == 64U) {
        pattern = 2421694657UL;
      } else if (CACHE_LINE_SIZE == 128U) {
        pattern = 788710956UL;
      } else {
        ASSERT_EQ(CACHE_LINE_SIZE, 256U);
        pattern = 163905UL;
      }
      bool expect_fp = pattern & (1UL << i);
      // fprintf(stderr, "expect_fp@%u: %d\n", i, (int)expect_fp);
      expect_bloom_not_match = !expect_fp;
      ASSERT_EQ("NOT_FOUND", Get(NthKey(i, 'n')));
    }
  }
}

namespace {
std::string MakeLongKey(size_t length, char c) {
  return std::string(length, c);
}
}  // anonymous namespace

TEST_P(PlainTableDBTest, IteratorLargeKeys) {
  Options options = CurrentOptions();

  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 0;
  plain_table_options.bloom_bits_per_key = 0;
  plain_table_options.hash_table_ratio = 0;

  options.table_factory.reset(NewPlainTableFactory(plain_table_options));
  options.create_if_missing = true;
  options.prefix_extractor.reset();
  DestroyAndReopen(&options);

  std::string key_list[] = {MakeLongKey(30, '0'), MakeLongKey(16, '1'),
                            MakeLongKey(32, '2'), MakeLongKey(60, '3'),
                            MakeLongKey(90, '4'), MakeLongKey(50, '5'),
                            MakeLongKey(26, '6')};

  for (size_t i = 0; i < 7; i++) {
    ASSERT_OK(Put(key_list[i], std::to_string(i)));
  }

  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  Iterator* iter = dbfull()->NewIterator(ReadOptions());
  iter->Seek(key_list[0]);

  for (size_t i = 0; i < 7; i++) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(key_list[i], iter->key().ToString());
    ASSERT_EQ(std::to_string(i), iter->value().ToString());
    iter->Next();
  }

  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());

  delete iter;
}

namespace {
std::string MakeLongKeyWithPrefix(size_t length, char c) {
  return "00000000" + std::string(length - 8, c);
}
}  // anonymous namespace

TEST_P(PlainTableDBTest, IteratorLargeKeysWithPrefix) {
  Options options = CurrentOptions();

  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 16;
  plain_table_options.bloom_bits_per_key = 0;
  plain_table_options.hash_table_ratio = 0.8;
  plain_table_options.index_sparseness = 3;
  plain_table_options.huge_page_tlb_size = 0;
  plain_table_options.encoding_type = kPrefix;

  options.table_factory.reset(NewPlainTableFactory(plain_table_options));
  options.create_if_missing = true;
  DestroyAndReopen(&options);

  std::string key_list[] = {
      MakeLongKeyWithPrefix(30, '0'), MakeLongKeyWithPrefix(16, '1'),
      MakeLongKeyWithPrefix(32, '2'), MakeLongKeyWithPrefix(60, '3'),
      MakeLongKeyWithPrefix(90, '4'), MakeLongKeyWithPrefix(50, '5'),
      MakeLongKeyWithPrefix(26, '6')};

  for (size_t i = 0; i < 7; i++) {
    ASSERT_OK(Put(key_list[i], std::to_string(i)));
  }

  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  Iterator* iter = dbfull()->NewIterator(ReadOptions());
  iter->Seek(key_list[0]);

  for (size_t i = 0; i < 7; i++) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(key_list[i], iter->key().ToString());
    ASSERT_EQ(std::to_string(i), iter->value().ToString());
    iter->Next();
  }

  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());

  delete iter;
}

TEST_P(PlainTableDBTest, IteratorReverseSuffixComparator) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // Set only one bucket to force bucket conflict.
  // Test index interval for the same prefix to be 1, 2 and 4
  test::SimpleSuffixReverseComparator comp;
  options.comparator = &comp;
  DestroyAndReopen(&options);

  ASSERT_OK(Put("1000000000foo002", "v_2"));
  ASSERT_OK(Put("0000000000000bar", "random"));
  ASSERT_OK(Put("1000000000foo001", "v1"));
  ASSERT_OK(Put("3000000000000bar", "bar_v"));
  ASSERT_OK(Put("1000000000foo003", "v__3"));
  ASSERT_OK(Put("1000000000foo004", "v__4"));
  ASSERT_OK(Put("1000000000foo005", "v__5"));
  ASSERT_OK(Put("1000000000foo007", "v__7"));
  ASSERT_OK(Put("1000000000foo008", "v__8"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  ASSERT_EQ("v1", Get("1000000000foo001"));
  ASSERT_EQ("v__3", Get("1000000000foo003"));
  Iterator* iter = dbfull()->NewIterator(ReadOptions());
  iter->Seek("1000000000foo009");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo008", iter->key().ToString());
  ASSERT_EQ("v__8", iter->value().ToString());

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo007", iter->key().ToString());
  ASSERT_EQ("v__7", iter->value().ToString());

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo005", iter->key().ToString());
  ASSERT_EQ("v__5", iter->value().ToString());

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo004", iter->key().ToString());
  ASSERT_EQ("v__4", iter->value().ToString());

  iter->Seek("3000000000000bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("3000000000000bar", iter->key().ToString());
  ASSERT_EQ("bar_v", iter->value().ToString());

  iter->Seek("1000000000foo005");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo005", iter->key().ToString());
  ASSERT_EQ("v__5", iter->value().ToString());

  iter->Seek("1000000000foo006");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo005", iter->key().ToString());
  ASSERT_EQ("v__5", iter->value().ToString());

  iter->Seek("1000000000foo008");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("1000000000foo008", iter->key().ToString());
  ASSERT_EQ("v__8", iter->value().ToString());

  iter->Seek("1000000000foo000");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("3000000000000bar", iter->key().ToString());

  delete iter;
}

TEST_P(PlainTableDBTest, HashBucketConflict) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (unsigned char i = 1; i <= 3; i++) {
      Options options = CurrentOptions();
      options.create_if_missing = true;
      // Set only one bucket to force bucket conflict.
      // Test index interval for the same prefix to be 1, 2 and 4

      PlainTableOptions plain_table_options;
      plain_table_options.user_key_len = 16;
      plain_table_options.bloom_bits_per_key = 0;
      plain_table_options.hash_table_ratio = 0;
      plain_table_options.index_sparseness = 2 ^ i;
      plain_table_options.huge_page_tlb_size = huge_page_tlb_size;

      options.table_factory.reset(NewPlainTableFactory(plain_table_options));

      DestroyAndReopen(&options);
      ASSERT_OK(Put("5000000000000fo0", "v1"));
      ASSERT_OK(Put("5000000000000fo1", "v2"));
      ASSERT_OK(Put("5000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo0", "v3"));
      ASSERT_OK(Put("2000000000000fo1", "v4"));
      ASSERT_OK(Put("2000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo3", "v"));

      ASSERT_OK(dbfull()->TEST_FlushMemTable());

      ASSERT_EQ("v1", Get("5000000000000fo0"));
      ASSERT_EQ("v2", Get("5000000000000fo1"));
      ASSERT_EQ("v3", Get("2000000000000fo0"));
      ASSERT_EQ("v4", Get("2000000000000fo1"));

      ASSERT_EQ("NOT_FOUND", Get("5000000000000bar"));
      ASSERT_EQ("NOT_FOUND", Get("2000000000000bar"));
      ASSERT_EQ("NOT_FOUND", Get("5000000000000fo8"));
      ASSERT_EQ("NOT_FOUND", Get("2000000000000fo8"));

      ReadOptions ro;
      Iterator* iter = dbfull()->NewIterator(ro);

      iter->Seek("5000000000000fo0");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo0", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo1", iter->key().ToString());

      iter->Seek("5000000000000fo1");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo1", iter->key().ToString());

      iter->Seek("2000000000000fo0");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo0", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo1", iter->key().ToString());

      iter->Seek("2000000000000fo1");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo1", iter->key().ToString());

      iter->Seek("2000000000000bar");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo0", iter->key().ToString());

      iter->Seek("5000000000000bar");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo0", iter->key().ToString());

      iter->Seek("2000000000000fo8");
      ASSERT_TRUE(!iter->Valid() ||
                  options.comparator->Compare(iter->key(), "20000001") > 0);

      iter->Seek("5000000000000fo8");
      ASSERT_TRUE(!iter->Valid());

      iter->Seek("1000000000000fo2");
      ASSERT_TRUE(!iter->Valid());

      iter->Seek("3000000000000fo2");
      ASSERT_TRUE(!iter->Valid());

      iter->Seek("8000000000000fo2");
      ASSERT_TRUE(!iter->Valid());

      ASSERT_OK(iter->status());
      delete iter;
    }
  }
}

TEST_P(PlainTableDBTest, HashBucketConflictReverseSuffixComparator) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (unsigned char i = 1; i <= 3; i++) {
      Options options = CurrentOptions();
      options.create_if_missing = true;
      test::SimpleSuffixReverseComparator comp;
      options.comparator = &comp;
      // Set only one bucket to force bucket conflict.
      // Test index interval for the same prefix to be 1, 2 and 4

      PlainTableOptions plain_table_options;
      plain_table_options.user_key_len = 16;
      plain_table_options.bloom_bits_per_key = 0;
      plain_table_options.hash_table_ratio = 0;
      plain_table_options.index_sparseness = 2 ^ i;
      plain_table_options.huge_page_tlb_size = huge_page_tlb_size;

      options.table_factory.reset(NewPlainTableFactory(plain_table_options));
      DestroyAndReopen(&options);
      ASSERT_OK(Put("5000000000000fo0", "v1"));
      ASSERT_OK(Put("5000000000000fo1", "v2"));
      ASSERT_OK(Put("5000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo0", "v3"));
      ASSERT_OK(Put("2000000000000fo1", "v4"));
      ASSERT_OK(Put("2000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo3", "v"));

      ASSERT_OK(dbfull()->TEST_FlushMemTable());

      ASSERT_EQ("v1", Get("5000000000000fo0"));
      ASSERT_EQ("v2", Get("5000000000000fo1"));
      ASSERT_EQ("v3", Get("2000000000000fo0"));
      ASSERT_EQ("v4", Get("2000000000000fo1"));

      ASSERT_EQ("NOT_FOUND", Get("5000000000000bar"));
      ASSERT_EQ("NOT_FOUND", Get("2000000000000bar"));
      ASSERT_EQ("NOT_FOUND", Get("5000000000000fo8"));
      ASSERT_EQ("NOT_FOUND", Get("2000000000000fo8"));

      ReadOptions ro;
      Iterator* iter = dbfull()->NewIterator(ro);

      iter->Seek("5000000000000fo1");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo1", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo0", iter->key().ToString());

      iter->Seek("5000000000000fo1");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo1", iter->key().ToString());

      iter->Seek("2000000000000fo1");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo1", iter->key().ToString());
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo0", iter->key().ToString());

      iter->Seek("2000000000000fo1");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo1", iter->key().ToString());

      iter->Seek("2000000000000var");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("2000000000000fo3", iter->key().ToString());

      iter->Seek("5000000000000var");
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("5000000000000fo2", iter->key().ToString());

      std::string seek_key = "2000000000000bar";
      iter->Seek(seek_key);
      ASSERT_TRUE(!iter->Valid() ||
                  options.prefix_extractor->Transform(iter->key()) !=
                      options.prefix_extractor->Transform(seek_key));

      iter->Seek("1000000000000fo2");
      ASSERT_TRUE(!iter->Valid());

      iter->Seek("3000000000000fo2");
      ASSERT_TRUE(!iter->Valid());

      iter->Seek("8000000000000fo2");
      ASSERT_TRUE(!iter->Valid());

      ASSERT_OK(iter->status());
      delete iter;
    }
  }
}

TEST_P(PlainTableDBTest, NonExistingKeyToNonEmptyBucket) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // Set only one bucket to force bucket conflict.
  // Test index interval for the same prefix to be 1, 2 and 4
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 16;
  plain_table_options.bloom_bits_per_key = 0;
  plain_table_options.hash_table_ratio = 0;
  plain_table_options.index_sparseness = 5;

  options.table_factory.reset(NewPlainTableFactory(plain_table_options));
  DestroyAndReopen(&options);
  ASSERT_OK(Put("5000000000000fo0", "v1"));
  ASSERT_OK(Put("5000000000000fo1", "v2"));
  ASSERT_OK(Put("5000000000000fo2", "v3"));

  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  ASSERT_EQ("v1", Get("5000000000000fo0"));
  ASSERT_EQ("v2", Get("5000000000000fo1"));
  ASSERT_EQ("v3", Get("5000000000000fo2"));

  ASSERT_EQ("NOT_FOUND", Get("8000000000000bar"));
  ASSERT_EQ("NOT_FOUND", Get("1000000000000bar"));

  Iterator* iter = dbfull()->NewIterator(ReadOptions());

  iter->Seek("5000000000000bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("5000000000000fo0", iter->key().ToString());

  iter->Seek("5000000000000fo8");
  ASSERT_TRUE(!iter->Valid());

  iter->Seek("1000000000000fo2");
  ASSERT_TRUE(!iter->Valid());

  iter->Seek("8000000000000fo2");
  ASSERT_TRUE(!iter->Valid());

  ASSERT_OK(iter->status());
  delete iter;
}

static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key_______%06d", i);
  return std::string(buf);
}

TEST_P(PlainTableDBTest, CompactionTrigger) {
  Options options = CurrentOptions();
  options.write_buffer_size = 120 << 10;  // 120KB
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 3;
  Reopen(&options);

  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    std::vector<std::string> values;
    // Write 120KB (10 values, each 12K)
    for (int i = 0; i < 10; i++) {
      values.push_back(rnd.RandomString(12 << 10));
      ASSERT_OK(Put(Key(i), values[i]));
    }
    ASSERT_OK(Put(Key(999), ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(NumTableFilesAtLevel(0), num + 1);
  }

  // generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(rnd.RandomString(10000));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Put(Key(999), ""));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
}

TEST_P(PlainTableDBTest, AdaptiveTable) {
  Options options = CurrentOptions();
  options.create_if_missing = true;

  options.table_factory.reset(NewPlainTableFactory());
  DestroyAndReopen(&options);

  ASSERT_OK(Put("1000000000000foo", "v1"));
  ASSERT_OK(Put("0000000000000bar", "v2"));
  ASSERT_OK(Put("1000000000000foo", "v3"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  options.create_if_missing = false;
  std::shared_ptr<TableFactory> block_based_factory(
      NewBlockBasedTableFactory());
  std::shared_ptr<TableFactory> plain_table_factory(NewPlainTableFactory());
  std::shared_ptr<TableFactory> dummy_factory;
  options.table_factory.reset(NewAdaptiveTableFactory(
      block_based_factory, block_based_factory, plain_table_factory));
  Reopen(&options);
  ASSERT_EQ("v3", Get("1000000000000foo"));
  ASSERT_EQ("v2", Get("0000000000000bar"));

  ASSERT_OK(Put("2000000000000foo", "v4"));
  ASSERT_OK(Put("3000000000000bar", "v5"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  ASSERT_EQ("v4", Get("2000000000000foo"));
  ASSERT_EQ("v5", Get("3000000000000bar"));

  Reopen(&options);
  ASSERT_EQ("v3", Get("1000000000000foo"));
  ASSERT_EQ("v2", Get("0000000000000bar"));
  ASSERT_EQ("v4", Get("2000000000000foo"));
  ASSERT_EQ("v5", Get("3000000000000bar"));

  options.paranoid_checks = false;
  options.table_factory.reset(NewBlockBasedTableFactory());
  Reopen(&options);
  ASSERT_NE("v3", Get("1000000000000foo"));

  options.paranoid_checks = false;
  options.table_factory.reset(NewPlainTableFactory());
  Reopen(&options);
  ASSERT_NE("v5", Get("3000000000000bar"));
}

INSTANTIATE_TEST_CASE_P(PlainTableDBTest, PlainTableDBTest, ::testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
