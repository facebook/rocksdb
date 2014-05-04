// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <algorithm>
#include <set>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/plain_table_reader.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

using std::unique_ptr;

namespace rocksdb {

class PlainTableDBTest {
 protected:
 private:
  std::string dbname_;
  Env* env_;
  DB* db_;

  Options last_options_;

 public:
  PlainTableDBTest() : env_(Env::Default()) {
    dbname_ = test::TmpDir() + "/plain_table_db_test";
    ASSERT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  ~PlainTableDBTest() {
    delete db_;
    ASSERT_OK(DestroyDB(dbname_, Options()));
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.table_factory.reset(NewPlainTableFactory(16, 2, 0.8, 3));
    options.prefix_extractor.reset(NewFixedPrefixTransform(8));
    options.allow_mmap_reads = true;
    return options;
  }

  DBImpl* dbfull() {
    return reinterpret_cast<DBImpl*>(db_);
  }

  void Reopen(Options* options = nullptr) {
    ASSERT_OK(TryReopen(options));
  }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(Options* options = nullptr) {
    //Destroy using last options
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

  Status Delete(const std::string& k) {
    return db_->Delete(WriteOptions(), k);
  }

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
    ASSERT_TRUE(
        db_->GetProperty("rocksdb.num-files-at-level" + NumberToString(level),
                         &property));
    return atoi(property.c_str());
  }

  // Return spread of files per level
  std::string FilesPerLevel() {
    std::string result;
    int last_non_zero_offset = 0;
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

TEST(PlainTableDBTest, Empty) {
  ASSERT_TRUE(dbfull() != nullptr);
  ASSERT_EQ("NOT_FOUND", Get("0000000000000foo"));
}

class TestPlainTableReader : public PlainTableReader {
 public:
  TestPlainTableReader(const EnvOptions& storage_options,
                       const InternalKeyComparator& icomparator,
                       uint64_t file_size, int bloom_bits_per_key,
                       double hash_table_ratio, size_t index_sparseness,
                       const TableProperties* table_properties,
                       unique_ptr<RandomAccessFile>&& file,
                       const Options& options, bool* expect_bloom_not_match)
      : PlainTableReader(options, std::move(file), storage_options, icomparator,
                         file_size, bloom_bits_per_key, hash_table_ratio,
                         index_sparseness, table_properties, 2 * 1024 * 1024),
        expect_bloom_not_match_(expect_bloom_not_match) {
    Status s = PopulateIndex(const_cast<TableProperties*>(table_properties));
    ASSERT_TRUE(s.ok());
  }

  virtual ~TestPlainTableReader() {}

 private:
  virtual bool MatchBloom(uint32_t hash) const override {
    bool ret = PlainTableReader::MatchBloom(hash);
    ASSERT_TRUE(!*expect_bloom_not_match_ || !ret);
    return ret;
  }
  bool* expect_bloom_not_match_;
};

extern const uint64_t kPlainTableMagicNumber;
class TestPlainTableFactory : public PlainTableFactory {
 public:
  explicit TestPlainTableFactory(bool* expect_bloom_not_match,
                                 uint32_t user_key_len, int bloom_bits_per_key,
                                 double hash_table_ratio,
                                 size_t index_sparseness,
                                 size_t huge_page_tlb_size)
      : PlainTableFactory(user_key_len, user_key_len, hash_table_ratio,
                          index_sparseness, huge_page_tlb_size),
        bloom_bits_per_key_(bloom_bits_per_key),
        hash_table_ratio_(hash_table_ratio),
        index_sparseness_(index_sparseness),
        expect_bloom_not_match_(expect_bloom_not_match) {}

  Status NewTableReader(const Options& options, const EnvOptions& soptions,
                        const InternalKeyComparator& internal_comparator,
                        unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
                        unique_ptr<TableReader>* table) const override {
    TableProperties* props = nullptr;
    auto s = ReadTableProperties(file.get(), file_size, kPlainTableMagicNumber,
                                 options.env, options.info_log.get(), &props);
    ASSERT_TRUE(s.ok());

    std::unique_ptr<PlainTableReader> new_reader(new TestPlainTableReader(
        soptions, internal_comparator, file_size, bloom_bits_per_key_,
        hash_table_ratio_, index_sparseness_, props, std::move(file), options,
        expect_bloom_not_match_));

    *table = std::move(new_reader);
    return s;
  }

 private:
  int bloom_bits_per_key_;
  double hash_table_ratio_;
  size_t index_sparseness_;
  bool* expect_bloom_not_match_;
};

TEST(PlainTableDBTest, Flush) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (int bloom_bits = 0; bloom_bits <= 117; bloom_bits += 117) {
      for (int total_order = 0; total_order <= 1; total_order++) {
        Options options = CurrentOptions();
        options.create_if_missing = true;
        // Set only one bucket to force bucket conflict.
        // Test index interval for the same prefix to be 1, 2 and 4
        if (total_order) {
          options.table_factory.reset(NewTotalOrderPlainTableFactory(
              16, bloom_bits, 2, huge_page_tlb_size));
        } else {
          options.table_factory.reset(NewPlainTableFactory(
              16, bloom_bits, 0.75, 16, huge_page_tlb_size));
        }
        DestroyAndReopen(&options);

        ASSERT_OK(Put("1000000000000foo", "v1"));
        ASSERT_OK(Put("0000000000000bar", "v2"));
        ASSERT_OK(Put("1000000000000foo", "v3"));
        dbfull()->TEST_FlushMemTable();

        TablePropertiesCollection ptc;
        reinterpret_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc);
        ASSERT_EQ(1U, ptc.size());
        auto row = ptc.begin();
        auto tp = row->second;
        ASSERT_EQ(total_order ? "4" : "12", (tp->user_collected_properties).at(
                                                "plain_table_hash_table_size"));
        ASSERT_EQ(total_order ? "9" : "0", (tp->user_collected_properties).at(
                                               "plain_table_sub_index_size"));

        ASSERT_EQ("v3", Get("1000000000000foo"));
        ASSERT_EQ("v2", Get("0000000000000bar"));
      }
    }
  }
}

TEST(PlainTableDBTest, Flush2) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (int bloom_bits = 0; bloom_bits <= 117; bloom_bits += 117) {
      for (int total_order = 0; total_order <= 1; total_order++) {
        bool expect_bloom_not_match = false;
        Options options = CurrentOptions();
        options.create_if_missing = true;
        // Set only one bucket to force bucket conflict.
        // Test index interval for the same prefix to be 1, 2 and 4
        if (total_order) {
          options.prefix_extractor = nullptr;
          options.table_factory.reset(
              new TestPlainTableFactory(&expect_bloom_not_match, 16, bloom_bits,
                                        0, 2, huge_page_tlb_size));
        } else {
          options.table_factory.reset(
              new TestPlainTableFactory(&expect_bloom_not_match, 16, bloom_bits,
                                        0.75, 16, huge_page_tlb_size));
        }
        DestroyAndReopen(&options);
        ASSERT_OK(Put("0000000000000bar", "b"));
        ASSERT_OK(Put("1000000000000foo", "v1"));
        dbfull()->TEST_FlushMemTable();

        ASSERT_OK(Put("1000000000000foo", "v2"));
        dbfull()->TEST_FlushMemTable();
        ASSERT_EQ("v2", Get("1000000000000foo"));

        ASSERT_OK(Put("0000000000000eee", "v3"));
        dbfull()->TEST_FlushMemTable();
        ASSERT_EQ("v3", Get("0000000000000eee"));

        ASSERT_OK(Delete("0000000000000bar"));
        dbfull()->TEST_FlushMemTable();
        ASSERT_EQ("NOT_FOUND", Get("0000000000000bar"));

        ASSERT_OK(Put("0000000000000eee", "v5"));
        ASSERT_OK(Put("9000000000000eee", "v5"));
        dbfull()->TEST_FlushMemTable();
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

TEST(PlainTableDBTest, Iterator) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (int bloom_bits = 0; bloom_bits <= 117; bloom_bits += 117) {
      for (int total_order = 0; total_order <= 1; total_order++) {
        bool expect_bloom_not_match = false;
        Options options = CurrentOptions();
        options.create_if_missing = true;
        // Set only one bucket to force bucket conflict.
        // Test index interval for the same prefix to be 1, 2 and 4
        if (total_order) {
          options.prefix_extractor = nullptr;
          options.table_factory.reset(
              new TestPlainTableFactory(&expect_bloom_not_match, 16, bloom_bits,
                                        0, 2, huge_page_tlb_size));
        } else {
          options.table_factory.reset(
              new TestPlainTableFactory(&expect_bloom_not_match, 16, bloom_bits,
                                        0.75, 16, huge_page_tlb_size));
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
        dbfull()->TEST_FlushMemTable();
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

        delete iter;
      }
    }
  }
}

namespace {
std::string MakeLongKey(size_t length, char c) {
  return std::string(length, c);
}
}  // namespace

TEST(PlainTableDBTest, IteratorLargeKeys) {
  Options options = CurrentOptions();
  options.table_factory.reset(NewTotalOrderPlainTableFactory(0, 0, 16));
  options.create_if_missing = true;
  options.prefix_extractor.reset();
  DestroyAndReopen(&options);

  std::string key_list[] = {
      MakeLongKey(30, '0'),
      MakeLongKey(16, '1'),
      MakeLongKey(32, '2'),
      MakeLongKey(60, '3'),
      MakeLongKey(90, '4'),
      MakeLongKey(50, '5'),
      MakeLongKey(26, '6')
  };

  for (size_t i = 0; i < 7; i++) {
    ASSERT_OK(Put(key_list[i], std::to_string(i)));
  }

  dbfull()->TEST_FlushMemTable();

  Iterator* iter = dbfull()->NewIterator(ReadOptions());
  iter->Seek(key_list[0]);

  for (size_t i = 0; i < 7; i++) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(key_list[i], iter->key().ToString());
    ASSERT_EQ(std::to_string(i), iter->value().ToString());
    iter->Next();
  }

  ASSERT_TRUE(!iter->Valid());

  delete iter;
}

// A test comparator which compare two strings in this way:
// (1) first compare prefix of 8 bytes in alphabet order,
// (2) if two strings share the same prefix, sort the other part of the string
//     in the reverse alphabet order.
class SimpleSuffixReverseComparator : public Comparator {
 public:
  SimpleSuffixReverseComparator() {}

  virtual const char* Name() const { return "SimpleSuffixReverseComparator"; }

  virtual int Compare(const Slice& a, const Slice& b) const {
    Slice prefix_a = Slice(a.data(), 8);
    Slice prefix_b = Slice(b.data(), 8);
    int prefix_comp = prefix_a.compare(prefix_b);
    if (prefix_comp != 0) {
      return prefix_comp;
    } else {
      Slice suffix_a = Slice(a.data() + 8, a.size() - 8);
      Slice suffix_b = Slice(b.data() + 8, b.size() - 8);
      return -(suffix_a.compare(suffix_b));
    }
  }
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const {}

  virtual void FindShortSuccessor(std::string* key) const {}
};

TEST(PlainTableDBTest, IteratorReverseSuffixComparator) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // Set only one bucket to force bucket conflict.
  // Test index interval for the same prefix to be 1, 2 and 4
  SimpleSuffixReverseComparator comp;
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
  dbfull()->TEST_FlushMemTable();
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

TEST(PlainTableDBTest, HashBucketConflict) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (unsigned char i = 1; i <= 3; i++) {
      Options options = CurrentOptions();
      options.create_if_missing = true;
      // Set only one bucket to force bucket conflict.
      // Test index interval for the same prefix to be 1, 2 and 4
      options.table_factory.reset(
          NewTotalOrderPlainTableFactory(16, 0, 2 ^ i, huge_page_tlb_size));
      DestroyAndReopen(&options);
      ASSERT_OK(Put("5000000000000fo0", "v1"));
      ASSERT_OK(Put("5000000000000fo1", "v2"));
      ASSERT_OK(Put("5000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo0", "v3"));
      ASSERT_OK(Put("2000000000000fo1", "v4"));
      ASSERT_OK(Put("2000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo3", "v"));

      dbfull()->TEST_FlushMemTable();

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

      delete iter;
    }
  }
}

TEST(PlainTableDBTest, HashBucketConflictReverseSuffixComparator) {
  for (size_t huge_page_tlb_size = 0; huge_page_tlb_size <= 2 * 1024 * 1024;
       huge_page_tlb_size += 2 * 1024 * 1024) {
    for (unsigned char i = 1; i <= 3; i++) {
      Options options = CurrentOptions();
      options.create_if_missing = true;
      SimpleSuffixReverseComparator comp;
      options.comparator = &comp;
      // Set only one bucket to force bucket conflict.
      // Test index interval for the same prefix to be 1, 2 and 4
      options.table_factory.reset(
          NewTotalOrderPlainTableFactory(16, 0, 2 ^ i, huge_page_tlb_size));
      DestroyAndReopen(&options);
      ASSERT_OK(Put("5000000000000fo0", "v1"));
      ASSERT_OK(Put("5000000000000fo1", "v2"));
      ASSERT_OK(Put("5000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo0", "v3"));
      ASSERT_OK(Put("2000000000000fo1", "v4"));
      ASSERT_OK(Put("2000000000000fo2", "v"));
      ASSERT_OK(Put("2000000000000fo3", "v"));

      dbfull()->TEST_FlushMemTable();

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

      delete iter;
    }
  }
}

TEST(PlainTableDBTest, NonExistingKeyToNonEmptyBucket) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // Set only one bucket to force bucket conflict.
  // Test index interval for the same prefix to be 1, 2 and 4
  options.table_factory.reset(NewTotalOrderPlainTableFactory(16, 0, 5));
  DestroyAndReopen(&options);
  ASSERT_OK(Put("5000000000000fo0", "v1"));
  ASSERT_OK(Put("5000000000000fo1", "v2"));
  ASSERT_OK(Put("5000000000000fo2", "v3"));

  dbfull()->TEST_FlushMemTable();

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

  delete iter;
}

static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key_______%06d", i);
  return std::string(buf);
}

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

TEST(PlainTableDBTest, CompactionTrigger) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 << 10; //100KB
  options.num_levels = 3;
  options.max_mem_compaction_level = 0;
  options.level0_file_num_compaction_trigger = 3;
  Reopen(&options);

  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
      num++) {
    std::vector<std::string> values;
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      values.push_back(RandomString(&rnd, 10000));
      ASSERT_OK(Put(Key(i), values[i]));
    }
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(NumTableFilesAtLevel(0), num + 1);
  }

  //generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(RandomString(&rnd, 10000));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
