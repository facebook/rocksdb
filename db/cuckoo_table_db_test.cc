//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "table/cuckoo/cuckoo_table_factory.h"
#include "table/cuckoo/cuckoo_table_reader.h"
#include "table/meta_blocks.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class CuckooTableDBTest : public testing::Test {
 private:
  std::string dbname_;
  Env* env_;
  DB* db_;

 public:
  CuckooTableDBTest() : env_(Env::Default()) {
    dbname_ = test::PerThreadDBPath("cuckoo_table_db_test");
    EXPECT_OK(DestroyDB(dbname_, Options()));
    db_ = nullptr;
    Reopen();
  }

  ~CuckooTableDBTest() override {
    delete db_;
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  Options CurrentOptions() {
    Options options;
    options.level_compaction_dynamic_level_bytes = false;
    options.table_factory.reset(NewCuckooTableFactory());
    options.memtable_factory.reset(NewHashLinkListRepFactory(4, 0, 3, true));
    options.allow_mmap_reads = true;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = false;
    return options;
  }

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_); }

  // The following util methods are copied from plain_table_db_test.
  void Reopen(Options* options = nullptr) {
    delete db_;
    db_ = nullptr;
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts = CurrentOptions();
      opts.create_if_missing = true;
    }
    ASSERT_OK(DB::Open(opts, dbname_, &db_));
  }

  void DestroyAndReopen(Options* options) {
    assert(options);
    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, *options));
    Reopen(options);
  }

  Status Put(const Slice& k, const Slice& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  std::string Get(const std::string& k) {
    ReadOptions options;
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
};

TEST_F(CuckooTableDBTest, Flush) {
  // Try with empty DB first.
  ASSERT_TRUE(dbfull() != nullptr);
  ASSERT_EQ("NOT_FOUND", Get("key2"));

  // Add some values to db.
  Options options = CurrentOptions();
  Reopen(&options);

  ASSERT_OK(Put("key1", "v1"));
  ASSERT_OK(Put("key2", "v2"));
  ASSERT_OK(Put("key3", "v3"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  TablePropertiesCollection ptc;
  ASSERT_OK(static_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc));
  VerifySstUniqueIds(ptc);
  ASSERT_EQ(1U, ptc.size());
  ASSERT_EQ(3U, ptc.begin()->second->num_entries);
  ASSERT_EQ("1", FilesPerLevel());

  ASSERT_EQ("v1", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("v3", Get("key3"));
  ASSERT_EQ("NOT_FOUND", Get("key4"));

  // Now add more keys and flush.
  ASSERT_OK(Put("key4", "v4"));
  ASSERT_OK(Put("key5", "v5"));
  ASSERT_OK(Put("key6", "v6"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  ASSERT_OK(static_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc));
  VerifySstUniqueIds(ptc);
  ASSERT_EQ(2U, ptc.size());
  auto row = ptc.begin();
  ASSERT_EQ(3U, row->second->num_entries);
  ASSERT_EQ(3U, (++row)->second->num_entries);
  ASSERT_EQ("2", FilesPerLevel());
  ASSERT_EQ("v1", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("v3", Get("key3"));
  ASSERT_EQ("v4", Get("key4"));
  ASSERT_EQ("v5", Get("key5"));
  ASSERT_EQ("v6", Get("key6"));

  ASSERT_OK(Delete("key6"));
  ASSERT_OK(Delete("key5"));
  ASSERT_OK(Delete("key4"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  ASSERT_OK(static_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc));
  VerifySstUniqueIds(ptc);
  ASSERT_EQ(3U, ptc.size());
  row = ptc.begin();
  ASSERT_EQ(3U, row->second->num_entries);
  ASSERT_EQ(3U, (++row)->second->num_entries);
  ASSERT_EQ(3U, (++row)->second->num_entries);
  ASSERT_EQ("3", FilesPerLevel());
  ASSERT_EQ("v1", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
  ASSERT_EQ("v3", Get("key3"));
  ASSERT_EQ("NOT_FOUND", Get("key4"));
  ASSERT_EQ("NOT_FOUND", Get("key5"));
  ASSERT_EQ("NOT_FOUND", Get("key6"));
}

TEST_F(CuckooTableDBTest, FlushWithDuplicateKeys) {
  Options options = CurrentOptions();
  Reopen(&options);
  ASSERT_OK(Put("key1", "v1"));
  ASSERT_OK(Put("key2", "v2"));
  ASSERT_OK(Put("key1", "v3"));  // Duplicate
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  TablePropertiesCollection ptc;
  ASSERT_OK(static_cast<DB*>(dbfull())->GetPropertiesOfAllTables(&ptc));
  VerifySstUniqueIds(ptc);
  ASSERT_EQ(1U, ptc.size());
  ASSERT_EQ(2U, ptc.begin()->second->num_entries);
  ASSERT_EQ("1", FilesPerLevel());
  ASSERT_EQ("v3", Get("key1"));
  ASSERT_EQ("v2", Get("key2"));
}

namespace {
static std::string Key(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "key_______%06d", i);
  return std::string(buf);
}
static std::string Uint64Key(uint64_t i) {
  std::string str;
  str.resize(8);
  memcpy(str.data(), static_cast<void*>(&i), 8);
  return str;
}
}  // namespace.

TEST_F(CuckooTableDBTest, Uint64Comparator) {
  Options options = CurrentOptions();
  options.comparator = test::Uint64Comparator();
  DestroyAndReopen(&options);

  ASSERT_OK(Put(Uint64Key(1), "v1"));
  ASSERT_OK(Put(Uint64Key(2), "v2"));
  ASSERT_OK(Put(Uint64Key(3), "v3"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  ASSERT_EQ("v1", Get(Uint64Key(1)));
  ASSERT_EQ("v2", Get(Uint64Key(2)));
  ASSERT_EQ("v3", Get(Uint64Key(3)));
  ASSERT_EQ("NOT_FOUND", Get(Uint64Key(4)));

  // Add more keys.
  ASSERT_OK(Delete(Uint64Key(2)));  // Delete.
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  ASSERT_OK(Put(Uint64Key(3), "v0"));  // Update.
  ASSERT_OK(Put(Uint64Key(4), "v4"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());
  ASSERT_EQ("v1", Get(Uint64Key(1)));
  ASSERT_EQ("NOT_FOUND", Get(Uint64Key(2)));
  ASSERT_EQ("v0", Get(Uint64Key(3)));
  ASSERT_EQ("v4", Get(Uint64Key(4)));
}

TEST_F(CuckooTableDBTest, CompactionIntoMultipleFiles) {
  // Create a big L0 file and check it compacts into multiple files in L1.
  Options options = CurrentOptions();
  options.write_buffer_size = 270 << 10;
  // Two SST files should be created, each containing 14 keys.
  // Number of buckets will be 16. Total size ~156 KB.
  options.target_file_size_base = 160 << 10;
  Reopen(&options);

  // Write 28 values, each 10016 B ~ 10KB
  for (int idx = 0; idx < 28; ++idx) {
    ASSERT_OK(Put(Key(idx), std::string(10000, 'a' + char(idx))));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ("1", FilesPerLevel());

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow trivial move */));
  ASSERT_EQ("0,2", FilesPerLevel());
  for (int idx = 0; idx < 28; ++idx) {
    ASSERT_EQ(std::string(10000, 'a' + char(idx)), Get(Key(idx)));
  }
}

TEST_F(CuckooTableDBTest, SameKeyInsertedInTwoDifferentFilesAndCompacted) {
  // Insert same key twice so that they go to different SST files. Then wait for
  // compaction and check if the latest value is stored and old value removed.
  Options options = CurrentOptions();
  options.write_buffer_size = 100 << 10;  // 100KB
  options.level0_file_num_compaction_trigger = 2;
  Reopen(&options);

  // Write 11 values, each 10016 B
  for (int idx = 0; idx < 11; ++idx) {
    ASSERT_OK(Put(Key(idx), std::string(10000, 'a')));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ("1", FilesPerLevel());

  // Generate one more file in level-0, and should trigger level-0 compaction
  for (int idx = 0; idx < 11; ++idx) {
    ASSERT_OK(Put(Key(idx), std::string(10000, 'a' + char(idx))));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));

  ASSERT_EQ("0,1", FilesPerLevel());
  for (int idx = 0; idx < 11; ++idx) {
    ASSERT_EQ(std::string(10000, 'a' + char(idx)), Get(Key(idx)));
  }
}

TEST_F(CuckooTableDBTest, AdaptiveTable) {
  Options options = CurrentOptions();

  // Ensure options compatible with PlainTable
  options.prefix_extractor.reset(NewCappedPrefixTransform(8));

  // Write some keys using cuckoo table.
  options.table_factory.reset(NewCuckooTableFactory());
  Reopen(&options);

  ASSERT_OK(Put("key1", "v1"));
  ASSERT_OK(Put("key2", "v2"));
  ASSERT_OK(Put("key3", "v3"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  // Write some keys using plain table.
  std::shared_ptr<TableFactory> block_based_factory(
      NewBlockBasedTableFactory());
  std::shared_ptr<TableFactory> plain_table_factory(NewPlainTableFactory());
  std::shared_ptr<TableFactory> cuckoo_table_factory(NewCuckooTableFactory());
  options.create_if_missing = false;
  options.table_factory.reset(
      NewAdaptiveTableFactory(plain_table_factory, block_based_factory,
                              plain_table_factory, cuckoo_table_factory));
  Reopen(&options);
  ASSERT_OK(Put("key4", "v4"));
  ASSERT_OK(Put("key1", "v5"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  // Write some keys using block based table.
  options.table_factory.reset(
      NewAdaptiveTableFactory(block_based_factory, block_based_factory,
                              plain_table_factory, cuckoo_table_factory));
  Reopen(&options);
  ASSERT_OK(Put("key5", "v6"));
  ASSERT_OK(Put("key2", "v7"));
  ASSERT_OK(dbfull()->TEST_FlushMemTable());

  ASSERT_EQ("v5", Get("key1"));
  ASSERT_EQ("v7", Get("key2"));
  ASSERT_EQ("v3", Get("key3"));
  ASSERT_EQ("v4", Get("key4"));
  ASSERT_EQ("v6", Get("key5"));
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  if (ROCKSDB_NAMESPACE::port::kLittleEndian) {
    ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
  } else {
    fprintf(stderr, "SKIPPED as Cuckoo table doesn't support Big Endian\n");
    return 0;
  }
}
