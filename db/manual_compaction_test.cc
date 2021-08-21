//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Test for issue 178: a manual compaction causes deleted data to reappear.
#include <cstdlib>

#include "port/port.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "test_util/testharness.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

// Reasoning: previously the number was 1100000. Since the keys are written to
// the batch in one write each write will result into one SST file. each write
// will result into one SST file. We reduced the write_buffer_size to 1K to
// basically have the same effect with however less number of keys, which
// results into less test runtime.
const int kNumKeys = 1100;

std::string Key1(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "my_key_%d", i);
  return buf;
}

std::string Key2(int i) {
  return Key1(i) + "_xxx";
}

class ManualCompactionTest : public testing::Test {
 public:
  ManualCompactionTest() {
    // Get rid of any state from an old run.
    dbname_ = test::PerThreadDBPath("rocksdb_manual_compaction_test");
    DestroyDB(dbname_, Options());
  }

  std::string dbname_;
};

class DestroyAllCompactionFilter : public CompactionFilter {
 public:
  DestroyAllCompactionFilter() {}

  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& existing_value,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return existing_value.ToString() == "destroy";
  }

  const char* Name() const override { return "DestroyAllCompactionFilter"; }
};

class LogCompactionFilter : public CompactionFilter {
 public:
  const char* Name() const override { return "LogCompactionFilter"; }

  bool Filter(int level, const Slice& key, const Slice& /*existing_value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    key_level_[key.ToString()] = level;
    return false;
  }

  void Reset() { key_level_.clear(); }

  size_t NumKeys() const { return key_level_.size(); }

  int KeyLevel(const Slice& key) {
    auto it = key_level_.find(key.ToString());
    if (it == key_level_.end()) {
      return -1;
    }
    return it->second;
  }

 private:
  mutable std::map<std::string, int> key_level_;
};

TEST_F(ManualCompactionTest, CompactTouchesAllKeys) {
  for (int iter = 0; iter < 2; ++iter) {
    DB* db;
    Options options;
    if (iter == 0) { // level compaction
      options.num_levels = 3;
      options.compaction_style = kCompactionStyleLevel;
    } else { // universal compaction
      options.compaction_style = kCompactionStyleUniversal;
    }
    options.create_if_missing = true;
    options.compression = kNoCompression;
    options.compaction_filter = new DestroyAllCompactionFilter();
    ASSERT_OK(DB::Open(options, dbname_, &db));

    ASSERT_OK(db->Put(WriteOptions(), Slice("key1"), Slice("destroy")));
    ASSERT_OK(db->Put(WriteOptions(), Slice("key2"), Slice("destroy")));
    ASSERT_OK(db->Put(WriteOptions(), Slice("key3"), Slice("value3")));
    ASSERT_OK(db->Put(WriteOptions(), Slice("key4"), Slice("destroy")));

    Slice key4("key4");
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, &key4));
    Iterator* itr = db->NewIterator(ReadOptions());
    itr->SeekToFirst();
    ASSERT_TRUE(itr->Valid());
    ASSERT_EQ("key3", itr->key().ToString());
    itr->Next();
    ASSERT_TRUE(!itr->Valid());
    delete itr;

    delete options.compaction_filter;
    delete db;
    DestroyDB(dbname_, options);
  }
}

TEST_F(ManualCompactionTest, Test) {
  // Open database.  Disable compression since it affects the creation
  // of layers and the code below is trying to test against a very
  // specific scenario.
  DB* db;
  Options db_options;
  db_options.write_buffer_size = 1024;
  db_options.create_if_missing = true;
  db_options.compression = kNoCompression;
  ASSERT_OK(DB::Open(db_options, dbname_, &db));

  // create first key range
  WriteBatch batch;
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(Key1(i), "value for range 1 key"));
  }
  ASSERT_OK(db->Write(WriteOptions(), &batch));

  // create second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(Key2(i), "value for range 2 key"));
  }
  ASSERT_OK(db->Write(WriteOptions(), &batch));

  // delete second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Delete(Key2(i)));
  }
  ASSERT_OK(db->Write(WriteOptions(), &batch));

  // compact database
  std::string start_key = Key1(0);
  std::string end_key = Key1(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  // commenting out the line below causes the example to work correctly
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), &least, &greatest));

  // count the keys
  Iterator* iter = db->NewIterator(ReadOptions());
  int num_keys = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    num_keys++;
  }
  delete iter;
  ASSERT_EQ(kNumKeys, num_keys) << "Bad number of keys";

  // close database
  delete db;
  DestroyDB(dbname_, Options());
}

TEST_F(ManualCompactionTest, SkipLevel) {
  DB* db;
  Options options;
  options.num_levels = 3;
  // Initially, flushed L0 files won't exceed 100.
  options.level0_file_num_compaction_trigger = 100;
  options.compaction_style = kCompactionStyleLevel;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  LogCompactionFilter* filter = new LogCompactionFilter();
  options.compaction_filter = filter;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  WriteOptions wo;
  FlushOptions fo;
  ASSERT_OK(db->Put(wo, "1", ""));
  ASSERT_OK(db->Flush(fo));
  ASSERT_OK(db->Put(wo, "2", ""));
  ASSERT_OK(db->Flush(fo));
  ASSERT_OK(db->Put(wo, "4", ""));
  ASSERT_OK(db->Put(wo, "8", ""));
  ASSERT_OK(db->Flush(fo));

  {
    // L0: 1, 2, [4, 8]
    // no file has keys in range [5, 7]
    Slice start("5");
    Slice end("7");
    filter->Reset();
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), &start, &end));
    ASSERT_EQ(0, filter->NumKeys());
  }

  {
    // L0: 1, 2, [4, 8]
    // [3, 7] overlaps with 4 in L0
    Slice start("3");
    Slice end("7");
    filter->Reset();
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), &start, &end));
    ASSERT_EQ(2, filter->NumKeys());
    ASSERT_EQ(0, filter->KeyLevel("4"));
    ASSERT_EQ(0, filter->KeyLevel("8"));
  }

  {
    // L0: 1, 2
    // L1: [4, 8]
    // no file has keys in range (-inf, 0]
    Slice end("0");
    filter->Reset();
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, &end));
    ASSERT_EQ(0, filter->NumKeys());
  }

  {
    // L0: 1, 2
    // L1: [4, 8]
    // no file has keys in range [9, inf)
    Slice start("9");
    filter->Reset();
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), &start, nullptr));
    ASSERT_EQ(0, filter->NumKeys());
  }

  {
    // L0: 1, 2
    // L1: [4, 8]
    // [2, 2] overlaps with 2 in L0
    Slice start("2");
    Slice end("2");
    filter->Reset();
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), &start, &end));
    ASSERT_EQ(1, filter->NumKeys());
    ASSERT_EQ(0, filter->KeyLevel("2"));
  }

  {
    // L0: 1
    // L1: 2, [4, 8]
    // [2, 5] overlaps with 2 and [4, 8) in L1, skip L0
    Slice start("2");
    Slice end("5");
    filter->Reset();
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), &start, &end));
    ASSERT_EQ(3, filter->NumKeys());
    ASSERT_EQ(1, filter->KeyLevel("2"));
    ASSERT_EQ(1, filter->KeyLevel("4"));
    ASSERT_EQ(1, filter->KeyLevel("8"));
  }

  {
    // L0: 1
    // L1: [2, 4, 8]
    // [0, inf) overlaps all files
    Slice start("0");
    filter->Reset();
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), &start, nullptr));
    ASSERT_EQ(4, filter->NumKeys());
    // 1 is first compacted to L1 and then further compacted into [2, 4, 8],
    // so finally the logged level for 1 is L1.
    ASSERT_EQ(1, filter->KeyLevel("1"));
    ASSERT_EQ(1, filter->KeyLevel("2"));
    ASSERT_EQ(1, filter->KeyLevel("4"));
    ASSERT_EQ(1, filter->KeyLevel("8"));
  }

  delete filter;
  delete db;
  DestroyDB(dbname_, options);
}

}  // anonymous namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
