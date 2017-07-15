//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <atomic>
#include <cstdlib>
#include <functional>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/wal_filter.h"

namespace rocksdb {

class DBTest2 : public DBTestBase {
 public:
  DBTest2() : DBTestBase("/db_test2") {}
};

class PrefixFullBloomWithReverseComparator
    : public DBTestBase,
      public ::testing::WithParamInterface<bool> {
 public:
  PrefixFullBloomWithReverseComparator()
      : DBTestBase("/prefix_bloom_reverse") {}
  virtual void SetUp() override { if_cache_filter_ = GetParam(); }
  bool if_cache_filter_;
};

TEST_P(PrefixFullBloomWithReverseComparator,
       PrefixFullBloomWithReverseComparator) {
  Options options = last_options_;
  options.comparator = ReverseBytewiseComparator();
  options.prefix_extractor.reset(NewCappedPrefixTransform(3));
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  if (if_cache_filter_) {
    bbto.no_block_cache = false;
    bbto.cache_index_and_filter_blocks = true;
    bbto.block_cache = NewLRUCache(1);
  }
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  ASSERT_OK(dbfull()->Put(WriteOptions(), "bar123", "foo"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "bar234", "foo2"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "foo123", "foo3"));

  dbfull()->Flush(FlushOptions());

  if (bbto.block_cache) {
    bbto.block_cache->EraseUnRefEntries();
  }

  unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("bar345");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar234", iter->key().ToString());
  ASSERT_EQ("foo2", iter->value().ToString());
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar123", iter->key().ToString());
  ASSERT_EQ("foo", iter->value().ToString());

  iter->Seek("foo234");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo123", iter->key().ToString());
  ASSERT_EQ("foo3", iter->value().ToString());

  iter->Seek("bar");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
}

INSTANTIATE_TEST_CASE_P(PrefixFullBloomWithReverseComparator,
                        PrefixFullBloomWithReverseComparator, testing::Bool());

TEST_F(DBTest2, IteratorPropertyVersionNumber) {
  Put("", "");
  Iterator* iter1 = db_->NewIterator(ReadOptions());
  std::string prop_value;
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  Put("", "");
  Flush();

  Iterator* iter2 = db_->NewIterator(ReadOptions());
  ASSERT_OK(
      iter2->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number2 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_GT(version_number2, version_number1);

  Put("", "");

  Iterator* iter3 = db_->NewIterator(ReadOptions());
  ASSERT_OK(
      iter3->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number3 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_EQ(version_number2, version_number3);

  iter1->SeekToFirst();
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1_new =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));
  ASSERT_EQ(version_number1, version_number1_new);

  delete iter1;
  delete iter2;
  delete iter3;
}

TEST_F(DBTest2, CacheIndexAndFilterWithDBRestart) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  Put(1, "a", "begin");
  Put(1, "z", "end");
  ASSERT_OK(Flush(1));
  TryReopenWithColumnFamilies({"default", "pikachu"}, options);

  std::string value;
  value = Get(1, "a");
}

TEST_F(DBTest2, MaxSuccessiveMergesChangeWithDBRecovery) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.max_successive_merges = 3;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  Put("poi", "Finch");
  db_->Merge(WriteOptions(), "poi", "Reese");
  db_->Merge(WriteOptions(), "poi", "Shaw");
  db_->Merge(WriteOptions(), "poi", "Root");
  options.max_successive_merges = 2;
  Reopen(options);
}

#ifndef ROCKSDB_LITE
class DBTestSharedWriteBufferAcrossCFs
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  DBTestSharedWriteBufferAcrossCFs()
      : DBTestBase("/db_test_shared_write_buffer") {}
  void SetUp() override {
    use_old_interface_ = std::get<0>(GetParam());
    cost_cache_ = std::get<1>(GetParam());
  }
  bool use_old_interface_;
  bool cost_cache_;
};

TEST_P(DBTestSharedWriteBufferAcrossCFs, SharedWriteBufferAcrossCFs) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;

  // Avoid undeterministic value by malloc_usable_size();
  // Force arena block size to 1
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::Arena:0", [&](void* arg) {
        size_t* block_size = static_cast<size_t*>(arg);
        *block_size = 1;
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::AllocateNewBlock:0", [&](void* arg) {
        std::pair<size_t*, size_t*>* pair =
            static_cast<std::pair<size_t*, size_t*>*>(arg);
        *std::get<0>(*pair) = *std::get<1>(*pair);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // The total soft write buffer size is about 105000
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 1024 * 1024);

  if (use_old_interface_) {
    options.db_write_buffer_size = 120000;  // this is the real limit
  } else if (!cost_cache_) {
    options.write_buffer_manager.reset(new WriteBufferManager(114285));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(114285, cache));
  }
  options.write_buffer_size = 500000;  // this is never hit
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  WriteOptions wo;
  wo.disableWAL = true;

  std::function<void()> wait_flush = [&]() {
    dbfull()->TEST_WaitForFlushMemTable(handles_[0]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[3]);
  };

  // Create some data and flush "default" and "nikitich" so that they
  // are newer CFs created.
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  Flush(3);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  Flush(0);
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
            static_cast<uint64_t>(1));
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
            static_cast<uint64_t>(1));

  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 1024 * 1024);
    ASSERT_LE(cache->GetUsage(), 2 * 1024 * 1024);
  }
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(60000), wo));
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 1024 * 1024);
    ASSERT_LE(cache->GetUsage(), 2 * 1024 * 1024);
  }
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  // No flush should trigger
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(1));
  }

  // Trigger a flush. Flushing "nikitich".
  ASSERT_OK(Put(3, Key(2), DummyString(30000), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Without hitting the threshold, no flush should trigger.
  ASSERT_OK(Put(2, Key(1), DummyString(30000), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Hit the write buffer limit again. "default"
  // will have been flushed.
  ASSERT_OK(Put(2, Key(2), DummyString(10000), wo));
  wait_flush();
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Trigger another flush. This time "dobrynia". "pikachu" should not
  // be flushed, althrough it was never flushed.
  ASSERT_OK(Put(1, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(80000), wo));
  wait_flush();
  ASSERT_OK(Put(1, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();

  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "nikitich"),
              static_cast<uint64_t>(2));
  }
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 1024 * 1024);
    Close();
    options.write_buffer_manager.reset();
    ASSERT_LT(cache->GetUsage(), 1024 * 1024);
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBTestSharedWriteBufferAcrossCFs,
                        DBTestSharedWriteBufferAcrossCFs,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, false),
                                          std::make_tuple(false, true)));

TEST_F(DBTest2, SharedWriteBufferLimitAcrossDB) {
  std::string dbname2 = test::TmpDir(env_) + "/db_shared_wb_db2";
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  // Avoid undeterministic value by malloc_usable_size();
  // Force arena block size to 1
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::Arena:0", [&](void* arg) {
        size_t* block_size = static_cast<size_t*>(arg);
        *block_size = 1;
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "Arena::AllocateNewBlock:0", [&](void* arg) {
        std::pair<size_t*, size_t*>* pair =
            static_cast<std::pair<size_t*, size_t*>*>(arg);
        *std::get<0>(*pair) = *std::get<1>(*pair);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  options.write_buffer_size = 500000;  // this is never hit
  // Use a write buffer total size so that the soft limit is about
  // 105000.
  options.write_buffer_manager.reset(new WriteBufferManager(120000));
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  ASSERT_OK(DestroyDB(dbname2, options));
  DB* db2 = nullptr;
  ASSERT_OK(DB::Open(options, dbname2, &db2));

  WriteOptions wo;
  wo.disableWAL = true;

  std::function<void()> wait_flush = [&]() {
    dbfull()->TEST_WaitForFlushMemTable(handles_[0]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
    dbfull()->TEST_WaitForFlushMemTable(handles_[2]);
    static_cast<DBImpl*>(db2)->TEST_WaitForFlushMemTable();
  };

  // Trigger a flush on cf2
  ASSERT_OK(Put(2, Key(1), DummyString(70000), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(20000), wo));
  wait_flush();

  // Insert to DB2
  ASSERT_OK(db2->Put(wo, Key(2), DummyString(20000)));
  wait_flush();

  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  static_cast<DBImpl*>(db2)->TEST_WaitForFlushMemTable();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default") +
                  GetNumberOfSstFilesForColumnFamily(db_, "cf1") +
                  GetNumberOfSstFilesForColumnFamily(db_, "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2, "default"),
              static_cast<uint64_t>(0));
  }

  // Triggering to flush another CF in DB1
  ASSERT_OK(db2->Put(wo, Key(2), DummyString(70000)));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf1"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2, "default"),
              static_cast<uint64_t>(0));
  }

  // Triggering flush in DB2.
  ASSERT_OK(db2->Put(wo, Key(3), DummyString(40000)));
  wait_flush();
  ASSERT_OK(db2->Put(wo, Key(1), DummyString(1)));
  wait_flush();
  static_cast<DBImpl*>(db2)->TEST_WaitForFlushMemTable();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf1"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_, "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2, "default"),
              static_cast<uint64_t>(1));
  }

  delete db2;
  ASSERT_OK(DestroyDB(dbname2, options));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

namespace {
  void ValidateKeyExistence(DB* db, const std::vector<Slice>& keys_must_exist,
    const std::vector<Slice>& keys_must_not_exist) {
    // Ensure that expected keys exist
    std::vector<std::string> values;
    if (keys_must_exist.size() > 0) {
      std::vector<Status> status_list =
        db->MultiGet(ReadOptions(), keys_must_exist, &values);
      for (size_t i = 0; i < keys_must_exist.size(); i++) {
        ASSERT_OK(status_list[i]);
      }
    }

    // Ensure that given keys don't exist
    if (keys_must_not_exist.size() > 0) {
      std::vector<Status> status_list =
        db->MultiGet(ReadOptions(), keys_must_not_exist, &values);
      for (size_t i = 0; i < keys_must_not_exist.size(); i++) {
        ASSERT_TRUE(status_list[i].IsNotFound());
      }
    }
  }

}  // namespace

TEST_F(DBTest2, WalFilterTest) {
  class TestWalFilter : public WalFilter {
  private:
    // Processing option that is requested to be applied at the given index
    WalFilter::WalProcessingOption wal_processing_option_;
    // Index at which to apply wal_processing_option_
    // At other indexes default wal_processing_option::kContinueProcessing is
    // returned.
    size_t apply_option_at_record_index_;
    // Current record index, incremented with each record encountered.
    size_t current_record_index_;

  public:
    TestWalFilter(WalFilter::WalProcessingOption wal_processing_option,
      size_t apply_option_for_record_index)
      : wal_processing_option_(wal_processing_option),
      apply_option_at_record_index_(apply_option_for_record_index),
      current_record_index_(0) {}

    virtual WalProcessingOption LogRecord(const WriteBatch& batch,
      WriteBatch* new_batch,
      bool* batch_changed) const override {
      WalFilter::WalProcessingOption option_to_return;

      if (current_record_index_ == apply_option_at_record_index_) {
        option_to_return = wal_processing_option_;
      }
      else {
        option_to_return = WalProcessingOption::kContinueProcessing;
      }

      // Filter is passed as a const object for RocksDB to not modify the
      // object, however we modify it for our own purpose here and hence
      // cast the constness away.
      (const_cast<TestWalFilter*>(this)->current_record_index_)++;

      return option_to_return;
    }

    virtual const char* Name() const override { return "TestWalFilter"; }
  };

  // Create 3 batches with two keys each
  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  // Test with all WAL processing options
  for (int option = 0;
    option < static_cast<int>(
    WalFilter::WalProcessingOption::kWalProcessingOptionMax);
  option++) {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({ "pikachu" }, options);

    // Write given keys in given batches
    for (size_t i = 0; i < batch_keys.size(); i++) {
      WriteBatch batch;
      for (size_t j = 0; j < batch_keys[i].size(); j++) {
        batch.Put(handles_[0], batch_keys[i][j], DummyString(1024));
      }
      dbfull()->Write(WriteOptions(), &batch);
    }

    WalFilter::WalProcessingOption wal_processing_option =
      static_cast<WalFilter::WalProcessingOption>(option);

    // Create a test filter that would apply wal_processing_option at the first
    // record
    size_t apply_option_for_record_index = 1;
    TestWalFilter test_wal_filter(wal_processing_option,
      apply_option_for_record_index);

    // Reopen database with option to use WAL filter
    options = OptionsForLogIterTest();
    options.wal_filter = &test_wal_filter;
    Status status =
      TryReopenWithColumnFamilies({ "default", "pikachu" }, options);
    if (wal_processing_option ==
      WalFilter::WalProcessingOption::kCorruptedRecord) {
      assert(!status.ok());
      // In case of corruption we can turn off paranoid_checks to reopen
      // databse
      options.paranoid_checks = false;
      ReopenWithColumnFamilies({ "default", "pikachu" }, options);
    }
    else {
      assert(status.ok());
    }

    // Compute which keys we expect to be found
    // and which we expect not to be found after recovery.
    std::vector<Slice> keys_must_exist;
    std::vector<Slice> keys_must_not_exist;
    switch (wal_processing_option) {
    case WalFilter::WalProcessingOption::kCorruptedRecord:
    case WalFilter::WalProcessingOption::kContinueProcessing: {
      fprintf(stderr, "Testing with complete WAL processing\n");
      // we expect all records to be processed
      for (size_t i = 0; i < batch_keys.size(); i++) {
        for (size_t j = 0; j < batch_keys[i].size(); j++) {
          keys_must_exist.push_back(Slice(batch_keys[i][j]));
        }
      }
      break;
    }
    case WalFilter::WalProcessingOption::kIgnoreCurrentRecord: {
      fprintf(stderr,
        "Testing with ignoring record %" ROCKSDB_PRIszt " only\n",
        apply_option_for_record_index);
      // We expect the record with apply_option_for_record_index to be not
      // found.
      for (size_t i = 0; i < batch_keys.size(); i++) {
        for (size_t j = 0; j < batch_keys[i].size(); j++) {
          if (i == apply_option_for_record_index) {
            keys_must_not_exist.push_back(Slice(batch_keys[i][j]));
          }
          else {
            keys_must_exist.push_back(Slice(batch_keys[i][j]));
          }
        }
      }
      break;
    }
    case WalFilter::WalProcessingOption::kStopReplay: {
      fprintf(stderr,
        "Testing with stopping replay from record %" ROCKSDB_PRIszt
        "\n",
        apply_option_for_record_index);
      // We expect records beyond apply_option_for_record_index to be not
      // found.
      for (size_t i = 0; i < batch_keys.size(); i++) {
        for (size_t j = 0; j < batch_keys[i].size(); j++) {
          if (i >= apply_option_for_record_index) {
            keys_must_not_exist.push_back(Slice(batch_keys[i][j]));
          }
          else {
            keys_must_exist.push_back(Slice(batch_keys[i][j]));
          }
        }
      }
      break;
    }
    default:
      assert(false);  // unhandled case
    }

    bool checked_after_reopen = false;

    while (true) {
      // Ensure that expected keys exists
      // and not expected keys don't exist after recovery
      ValidateKeyExistence(db_, keys_must_exist, keys_must_not_exist);

      if (checked_after_reopen) {
        break;
      }

      // reopen database again to make sure previous log(s) are not used
      //(even if they were skipped)
      // reopn database with option to use WAL filter
      options = OptionsForLogIterTest();
      ReopenWithColumnFamilies({ "default", "pikachu" }, options);

      checked_after_reopen = true;
    }
  }
}

TEST_F(DBTest2, WalFilterTestWithChangeBatch) {
  class ChangeBatchHandler : public WriteBatch::Handler {
  private:
    // Batch to insert keys in
    WriteBatch* new_write_batch_;
    // Number of keys to add in the new batch
    size_t num_keys_to_add_in_new_batch_;
    // Number of keys added to new batch
    size_t num_keys_added_;

  public:
    ChangeBatchHandler(WriteBatch* new_write_batch,
      size_t num_keys_to_add_in_new_batch)
      : new_write_batch_(new_write_batch),
      num_keys_to_add_in_new_batch_(num_keys_to_add_in_new_batch),
      num_keys_added_(0) {}
    virtual void Put(const Slice& key, const Slice& value) override {
      if (num_keys_added_ < num_keys_to_add_in_new_batch_) {
        new_write_batch_->Put(key, value);
        ++num_keys_added_;
      }
    }
  };

  class TestWalFilterWithChangeBatch : public WalFilter {
  private:
    // Index at which to start changing records
    size_t change_records_from_index_;
    // Number of keys to add in the new batch
    size_t num_keys_to_add_in_new_batch_;
    // Current record index, incremented with each record encountered.
    size_t current_record_index_;

  public:
    TestWalFilterWithChangeBatch(size_t change_records_from_index,
      size_t num_keys_to_add_in_new_batch)
      : change_records_from_index_(change_records_from_index),
      num_keys_to_add_in_new_batch_(num_keys_to_add_in_new_batch),
      current_record_index_(0) {}

    virtual WalProcessingOption LogRecord(const WriteBatch& batch,
      WriteBatch* new_batch,
      bool* batch_changed) const override {
      if (current_record_index_ >= change_records_from_index_) {
        ChangeBatchHandler handler(new_batch, num_keys_to_add_in_new_batch_);
        batch.Iterate(&handler);
        *batch_changed = true;
      }

      // Filter is passed as a const object for RocksDB to not modify the
      // object, however we modify it for our own purpose here and hence
      // cast the constness away.
      (const_cast<TestWalFilterWithChangeBatch*>(this)
        ->current_record_index_)++;

      return WalProcessingOption::kContinueProcessing;
    }

    virtual const char* Name() const override {
      return "TestWalFilterWithChangeBatch";
    }
  };

  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({ "pikachu" }, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      batch.Put(handles_[0], batch_keys[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  // Create a test filter that would apply wal_processing_option at the first
  // record
  size_t change_records_from_index = 1;
  size_t num_keys_to_add_in_new_batch = 1;
  TestWalFilterWithChangeBatch test_wal_filter_with_change_batch(
    change_records_from_index, num_keys_to_add_in_new_batch);

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_with_change_batch;
  ReopenWithColumnFamilies({ "default", "pikachu" }, options);

  // Ensure that all keys exist before change_records_from_index_
  // And after that index only single key exists
  // as our filter adds only single key for each batch
  std::vector<Slice> keys_must_exist;
  std::vector<Slice> keys_must_not_exist;

  for (size_t i = 0; i < batch_keys.size(); i++) {
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      if (i >= change_records_from_index && j >= num_keys_to_add_in_new_batch) {
        keys_must_not_exist.push_back(Slice(batch_keys[i][j]));
      }
      else {
        keys_must_exist.push_back(Slice(batch_keys[i][j]));
      }
    }
  }

  bool checked_after_reopen = false;

  while (true) {
    // Ensure that expected keys exists
    // and not expected keys don't exist after recovery
    ValidateKeyExistence(db_, keys_must_exist, keys_must_not_exist);

    if (checked_after_reopen) {
      break;
    }

    // reopen database again to make sure previous log(s) are not used
    //(even if they were skipped)
    // reopn database with option to use WAL filter
    options = OptionsForLogIterTest();
    ReopenWithColumnFamilies({ "default", "pikachu" }, options);

    checked_after_reopen = true;
  }
}

TEST_F(DBTest2, WalFilterTestWithChangeBatchExtraKeys) {
  class TestWalFilterWithChangeBatchAddExtraKeys : public WalFilter {
  public:
    virtual WalProcessingOption LogRecord(const WriteBatch& batch,
      WriteBatch* new_batch,
      bool* batch_changed) const override {
      *new_batch = batch;
      new_batch->Put("key_extra", "value_extra");
      *batch_changed = true;
      return WalProcessingOption::kContinueProcessing;
    }

    virtual const char* Name() const override {
      return "WalFilterTestWithChangeBatchExtraKeys";
    }
  };

  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({ "pikachu" }, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      batch.Put(handles_[0], batch_keys[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  // Create a test filter that would add extra keys
  TestWalFilterWithChangeBatchAddExtraKeys test_wal_filter_extra_keys;

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_extra_keys;
  Status status = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(status.IsNotSupported());

  // Reopen without filter, now reopen should succeed - previous
  // attempt to open must not have altered the db.
  options = OptionsForLogIterTest();
  ReopenWithColumnFamilies({ "default", "pikachu" }, options);

  std::vector<Slice> keys_must_exist;
  std::vector<Slice> keys_must_not_exist;  // empty vector

  for (size_t i = 0; i < batch_keys.size(); i++) {
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      keys_must_exist.push_back(Slice(batch_keys[i][j]));
    }
  }

  ValidateKeyExistence(db_, keys_must_exist, keys_must_not_exist);
}

TEST_F(DBTest2, WalFilterTestWithColumnFamilies) {
  class TestWalFilterWithColumnFamilies : public WalFilter {
  private:
    // column_family_id -> log_number map (provided to WALFilter)
    std::map<uint32_t, uint64_t> cf_log_number_map_;
    // column_family_name -> column_family_id map (provided to WALFilter)
    std::map<std::string, uint32_t> cf_name_id_map_;
    // column_family_name -> keys_found_in_wal map
    // We store keys that are applicable to the column_family
    // during recovery (i.e. aren't already flushed to SST file(s))
    // for verification against the keys we expect.
    std::map<uint32_t, std::vector<std::string>> cf_wal_keys_;
  public:
    virtual void ColumnFamilyLogNumberMap(
      const std::map<uint32_t, uint64_t>& cf_lognumber_map,
      const std::map<std::string, uint32_t>& cf_name_id_map) override {
      cf_log_number_map_ = cf_lognumber_map;
      cf_name_id_map_ = cf_name_id_map;
    }

    virtual WalProcessingOption LogRecordFound(unsigned long long log_number,
      const std::string& log_file_name,
      const WriteBatch& batch,
      WriteBatch* new_batch,
      bool* batch_changed) override {
      class LogRecordBatchHandler : public WriteBatch::Handler {
      private:
        const std::map<uint32_t, uint64_t> & cf_log_number_map_;
        std::map<uint32_t, std::vector<std::string>> & cf_wal_keys_;
        unsigned long long log_number_;
      public:
        LogRecordBatchHandler(unsigned long long current_log_number,
          const std::map<uint32_t, uint64_t> & cf_log_number_map,
          std::map<uint32_t, std::vector<std::string>> & cf_wal_keys) :
          cf_log_number_map_(cf_log_number_map),
          cf_wal_keys_(cf_wal_keys),
          log_number_(current_log_number){}

        virtual Status PutCF(uint32_t column_family_id, const Slice& key,
          const Slice& /*value*/) override {
          auto it = cf_log_number_map_.find(column_family_id);
          assert(it != cf_log_number_map_.end());
          unsigned long long log_number_for_cf = it->second;
          // If the current record is applicable for column_family_id
          // (i.e. isn't flushed to SST file(s) for column_family_id)
          // add it to the cf_wal_keys_ map for verification.
          if (log_number_ >= log_number_for_cf) {
            cf_wal_keys_[column_family_id].push_back(std::string(key.data(),
              key.size()));
          }
          return Status::OK();
        }
      } handler(log_number, cf_log_number_map_, cf_wal_keys_);

      batch.Iterate(&handler);

      return WalProcessingOption::kContinueProcessing;
    }

    virtual const char* Name() const override {
      return "WalFilterTestWithColumnFamilies";
    }

    const std::map<uint32_t, std::vector<std::string>>& GetColumnFamilyKeys() {
      return cf_wal_keys_;
    }

    const std::map<std::string, uint32_t> & GetColumnFamilyNameIdMap() {
      return cf_name_id_map_;
    }
  };

  std::vector<std::vector<std::string>> batch_keys_pre_flush(3);

  batch_keys_pre_flush[0].push_back("key1");
  batch_keys_pre_flush[0].push_back("key2");
  batch_keys_pre_flush[1].push_back("key3");
  batch_keys_pre_flush[1].push_back("key4");
  batch_keys_pre_flush[2].push_back("key5");
  batch_keys_pre_flush[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({ "pikachu" }, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys_pre_flush.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys_pre_flush[i].size(); j++) {
      batch.Put(handles_[0], batch_keys_pre_flush[i][j], DummyString(1024));
      batch.Put(handles_[1], batch_keys_pre_flush[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  //Flush default column-family
  db_->Flush(FlushOptions(), handles_[0]);

  // Do some more writes
  std::vector<std::vector<std::string>> batch_keys_post_flush(3);

  batch_keys_post_flush[0].push_back("key7");
  batch_keys_post_flush[0].push_back("key8");
  batch_keys_post_flush[1].push_back("key9");
  batch_keys_post_flush[1].push_back("key10");
  batch_keys_post_flush[2].push_back("key11");
  batch_keys_post_flush[2].push_back("key12");

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      batch.Put(handles_[0], batch_keys_post_flush[i][j], DummyString(1024));
      batch.Put(handles_[1], batch_keys_post_flush[i][j], DummyString(1024));
    }
    dbfull()->Write(WriteOptions(), &batch);
  }

  // On Recovery we should only find the second batch applicable to default CF
  // But both batches applicable to pikachu CF

  // Create a test filter that would add extra keys
  TestWalFilterWithColumnFamilies test_wal_filter_column_families;

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_column_families;
  Status status =
    TryReopenWithColumnFamilies({ "default", "pikachu" }, options);
  ASSERT_TRUE(status.ok());

  // verify that handles_[0] only has post_flush keys
  // while handles_[1] has pre and post flush keys
  auto cf_wal_keys = test_wal_filter_column_families.GetColumnFamilyKeys();
  auto name_id_map = test_wal_filter_column_families.GetColumnFamilyNameIdMap();
  size_t index = 0;
  auto keys_cf = cf_wal_keys[name_id_map[kDefaultColumnFamilyName]];
  //default column-family, only post_flush keys are expected
  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_post_flush[i][j]);
      ASSERT_TRUE(key_from_the_log.compare(batch_key) == 0);
    }
  }
  ASSERT_TRUE(index == keys_cf.size());

  index = 0;
  keys_cf = cf_wal_keys[name_id_map["pikachu"]];
  //pikachu column-family, all keys are expected
  for (size_t i = 0; i < batch_keys_pre_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_pre_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_pre_flush[i][j]);
      ASSERT_TRUE(key_from_the_log.compare(batch_key) == 0);
    }
  }

  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_post_flush[i][j]);
      ASSERT_TRUE(key_from_the_log.compare(batch_key) == 0);
    }
  }
  ASSERT_TRUE(index == keys_cf.size());
}

TEST_F(DBTest2, PresetCompressionDict) {
  const size_t kBlockSizeBytes = 4 << 10;
  const size_t kL0FileBytes = 128 << 10;
  const size_t kApproxPerBlockOverheadBytes = 50;
  const int kNumL0Files = 5;

  Options options;
  options.allow_concurrent_memtable_write = false;
  options.arena_block_size = kBlockSizeBytes;
  options.compaction_style = kCompactionStyleUniversal;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(kL0FileBytes / kBlockSizeBytes));
  options.num_levels = 2;
  options.target_file_size_base = kL0FileBytes;
  options.target_file_size_multiplier = 2;
  options.write_buffer_size = kL0FileBytes;
  BlockBasedTableOptions table_options;
  table_options.block_size = kBlockSizeBytes;
  std::vector<CompressionType> compression_types;
  if (Zlib_Supported()) {
    compression_types.push_back(kZlibCompression);
  }
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  compression_types.push_back(kLZ4Compression);
  compression_types.push_back(kLZ4HCCompression);
#endif                          // LZ4_VERSION_NUMBER >= 10400
  if (ZSTD_Supported()) {
    compression_types.push_back(kZSTD);
  }

  for (auto compression_type : compression_types) {
    options.compression = compression_type;
    size_t prev_out_bytes;
    for (int i = 0; i < 2; ++i) {
      // First iteration: compress without preset dictionary
      // Second iteration: compress with preset dictionary
      // To make sure the compression dictionary was actually used, we verify
      // the compressed size is smaller in the second iteration. Also in the
      // second iteration, verify the data we get out is the same data we put
      // in.
      if (i) {
        options.compression_opts.max_dict_bytes = kBlockSizeBytes;
      } else {
        options.compression_opts.max_dict_bytes = 0;
      }

      options.statistics = rocksdb::CreateDBStatistics();
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      CreateAndReopenWithCF({"pikachu"}, options);
      Random rnd(301);
      std::string seq_data =
          RandomString(&rnd, kBlockSizeBytes - kApproxPerBlockOverheadBytes);

      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      for (int j = 0; j < kNumL0Files; ++j) {
        for (size_t k = 0; k < kL0FileBytes / kBlockSizeBytes + 1; ++k) {
          ASSERT_OK(Put(1, Key(static_cast<int>(
                               j * (kL0FileBytes / kBlockSizeBytes) + k)),
                        seq_data));
        }
        dbfull()->TEST_WaitForFlushMemTable(handles_[1]);
        ASSERT_EQ(j + 1, NumTableFilesAtLevel(0, 1));
      }
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr);
      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      ASSERT_GT(NumTableFilesAtLevel(1, 1), 0);

      size_t out_bytes = 0;
      std::vector<std::string> files;
      GetSstFiles(dbname_, &files);
      for (const auto& file : files) {
        uint64_t curr_bytes;
        env_->GetFileSize(dbname_ + "/" + file, &curr_bytes);
        out_bytes += static_cast<size_t>(curr_bytes);
      }

      for (size_t j = 0; j < kNumL0Files * (kL0FileBytes / kBlockSizeBytes);
           j++) {
        ASSERT_EQ(seq_data, Get(1, Key(static_cast<int>(j))));
      }
      if (i) {
        ASSERT_GT(prev_out_bytes, out_bytes);
      }
      prev_out_bytes = out_bytes;
      DestroyAndReopen(options);
    }
  }
}

class CompactionCompressionListener : public EventListener {
 public:
  explicit CompactionCompressionListener(Options* db_options)
      : db_options_(db_options) {}

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    // Figure out last level with files
    int bottommost_level = 0;
    for (int level = 0; level < db->NumberLevels(); level++) {
      std::string files_at_level;
      ASSERT_TRUE(
          db->GetProperty("rocksdb.num-files-at-level" + NumberToString(level),
                          &files_at_level));
      if (files_at_level != "0") {
        bottommost_level = level;
      }
    }

    if (db_options_->bottommost_compression != kDisableCompressionOption &&
        ci.output_level == bottommost_level && ci.output_level >= 2) {
      ASSERT_EQ(ci.compression, db_options_->bottommost_compression);
    } else if (db_options_->compression_per_level.size() != 0) {
      ASSERT_EQ(ci.compression,
                db_options_->compression_per_level[ci.output_level]);
    } else {
      ASSERT_EQ(ci.compression, db_options_->compression);
    }
    max_level_checked = std::max(max_level_checked, ci.output_level);
  }

  int max_level_checked = 0;
  const Options* db_options_;
};

TEST_F(DBTest2, CompressionOptions) {
  if (!Zlib_Supported() || !Snappy_Supported()) {
    return;
  }

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 100;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 7;
  options.max_background_compactions = 1;

  CompactionCompressionListener* listener =
      new CompactionCompressionListener(&options);
  options.listeners.emplace_back(listener);

  const int kKeySize = 5;
  const int kValSize = 20;
  Random rnd(301);

  for (int iter = 0; iter <= 2; iter++) {
    listener->max_level_checked = 0;

    if (iter == 0) {
      // Use different compression algorithms for different levels but
      // always use Zlib for bottommost level
      options.compression_per_level = {kNoCompression,     kNoCompression,
                                       kNoCompression,     kSnappyCompression,
                                       kSnappyCompression, kSnappyCompression,
                                       kZlibCompression};
      options.compression = kNoCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 1) {
      // Use Snappy except for bottommost level use ZLib
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 2) {
      // Use Snappy everywhere
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kDisableCompressionOption;
    }

    DestroyAndReopen(options);
    // Write 10 random files
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 5; j++) {
        ASSERT_OK(
            Put(RandomString(&rnd, kKeySize), RandomString(&rnd, kValSize)));
      }
      ASSERT_OK(Flush());
      dbfull()->TEST_WaitForCompact();
    }

    // Make sure that we wrote enough to check all 7 levels
    ASSERT_EQ(listener->max_level_checked, 6);
  }
}

class CompactionStallTestListener : public EventListener {
 public:
  CompactionStallTestListener() : compacted_files_cnt_(0) {}

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.cf_name, "default");
    ASSERT_EQ(ci.base_input_level, 0);
    ASSERT_EQ(ci.compaction_reason, CompactionReason::kLevelL0FilesNum);
    compacted_files_cnt_ += ci.input_files.size();
  }
  std::atomic<size_t> compacted_files_cnt_;
};

TEST_F(DBTest2, CompactionStall) {
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkCompaction", "DBTest2::CompactionStall:0"},
       {"DBImpl::BGWorkCompaction", "DBTest2::CompactionStall:1"},
       {"DBTest2::CompactionStall:2",
        "DBImpl::NotifyOnCompactionCompleted::UnlockMutex"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_background_compactions = 40;
  CompactionStallTestListener* listener = new CompactionStallTestListener();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  Random rnd(301);

  // 4 Files in L0
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(RandomString(&rnd, 10), RandomString(&rnd, 10)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for compaction to be triggered
  TEST_SYNC_POINT("DBTest2::CompactionStall:0");

  // Clear "DBImpl::BGWorkCompaction" SYNC_POINT since we want to hold it again
  // at DBTest2::CompactionStall::1
  rocksdb::SyncPoint::GetInstance()->ClearTrace();

  // Another 6 L0 files to trigger compaction again
  for (int i = 0; i < 6; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(RandomString(&rnd, 10), RandomString(&rnd, 10)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for another compaction to be triggered
  TEST_SYNC_POINT("DBTest2::CompactionStall:1");

  // Hold NotifyOnCompactionCompleted in the unlock mutex section
  TEST_SYNC_POINT("DBTest2::CompactionStall:2");

  dbfull()->TEST_WaitForCompact();
  ASSERT_LT(NumTableFilesAtLevel(0),
            options.level0_file_num_compaction_trigger);
  ASSERT_GT(listener->compacted_files_cnt_.load(),
            10 - options.level0_file_num_compaction_trigger);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

#endif  // ROCKSDB_LITE

TEST_F(DBTest2, FirstSnapshotTest) {
  Options options;
  options.write_buffer_size = 100000;  // Small write buffer
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // This snapshot will have sequence number 0 what is expected behaviour.
  const Snapshot* s1 = db_->GetSnapshot();

  Put(1, "k1", std::string(100000, 'x'));  // Fill memtable
  Put(1, "k2", std::string(100000, 'y'));  // Trigger flush

  db_->ReleaseSnapshot(s1);
}

class PinL0IndexAndFilterBlocksTest : public DBTestBase,
                                      public testing::WithParamInterface<bool> {
 public:
  PinL0IndexAndFilterBlocksTest() : DBTestBase("/db_pin_l0_index_bloom_test") {}
  virtual void SetUp() override { infinite_max_files_ = GetParam(); }

  void CreateTwoLevels(Options* options, bool close_afterwards) {
    if (infinite_max_files_) {
      options->max_open_files = -1;
    }
    options->create_if_missing = true;
    options->statistics = rocksdb::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(20));
    options->table_factory.reset(new BlockBasedTableFactory(table_options));
    CreateAndReopenWithCF({"pikachu"}, *options);

    Put(1, "a", "begin");
    Put(1, "z", "end");
    ASSERT_OK(Flush(1));
    // move this table to L1
    dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]);

    // reset block cache
    table_options.block_cache = NewLRUCache(64 * 1024);
    options->table_factory.reset(NewBlockBasedTableFactory(table_options));
    TryReopenWithColumnFamilies({"default", "pikachu"}, *options);
    // create new table at L0
    Put(1, "a2", "begin2");
    Put(1, "z2", "end2");
    ASSERT_OK(Flush(1));

    if (close_afterwards) {
      Close();  // This ensures that there is no ref to block cache entries
    }
    table_options.block_cache->EraseUnRefEntries();
  }

  bool infinite_max_files_;
};

TEST_P(PinL0IndexAndFilterBlocksTest,
       IndexAndFilterBlocksOfNewTableAddedToCacheWithPinning) {
  Options options = CurrentOptions();
  if (infinite_max_files_) {
    options.max_open_files = -1;
  }
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));

  // index/filter blocks added to block cache right after table creation.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // only index/filter were added
  ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_ADD));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));

  std::string value;
  // Miss and hit count should remain the same, they're all pinned.
  db_->KeyMayExist(ReadOptions(), handles_[1], "key", &value);
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // Miss and hit count should remain the same, they're all pinned.
  value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
}

TEST_P(PinL0IndexAndFilterBlocksTest,
       MultiLevelIndexAndFilterBlocksCachedWithPinning) {
  Options options = CurrentOptions();
  PinL0IndexAndFilterBlocksTest::CreateTwoLevels(&options, false);
  // get base cache values
  uint64_t fm = TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  uint64_t fh = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  uint64_t im = TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  uint64_t ih = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);

  std::string value;
  // this should be read from L0
  // so cache values don't change
  value = Get(1, "a2");
  ASSERT_EQ(fm, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(im, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // this should be read from L1
  // the file is opened, prefetching results in a cache filter miss
  // the block is loaded and added to the cache,
  // then the get results in a cache hit for L1
  // When we have inifinite max_files, there is still cache miss because we have
  // reset the block cache
  value = Get(1, "a");
  ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
}

TEST_P(PinL0IndexAndFilterBlocksTest, DisablePrefetchingNonL0IndexAndFilter) {
  Options options = CurrentOptions();
  // This ensures that db does not ref anything in the block cache, so
  // EraseUnRefEntries could clear them up.
  bool close_afterwards = true;
  PinL0IndexAndFilterBlocksTest::CreateTwoLevels(&options, close_afterwards);

  // Get base cache values
  uint64_t fm = TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  uint64_t fh = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  uint64_t im = TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  uint64_t ih = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);

  // Reopen database. If max_open_files is set as -1, table readers will be
  // preloaded. This will trigger a BlockBasedTable::Open() and prefetch
  // L0 index and filter. Level 1's prefetching is disabled in DB::Open()
  TryReopenWithColumnFamilies({"default", "pikachu"}, options);

  if (infinite_max_files_) {
    // After reopen, cache miss are increased by one because we read (and only
    // read) filter and index on L0
    ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    // If max_open_files is not -1, we do not preload table readers, so there is
    // no change.
    ASSERT_EQ(fm, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }
  std::string value;
  // this should be read from L0
  value = Get(1, "a2");
  // If max_open_files is -1, we have pinned index and filter in Rep, so there
  // will not be changes in index and filter misses or hits. If max_open_files
  // is not -1, Get() will open a TableReader and prefetch index and filter.
  ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // this should be read from L1
  value = Get(1, "a");
  if (infinite_max_files_) {
    // In inifinite max files case, there's a cache miss in executing Get()
    // because index and filter are not prefetched before.
    ASSERT_EQ(fm + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    // In this case, cache miss will be increased by one in
    // BlockBasedTable::Open() because this is not in DB::Open() code path so we
    // will prefetch L1's index and filter. Cache hit will also be increased by
    // one because Get() will read index and filter from the block cache
    // prefetched in previous Open() call.
    ASSERT_EQ(fm + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }
}

INSTANTIATE_TEST_CASE_P(PinL0IndexAndFilterBlocksTest,
                        PinL0IndexAndFilterBlocksTest, ::testing::Bool());

#ifndef ROCKSDB_LITE
TEST_F(DBTest2, MaxCompactionBytesTest) {
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      new SpecialSkipListFactory(DBTestBase::kNumKeysByGenerateNewRandomFile));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 200 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 100 << 10;
  // Infinite for full compaction.
  options.max_compaction_bytes = options.target_file_size_base * 100;

  Reopen(options);

  Random rnd(301);

  for (int num = 0; num < 8; num++) {
    GenerateNewRandomFile(&rnd);
  }
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,8", FilesPerLevel(0));

  // When compact from Ln -> Ln+1, cut a file if the file overlaps with
  // more than three files in Ln+1.
  options.max_compaction_bytes = options.target_file_size_base * 3;
  Reopen(options);

  GenerateNewRandomFile(&rnd);
  // Add three more small files that overlap with the previous file
  for (int i = 0; i < 3; i++) {
    Put("a", "z");
    ASSERT_OK(Flush());
  }
  dbfull()->TEST_WaitForCompact();

  // Output files to L1 are cut to three pieces, according to
  // options.max_compaction_bytes
  ASSERT_EQ("0,3,8", FilesPerLevel(0));
}

static void UniqueIdCallback(void* arg) {
  int* result = reinterpret_cast<int*>(arg);
  if (*result == -1) {
    *result = 0;
  }

  rocksdb::SyncPoint::GetInstance()->ClearTrace();
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
}

class MockPersistentCache : public PersistentCache {
 public:
  explicit MockPersistentCache(const bool is_compressed, const size_t max_size)
      : is_compressed_(is_compressed), max_size_(max_size) {
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
  }

  virtual ~MockPersistentCache() {}

  PersistentCache::StatsType Stats() override {
    return PersistentCache::StatsType();
  }

  Status Insert(const Slice& page_key, const char* data,
                const size_t size) override {
    MutexLock _(&lock_);

    if (size_ > max_size_) {
      size_ -= data_.begin()->second.size();
      data_.erase(data_.begin());
    }

    data_.insert(std::make_pair(page_key.ToString(), std::string(data, size)));
    size_ += size;
    return Status::OK();
  }

  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override {
    MutexLock _(&lock_);
    auto it = data_.find(page_key.ToString());
    if (it == data_.end()) {
      return Status::NotFound();
    }

    assert(page_key.ToString() == it->first);
    data->reset(new char[it->second.size()]);
    memcpy(data->get(), it->second.c_str(), it->second.size());
    *size = it->second.size();
    return Status::OK();
  }

  bool IsCompressed() override { return is_compressed_; }

  std::string GetPrintableOptions() const override {
    return "MockPersistentCache";
  }

  port::Mutex lock_;
  std::map<std::string, std::string> data_;
  const bool is_compressed_ = true;
  size_t size_ = 0;
  const size_t max_size_ = 10 * 1024;  // 10KiB
};

#ifndef OS_SOLARIS // GetUniqueIdFromFile is not implemented
TEST_F(DBTest2, PersistentCache) {
  int num_iter = 80;

  Options options;
  options.write_buffer_size = 64 * 1024;  // small write buffer
  options.statistics = rocksdb::CreateDBStatistics();
  options = CurrentOptions(options);

  auto bsizes = {/*no block cache*/ 0, /*1M*/ 1 * 1024 * 1024};
  auto types = {/*compressed*/ 1, /*uncompressed*/ 0};
  for (auto bsize : bsizes) {
    for (auto type : types) {
      BlockBasedTableOptions table_options;
      table_options.persistent_cache.reset(
          new MockPersistentCache(type, 10 * 1024));
      table_options.no_block_cache = true;
      table_options.block_cache = bsize ? NewLRUCache(bsize) : nullptr;
      table_options.block_cache_compressed = nullptr;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));

      DestroyAndReopen(options);
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
          str = RandomString(&rnd, 1000);
        }
        values.push_back(str);
        ASSERT_OK(Put(1, Key(i), values[i]));
      }

      // flush all data from memtable so that reads are from block cache
      ASSERT_OK(Flush(1));

      for (int i = 0; i < num_iter; i++) {
        ASSERT_EQ(Get(1, Key(i)), values[i]);
      }

      auto hit = options.statistics->getTickerCount(PERSISTENT_CACHE_HIT);
      auto miss = options.statistics->getTickerCount(PERSISTENT_CACHE_MISS);

      ASSERT_GT(hit, 0);
      ASSERT_GT(miss, 0);
    }
  }
}
#endif // !OS_SOLARIS

namespace {
void CountSyncPoint() {
  TEST_SYNC_POINT_CALLBACK("DBTest2::MarkedPoint", nullptr /* arg */);
}
}  // namespace

TEST_F(DBTest2, SyncPointMarker) {
  std::atomic<int> sync_point_called(0);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBTest2::MarkedPoint",
      [&](void* arg) { sync_point_called.fetch_add(1); });

  // The first dependency enforces Marker can be loaded before MarkedPoint.
  // The second checks that thread 1's MarkedPoint should be disabled here.
  // Execution order:
  // |   Thread 1    |  Thread 2   |
  // |               |   Marker    |
  // |  MarkedPoint  |             |
  // | Thread1First  |             |
  // |               | MarkedPoint |
  rocksdb::SyncPoint::GetInstance()->LoadDependencyAndMarkers(
      {{"DBTest2::SyncPointMarker:Thread1First", "DBTest2::MarkedPoint"}},
      {{"DBTest2::SyncPointMarker:Marker", "DBTest2::MarkedPoint"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  std::function<void()> func1 = [&]() {
    CountSyncPoint();
    TEST_SYNC_POINT("DBTest2::SyncPointMarker:Thread1First");
  };

  std::function<void()> func2 = [&]() {
    TEST_SYNC_POINT("DBTest2::SyncPointMarker:Marker");
    CountSyncPoint();
  };

  auto thread1 = port::Thread(func1);
  auto thread2 = port::Thread(func2);
  thread1.join();
  thread2.join();

  // Callback is only executed once
  ASSERT_EQ(sync_point_called.load(), 1);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
#endif

size_t GetEncodedEntrySize(size_t key_size, size_t value_size) {
  std::string buffer;

  PutVarint32(&buffer, static_cast<uint32_t>(0));
  PutVarint32(&buffer, static_cast<uint32_t>(key_size));
  PutVarint32(&buffer, static_cast<uint32_t>(value_size));

  return buffer.size() + key_size + value_size;
}

TEST_F(DBTest2, ReadAmpBitmap) {
  Options options = CurrentOptions();
  BlockBasedTableOptions bbto;
  uint32_t bytes_per_bit[2] = {1, 16};
  for (size_t k = 0; k < 2; k++) {
    // Disable delta encoding to make it easier to calculate read amplification
    bbto.use_delta_encoding = false;
    // Huge block cache to make it easier to calculate read amplification
    bbto.block_cache = NewLRUCache(1024 * 1024 * 1024);
    bbto.read_amp_bytes_per_bit = bytes_per_bit[k];
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.statistics = rocksdb::CreateDBStatistics();
    DestroyAndReopen(options);

    const size_t kNumEntries = 10000;

    Random rnd(301);
    for (size_t i = 0; i < kNumEntries; i++) {
      ASSERT_OK(Put(Key(static_cast<int>(i)), RandomString(&rnd, 100)));
    }
    ASSERT_OK(Flush());

    Close();
    Reopen(options);

    // Read keys/values randomly and verify that reported read amp error
    // is less than 2%
    uint64_t total_useful_bytes = 0;
    std::set<int> read_keys;
    std::string value;
    for (size_t i = 0; i < kNumEntries * 5; i++) {
      int key_idx = rnd.Next() % kNumEntries;
      std::string key = Key(key_idx);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(key_idx) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        total_useful_bytes +=
            GetEncodedEntrySize(internal_key.size(), value.size());
        read_keys.insert(key_idx);
      }

      double expected_read_amp =
          static_cast<double>(total_useful_bytes) /
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double read_amp =
          static_cast<double>(options.statistics->getTickerCount(
              READ_AMP_ESTIMATE_USEFUL_BYTES)) /
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double error_pct = fabs(expected_read_amp - read_amp) * 100;
      // Error between reported read amp and real read amp should be less than
      // 2%
      EXPECT_LE(error_pct, 2);
    }

    // Make sure we read every thing in the DB (which is smaller than our cache)
    Iterator* iter = db_->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->value().ToString(), Get(iter->key().ToString()));
    }
    delete iter;

    // Read amp is on average 100% since we read all what we loaded in memory
    if (k == 0) {
      ASSERT_EQ(
          options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES),
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES));
    } else {
      ASSERT_NEAR(
          options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES) *
              1.0f /
              options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES),
          1, .01);
    }
  }
}

#ifndef OS_SOLARIS // GetUniqueIdFromFile is not implemented
TEST_F(DBTest2, ReadAmpBitmapLiveInCacheAfterDBClose) {
  if (dbname_.find("dev/shm") != std::string::npos) {
    // /dev/shm dont support getting a unique file id, this mean that
    // running this test on /dev/shm will fail because lru_cache will load
    // the blocks again regardless of them being already in the cache
    return;
  }
  uint32_t bytes_per_bit[2] = {1, 16};
  for (size_t k = 0; k < 2; k++) {
    std::shared_ptr<Cache> lru_cache = NewLRUCache(1024 * 1024 * 1024);
    std::shared_ptr<Statistics> stats = rocksdb::CreateDBStatistics();

    Options options = CurrentOptions();
    BlockBasedTableOptions bbto;
    // Disable delta encoding to make it easier to calculate read amplification
    bbto.use_delta_encoding = false;
    // Huge block cache to make it easier to calculate read amplification
    bbto.block_cache = lru_cache;
    bbto.read_amp_bytes_per_bit = bytes_per_bit[k];
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.statistics = stats;
    DestroyAndReopen(options);

    const int kNumEntries = 10000;

    Random rnd(301);
    for (int i = 0; i < kNumEntries; i++) {
      ASSERT_OK(Put(Key(i), RandomString(&rnd, 100)));
    }
    ASSERT_OK(Flush());

    Close();
    Reopen(options);

    uint64_t total_useful_bytes = 0;
    std::set<int> read_keys;
    std::string value;
    // Iter1: Read half the DB, Read even keys
    // Key(0), Key(2), Key(4), Key(6), Key(8), ...
    for (int i = 0; i < kNumEntries; i += 2) {
      std::string key = Key(i);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(i) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        total_useful_bytes +=
            GetEncodedEntrySize(internal_key.size(), value.size());
        read_keys.insert(i);
      }
    }

    size_t total_useful_bytes_iter1 =
        options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
    size_t total_loaded_bytes_iter1 =
        options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

    Close();
    std::shared_ptr<Statistics> new_statistics = rocksdb::CreateDBStatistics();
    // Destroy old statistics obj that the blocks in lru_cache are pointing to
    options.statistics.reset();
    // Use the statistics object that we just created
    options.statistics = new_statistics;
    Reopen(options);

    // Iter2: Read half the DB, Read odd keys
    // Key(1), Key(3), Key(5), Key(7), Key(9), ...
    for (int i = 1; i < kNumEntries; i += 2) {
      std::string key = Key(i);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(i) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        total_useful_bytes +=
            GetEncodedEntrySize(internal_key.size(), value.size());
        read_keys.insert(i);
      }
    }

    size_t total_useful_bytes_iter2 =
        options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
    size_t total_loaded_bytes_iter2 =
        options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);


    // Read amp is on average 100% since we read all what we loaded in memory
    if (k == 0) {
      ASSERT_EQ(total_useful_bytes_iter1 + total_useful_bytes_iter2,
                total_loaded_bytes_iter1 + total_loaded_bytes_iter2);
    } else {
      ASSERT_NEAR((total_useful_bytes_iter1 + total_useful_bytes_iter2) * 1.0f /
                      (total_loaded_bytes_iter1 + total_loaded_bytes_iter2),
                  1, .01);
    }
  }
}
#endif // !OS_SOLARIS

#ifndef ROCKSDB_LITE
TEST_F(DBTest2, AutomaticCompactionOverlapManualCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.IncreaseParallelism(20);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  auto get_stat = [](std::string level_str, LevelStatType type,
                     std::map<std::string, double> props) {
    auto prop_str =
        level_str + "." +
        InternalStats::compaction_level_stats.at(type).property_name.c_str();
    auto prop_item = props.find(prop_str);
    return prop_item == props.end() ? 0 : prop_item->second;
  };

  // Trivial move 2 files to L2
  ASSERT_EQ("0,0,2", FilesPerLevel());
  // Also test that the stats GetMapProperty API reporting the same result
  {
    std::map<std::string, double> prop;
    ASSERT_TRUE(dbfull()->GetMapProperty("rocksdb.cfstats", &prop));
    ASSERT_EQ(0, get_stat("L0", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(0, get_stat("L1", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(2, get_stat("L2", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(2, get_stat("Sum", LevelStatType::NUM_FILES, prop));
  }

  // While the compaction is running, we will create 2 new files that
  // can fit in L2, these 2 files will be moved to L2 and overlap with
  // the running compaction and break the LSM consistency.
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* arg) {
        ASSERT_OK(
            dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "2"},
                                  {"max_bytes_for_level_base", "1"}}));
        ASSERT_OK(Put(Key(6), "a"));
        ASSERT_OK(Put(Key(7), "a"));
        ASSERT_OK(Flush());

        ASSERT_OK(Put(Key(8), "a"));
        ASSERT_OK(Put(Key(9), "a"));
        ASSERT_OK(Flush());
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Run a manual compaction that will compact the 2 files in L2
  // into 1 file in L2
  cro.exclusive_manual_compaction = false;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  // Test that the stats GetMapProperty API reporting 1 file in L2
  {
    std::map<std::string, double> prop;
    ASSERT_TRUE(dbfull()->GetMapProperty("rocksdb.cfstats", &prop));
    ASSERT_EQ(1, get_stat("L2", LevelStatType::NUM_FILES, prop));
  }
}

TEST_F(DBTest2, ManualCompactionOverlapManualCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 2;
  options.IncreaseParallelism(20);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Trivial move 2 files to L1
  ASSERT_EQ("0,2", FilesPerLevel());

  std::function<void()> bg_manual_compact = [&]() {
    std::string k1 = Key(6);
    std::string k2 = Key(9);
    Slice k1s(k1);
    Slice k2s(k2);
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;
    ASSERT_OK(db_->CompactRange(cro, &k1s, &k2s));
  };
  rocksdb::port::Thread bg_thread;

  // While the compaction is running, we will create 2 new files that
  // can fit in L1, these 2 files will be moved to L1 and overlap with
  // the running compaction and break the LSM consistency.
  std::atomic<bool> flag(false);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* arg) {
        if (flag.exchange(true)) {
          // We want to make sure to call this callback only once
          return;
        }
        ASSERT_OK(Put(Key(6), "a"));
        ASSERT_OK(Put(Key(7), "a"));
        ASSERT_OK(Flush());

        ASSERT_OK(Put(Key(8), "a"));
        ASSERT_OK(Put(Key(9), "a"));
        ASSERT_OK(Flush());

        // Start a non-exclusive manual compaction in a bg thread
        bg_thread = port::Thread(bg_manual_compact);
        // This manual compaction conflict with the other manual compaction
        // so it should wait until the first compaction finish
        env_->SleepForMicroseconds(1000000);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Run a manual compaction that will compact the 2 files in L1
  // into 1 file in L1
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  bg_thread.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, OptimizeForPointLookup) {
  Options options = CurrentOptions();
  Close();
  options.OptimizeForPointLookup(2);
  ASSERT_OK(DB::Open(options, dbname_, &db_));

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_EQ("v1", Get("foo"));
  Flush();
  ASSERT_EQ("v1", Get("foo"));
}

#endif  // ROCKSDB_LITE

TEST_F(DBTest2, GetRaceFlush1) {
  ASSERT_OK(Put("foo", "v1"));

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:1", "DBTest2::GetRaceFlush:1"},
       {"DBTest2::GetRaceFlush:2", "DBImpl::GetImpl:2"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    Flush();
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:2");
  });

  // Get() is issued after the first Put(), so it should see either
  // "v1" or "v2".
  ASSERT_NE("NOT_FOUND", Get("foo"));
  t1.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, GetRaceFlush2) {
  ASSERT_OK(Put("foo", "v1"));

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:3", "DBTest2::GetRaceFlush:1"},
       {"DBTest2::GetRaceFlush:2", "DBImpl::GetImpl:4"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    Flush();
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:2");
  });

  // Get() is issued after the first Put(), so it should see either
  // "v1" or "v2".
  ASSERT_NE("NOT_FOUND", Get("foo"));
  t1.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, DirectIO) {
  if (!IsDirectIOSupported()) {
    return;
  }
  Options options = CurrentOptions();
  options.use_direct_reads = options.use_direct_io_for_flush_and_compaction =
      true;
  options.allow_mmap_reads = options.allow_mmap_writes = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Reopen(options);
}

TEST_F(DBTest2, MemtableOnlyIterator) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "first"));
  ASSERT_OK(Put(1, "bar", "second"));

  ReadOptions ropt;
  ropt.read_tier = kMemtableTier;
  std::string value;
  Iterator* it = nullptr;

  // Before flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);

  // Memtable-only iterator (read_tier=kMemtableTier); data not flushed yet.
  it = db_->NewIterator(ropt, handles_[1]);
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(2, count);
  delete it;

  Flush(1);

  // After flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);
  // nothing should be returned using memtable-only iterator after flushing.
  it = db_->NewIterator(ropt, handles_[1]);
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(0, count);
  delete it;

  // Add a key to memtable
  ASSERT_OK(Put(1, "foobar", "third"));
  it = db_->NewIterator(ropt, handles_[1]);
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("foobar", it->key().ToString());
    ASSERT_EQ("third", it->value().ToString());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(1, count);
  delete it;
}

TEST_F(DBTest2, LowPriWrite) {
  Options options = CurrentOptions();
  // Compaction pressure should trigger since 6 files
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 12;
  options.level0_stop_writes_trigger = 30;
  options.delayed_write_rate = 8 * 1024 * 1024;
  Reopen(options);

  std::atomic<int> rate_limit_count(0);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "GenericRateLimiter::Request:1", [&](void* arg) {
        rate_limit_count.fetch_add(1);
        int64_t* rate_bytes_per_sec = static_cast<int64_t*>(arg);
        ASSERT_EQ(1024 * 1024, *rate_bytes_per_sec);
      });
  // Block compaction
  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBTest.LowPriWrite:0", "DBImpl::BGWorkCompaction"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  WriteOptions wo;
  for (int i = 0; i < 6; i++) {
    wo.low_pri = false;
    Put("", "", wo);
    wo.low_pri = true;
    Put("", "", wo);
    Flush();
  }
  ASSERT_EQ(0, rate_limit_count.load());
  wo.low_pri = true;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());
  wo.low_pri = false;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());

  TEST_SYNC_POINT("DBTest.LowPriWrite:0");
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  dbfull()->TEST_WaitForCompact();
  wo.low_pri = true;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());
  wo.low_pri = false;
  Put("", "", wo);
  ASSERT_EQ(1, rate_limit_count.load());
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
