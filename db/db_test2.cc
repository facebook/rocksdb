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
#include "rocksdb/wal_filter.h"

namespace rocksdb {

class DBTest2 : public DBTestBase {
 public:
  DBTest2() : DBTestBase("/db_test2") {}
};

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

#ifndef ROCKSDB_LITE
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
      if (i >= change_records_from_index && j >= 
        num_keys_to_add_in_new_batch) {
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
  Status status = TryReopenWithColumnFamilies({ "default", "pikachu" }, 
    options);
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

    virtual WalProcessingOption LogRecord(unsigned long long log_number,
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

    const std::map<uint32_t, std::vector<std::string>> & 
    GetColumnFamilyKeys() {
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
  auto name_id_map = 
    test_wal_filter_column_families.GetColumnFamilyNameIdMap();
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
#endif  // ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
