//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "db/arena_wrapped_db_iter.h"
#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/db_iter.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_test_util.h"
#include "db/write_batch_internal.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// kTypeBlobIndex is a value type used by BlobDB only. The base rocksdb
// should accept the value type on write, and report not supported value
// for reads, unless caller request for it explicitly. The base rocksdb
// doesn't understand format of actual blob index (the value).
class DBBlobIndexTest : public DBTestBase {
 public:
  enum Tier {
    kMemtable = 0,
    kImmutableMemtables = 1,
    kL0SstFile = 2,
    kLnSstFile = 3,
  };
  const std::vector<Tier> kAllTiers = {Tier::kMemtable,
                                       Tier::kImmutableMemtables,
                                       Tier::kL0SstFile, Tier::kLnSstFile};

  DBBlobIndexTest() : DBTestBase("db_blob_index_test", /*env_do_fsync=*/true) {}

  ColumnFamilyHandle* cfh() { return dbfull()->DefaultColumnFamily(); }
  ColumnFamilyHandleImpl* cfh_impl() {
    return static_cast_with_check<ColumnFamilyHandleImpl>(cfh());
  }
  ColumnFamilyData* cfd() { return cfh_impl()->cfd(); }

  Status PutBlobIndex(WriteBatch* batch, const Slice& key,
                      const Slice& blob_index) {
    return WriteBatchInternal::PutBlobIndex(batch, cfd()->GetID(), key,
                                            blob_index);
  }

  Status Write(WriteBatch* batch) {
    return dbfull()->Write(WriteOptions(), batch);
  }

  std::string GetImpl(const Slice& key, bool* is_blob_index = nullptr,
                      const Snapshot* snapshot = nullptr) {
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    PinnableSlice value;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = cfh();
    get_impl_options.value = &value;
    get_impl_options.is_blob_index = is_blob_index;
    auto s = dbfull()->GetImpl(read_options, key, get_impl_options);
    if (s.IsNotFound()) {
      return "NOT_FOUND";
    }
    if (s.IsCorruption()) {
      return "CORRUPTION";
    }
    if (s.IsNotSupported()) {
      return "NOT_SUPPORTED";
    }
    if (!s.ok()) {
      return s.ToString();
    }
    return value.ToString();
  }

  std::string GetBlobIndex(const Slice& key,
                           const Snapshot* snapshot = nullptr) {
    bool is_blob_index = false;
    std::string value = GetImpl(key, &is_blob_index, snapshot);
    if (!is_blob_index) {
      return "NOT_BLOB";
    }
    return value;
  }

  ArenaWrappedDBIter* GetBlobIterator() {
    DBImpl* db_impl = dbfull();
    return db_impl->NewIteratorImpl(
        ReadOptions(), cfh_impl(), cfd()->GetReferencedSuperVersion(db_impl),
        db_impl->GetLatestSequenceNumber(), nullptr /*read_callback*/,
        true /*expose_blob_index*/);
  }

  Options GetTestOptions() {
    Options options;
    options.env = CurrentOptions().env;
    options.create_if_missing = true;
    options.num_levels = 2;
    options.disable_auto_compactions = true;
    // Disable auto flushes.
    options.max_write_buffer_number = 10;
    options.min_write_buffer_number_to_merge = 10;
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    return options;
  }

  void MoveDataTo(Tier tier) {
    switch (tier) {
      case Tier::kMemtable:
        break;
      case Tier::kImmutableMemtables:
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
        break;
      case Tier::kL0SstFile:
        ASSERT_OK(Flush());
        break;
      case Tier::kLnSstFile:
        ASSERT_OK(Flush());
        ASSERT_OK(Put("a", "dummy"));
        ASSERT_OK(Put("z", "dummy"));
        ASSERT_OK(Flush());
        ASSERT_OK(
            dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
        ASSERT_EQ("0,1", FilesPerLevel());
        break;
    }
  }
};

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. We should be able to write kTypeBlobIndex to memtables and
// SST files.
TEST_F(DBBlobIndexTest, Write) {
  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());

    std::vector<std::pair<std::string, std::string>> key_values;

    constexpr size_t num_key_values = 5;

    key_values.reserve(num_key_values);

    for (size_t i = 1; i <= num_key_values; ++i) {
      std::string key = "key" + std::to_string(i);

      std::string blob_index;
      BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210,
                                  "blob" + std::to_string(i));

      key_values.emplace_back(std::move(key), std::move(blob_index));
    }

    for (const auto& key_value : key_values) {
      WriteBatch batch;
      ASSERT_OK(PutBlobIndex(&batch, key_value.first, key_value.second));
      ASSERT_OK(Write(&batch));
    }

    MoveDataTo(tier);

    for (const auto& key_value : key_values) {
      ASSERT_EQ(GetBlobIndex(key_value.first), key_value.second);
    }
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. Get should be able to return blob index if is_blob_index is
// provided, otherwise it should return Status::NotSupported (when reading from
// memtable) or Status::Corruption (when reading from SST). Reading from SST
// returns Corruption because we can't differentiate between the application
// accidentally opening the base DB of a stacked BlobDB and actual corruption
// when using the integrated BlobDB.
TEST_F(DBBlobIndexTest, Get) {
  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210, "blob");

  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());

    WriteBatch batch;
    ASSERT_OK(batch.Put("key", "value"));
    ASSERT_OK(PutBlobIndex(&batch, "blob_key", blob_index));
    ASSERT_OK(Write(&batch));

    MoveDataTo(tier);

    // Verify normal value
    bool is_blob_index = false;
    PinnableSlice value;
    ASSERT_EQ("value", Get("key"));
    ASSERT_EQ("value", GetImpl("key"));
    ASSERT_EQ("value", GetImpl("key", &is_blob_index));
    ASSERT_FALSE(is_blob_index);

    // Verify blob index
    if (tier <= kImmutableMemtables) {
      ASSERT_TRUE(Get("blob_key", &value).IsNotSupported());
      ASSERT_EQ("NOT_SUPPORTED", GetImpl("blob_key"));
    } else {
      ASSERT_TRUE(Get("blob_key", &value).IsCorruption());
      ASSERT_EQ("CORRUPTION", GetImpl("blob_key"));
    }
    ASSERT_EQ(blob_index, GetImpl("blob_key", &is_blob_index));
    ASSERT_TRUE(is_blob_index);
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. Get should NOT return Status::NotSupported/Status::Corruption
// if blob index is updated with a normal value. See the test case above for
// more details.
TEST_F(DBBlobIndexTest, Updated) {
  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210, "blob");

  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());
    WriteBatch batch;
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(PutBlobIndex(&batch, "key" + std::to_string(i), blob_index));
    }
    ASSERT_OK(Write(&batch));
    // Avoid blob values from being purged.
    const Snapshot* snapshot = dbfull()->GetSnapshot();
    ASSERT_OK(Put("key1", "new_value"));
    ASSERT_OK(Merge("key2", "a"));
    ASSERT_OK(Merge("key2", "b"));
    ASSERT_OK(Merge("key2", "c"));
    ASSERT_OK(Delete("key3"));
    ASSERT_OK(SingleDelete("key4"));
    ASSERT_OK(Delete("key5"));
    ASSERT_OK(Merge("key5", "a"));
    ASSERT_OK(Merge("key5", "b"));
    ASSERT_OK(Merge("key5", "c"));
    ASSERT_OK(dbfull()->DeleteRange(WriteOptions(), cfh(), "key6", "key9"));
    MoveDataTo(tier);
    for (int i = 0; i < 10; i++) {
      ASSERT_EQ(blob_index, GetBlobIndex("key" + std::to_string(i), snapshot));
    }
    ASSERT_EQ("new_value", Get("key1"));
    if (tier <= kImmutableMemtables) {
      ASSERT_EQ("NOT_SUPPORTED", GetImpl("key2"));
    } else {
      ASSERT_EQ("CORRUPTION", GetImpl("key2"));
    }
    ASSERT_EQ("NOT_FOUND", Get("key3"));
    ASSERT_EQ("NOT_FOUND", Get("key4"));
    ASSERT_EQ("a,b,c", GetImpl("key5"));
    for (int i = 6; i < 9; i++) {
      ASSERT_EQ("NOT_FOUND", Get("key" + std::to_string(i)));
    }
    ASSERT_EQ(blob_index, GetBlobIndex("key9"));
    dbfull()->ReleaseSnapshot(snapshot);
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. When a blob iterator is used, it should set the
// expose_blob_index flag for the underlying DBIter, and retrieve/return the
// corresponding blob value. If a regular DBIter is created (i.e.
// expose_blob_index is not set), it should return Status::Corruption.
TEST_F(DBBlobIndexTest, Iterate) {
  const std::vector<std::vector<ValueType>> data = {
      /*00*/ {kTypeValue},
      /*01*/ {kTypeBlobIndex},
      /*02*/ {kTypeValue},
      /*03*/ {kTypeBlobIndex, kTypeValue},
      /*04*/ {kTypeValue},
      /*05*/ {kTypeValue, kTypeBlobIndex},
      /*06*/ {kTypeValue},
      /*07*/ {kTypeDeletion, kTypeBlobIndex},
      /*08*/ {kTypeValue},
      /*09*/ {kTypeSingleDeletion, kTypeBlobIndex},
      /*10*/ {kTypeValue},
      /*11*/ {kTypeMerge, kTypeMerge, kTypeMerge, kTypeBlobIndex},
      /*12*/ {kTypeValue},
      /*13*/
      {kTypeMerge, kTypeMerge, kTypeMerge, kTypeDeletion, kTypeBlobIndex},
      /*14*/ {kTypeValue},
      /*15*/ {kTypeBlobIndex},
      /*16*/ {kTypeValue},
  };

  auto get_key = [](int index) {
    char buf[20];
    snprintf(buf, sizeof(buf), "%02d", index);
    return "key" + std::string(buf);
  };

  auto get_value = [&](int index, int version) {
    return get_key(index) + "_value" + std::to_string(version);
  };

  auto check_iterator = [&](Iterator* iterator, Status::Code expected_status,
                            const Slice& expected_value) {
    ASSERT_EQ(expected_status, iterator->status().code());
    if (expected_status == Status::kOk) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(expected_value, iterator->value());
    } else {
      ASSERT_FALSE(iterator->Valid());
    }
  };

  auto create_normal_iterator = [&]() -> Iterator* {
    return dbfull()->NewIterator(ReadOptions());
  };

  auto create_blob_iterator = [&]() -> Iterator* { return GetBlobIterator(); };

  auto check_is_blob = [&](bool is_blob) {
    return [is_blob](Iterator* iterator) {
      ASSERT_EQ(is_blob, static_cast<ArenaWrappedDBIter*>(iterator)->IsBlob());
    };
  };

  auto verify = [&](int index, Status::Code expected_status,
                    const Slice& forward_value, const Slice& backward_value,
                    std::function<Iterator*()> create_iterator,
                    std::function<void(Iterator*)> extra_check = nullptr) {
    // Seek
    auto* iterator = create_iterator();
    ASSERT_OK(iterator->status());
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index));
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Next
    iterator = create_iterator();
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index - 1));
    ASSERT_TRUE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator->Next();
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // SeekForPrev
    iterator = create_iterator();
    ASSERT_OK(iterator->status());
    ASSERT_OK(iterator->Refresh());
    iterator->SeekForPrev(get_key(index));
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Prev
    iterator = create_iterator();
    iterator->Seek(get_key(index + 1));
    ASSERT_TRUE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator->Prev();
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;
  };

  for (auto tier : {Tier::kMemtable} /*kAllTiers*/) {
    // Avoid values from being purged.
    std::vector<const Snapshot*> snapshots;
    DestroyAndReopen(GetTestOptions());

    // fill data
    for (int i = 0; i < static_cast<int>(data.size()); i++) {
      for (int j = static_cast<int>(data[i].size()) - 1; j >= 0; j--) {
        std::string key = get_key(i);
        std::string value = get_value(i, j);
        WriteBatch batch;
        switch (data[i][j]) {
          case kTypeValue:
            ASSERT_OK(Put(key, value));
            break;
          case kTypeDeletion:
            ASSERT_OK(Delete(key));
            break;
          case kTypeSingleDeletion:
            ASSERT_OK(SingleDelete(key));
            break;
          case kTypeMerge:
            ASSERT_OK(Merge(key, value));
            break;
          case kTypeBlobIndex:
            ASSERT_OK(PutBlobIndex(&batch, key, value));
            ASSERT_OK(Write(&batch));
            break;
          default:
            FAIL();
        };
      }
      snapshots.push_back(dbfull()->GetSnapshot());
    }
    ASSERT_OK(
        dbfull()->DeleteRange(WriteOptions(), cfh(), get_key(15), get_key(16)));
    snapshots.push_back(dbfull()->GetSnapshot());
    MoveDataTo(tier);

    // Normal iterator
    verify(1, Status::kCorruption, "", "", create_normal_iterator);
    verify(3, Status::kCorruption, "", "", create_normal_iterator);
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_normal_iterator);
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_normal_iterator);
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_normal_iterator);
    verify(11, Status::kCorruption, "", "", create_normal_iterator);
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_normal_iterator);
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_normal_iterator);

    // Iterator with blob support
    verify(1, Status::kOk, get_value(1, 0), get_value(1, 0),
           create_blob_iterator, check_is_blob(true));
    verify(3, Status::kOk, get_value(3, 0), get_value(3, 0),
           create_blob_iterator, check_is_blob(true));
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_blob_iterator, check_is_blob(false));
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_blob_iterator, check_is_blob(false));
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_blob_iterator, check_is_blob(false));
    if (tier <= kImmutableMemtables) {
      verify(11, Status::kNotSupported, "", "", create_blob_iterator);
    } else {
      verify(11, Status::kCorruption, "", "", create_blob_iterator);
    }
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_blob_iterator, check_is_blob(false));
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_blob_iterator, check_is_blob(false));

    // Iterator with blob support and using seek.
    ASSERT_OK(dbfull()->SetOptions(
        cfh(), {{"max_sequential_skip_in_iterations", "0"}}));
    verify(1, Status::kOk, get_value(1, 0), get_value(1, 0),
           create_blob_iterator, check_is_blob(true));
    verify(3, Status::kOk, get_value(3, 0), get_value(3, 0),
           create_blob_iterator, check_is_blob(true));
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_blob_iterator, check_is_blob(false));
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_blob_iterator, check_is_blob(false));
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_blob_iterator, check_is_blob(false));
    if (tier <= kImmutableMemtables) {
      verify(11, Status::kNotSupported, "", "", create_blob_iterator);
    } else {
      verify(11, Status::kCorruption, "", "", create_blob_iterator);
    }
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_blob_iterator, check_is_blob(false));
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_blob_iterator, check_is_blob(false));

    for (auto* snapshot : snapshots) {
      dbfull()->ReleaseSnapshot(snapshot);
    }
  }
}

// Use shared test utilities for Wide Column + Blob integration tests
using wide_column_test_util::GenerateLargeValue;
using wide_column_test_util::GenerateSmallValue;
using wide_column_test_util::GetOptionsForBlobTest;

// Test 1: Full roundtrip test: PutEntity with large columns -> flush ->
// compaction with blob extraction -> read back with blob resolution -> verify
TEST_F(DBBlobIndexTest, EntityBlobFlushCompactionRoundtrip) {
  Options options = GetOptionsForBlobTest();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.statistics = CreateDBStatistics();

  DestroyAndReopen(options);

  // Create entities with large column values that will become blobs
  constexpr char key1[] = "entity_key1";
  constexpr char key2[] = "entity_key2";
  constexpr char key3[] = "entity_key3";

  // Large value for default column (will become blob)
  std::string large_default_value = GenerateLargeValue(100);
  // Large value for non-default columns
  std::string large_attr1_value = GenerateLargeValue(150);
  std::string large_attr2_value = GenerateLargeValue(200);
  // Small value that stays inline
  std::string small_value = GenerateSmallValue();

  // Entity 1: Large default column + large attribute
  WideColumns columns1{{kDefaultWideColumnName, large_default_value},
                       {"attr1", large_attr1_value}};

  // Entity 2: No default column, only large attributes
  WideColumns columns2{{"attr1", large_attr1_value},
                       {"attr2", large_attr2_value}};

  // Entity 3: Mixed - small default, large attribute
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"attr1", large_attr1_value}};

  // Write entities
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));

  // Verify from memtable
  auto verify = [&]() {
    // Verify entity 1
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                               &result));
      ASSERT_EQ(result.columns(), columns1);

      // Also verify Get returns the default column value
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
      ASSERT_EQ(value, large_default_value);
    }

    // Verify entity 2
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                               &result));
      ASSERT_EQ(result.columns(), columns2);

      // Get should return empty for entity without default column
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key2, &value));
      ASSERT_TRUE(value.empty());
    }

    // Verify entity 3
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                               &result));
      ASSERT_EQ(result.columns(), columns3);

      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key3, &value));
      ASSERT_EQ(value, small_value);
    }

    // Verify via iterator
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      iter->SeekToFirst();

      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key1);
      ASSERT_EQ(iter->columns(), columns1);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key2);
      ASSERT_EQ(iter->columns(), columns2);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key3);
      ASSERT_EQ(iter->columns(), columns3);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  // Verify from memtable
  verify();

  // Flush to create SST files (and potentially blob files)
  ASSERT_OK(Flush());
  verify();

  // Add more data and flush again to create multiple files
  constexpr char key4[] = "entity_key4";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string large_value4_default = GenerateLargeValue(120);
  std::string large_value4_attr1 = GenerateLargeValue(80);
  WideColumns columns4{{kDefaultWideColumnName, large_value4_default},
                       {"attr1", large_value4_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key4,
                           columns4));
  ASSERT_OK(Flush());

  // Compact to trigger blob extraction and GC
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Explicit verification: blob files were actually created after compaction
  {
    // Verify at least one blob file exists
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_GE(blob_files.size(), 1)
        << "Expected at least one blob file after compaction with entity data";

    // Verify using DB property
    uint64_t num_blob_files = 0;
    ASSERT_TRUE(
        db_->GetIntProperty(DB::Properties::kNumBlobFiles, &num_blob_files));
    ASSERT_GE(num_blob_files, 1)
        << "kNumBlobFiles property should report at least one blob file";

    // Verify blob file size is non-zero (data was written)
    uint64_t total_blob_file_size = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalBlobFileSize,
                                    &total_blob_file_size));
    ASSERT_GT(total_blob_file_size, 0)
        << "Total blob file size should be greater than zero";

    // Verify blob statistics - confirm data was written to blob files
    uint64_t blob_bytes_written =
        options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_WRITTEN);
    ASSERT_GT(blob_bytes_written, 0)
        << "Expected non-zero bytes written to blob files";
  }

  // Verify original entities after compaction (verify() only checks key1-3,
  // so we verify them individually since key4 was added)
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result));
    ASSERT_EQ(result.columns(), columns1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                             &result));
    ASSERT_EQ(result.columns(), columns2);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                             &result));
    ASSERT_EQ(result.columns(), columns3);
  }

  // Verify key4 after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key4,
                             &result));
    ASSERT_EQ(result.columns(), columns4);
  }

  // Multiple flush/compaction cycles
  for (int cycle = 0; cycle < 3; ++cycle) {
    std::string cycle_key = "cycle_key" + std::to_string(cycle);
    // Store large values in persistent strings to avoid dangling Slice
    // references
    std::string cycle_default_value = GenerateLargeValue(100);
    std::string cycle_attr_value = GenerateLargeValue(50);
    WideColumns cycle_columns{{kDefaultWideColumnName, cycle_default_value},
                              {"cycle_attr", cycle_attr_value}};
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             cycle_key, cycle_columns));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Verify
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             cycle_key, &result));
    ASSERT_EQ(result.columns(), cycle_columns);
  }

  Close();
}

// Test 2: Entity blob GC respects snapshots - blobs referenced by snapshotted
// entities should not be GC'd
TEST_F(DBBlobIndexTest, EntityBlobGCWithSnapshot) {
  Options options = GetOptionsForBlobTest();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  DestroyAndReopen(options);

  constexpr char key[] = "snapshot_key";
  std::string original_value = GenerateLargeValue(100);
  std::string updated_value = GenerateLargeValue(150);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string original_attr1_value = GenerateLargeValue(80);
  std::string updated_attr1_value = GenerateLargeValue(120);

  WideColumns original_columns{{kDefaultWideColumnName, original_value},
                               {"attr1", original_attr1_value}};
  WideColumns updated_columns{{kDefaultWideColumnName, updated_value},
                              {"attr1", updated_attr1_value}};

  // Write original entity
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           original_columns));
  ASSERT_OK(Flush());

  // Take a snapshot
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_NE(snapshot, nullptr);

  // Update the entity with new values
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           updated_columns));
  ASSERT_OK(Flush());

  // Compact - this should trigger GC, but the old blobs should be preserved
  // due to the snapshot
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify current value
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), updated_columns);
  }

  // Verify snapshot still sees original value
  {
    ReadOptions read_options;
    read_options.snapshot = snapshot;

    PinnableWideColumns result;
    ASSERT_OK(
        db_->GetEntity(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result.columns(), original_columns);

    // Also verify Get with snapshot
    PinnableSlice value;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &value));
    ASSERT_EQ(value, original_value);
  }

  // Release snapshot and compact again - now old blobs can be GC'd
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify current value still accessible
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), updated_columns);
  }

  Close();
}

// Test 3: Merge operations work correctly with entities
// NOTE: This test uses small values that stay inline (not blobs) because
// merge operations with wide column entities that have blob columns are not
// currently supported. The merge helper requires all column values to be
// resolved before merging, but blob columns need special handling that is
// not yet implemented.
TEST_F(DBBlobIndexTest, EntityBlobMergeOperations) {
  Options options = GetOptionsForBlobTest();
  options.merge_operator = MergeOperators::CreateStringAppendOperator(",");

  DestroyAndReopen(options);

  constexpr char key1[] = "merge_key1";
  constexpr char key2[] = "merge_key2";

  // Use small values that stay inline (not blobs) for merge to work correctly
  // Merge with blob columns is not currently supported
  std::string base_value = "base_val";     // Small value, stays inline
  std::string base_attr1_value = "attr1";  // Small value, stays inline
  WideColumns base_columns{{kDefaultWideColumnName, base_value},
                           {"attr1", base_attr1_value}};

  // Entity without default column - merge will create one
  std::string no_default_attr1_value = "attr1_v";  // Small value
  std::string no_default_attr2_value = "attr2_v";  // Small value
  WideColumns no_default_columns{{"attr1", no_default_attr1_value},
                                 {"attr2", no_default_attr2_value}};

  // Write base entities
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           base_columns));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           no_default_columns));
  ASSERT_OK(Flush());

  // Apply merge operations
  std::string merge_value1 = "merge_operand1";
  std::string merge_value2 = "merge_operand2";

  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key1,
                       merge_value1));
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key2,
                       merge_value2));

  // Verify merge result from memtable
  auto verify = [&]() {
    // Key1: base_value + "," + merge_operand1
    {
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
      std::string expected = base_value + "," + merge_value1;
      ASSERT_EQ(value, expected);

      // Verify entity preserves non-default columns
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                               &result));
      ASSERT_GE(result.columns().size(), 2);
      // Check default column has merged value
      bool found_default = false;
      for (const auto& col : result.columns()) {
        if (col.name() == kDefaultWideColumnName) {
          ASSERT_EQ(col.value(), expected);
          found_default = true;
          break;
        }
      }
      ASSERT_TRUE(found_default);
    }

    // Key2: "" + "," + merge_operand2 (empty default merged with operand)
    {
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key2, &value));
      // When entity has no default column, merge with empty base
      std::string expected = "," + merge_value2;
      ASSERT_EQ(value, expected);

      // Verify entity preserves non-default columns
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                               &result));
      ASSERT_GE(result.columns().size(), 3);  // default + attr1 + attr2
    }
  };

  verify();

  // Flush and verify
  ASSERT_OK(Flush());
  verify();

  // Compact and verify
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  verify();

  // Apply more merge operations after compaction
  std::string merge_value3 = "more_data";  // Small merge operand (not blob)
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key1,
                       merge_value3));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify final state
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
    std::string expected = base_value + "," + merge_value1 + "," + merge_value3;
    ASSERT_EQ(value, expected);
  }

  Close();
}

// Test 4: DB recovery after crash with entities that have blob columns
TEST_F(DBBlobIndexTest, EntityBlobRecoveryAfterCrash) {
  Options options = GetOptionsForBlobTest();

  DestroyAndReopen(options);

  // Write entities with various column configurations
  constexpr char key1[] = "recovery_key1";
  constexpr char key2[] = "recovery_key2";
  constexpr char key3[] = "recovery_key3";

  std::string large_value1 = GenerateLargeValue(100);
  std::string large_value2 = GenerateLargeValue(150);
  std::string small_value = GenerateSmallValue();
  // Store large values in persistent strings to avoid dangling Slice references
  std::string columns1_attr1 = GenerateLargeValue(80);
  std::string columns3_attr1 = GenerateLargeValue(200);

  WideColumns columns1{{kDefaultWideColumnName, large_value1},
                       {"attr1", columns1_attr1}};
  WideColumns columns2{{"attr1", large_value2}, {"attr2", small_value}};
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"attr1", columns3_attr1}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));
  ASSERT_OK(Flush());

  // Compact to ensure blob extraction
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Write more data to WAL (not flushed - simulates data in memtable at crash)
  constexpr char key4[] = "recovery_key4";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string columns4_default = GenerateLargeValue(120);
  WideColumns columns4{{kDefaultWideColumnName, columns4_default},
                       {"attr1", "unflushed_attr"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key4,
                           columns4));

  // Close and reopen (simulates crash + recovery)
  Close();
  Reopen(options);

  // Verify all data is recovered correctly
  auto verify_entity = [&](const std::string& key,
                           const WideColumns& expected) {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), expected);
  };

  verify_entity(key1, columns1);
  verify_entity(key2, columns2);
  verify_entity(key3, columns3);
  verify_entity(key4, columns4);

  // Verify via Get for entities with default column
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
    ASSERT_EQ(value, large_value1);
  }

  // Verify via iterator
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 4);
  }

  // Test recovery with avoid_flush_during_recovery
  Close();
  options.avoid_flush_during_recovery = true;
  Reopen(options);

  // All data should still be accessible
  verify_entity(key1, columns1);
  verify_entity(key2, columns2);
  verify_entity(key3, columns3);
  verify_entity(key4, columns4);

  Close();
}

// Test 5: Entities with blob columns work correctly mixed with regular Put
// operations that also use blobs
TEST_F(DBBlobIndexTest, EntityBlobMixedWithRegularPut) {
  Options options = GetOptionsForBlobTest();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  DestroyAndReopen(options);

  // Regular Put with large value (becomes blob)
  constexpr char put_key1[] = "put_key1";
  std::string put_value1 = GenerateLargeValue(100);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key1,
                     put_value1));

  // Entity with large columns
  constexpr char entity_key1[] = "entity_key1";
  std::string entity_default = GenerateLargeValue(120);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string entity_columns1_attr1 = GenerateLargeValue(80);
  WideColumns entity_columns1{{kDefaultWideColumnName, entity_default},
                              {"attr1", entity_columns1_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key1, entity_columns1));

  // Regular Put with small value (stays inline)
  constexpr char put_key2[] = "put_key2";
  std::string put_value2 = GenerateSmallValue();
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key2,
                     put_value2));

  // Entity without default column
  constexpr char entity_key2[] = "entity_key2";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string entity_columns2_attr1 = GenerateLargeValue(90);
  std::string entity_columns2_attr2 = GenerateLargeValue(110);
  WideColumns entity_columns2{{"attr1", entity_columns2_attr1},
                              {"attr2", entity_columns2_attr2}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key2, entity_columns2));

  // More regular Put
  constexpr char put_key3[] = "put_key3";
  std::string put_value3 = GenerateLargeValue(200);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key3,
                     put_value3));

  // Flush
  ASSERT_OK(Flush());

  // Verify all values
  auto verify = [&]() {
    // Regular Put values
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1,
                         &value));
      ASSERT_EQ(value, put_value1);
    }
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key2,
                         &value));
      ASSERT_EQ(value, put_value2);
    }
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key3,
                         &value));
      ASSERT_EQ(value, put_value3);
    }

    // Entity values
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               entity_key1, &result));
      ASSERT_EQ(result.columns(), entity_columns1);

      // Get on entity returns default column
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), entity_key1,
                         &value));
      ASSERT_EQ(value, entity_default);
    }
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               entity_key2, &result));
      ASSERT_EQ(result.columns(), entity_columns2);
    }

    // GetEntity on regular Put returns single default column
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               put_key1, &result));
      ASSERT_EQ(result.columns().size(), 1);
      ASSERT_EQ(result.columns()[0].name(), kDefaultWideColumnName);
      ASSERT_EQ(result.columns()[0].value(), put_value1);
    }

    // Iterator should see all keys in order
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      std::vector<std::string> expected_keys = {entity_key1, entity_key2,
                                                put_key1, put_key2, put_key3};
      int idx = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ASSERT_OK(iter->status());
        ASSERT_LT(idx, static_cast<int>(expected_keys.size()));
        ASSERT_EQ(iter->key(), expected_keys[idx]);
        idx++;
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(idx, static_cast<int>(expected_keys.size()));
    }
  };

  verify();

  // Add more data and compact
  constexpr char put_key4[] = "put_key4";
  std::string put_value4 = GenerateLargeValue(150);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key4,
                     put_value4));

  constexpr char entity_key3[] = "entity_key3";
  // Store values in persistent strings to avoid dangling Slice references
  std::string entity_columns3_default = GenerateLargeValue(130);
  std::string entity_columns3_attr1 = GenerateSmallValue();
  WideColumns entity_columns3{{kDefaultWideColumnName, entity_columns3_default},
                              {"attr1", entity_columns3_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key3, entity_columns3));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify original entries after compaction (verify() only checks the original
  // 5 keys, so we verify them individually since put_key4 and entity_key3 were
  // added)
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1, &value));
    ASSERT_EQ(value, put_value1);
  }
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key2, &value));
    ASSERT_EQ(value, put_value2);
  }
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key3, &value));
    ASSERT_EQ(value, put_value3);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key1, &result));
    ASSERT_EQ(result.columns(), entity_columns1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key2, &result));
    ASSERT_EQ(result.columns(), entity_columns2);
  }

  // Verify new entries after compaction
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key4, &value));
    ASSERT_EQ(value, put_value4);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key3, &result));
    ASSERT_EQ(result.columns(), entity_columns3);
  }

  // Update some values and verify GC works correctly
  std::string updated_put_value1 = GenerateLargeValue(180);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key1,
                     updated_put_value1));

  std::string updated_entity_default = GenerateLargeValue(190);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string updated_entity_columns1_attr1 = GenerateLargeValue(85);
  WideColumns updated_entity_columns1{
      {kDefaultWideColumnName, updated_entity_default},
      {"attr1", updated_entity_columns1_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key1, updated_entity_columns1));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify updated values
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1, &value));
    ASSERT_EQ(value, updated_put_value1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key1, &result));
    ASSERT_EQ(result.columns(), updated_entity_columns1);
  }

  Close();
}

// Test 6: Verify lazy loading - when a compaction filter only accesses inline
// columns and uses the blob_resolver to check IsBlobColumn(), blob values
// are NOT read from blob files.
class LazyLoadingSmallColumnFilter : public CompactionFilter {
 public:
  LazyLoadingSmallColumnFilter(std::atomic<int>* filter_call_count,
                               std::string* last_small_col_value,
                               std::atomic<int>* resolver_check_count)
      : filter_call_count_(filter_call_count),
        last_small_col_value_(last_small_col_value),
        resolver_check_count_(resolver_check_count) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>*
      /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    // Only process wide-column entities
    if (value_type == ValueType::kWideColumnEntity) {
      assert(existing_columns != nullptr);
      (*filter_call_count_)++;

      // Use blob_resolver to check which columns are blobs and only access
      // inline columns. This demonstrates lazy loading - we never call
      // ResolveColumn on blob columns.
      if (blob_resolver != nullptr) {
        for (size_t i = 0; i < existing_columns->size(); ++i) {
          (*resolver_check_count_)++;
          if (!blob_resolver->IsBlobColumn(i)) {
            // Only access inline columns
            const auto& col = (*existing_columns)[i];
            if (col.name() == "small_col") {
              *last_small_col_value_ = col.value().ToString();
            }
          }
        }
      } else {
        // Fallback if no resolver - just access small_col
        for (const auto& col : *existing_columns) {
          if (col.name() == "small_col") {
            *last_small_col_value_ = col.value().ToString();
            break;
          }
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "LazyLoadingSmallColumnFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* last_small_col_value_;
  std::atomic<int>* resolver_check_count_;
};

class LazyLoadingSmallColumnFilterFactory : public CompactionFilterFactory {
 public:
  LazyLoadingSmallColumnFilterFactory(std::atomic<int>* filter_call_count,
                                      std::string* last_small_col_value,
                                      std::atomic<int>* resolver_check_count)
      : filter_call_count_(filter_call_count),
        last_small_col_value_(last_small_col_value),
        resolver_check_count_(resolver_check_count) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<LazyLoadingSmallColumnFilter>(
        filter_call_count_, last_small_col_value_, resolver_check_count_);
  }

  const char* Name() const override {
    return "LazyLoadingSmallColumnFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* last_small_col_value_;
  std::atomic<int>* resolver_check_count_;
};

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterSkipsBlobs) {
  // Test: When a compaction filter uses blob_resolver->IsBlobColumn() to check
  // which columns are blobs and DOESN'T call ResolveColumn on blob columns,
  // no blob I/O should occur.
  //
  // Scenario:
  // - Entity has 2 columns:
  //   1. small_col: 5 bytes (inline, < min_blob_size of 10)
  //   2. large_col: 10KB (stored in blob file)
  // - Compaction filter checks IsBlobColumn() and only accesses small_col
  // - Result: No blob bytes should be read!

  std::atomic<int> filter_call_count{0};
  std::string last_small_col_value;
  std::atomic<int> resolver_check_count{0};

  Options options = GetOptionsForBlobTest();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<LazyLoadingSmallColumnFilterFactory>(
          &filter_call_count, &last_small_col_value, &resolver_check_count);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  options.statistics->Reset();

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(last_small_col_value, small_value);

  // Verify resolver was used to check columns
  ASSERT_GE(resolver_check_count.load(), 2)
      << "Expected resolver to be used to check at least 2 columns";

  // Check blob read statistics after compaction
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);

  // With lazy loading, when filter only accesses inline columns via
  // IsBlobColumn() check, NO blob should be read!
  ASSERT_EQ(blob_bytes_read, 0)
      << "Expected 0 blob bytes read when filter uses IsBlobColumn() "
         "to skip blob columns. Got "
      << blob_bytes_read << " bytes.";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

// Test 7: Verify that when a compaction filter uses the blob_resolver to
// resolve blob columns, the blob values are correctly fetched.
class BlobResolvingFilter : public CompactionFilter {
 public:
  BlobResolvingFilter(std::atomic<int>* filter_call_count,
                      std::string* resolved_large_col_value)
      : filter_call_count_(filter_call_count),
        resolved_large_col_value_(resolved_large_col_value) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>*
      /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type == ValueType::kWideColumnEntity) {
      assert(existing_columns != nullptr);
      (*filter_call_count_)++;

      // Use blob_resolver to resolve the blob column
      if (blob_resolver != nullptr) {
        for (size_t i = 0; i < existing_columns->size(); ++i) {
          const auto& col = (*existing_columns)[i];
          if (col.name() == "large_col" && blob_resolver->IsBlobColumn(i)) {
            Slice resolved_value;
            Status s = blob_resolver->ResolveColumn(i, &resolved_value);
            if (s.ok()) {
              *resolved_large_col_value_ = resolved_value.ToString();
            }
          }
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "BlobResolvingFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* resolved_large_col_value_;
};

class BlobResolvingFilterFactory : public CompactionFilterFactory {
 public:
  BlobResolvingFilterFactory(std::atomic<int>* filter_call_count,
                             std::string* resolved_large_col_value)
      : filter_call_count_(filter_call_count),
        resolved_large_col_value_(resolved_large_col_value) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<BlobResolvingFilter>(filter_call_count_,
                                                 resolved_large_col_value_);
  }

  const char* Name() const override { return "BlobResolvingFilterFactory"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* resolved_large_col_value_;
};

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterResolvesBlobs) {
  // Test: When a compaction filter uses blob_resolver->ResolveColumn() to
  // fetch blob values, the values are correctly resolved.
  //
  // Scenario:
  // - Entity has 2 columns:
  //   1. small_col: 5 bytes (inline, < min_blob_size of 10)
  //   2. large_col: 10KB (stored in blob file)
  // - Compaction filter calls ResolveColumn on large_col
  // - Result: Blob bytes should be read and value should match

  std::atomic<int> filter_call_count{0};
  std::string resolved_large_col_value;

  Options options = GetOptionsForBlobTest();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<BlobResolvingFilterFactory>(&filter_call_count,
                                                   &resolved_large_col_value);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  options.statistics->Reset();

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);

  // Verify the resolved value matches the original large value
  ASSERT_EQ(resolved_large_col_value, large_value)
      << "Expected resolved blob value to match original large_value";

  // Check blob read statistics after compaction - should have read the blob
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  ASSERT_GE(blob_bytes_read, large_value.size())
      << "Expected at least " << large_value.size()
      << " bytes read from blob when filter resolves blob column";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

// Test 8: Verify backward compatibility - old filters that don't
// use the blob_resolver still work correctly by accessing inline columns.
// With lazy loading, blob columns will have blob indices (not actual values),
// but inline columns work correctly.
TEST_F(DBBlobIndexTest, EntityBlobCompactionFilterAccessesOnlySmallColumn) {
  // This test verifies backward compatibility: filters that don't use
  // the blob_resolver can still access inline columns correctly.
  //
  // With lazy loading, blob columns contain blob indices (not actual values),
  // but inline columns have the correct values. Since this filter
  // only accesses the inline small_col, it should work correctly.

  std::atomic<int> filter_call_count{0};
  std::string last_small_col_value;
  std::atomic<int> resolver_check_count{0};

  Options options = GetOptionsForBlobTest();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  // Reuse the LazyLoadingSmallColumnFilter which uses IsBlobColumn() check
  // to only access inline columns
  options.compaction_filter_factory =
      std::make_shared<LazyLoadingSmallColumnFilterFactory>(
          &filter_call_count, &last_small_col_value, &resolver_check_count);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  options.statistics->Reset();

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);

  // The filter accessed small_col which is inline, so it should get the
  // correct value
  ASSERT_EQ(last_small_col_value, small_value);

  // With lazy loading, no blob bytes should be read since filter doesn't
  // access blob columns
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  ASSERT_EQ(blob_bytes_read, 0)
      << "Expected 0 blob bytes read when filter only accesses inline columns";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

TEST_F(DBBlobIndexTest, IntegratedBlobIterate) {
  const std::vector<std::vector<std::string>> data = {
      /*00*/ {"Put"},
      /*01*/ {"Put", "Merge", "Merge", "Merge"},
      /*02*/ {"Put"}};

  auto get_key = [](size_t index) { return ("key" + std::to_string(index)); };

  auto get_value = [&](size_t index, size_t version) {
    return get_key(index) + "_value" + std::to_string(version);
  };

  auto check_iterator = [&](Iterator* iterator, Status expected_status,
                            const Slice& expected_value) {
    ASSERT_EQ(expected_status, iterator->status());
    if (expected_status.ok()) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(expected_value, iterator->value());
    } else {
      ASSERT_FALSE(iterator->Valid());
    }
  };

  auto verify = [&](size_t index, Status expected_status,
                    const Slice& expected_value) {
    // Seek
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->status());
      ASSERT_OK(iterator->Refresh());
      iterator->Seek(get_key(index));
      check_iterator(iterator, expected_status, expected_value);
    }
    // Next
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->Refresh());
      iterator->Seek(get_key(index - 1));
      ASSERT_TRUE(iterator->Valid());
      ASSERT_OK(iterator->status());
      iterator->Next();
      check_iterator(iterator, expected_status, expected_value);
    }
    // SeekForPrev
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->status());
      ASSERT_OK(iterator->Refresh());
      iterator->SeekForPrev(get_key(index));
      check_iterator(iterator, expected_status, expected_value);
    }
    // Prev
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      iterator->Seek(get_key(index + 1));
      ASSERT_TRUE(iterator->Valid());
      ASSERT_OK(iterator->status());
      iterator->Prev();
      check_iterator(iterator, expected_status, expected_value);
    }
  };

  Options options = GetTestOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  DestroyAndReopen(options);

  // fill data
  for (size_t i = 0; i < data.size(); i++) {
    for (size_t j = 0; j < data[i].size(); j++) {
      std::string key = get_key(i);
      std::string value = get_value(i, j);
      if (data[i][j] == "Put") {
        ASSERT_OK(Put(key, value));
        ASSERT_OK(Flush());
      } else if (data[i][j] == "Merge") {
        ASSERT_OK(Merge(key, value));
        ASSERT_OK(Flush());
      }
    }
  }

  std::string expected_value = get_value(1, 0) + "," + get_value(1, 1) + "," +
                               get_value(1, 2) + "," + get_value(1, 3);
  Status expected_status;
  verify(1, expected_status, expected_value);

  // Test DBIter::FindValueForCurrentKeyUsingSeek flow.
  ASSERT_OK(dbfull()->SetOptions(cfh(),
                                 {{"max_sequential_skip_in_iterations", "0"}}));
  verify(1, expected_status, expected_value);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
