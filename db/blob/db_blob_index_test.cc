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

  ColumnFamilyData* cfd() {
    return static_cast_with_check<ColumnFamilyHandleImpl>(cfh())->cfd();
  }

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
    return dbfull()->NewIteratorImpl(
        ReadOptions(), cfd(), dbfull()->GetLatestSequenceNumber(),
        nullptr /*read_callback*/, true /*expose_blob_index*/);
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
#ifndef ROCKSDB_LITE
        ASSERT_EQ("0,1", FilesPerLevel());
#endif  // !ROCKSDB_LITE
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
      ASSERT_EQ(is_blob,
                reinterpret_cast<ArenaWrappedDBIter*>(iterator)->IsBlob());
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

#ifndef ROCKSDB_LITE
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
#endif  // !ROCKSDB_LITE

    for (auto* snapshot : snapshots) {
      dbfull()->ReleaseSnapshot(snapshot);
    }
  }
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

#ifndef ROCKSDB_LITE
  // Test DBIter::FindValueForCurrentKeyUsingSeek flow.
  ASSERT_OK(dbfull()->SetOptions(cfh(),
                                 {{"max_sequential_skip_in_iterations", "0"}}));
  verify(1, expected_status, expected_value);
#endif  // !ROCKSDB_LITE
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
