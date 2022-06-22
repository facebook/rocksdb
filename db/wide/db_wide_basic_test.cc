//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>

#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

class DBWideBasicTest : public DBTestBase {
 protected:
  DBWideBasicTest()
      : DBTestBase("db_wide_basic_test", /* env_do_fsync */ false) {}
};

TEST_F(DBWideBasicTest, Entity) {
  Options options = GetDefaultOptions();

  constexpr size_t num_keys = 2;

  // Use the DB::PutEntity API
  constexpr char first_key[] = "first";
  WideColumns first_columns{{"foo", "bar"}, {"hello", "world"}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           first_key, first_columns));

  // Use WriteBatch
  constexpr char second_key[] = "second";
  WideColumns second_columns{{"one", "two"}, {"three", "four"}};

  WriteBatch batch;
  ASSERT_OK(
      batch.PutEntity(db_->DefaultColumnFamily(), second_key, second_columns));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  // Note: currently, read APIs are supposed to return NotSupported
  auto verify = [&]() {
    {
      PinnableSlice result;
      ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), first_key,
                           &result)
                      .IsNotSupported());
    }

    {
      PinnableSlice result;
      ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(),
                           second_key, &result)
                      .IsNotSupported());
    }

    {
      std::array<Slice, num_keys> keys{{first_key, second_key}};
      std::array<PinnableSlice, num_keys> values;
      std::array<Status, num_keys> statuses;

      db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                    &keys[0], &values[0], &statuses[0]);

      ASSERT_TRUE(values[0].empty());
      ASSERT_TRUE(statuses[0].IsNotSupported());

      ASSERT_TRUE(values[1].empty());
      ASSERT_TRUE(statuses[1].IsNotSupported());
    }

    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

      iter->SeekToFirst();
      ASSERT_FALSE(iter->Valid());
      ASSERT_TRUE(iter->status().IsNotSupported());

      iter->SeekToLast();
      ASSERT_FALSE(iter->Valid());
      ASSERT_TRUE(iter->status().IsNotSupported());
    }
  };

  // Try reading from memtable
  verify();

  // Try reading after recovery
  options.avoid_flush_during_recovery = true;
  Reopen(options);

  verify();

  // Try reading from storage
  ASSERT_OK(Flush());

  verify();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
