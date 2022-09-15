//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <memory>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/testutil.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class DBWideBasicTest : public DBTestBase {
 protected:
  explicit DBWideBasicTest()
      : DBTestBase("db_wide_basic_test", /* env_do_fsync */ false) {}
};

TEST_F(DBWideBasicTest, PutEntity) {
  Options options = GetDefaultOptions();

  // Write a couple of wide-column entities and a plain old key-value, then read
  // them back.
  constexpr char first_key[] = "first";
  constexpr char first_value_of_default_column[] = "hello";
  WideColumns first_columns{
      {kDefaultWideColumnName, first_value_of_default_column},
      {"attr_name1", "foo"},
      {"attr_name2", "bar"}};

  constexpr char second_key[] = "second";
  WideColumns second_columns{{"attr_one", "two"}, {"attr_three", "four"}};

  constexpr char third_key[] = "third";
  constexpr char third_value[] = "baz";

  auto verify = [&]() {
    const WideColumns expected_third_columns{
        {kDefaultWideColumnName, third_value}};

    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), first_key,
                         &result));
      ASSERT_EQ(result, first_value_of_default_column);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               first_key, &result));
      ASSERT_EQ(result.columns(), first_columns);
    }

    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), second_key,
                         &result));
      ASSERT_TRUE(result.empty());
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               second_key, &result));
      ASSERT_EQ(result.columns(), second_columns);
    }

    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), third_key,
                         &result));
      ASSERT_EQ(result, third_value);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               third_key, &result));

      ASSERT_EQ(result.columns(), expected_third_columns);
    }

    {
      constexpr size_t num_keys = 3;

      std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};
      std::array<PinnableSlice, num_keys> values;
      std::array<Status, num_keys> statuses;

      db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                    &keys[0], &values[0], &statuses[0]);

      ASSERT_OK(statuses[0]);
      ASSERT_EQ(values[0], first_value_of_default_column);

      ASSERT_OK(statuses[1]);
      ASSERT_TRUE(values[1].empty());

      ASSERT_OK(statuses[2]);
      ASSERT_EQ(values[2], third_value);
    }

    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), first_value_of_default_column);
      ASSERT_EQ(iter->columns(), first_columns);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_EQ(iter->columns(), second_columns);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), third_key);
      ASSERT_EQ(iter->value(), third_value);
      ASSERT_EQ(iter->columns(), expected_third_columns);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());

      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), third_key);
      ASSERT_EQ(iter->value(), third_value);
      ASSERT_EQ(iter->columns(), expected_third_columns);

      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_EQ(iter->columns(), second_columns);

      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), first_value_of_default_column);
      ASSERT_EQ(iter->columns(), first_columns);

      iter->Prev();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  // Use the DB::PutEntity API to write the first entity
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           first_key, first_columns));

  // Use WriteBatch to write the second entity
  WriteBatch batch;
  ASSERT_OK(
      batch.PutEntity(db_->DefaultColumnFamily(), second_key, second_columns));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  // Use Put to write the plain key-value
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), third_key,
                     third_value));

  // Try reading from memtable
  verify();

  // Try reading after recovery
  Close();
  options.avoid_flush_during_recovery = true;
  Reopen(options);

  verify();

  // Try reading from storage
  ASSERT_OK(Flush());

  verify();
}

TEST_F(DBWideBasicTest, PutEntityColumnFamily) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"corinthian"}, options);

  // Use the DB::PutEntity API
  constexpr char first_key[] = "first";
  WideColumns first_columns{{"attr_name1", "foo"}, {"attr_name2", "bar"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), handles_[1], first_key, first_columns));

  // Use WriteBatch
  constexpr char second_key[] = "second";
  WideColumns second_columns{{"attr_one", "two"}, {"attr_three", "four"}};

  WriteBatch batch;
  ASSERT_OK(batch.PutEntity(handles_[1], second_key, second_columns));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
}

TEST_F(DBWideBasicTest, PutEntityMergeNotSupported) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);

  constexpr char first_key[] = "first";
  constexpr char second_key[] = "second";

  // Note: Merge is currently not supported for wide-column entities
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
      constexpr size_t num_keys = 2;

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

  // Use the DB::PutEntity API
  WideColumns first_columns{{"attr_name1", "foo"}, {"attr_name2", "bar"}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           first_key, first_columns));

  // Use WriteBatch
  WideColumns second_columns{{"attr_one", "two"}, {"attr_three", "four"}};

  WriteBatch batch;
  ASSERT_OK(
      batch.PutEntity(db_->DefaultColumnFamily(), second_key, second_columns));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  // Add a couple of merge operands
  constexpr char merge_operand[] = "bla";

  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), first_key,
                       merge_operand));
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), second_key,
                       merge_operand));

  // Try reading when PutEntity is in storage, Merge is in memtable
  verify();

  // Try reading when PutEntity and Merge are both in storage
  ASSERT_OK(Flush());

  verify();

  // Try reading when PutEntity and Merge are both in memtable
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           first_key, first_columns));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), first_key,
                       merge_operand));
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), second_key,
                       merge_operand));

  verify();
}

TEST_F(DBWideBasicTest, PutEntityTimestampError) {
  // Note: timestamps are currently not supported

  Options options = GetDefaultOptions();
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();

  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options, "corinthian", &handle));
  std::unique_ptr<ColumnFamilyHandle> handle_guard(handle);

  // Use the DB::PutEntity API
  constexpr char first_key[] = "first";
  WideColumns first_columns{{"attr_name1", "foo"}, {"attr_name2", "bar"}};

  ASSERT_TRUE(db_->PutEntity(WriteOptions(), handle, first_key, first_columns)
                  .IsInvalidArgument());

  // Use WriteBatch
  constexpr char second_key[] = "second";
  WideColumns second_columns{{"doric", "column"}, {"ionic", "column"}};

  WriteBatch batch;
  ASSERT_TRUE(
      batch.PutEntity(handle, second_key, second_columns).IsInvalidArgument());
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
}

TEST_F(DBWideBasicTest, PutEntitySerializationError) {
  // Make sure duplicate columns are caught

  Options options = GetDefaultOptions();

  // Use the DB::PutEntity API
  constexpr char first_key[] = "first";
  WideColumns first_columns{{"foo", "bar"}, {"foo", "baz"}};

  ASSERT_TRUE(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             first_key, first_columns)
                  .IsCorruption());

  // Use WriteBatch
  constexpr char second_key[] = "second";
  WideColumns second_columns{{"column", "doric"}, {"column", "ionic"}};

  WriteBatch batch;
  ASSERT_TRUE(
      batch.PutEntity(db_->DefaultColumnFamily(), second_key, second_columns)
          .IsCorruption());
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
