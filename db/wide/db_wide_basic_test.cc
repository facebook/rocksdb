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

TEST_F(DBWideBasicTest, MergePlainKeyValue) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);

  // Put + Merge
  constexpr char first_key[] = "first";
  constexpr char first_base_value[] = "hello";
  constexpr char first_merge_op[] = "world";

  // Delete + Merge
  constexpr char second_key[] = "second";
  constexpr char second_merge_op[] = "foo";

  // Merge without any preceding KV
  constexpr char third_key[] = "third";
  constexpr char third_merge_op[] = "bar";

  auto write_base = [&]() {
    // Write "base" KVs: a Put for the 1st key and a Delete for the 2nd one;
    // note there is no "base" KV for the 3rd
    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), first_key,
                       first_base_value));
    ASSERT_OK(
        db_->Delete(WriteOptions(), db_->DefaultColumnFamily(), second_key));
  };

  auto write_merge = [&]() {
    // Write Merge operands
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), first_key,
                         first_merge_op));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), second_key,
                         second_merge_op));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), third_key,
                         third_merge_op));
  };

  const std::string expected_first_column(std::string(first_base_value) + "," +
                                          first_merge_op);
  const WideColumns expected_first_columns{
      {kDefaultWideColumnName, expected_first_column}};
  const WideColumns expected_second_columns{
      {kDefaultWideColumnName, second_merge_op}};
  const WideColumns expected_third_columns{
      {kDefaultWideColumnName, third_merge_op}};

  auto verify = [&]() {
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               first_key, &result));
      ASSERT_EQ(result.columns(), expected_first_columns);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               second_key, &result));
      ASSERT_EQ(result.columns(), expected_second_columns);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               third_key, &result));

      ASSERT_EQ(result.columns(), expected_third_columns);
    }

    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), expected_first_columns[0].value());
      ASSERT_EQ(iter->columns(), expected_first_columns);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_EQ(iter->value(), expected_second_columns[0].value());
      ASSERT_EQ(iter->columns(), expected_second_columns);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), third_key);
      ASSERT_EQ(iter->value(), expected_third_columns[0].value());
      ASSERT_EQ(iter->columns(), expected_third_columns);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());

      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), third_key);
      ASSERT_EQ(iter->value(), expected_third_columns[0].value());
      ASSERT_EQ(iter->columns(), expected_third_columns);

      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_EQ(iter->value(), expected_second_columns[0].value());
      ASSERT_EQ(iter->columns(), expected_second_columns);

      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), expected_first_columns[0].value());
      ASSERT_EQ(iter->columns(), expected_first_columns);

      iter->Prev();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  {
    // Base KVs (if any) and Merge operands both in memtable (note: we take a
    // snapshot in between to make sure they do not get reconciled during the
    // subsequent flush)
    write_base();
    ManagedSnapshot snapshot(db_);
    write_merge();
    verify();

    // Base KVs (if any) and Merge operands both in storage
    ASSERT_OK(Flush());
    verify();
  }

  // Base KVs (if any) in storage, Merge operands in memtable
  DestroyAndReopen(options);
  write_base();
  ASSERT_OK(Flush());
  write_merge();
  verify();
}

TEST_F(DBWideBasicTest, MergeEntity) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;

  const std::string delim("|");
  options.merge_operator = MergeOperators::CreateStringAppendOperator(delim);

  Reopen(options);

  // Test Merge with two entities: one that has the default column and one that
  // doesn't
  constexpr char first_key[] = "first";
  WideColumns first_columns{{kDefaultWideColumnName, "a"},
                            {"attr_name1", "foo"},
                            {"attr_name2", "bar"}};
  constexpr char first_merge_operand[] = "bla1";

  constexpr char second_key[] = "second";
  WideColumns second_columns{{"attr_one", "two"}, {"attr_three", "four"}};
  constexpr char second_merge_operand[] = "bla2";

  auto write_base = [&]() {
    // Use the DB::PutEntity API
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             first_key, first_columns));

    // Use WriteBatch
    WriteBatch batch;
    ASSERT_OK(batch.PutEntity(db_->DefaultColumnFamily(), second_key,
                              second_columns));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
  };

  auto write_merge = [&]() {
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), first_key,
                         first_merge_operand));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), second_key,
                         second_merge_operand));
  };

  const std::string first_expected_default(first_columns[0].value().ToString() +
                                           delim + first_merge_operand);
  const std::string second_expected_default(delim + second_merge_operand);

  auto verify_basic = [&]() {
    WideColumns first_expected_columns{
        {kDefaultWideColumnName, first_expected_default},
        first_columns[1],
        first_columns[2]};

    WideColumns second_expected_columns{
        {kDefaultWideColumnName, second_expected_default},
        second_columns[0],
        second_columns[1]};

    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), first_key,
                         &result));
      ASSERT_EQ(result, first_expected_default);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               first_key, &result));
      ASSERT_EQ(result.columns(), first_expected_columns);
    }

    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), second_key,
                         &result));
      ASSERT_EQ(result, second_expected_default);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               second_key, &result));
      ASSERT_EQ(result.columns(), second_expected_columns);
    }

    {
      constexpr size_t num_keys = 2;

      std::array<Slice, num_keys> keys{{first_key, second_key}};
      std::array<PinnableSlice, num_keys> values;
      std::array<Status, num_keys> statuses;

      db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                    &keys[0], &values[0], &statuses[0]);

      ASSERT_EQ(values[0], first_expected_default);
      ASSERT_OK(statuses[0]);

      ASSERT_EQ(values[1], second_expected_default);
      ASSERT_OK(statuses[1]);
    }

    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), first_expected_default);
      ASSERT_EQ(iter->columns(), first_expected_columns);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_EQ(iter->value(), second_expected_default);
      ASSERT_EQ(iter->columns(), second_expected_columns);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());

      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_EQ(iter->value(), second_expected_default);
      ASSERT_EQ(iter->columns(), second_expected_columns);

      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), first_expected_default);
      ASSERT_EQ(iter->columns(), first_expected_columns);

      iter->Prev();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  auto verify_merge_ops_pre_compaction = [&]() {
    constexpr size_t num_merge_operands = 2;

    GetMergeOperandsOptions get_merge_opts;
    get_merge_opts.expected_max_number_of_operands = num_merge_operands;

    {
      std::array<PinnableSlice, num_merge_operands> merge_operands;
      int number_of_operands = 0;

      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      first_key, &merge_operands[0],
                                      &get_merge_opts, &number_of_operands));

      ASSERT_EQ(number_of_operands, num_merge_operands);
      ASSERT_EQ(merge_operands[0], first_columns[0].value());
      ASSERT_EQ(merge_operands[1], first_merge_operand);
    }

    {
      std::array<PinnableSlice, num_merge_operands> merge_operands;
      int number_of_operands = 0;

      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      second_key, &merge_operands[0],
                                      &get_merge_opts, &number_of_operands));

      ASSERT_EQ(number_of_operands, num_merge_operands);
      ASSERT_TRUE(merge_operands[0].empty());
      ASSERT_EQ(merge_operands[1], second_merge_operand);
    }
  };

  auto verify_merge_ops_post_compaction = [&]() {
    constexpr size_t num_merge_operands = 1;

    GetMergeOperandsOptions get_merge_opts;
    get_merge_opts.expected_max_number_of_operands = num_merge_operands;

    {
      std::array<PinnableSlice, num_merge_operands> merge_operands;
      int number_of_operands = 0;

      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      first_key, &merge_operands[0],
                                      &get_merge_opts, &number_of_operands));

      ASSERT_EQ(number_of_operands, num_merge_operands);
      ASSERT_EQ(merge_operands[0], first_expected_default);
    }

    {
      std::array<PinnableSlice, num_merge_operands> merge_operands;
      int number_of_operands = 0;

      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      second_key, &merge_operands[0],
                                      &get_merge_opts, &number_of_operands));

      ASSERT_EQ(number_of_operands, num_merge_operands);
      ASSERT_EQ(merge_operands[0], second_expected_default);
    }
  };

  {
    // Base KVs and Merge operands both in memtable (note: we take a snapshot in
    // between to make sure they do not get reconciled during the subsequent
    // flush)
    write_base();
    ManagedSnapshot snapshot(db_);
    write_merge();
    verify_basic();
    verify_merge_ops_pre_compaction();

    // Base KVs and Merge operands both in storage
    ASSERT_OK(Flush());
    verify_basic();
    verify_merge_ops_pre_compaction();
  }

  // Base KVs in storage, Merge operands in memtable
  DestroyAndReopen(options);
  write_base();
  ASSERT_OK(Flush());
  write_merge();
  verify_basic();
  verify_merge_ops_pre_compaction();

  // Flush and compact
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /* begin */ nullptr,
                              /* end */ nullptr));
  verify_basic();
  verify_merge_ops_post_compaction();
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
