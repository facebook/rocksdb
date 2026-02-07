//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <cctype>
#include <memory>

#include "db/db_test_util.h"
#include "db/wide/wide_column_test_util.h"
#include "port/stack_trace.h"
#include "test_util/testutil.h"
#include "util/overload.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// Use shared test utilities for Wide Column + Blob integration tests
using wide_column_test_util::GenerateLargeValue;
using wide_column_test_util::GenerateSmallValue;
using wide_column_test_util::GetOptionsForBlobTest;

class DBWideBasicTest : public DBTestBase {
 protected:
  explicit DBWideBasicTest()
      : DBTestBase("db_wide_basic_test", /* env_do_fsync */ false) {}

  // Helper: runs the EntityBlobAfterFlush test logic with the given options.
  void RunEntityBlobAfterFlush(const Options& options);

  // Helper: runs the EntityBlobAfterCompaction test logic with the given
  // options.
  void RunEntityBlobAfterCompaction(const Options& options);

  // Helper: runs the CompactionFilterWithBlobGC test logic with the given
  // options. The options must have compaction_filter already set.
  void RunCompactionFilterWithBlobGC(const Options& options,
                                     std::atomic<int>* ttl_threshold);
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
                    keys.data(), values.data(), statuses.data());

      ASSERT_OK(statuses[0]);
      ASSERT_EQ(values[0], first_value_of_default_column);

      ASSERT_OK(statuses[1]);
      ASSERT_TRUE(values[1].empty());

      ASSERT_OK(statuses[2]);
      ASSERT_EQ(values[2], third_value);
    }

    {
      constexpr size_t num_keys = 3;

      std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};
      std::array<PinnableWideColumns, num_keys> results;
      std::array<Status, num_keys> statuses;

      db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                          keys.data(), results.data(), statuses.data());

      ASSERT_OK(statuses[0]);
      ASSERT_EQ(results[0].columns(), first_columns);

      ASSERT_OK(statuses[1]);
      ASSERT_EQ(results[1].columns(), second_columns);

      ASSERT_OK(statuses[2]);
      ASSERT_EQ(results[2].columns(), expected_third_columns);
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

  // Reopen as Readonly DB and verify
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
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

TEST_F(DBWideBasicTest, GetEntityAsPinnableAttributeGroups) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"hot_cf", "cold_cf"}, options);

  constexpr int kDefaultCfHandleIndex = 0;
  constexpr int kHotCfHandleIndex = 1;
  constexpr int kColdCfHandleIndex = 2;

  constexpr char first_key[] = "first";
  WideColumns first_default_columns{
      {"default_cf_col_1_name", "first_key_default_cf_col_1_value"},
      {"default_cf_col_2_name", "first_key_default_cf_col_2_value"}};
  WideColumns first_hot_columns{
      {"hot_cf_col_1_name", "first_key_hot_cf_col_1_value"},
      {"hot_cf_col_2_name", "first_key_hot_cf_col_2_value"}};
  WideColumns first_cold_columns{
      {"cold_cf_col_1_name", "first_key_cold_cf_col_1_value"}};

  constexpr char second_key[] = "second";
  WideColumns second_hot_columns{
      {"hot_cf_col_1_name", "second_key_hot_cf_col_1_value"}};
  WideColumns second_cold_columns{
      {"cold_cf_col_1_name", "second_key_cold_cf_col_1_value"}};

  AttributeGroups first_key_attribute_groups{
      AttributeGroup(handles_[kDefaultCfHandleIndex], first_default_columns),
      AttributeGroup(handles_[kHotCfHandleIndex], first_hot_columns),
      AttributeGroup(handles_[kColdCfHandleIndex], first_cold_columns)};
  AttributeGroups second_key_attribute_groups{
      AttributeGroup(handles_[kHotCfHandleIndex], second_hot_columns),
      AttributeGroup(handles_[kColdCfHandleIndex], second_cold_columns)};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), first_key, first_key_attribute_groups));
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), second_key, second_key_attribute_groups));

  std::vector<ColumnFamilyHandle*> all_cfs = handles_;
  std::vector<ColumnFamilyHandle*> default_and_hot_cfs{
      {handles_[kDefaultCfHandleIndex], handles_[kHotCfHandleIndex]}};
  std::vector<ColumnFamilyHandle*> hot_and_cold_cfs{
      {handles_[kHotCfHandleIndex], handles_[kColdCfHandleIndex]}};
  std::vector<ColumnFamilyHandle*> default_null_and_hot_cfs{
      handles_[kDefaultCfHandleIndex], nullptr, handles_[kHotCfHandleIndex],
      nullptr};
  auto create_result =
      [](const std::vector<ColumnFamilyHandle*>& column_families)
      -> PinnableAttributeGroups {
    PinnableAttributeGroups result;
    for (size_t i = 0; i < column_families.size(); ++i) {
      result.emplace_back(column_families[i]);
    }
    return result;
  };
  {
    // Case 1. Invalid Argument (passing in null CF)
    AttributeGroups ag{
        AttributeGroup(nullptr, first_default_columns),
        AttributeGroup(handles_[kHotCfHandleIndex], first_hot_columns)};
    ASSERT_NOK(db_->PutEntity(WriteOptions(), first_key, ag));

    PinnableAttributeGroups result = create_result(default_null_and_hot_cfs);
    Status s = db_->GetEntity(ReadOptions(), first_key, &result);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsInvalidArgument());
    // Valid CF, but failed with Incomplete status due to other attribute groups
    ASSERT_TRUE(result[0].status().IsIncomplete());
    // Null CF
    ASSERT_TRUE(result[1].status().IsInvalidArgument());
    // Valid CF, but failed with Incomplete status due to other attribute groups
    ASSERT_TRUE(result[2].status().IsIncomplete());
    // Null CF, but failed with Incomplete status because the nullcheck break
    // out early in the loop
    ASSERT_TRUE(result[3].status().IsIncomplete());
  }
  {
    // Case 2. Get first key from default cf and hot_cf and second key from
    // hot_cf and cold_cf
    constexpr size_t num_column_families = 2;
    PinnableAttributeGroups first_key_result =
        create_result(default_and_hot_cfs);
    PinnableAttributeGroups second_key_result = create_result(hot_and_cold_cfs);

    // GetEntity for first_key
    ASSERT_OK(db_->GetEntity(ReadOptions(), first_key, &first_key_result));
    ASSERT_EQ(num_column_families, first_key_result.size());
    // We expect to get values for all keys and CFs
    for (size_t i = 0; i < num_column_families; ++i) {
      ASSERT_OK(first_key_result[i].status());
    }
    // verify values for first key (default cf and hot cf)
    ASSERT_EQ(first_default_columns, first_key_result[0].columns());
    ASSERT_EQ(first_hot_columns, first_key_result[1].columns());

    // GetEntity for second_key
    ASSERT_OK(db_->GetEntity(ReadOptions(), second_key, &second_key_result));
    ASSERT_EQ(num_column_families, second_key_result.size());
    // We expect to get values for all keys and CFs
    for (size_t i = 0; i < num_column_families; ++i) {
      ASSERT_OK(second_key_result[i].status());
    }
    // verify values for second key (hot cf and cold cf)
    ASSERT_EQ(second_hot_columns, second_key_result[0].columns());
    ASSERT_EQ(second_cold_columns, second_key_result[1].columns());
  }
  {
    // Case 3. Get first key and second key from all cfs. For the second key, we
    // don't expect to get columns from default cf.
    constexpr size_t num_column_families = 3;
    PinnableAttributeGroups first_key_result = create_result(all_cfs);
    PinnableAttributeGroups second_key_result = create_result(all_cfs);

    // GetEntity for first_key
    ASSERT_OK(db_->GetEntity(ReadOptions(), first_key, &first_key_result));
    ASSERT_EQ(num_column_families, first_key_result.size());
    // We expect to get values for all keys and CFs
    for (size_t i = 0; i < num_column_families; ++i) {
      ASSERT_OK(first_key_result[i].status());
    }
    // verify values for first key
    ASSERT_EQ(first_default_columns, first_key_result[0].columns());
    ASSERT_EQ(first_hot_columns, first_key_result[1].columns());
    ASSERT_EQ(first_cold_columns, first_key_result[2].columns());

    // GetEntity for second_key
    ASSERT_OK(db_->GetEntity(ReadOptions(), second_key, &second_key_result));
    ASSERT_EQ(num_column_families, second_key_result.size());
    // key does not exist in default cf
    ASSERT_NOK(second_key_result[0].status());
    ASSERT_TRUE(second_key_result[0].status().IsNotFound());

    // verify values for second key (hot cf and cold cf)
    ASSERT_OK(second_key_result[1].status());
    ASSERT_OK(second_key_result[2].status());
    ASSERT_EQ(second_hot_columns, second_key_result[1].columns());
    ASSERT_EQ(second_cold_columns, second_key_result[2].columns());
  }
}

TEST_F(DBWideBasicTest, MultiCFMultiGetEntity) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"corinthian"}, options);

  constexpr char first_key[] = "first";
  WideColumns first_columns{{"attr_name1", "foo"}, {"attr_name2", "bar"}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           first_key, first_columns));

  constexpr char second_key[] = "second";
  WideColumns second_columns{{"attr_one", "two"}, {"attr_three", "four"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), handles_[1], second_key, second_columns));

  constexpr size_t num_keys = 2;

  std::array<ColumnFamilyHandle*, num_keys> column_families{
      {db_->DefaultColumnFamily(), handles_[1]}};
  std::array<Slice, num_keys> keys{{first_key, second_key}};
  std::array<PinnableWideColumns, num_keys> results;
  std::array<Status, num_keys> statuses;

  db_->MultiGetEntity(ReadOptions(), num_keys, column_families.data(),
                      keys.data(), results.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(results[0].columns(), first_columns);

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(results[1].columns(), second_columns);
}

TEST_F(DBWideBasicTest, MultiCFMultiGetEntityAsPinnableAttributeGroups) {
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"hot_cf", "cold_cf"}, options);

  constexpr int kDefaultCfHandleIndex = 0;
  constexpr int kHotCfHandleIndex = 1;
  constexpr int kColdCfHandleIndex = 2;

  constexpr char first_key[] = "first";
  WideColumns first_default_columns{
      {"default_cf_col_1_name", "first_key_default_cf_col_1_value"},
      {"default_cf_col_2_name", "first_key_default_cf_col_2_value"}};
  WideColumns first_hot_columns{
      {"hot_cf_col_1_name", "first_key_hot_cf_col_1_value"},
      {"hot_cf_col_2_name", "first_key_hot_cf_col_2_value"}};
  WideColumns first_cold_columns{
      {"cold_cf_col_1_name", "first_key_cold_cf_col_1_value"}};
  constexpr char second_key[] = "second";
  WideColumns second_hot_columns{
      {"hot_cf_col_1_name", "second_key_hot_cf_col_1_value"}};
  WideColumns second_cold_columns{
      {"cold_cf_col_1_name", "second_key_cold_cf_col_1_value"}};

  AttributeGroups first_key_attribute_groups{
      AttributeGroup(handles_[kDefaultCfHandleIndex], first_default_columns),
      AttributeGroup(handles_[kHotCfHandleIndex], first_hot_columns),
      AttributeGroup(handles_[kColdCfHandleIndex], first_cold_columns)};
  AttributeGroups second_key_attribute_groups{
      AttributeGroup(handles_[kHotCfHandleIndex], second_hot_columns),
      AttributeGroup(handles_[kColdCfHandleIndex], second_cold_columns)};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), first_key, first_key_attribute_groups));
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), second_key, second_key_attribute_groups));

  constexpr size_t num_keys = 2;
  std::array<Slice, num_keys> keys = {first_key, second_key};
  std::vector<ColumnFamilyHandle*> all_cfs = handles_;
  std::vector<ColumnFamilyHandle*> default_and_hot_cfs{
      {handles_[kDefaultCfHandleIndex], handles_[kHotCfHandleIndex]}};
  std::vector<ColumnFamilyHandle*> hot_and_cold_cfs{
      {handles_[kHotCfHandleIndex], handles_[kColdCfHandleIndex]}};
  std::vector<ColumnFamilyHandle*> null_and_hot_cfs{
      nullptr, handles_[kHotCfHandleIndex], nullptr};
  auto create_result =
      [](const std::vector<ColumnFamilyHandle*>& column_families)
      -> PinnableAttributeGroups {
    PinnableAttributeGroups result;
    for (size_t i = 0; i < column_families.size(); ++i) {
      result.emplace_back(column_families[i]);
    }
    return result;
  };
  {
    // Check for invalid read option argument
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kGetEntity;
    std::vector<PinnableAttributeGroups> results;
    for (size_t i = 0; i < num_keys; ++i) {
      results.emplace_back(create_result(all_cfs));
    }
    db_->MultiGetEntity(read_options, num_keys, keys.data(), results.data());
    for (size_t i = 0; i < num_keys; ++i) {
      for (size_t j = 0; j < all_cfs.size(); ++j) {
        ASSERT_NOK(results[i][j].status());
        ASSERT_TRUE(results[i][j].status().IsInvalidArgument());
      }
    }
    // Check for invalid column family in Attribute Group result
    results.clear();
    results.emplace_back(create_result(null_and_hot_cfs));
    results.emplace_back(create_result(all_cfs));
    db_->MultiGetEntity(ReadOptions(), num_keys, keys.data(), results.data());

    // First one failed due to null CFs in the AttributeGroup
    // Null CF
    ASSERT_NOK(results[0][0].status());
    ASSERT_TRUE(results[0][0].status().IsInvalidArgument());
    // Valid CF, but failed with incomplete status because of other attribute
    // groups
    ASSERT_NOK(results[0][1].status());
    ASSERT_TRUE(results[0][1].status().IsIncomplete());
    // Null CF
    ASSERT_NOK(results[0][2].status());
    ASSERT_TRUE(results[0][2].status().IsInvalidArgument());

    // Second one failed with Incomplete because first one failed
    ASSERT_NOK(results[1][0].status());
    ASSERT_TRUE(results[1][0].status().IsIncomplete());
    ASSERT_NOK(results[1][1].status());
    ASSERT_TRUE(results[1][1].status().IsIncomplete());
    ASSERT_NOK(results[1][2].status());
    ASSERT_TRUE(results[1][2].status().IsIncomplete());
  }
  {
    // Case 1. Get first key from default cf and hot_cf and second key from
    // hot_cf and cold_cf
    std::vector<PinnableAttributeGroups> results;
    PinnableAttributeGroups first_key_result =
        create_result(default_and_hot_cfs);
    PinnableAttributeGroups second_key_result = create_result(hot_and_cold_cfs);
    results.emplace_back(std::move(first_key_result));
    results.emplace_back(std::move(second_key_result));

    db_->MultiGetEntity(ReadOptions(), num_keys, keys.data(), results.data());
    ASSERT_EQ(2, results.size());
    // We expect to get values for all keys and CFs
    for (size_t i = 0; i < num_keys; ++i) {
      for (size_t j = 0; j < 2; ++j) {
        ASSERT_OK(results[i][j].status());
      }
    }
    // verify values for first key (default cf and hot cf)
    ASSERT_EQ(2, results[0].size());
    ASSERT_EQ(first_default_columns, results[0][0].columns());
    ASSERT_EQ(first_hot_columns, results[0][1].columns());

    // verify values for second key (hot cf and cold cf)
    ASSERT_EQ(2, results[1].size());
    ASSERT_EQ(second_hot_columns, results[1][0].columns());
    ASSERT_EQ(second_cold_columns, results[1][1].columns());
  }
  {
    // Case 2. Get first key and second key from all cfs. For the second key, we
    // don't expect to get columns from default cf.
    std::vector<PinnableAttributeGroups> results;
    PinnableAttributeGroups first_key_result = create_result(all_cfs);
    PinnableAttributeGroups second_key_result = create_result(all_cfs);
    results.emplace_back(std::move(first_key_result));
    results.emplace_back(std::move(second_key_result));

    db_->MultiGetEntity(ReadOptions(), num_keys, keys.data(), results.data());
    // verify first key
    for (size_t i = 0; i < all_cfs.size(); ++i) {
      ASSERT_OK(results[0][i].status());
    }
    ASSERT_EQ(3, results[0].size());
    ASSERT_EQ(first_default_columns, results[0][0].columns());
    ASSERT_EQ(first_hot_columns, results[0][1].columns());
    ASSERT_EQ(first_cold_columns, results[0][2].columns());

    // verify second key
    // key does not exist in default cf
    ASSERT_NOK(results[1][0].status());
    ASSERT_TRUE(results[1][0].status().IsNotFound());
    ASSERT_TRUE(results[1][0].columns().empty());

    // key exists in hot_cf and cold_cf
    ASSERT_OK(results[1][1].status());
    ASSERT_EQ(second_hot_columns, results[1][1].columns());
    ASSERT_OK(results[1][2].status());
    ASSERT_EQ(second_cold_columns, results[1][2].columns());
  }
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
      constexpr size_t num_keys = 3;

      std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};
      std::array<PinnableWideColumns, num_keys> results;
      std::array<Status, num_keys> statuses;

      db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                          keys.data(), results.data(), statuses.data());

      ASSERT_OK(statuses[0]);
      ASSERT_EQ(results[0].columns(), expected_first_columns);

      ASSERT_OK(statuses[1]);
      ASSERT_EQ(results[1].columns(), expected_second_columns);

      ASSERT_OK(statuses[2]);
      ASSERT_EQ(results[2].columns(), expected_third_columns);
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
                    keys.data(), values.data(), statuses.data());

      ASSERT_EQ(values[0], first_expected_default);
      ASSERT_OK(statuses[0]);

      ASSERT_EQ(values[1], second_expected_default);
      ASSERT_OK(statuses[1]);
    }

    {
      constexpr size_t num_keys = 2;

      std::array<Slice, num_keys> keys{{first_key, second_key}};
      std::array<PinnableWideColumns, num_keys> results;
      std::array<Status, num_keys> statuses;

      db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                          keys.data(), results.data(), statuses.data());

      ASSERT_OK(statuses[0]);
      ASSERT_EQ(results[0].columns(), first_expected_columns);

      ASSERT_OK(statuses[1]);
      ASSERT_EQ(results[1].columns(), second_expected_columns);
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
                                      first_key, merge_operands.data(),
                                      &get_merge_opts, &number_of_operands));

      ASSERT_EQ(number_of_operands, num_merge_operands);
      ASSERT_EQ(merge_operands[0], first_columns[0].value());
      ASSERT_EQ(merge_operands[1], first_merge_operand);
    }

    {
      std::array<PinnableSlice, num_merge_operands> merge_operands;
      int number_of_operands = 0;

      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      second_key, merge_operands.data(),
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
                                      first_key, merge_operands.data(),
                                      &get_merge_opts, &number_of_operands));

      ASSERT_EQ(number_of_operands, num_merge_operands);
      ASSERT_EQ(merge_operands[0], first_expected_default);
    }

    {
      std::array<PinnableSlice, num_merge_operands> merge_operands;
      int number_of_operands = 0;

      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      second_key, merge_operands.data(),
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

class DBWideMergeV3Test : public DBWideBasicTest {
 protected:
  void RunTest(const WideColumns& first_expected,
               const WideColumns& second_expected,
               const WideColumns& third_expected) {
    // Note: we'll take some snapshots to prevent merging during flush
    snapshots_.reserve(6);

    // Test reading from memtables
    WriteKeyValues();
    VerifyKeyValues(first_expected, second_expected, third_expected);
    VerifyMergeOperandCount(first_key, 2);
    VerifyMergeOperandCount(second_key, 3);
    VerifyMergeOperandCount(third_key, 3);

    // Test reading from SST files
    ASSERT_OK(Flush());
    VerifyKeyValues(first_expected, second_expected, third_expected);
    VerifyMergeOperandCount(first_key, 2);
    VerifyMergeOperandCount(second_key, 3);
    VerifyMergeOperandCount(third_key, 3);

    // Test reading from SSTs after compaction. Note that we write the same KVs
    // and flush again so we have two overlapping files. We also release the
    // snapshots so that the compaction can merge all keys.
    WriteKeyValues();
    ASSERT_OK(Flush());

    snapshots_.clear();

    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /* begin */ nullptr,
                                /* end */ nullptr));
    VerifyKeyValues(first_expected, second_expected, third_expected);
    VerifyMergeOperandCount(first_key, 1);
    VerifyMergeOperandCount(second_key, 1);
    VerifyMergeOperandCount(third_key, 1);
  }

  void WriteKeyValues() {
    // Base values
    ASSERT_OK(db_->Delete(WriteOptions(), db_->DefaultColumnFamily(),
                          first_key));  // no base value
    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), second_key,
                       second_base_value));  // plain base value
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             third_key,
                             third_columns));  // wide-column base value

    snapshots_.emplace_back(db_);

    // First round of merge operands
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), first_key,
                         first_merge_op1));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), second_key,
                         second_merge_op1));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), third_key,
                         third_merge_op1));

    snapshots_.emplace_back(db_);

    // Second round of merge operands
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), first_key,
                         first_merge_op2));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), second_key,
                         second_merge_op2));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), third_key,
                         third_merge_op2));

    snapshots_.emplace_back(db_);
  }

  void VerifyKeyValues(const WideColumns& first_expected,
                       const WideColumns& second_expected,
                       const WideColumns& third_expected) {
    assert(!first_expected.empty() &&
           first_expected[0].name() == kDefaultWideColumnName);
    assert(!second_expected.empty() &&
           second_expected[0].name() == kDefaultWideColumnName);
    assert(!third_expected.empty() &&
           third_expected[0].name() == kDefaultWideColumnName);

    // Get
    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), first_key,
                         &result));
      ASSERT_EQ(result, first_expected[0].value());
    }

    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), second_key,
                         &result));
      ASSERT_EQ(result, second_expected[0].value());
    }

    {
      PinnableSlice result;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), third_key,
                         &result));
      ASSERT_EQ(result, third_expected[0].value());
    }

    // MultiGet
    {
      std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};
      std::array<PinnableSlice, num_keys> values;
      std::array<Status, num_keys> statuses;

      db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                    keys.data(), values.data(), statuses.data());
      ASSERT_OK(statuses[0]);
      ASSERT_EQ(values[0], first_expected[0].value());
      ASSERT_OK(statuses[1]);
      ASSERT_EQ(values[1], second_expected[0].value());
      ASSERT_OK(statuses[2]);
      ASSERT_EQ(values[2], third_expected[0].value());
    }

    // GetEntity
    {
      PinnableWideColumns result;

      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               first_key, &result));
      ASSERT_EQ(result.columns(), first_expected);
    }

    {
      PinnableWideColumns result;

      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               second_key, &result));
      ASSERT_EQ(result.columns(), second_expected);
    }

    {
      PinnableWideColumns result;

      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               third_key, &result));
      ASSERT_EQ(result.columns(), third_expected);
    }

    // MultiGetEntity
    {
      std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};
      std::array<PinnableWideColumns, num_keys> results;
      std::array<Status, num_keys> statuses;

      db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                          keys.data(), results.data(), statuses.data());
      ASSERT_OK(statuses[0]);
      ASSERT_EQ(results[0].columns(), first_expected);
      ASSERT_OK(statuses[1]);
      ASSERT_EQ(results[1].columns(), second_expected);
      ASSERT_OK(statuses[2]);
      ASSERT_EQ(results[2].columns(), third_expected);
    }

    // Iterator
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), first_expected[0].value());
      ASSERT_EQ(iter->columns(), first_expected);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_EQ(iter->value(), second_expected[0].value());
      ASSERT_EQ(iter->columns(), second_expected);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), third_key);
      ASSERT_EQ(iter->value(), third_expected[0].value());
      ASSERT_EQ(iter->columns(), third_expected);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());

      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), third_key);
      ASSERT_EQ(iter->value(), third_expected[0].value());
      ASSERT_EQ(iter->columns(), third_expected);

      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), second_key);
      ASSERT_EQ(iter->value(), second_expected[0].value());
      ASSERT_EQ(iter->columns(), second_expected);

      iter->Prev();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), first_key);
      ASSERT_EQ(iter->value(), first_expected[0].value());
      ASSERT_EQ(iter->columns(), first_expected);

      iter->Prev();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  }

  void VerifyMergeOperandCount(const Slice& key, int expected_merge_ops) {
    GetMergeOperandsOptions get_merge_opts;
    get_merge_opts.expected_max_number_of_operands = expected_merge_ops;

    std::vector<PinnableSlice> merge_operands(expected_merge_ops);
    int number_of_operands = 0;

    ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                    key, merge_operands.data(), &get_merge_opts,
                                    &number_of_operands));
    ASSERT_EQ(number_of_operands, expected_merge_ops);
  }

  std::vector<ManagedSnapshot> snapshots_;

  static constexpr size_t num_keys = 3;

  static constexpr char first_key[] = "first";
  static constexpr char first_merge_op1[] = "hello";
  static constexpr char first_merge_op1_upper[] = "HELLO";
  static constexpr char first_merge_op2[] = "world";
  static constexpr char first_merge_op2_upper[] = "WORLD";

  static constexpr char second_key[] = "second";
  static constexpr char second_base_value[] = "foo";
  static constexpr char second_base_value_upper[] = "FOO";
  static constexpr char second_merge_op1[] = "bar";
  static constexpr char second_merge_op1_upper[] = "BAR";
  static constexpr char second_merge_op2[] = "baz";
  static constexpr char second_merge_op2_upper[] = "BAZ";

  static constexpr char third_key[] = "third";
  static const WideColumns third_columns;
  static constexpr char third_merge_op1[] = "three";
  static constexpr char third_merge_op1_upper[] = "THREE";
  static constexpr char third_merge_op2[] = "four";
  static constexpr char third_merge_op2_upper[] = "FOUR";
};

const WideColumns DBWideMergeV3Test::third_columns{{"one", "ONE"},
                                                   {"two", "TWO"}};

TEST_F(DBWideMergeV3Test, MergeV3WideColumnOutput) {
  // A test merge operator that always returns a wide-column result. It adds any
  // base values and merge operands to a single wide-column entity, and converts
  // all column values to uppercase. In addition, it puts "none", "plain", or
  // "wide" into the value of the default column depending on the type of the
  // base value (if any).
  static constexpr char kNone[] = "none";
  static constexpr char kPlain[] = "plain";
  static constexpr char kWide[] = "wide";

  class WideColumnOutputMergeOperator : public MergeOperator {
   public:
    bool FullMergeV3(const MergeOperationInputV3& merge_in,
                     MergeOperationOutputV3* merge_out) const override {
      assert(merge_out);

      merge_out->new_value = MergeOperationOutputV3::NewColumns();
      auto& new_columns =
          std::get<MergeOperationOutputV3::NewColumns>(merge_out->new_value);

      auto upper = [](std::string str) {
        for (char& c : str) {
          c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        }

        return str;
      };

      std::visit(overload{[&](const std::monostate&) {
                            new_columns.emplace_back(
                                kDefaultWideColumnName.ToString(), kNone);
                          },
                          [&](const Slice& value) {
                            new_columns.emplace_back(
                                kDefaultWideColumnName.ToString(), kPlain);

                            const std::string val = value.ToString();
                            new_columns.emplace_back(val, upper(val));
                          },
                          [&](const WideColumns& columns) {
                            new_columns.emplace_back(
                                kDefaultWideColumnName.ToString(), kWide);

                            for (const auto& column : columns) {
                              new_columns.emplace_back(
                                  column.name().ToString(),
                                  upper(column.value().ToString()));
                            }
                          }},
                 merge_in.existing_value);

      for (const auto& operand : merge_in.operand_list) {
        const std::string op = operand.ToString();
        new_columns.emplace_back(op, upper(op));
      }

      return true;
    }

    const char* Name() const override {
      return "WideColumnOutputMergeOperator";
    }
  };

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.merge_operator = std::make_shared<WideColumnOutputMergeOperator>();
  Reopen(options);

  // Expected results
  // Lexicographical order: [default] < hello < world
  const WideColumns first_expected{{kDefaultWideColumnName, kNone},
                                   {first_merge_op1, first_merge_op1_upper},
                                   {first_merge_op2, first_merge_op2_upper}};
  // Lexicographical order: [default] < bar < baz < foo
  const WideColumns second_expected{
      {kDefaultWideColumnName, kPlain},
      {second_merge_op1, second_merge_op1_upper},
      {second_merge_op2, second_merge_op2_upper},
      {second_base_value, second_base_value_upper}};
  // Lexicographical order: [default] < four < one < three < two
  const WideColumns third_expected{
      {kDefaultWideColumnName, kWide},
      {third_merge_op2, third_merge_op2_upper},
      {third_columns[0].name(), third_columns[0].value()},
      {third_merge_op1, third_merge_op1_upper},
      {third_columns[1].name(), third_columns[1].value()}};

  RunTest(first_expected, second_expected, third_expected);
}

TEST_F(DBWideMergeV3Test, MergeV3PlainOutput) {
  // A test merge operator that always returns a plain value as result, namely
  // the total number of operands serialized as a string. Base values are also
  // counted as operands; specifically, a plain base value is counted as one
  // operand, while a wide-column base value is counted as as many operands as
  // the number of columns.
  class PlainOutputMergeOperator : public MergeOperator {
   public:
    bool FullMergeV3(const MergeOperationInputV3& merge_in,
                     MergeOperationOutputV3* merge_out) const override {
      assert(merge_out);

      size_t count = 0;
      std::visit(
          overload{[&](const std::monostate&) {},
                   [&](const Slice&) { count = 1; },
                   [&](const WideColumns& columns) { count = columns.size(); }},
          merge_in.existing_value);

      count += merge_in.operand_list.size();

      merge_out->new_value = std::string();
      std::get<std::string>(merge_out->new_value) = std::to_string(count);

      return true;
    }

    const char* Name() const override { return "PlainOutputMergeOperator"; }
  };

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.merge_operator = std::make_shared<PlainOutputMergeOperator>();
  Reopen(options);

  const WideColumns first_expected{{kDefaultWideColumnName, "2"}};
  const WideColumns second_expected{{kDefaultWideColumnName, "3"}};
  const WideColumns third_expected{{kDefaultWideColumnName, "4"}};

  RunTest(first_expected, second_expected, third_expected);
}

TEST_F(DBWideBasicTest, CompactionFilter) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;

  // Wide-column entity with default column
  constexpr char first_key[] = "first";
  WideColumns first_columns{{kDefaultWideColumnName, "a"},
                            {"attr_name1", "foo"},
                            {"attr_name2", "bar"}};
  WideColumns first_columns_uppercase{{kDefaultWideColumnName, "A"},
                                      {"attr_name1", "FOO"},
                                      {"attr_name2", "BAR"}};

  // Wide-column entity without default column
  constexpr char second_key[] = "second";
  WideColumns second_columns{{"attr_one", "two"}, {"attr_three", "four"}};
  WideColumns second_columns_uppercase{{"attr_one", "TWO"},
                                       {"attr_three", "FOUR"}};

  // Plain old key-value
  constexpr char last_key[] = "last";
  constexpr char last_value[] = "baz";
  constexpr char last_value_uppercase[] = "BAZ";

  auto write = [&] {
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             first_key, first_columns));
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             second_key, second_columns));

    ASSERT_OK(Flush());

    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), last_key,
                       last_value));

    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /* begin */ nullptr,
                                /* end */ nullptr));
  };

  // Test a compaction filter that keeps all entries
  {
    class KeepFilter : public CompactionFilter {
     public:
      Decision FilterV4(
          int /* level */, const Slice& /* key */, ValueType /* value_type */,
          const Slice* /* existing_value */,
          const WideColumns* /* existing_columns */,
          std::string* /* new_value */,
          std::vector<std::pair<std::string, std::string>>* /* new_columns */,
          std::string* /* skip_until */,
          WideColumnBlobResolver* /* blob_resolver */ =
              nullptr) const override {
        return Decision::kKeep;
      }

      const char* Name() const override { return "KeepFilter"; }
    };

    KeepFilter filter;
    options.compaction_filter = &filter;

    DestroyAndReopen(options);

    write();

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               first_key, &result));
      ASSERT_EQ(result.columns(), first_columns);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               second_key, &result));
      ASSERT_EQ(result.columns(), second_columns);
    }

    // Note: GetEntity should return an entity with a single default column,
    // since last_key is a plain key-value
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               last_key, &result));

      WideColumns expected_columns{{kDefaultWideColumnName, last_value}};
      ASSERT_EQ(result.columns(), expected_columns);
    }
  }

  // Test a compaction filter that removes all entries
  {
    class RemoveFilter : public CompactionFilter {
     public:
      Decision FilterV4(
          int /* level */, const Slice& /* key */, ValueType /* value_type */,
          const Slice* /* existing_value */,
          const WideColumns* /* existing_columns */,
          std::string* /* new_value */,
          std::vector<std::pair<std::string, std::string>>* /* new_columns */,
          std::string* /* skip_until */,
          WideColumnBlobResolver* /* blob_resolver */ =
              nullptr) const override {
        return Decision::kRemove;
      }

      const char* Name() const override { return "RemoveFilter"; }
    };

    RemoveFilter filter;
    options.compaction_filter = &filter;

    DestroyAndReopen(options);

    write();

    {
      PinnableWideColumns result;
      ASSERT_TRUE(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                                 first_key, &result)
                      .IsNotFound());
    }

    {
      PinnableWideColumns result;
      ASSERT_TRUE(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                                 second_key, &result)
                      .IsNotFound());
    }

    {
      PinnableWideColumns result;
      ASSERT_TRUE(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                                 last_key, &result)
                      .IsNotFound());
    }
  }

  // Test a compaction filter that changes the values of entries to uppercase.
  // The new entry is always a plain key-value; if the existing entry is a
  // wide-column entity, only the value of its first column is kept.
  {
    class ChangeValueFilter : public CompactionFilter {
     public:
      Decision FilterV4(
          int /* level */, const Slice& /* key */, ValueType value_type,
          const Slice* existing_value, const WideColumns* existing_columns,
          std::string* new_value,
          std::vector<std::pair<std::string, std::string>>* /* new_columns */,
          std::string* /* skip_until */,
          WideColumnBlobResolver* /* blob_resolver */ =
              nullptr) const override {
        assert(new_value);

        auto upper = [](const std::string& str) {
          std::string result(str);

          for (char& c : result) {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
          }

          return result;
        };

        if (value_type == ValueType::kWideColumnEntity) {
          assert(existing_columns);

          if (!existing_columns->empty()) {
            *new_value = upper(existing_columns->front().value().ToString());
          }
        } else {
          assert(existing_value);

          *new_value = upper(existing_value->ToString());
        }

        return Decision::kChangeValue;
      }

      const char* Name() const override { return "ChangeValueFilter"; }
    };

    ChangeValueFilter filter;
    options.compaction_filter = &filter;

    DestroyAndReopen(options);

    write();

    // Note: GetEntity should return entities with a single default column,
    // since all entries are now plain key-values
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               first_key, &result));

      WideColumns expected_columns{
          {kDefaultWideColumnName, first_columns_uppercase[0].value()}};
      ASSERT_EQ(result.columns(), expected_columns);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               second_key, &result));

      WideColumns expected_columns{
          {kDefaultWideColumnName, second_columns_uppercase[0].value()}};
      ASSERT_EQ(result.columns(), expected_columns);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               last_key, &result));

      WideColumns expected_columns{
          {kDefaultWideColumnName, last_value_uppercase}};
      ASSERT_EQ(result.columns(), expected_columns);
    }
  }

  // Test a compaction filter that changes the column values of entries to
  // uppercase. The new entry is always a wide-column entity; if the existing
  // entry is a plain key-value, it is converted to a wide-column entity with a
  // single default column.
  {
    class ChangeEntityFilter : public CompactionFilter {
     public:
      Decision FilterV4(
          int /* level */, const Slice& /* key */, ValueType value_type,
          const Slice* existing_value, const WideColumns* existing_columns,
          std::string* /* new_value */,
          std::vector<std::pair<std::string, std::string>>* new_columns,
          std::string* /* skip_until */,
          WideColumnBlobResolver* /* blob_resolver */ =
              nullptr) const override {
        assert(new_columns);

        auto upper = [](const std::string& str) {
          std::string result(str);

          for (char& c : result) {
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
          }

          return result;
        };

        if (value_type == ValueType::kWideColumnEntity) {
          assert(existing_columns);

          for (const auto& column : *existing_columns) {
            new_columns->emplace_back(column.name().ToString(),
                                      upper(column.value().ToString()));
          }
        } else {
          assert(existing_value);

          new_columns->emplace_back(kDefaultWideColumnName.ToString(),
                                    upper(existing_value->ToString()));
        }

        return Decision::kChangeWideColumnEntity;
      }

      const char* Name() const override { return "ChangeEntityFilter"; }
    };

    ChangeEntityFilter filter;
    options.compaction_filter = &filter;

    DestroyAndReopen(options);

    write();

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               first_key, &result));
      ASSERT_EQ(result.columns(), first_columns_uppercase);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               second_key, &result));
      ASSERT_EQ(result.columns(), second_columns_uppercase);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               last_key, &result));

      WideColumns expected_columns{
          {kDefaultWideColumnName, last_value_uppercase}};
      ASSERT_EQ(result.columns(), expected_columns);
    }
  }
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

TEST_F(DBWideBasicTest, PinnableWideColumnsMove) {
  Options options = GetDefaultOptions();

  constexpr char key1[] = "foo";
  constexpr char value[] = "bar";
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), key1, value));

  constexpr char key2[] = "baz";
  const WideColumns columns{{"quux", "corge"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns));

  ASSERT_OK(db_->Flush(FlushOptions()));

  const auto test_move = [&](bool fill_cache) {
    ReadOptions read_options;
    read_options.fill_cache = fill_cache;

    {
      const WideColumns expected_columns{{kDefaultWideColumnName, value}};

      {
        PinnableWideColumns result;
        ASSERT_OK(db_->GetEntity(read_options, db_->DefaultColumnFamily(), key1,
                                 &result));
        ASSERT_EQ(result.columns(), expected_columns);

        PinnableWideColumns move_target(std::move(result));
        ASSERT_EQ(move_target.columns(), expected_columns);
      }

      {
        PinnableWideColumns result;
        ASSERT_OK(db_->GetEntity(read_options, db_->DefaultColumnFamily(), key1,
                                 &result));
        ASSERT_EQ(result.columns(), expected_columns);

        PinnableWideColumns move_target;
        move_target = std::move(result);
        ASSERT_EQ(move_target.columns(), expected_columns);
      }
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(read_options, db_->DefaultColumnFamily(), key2,
                               &result));
      ASSERT_EQ(result.columns(), columns);

      PinnableWideColumns move_target(std::move(result));
      ASSERT_EQ(move_target.columns(), columns);
    }

    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(read_options, db_->DefaultColumnFamily(), key2,
                               &result));
      ASSERT_EQ(result.columns(), columns);

      PinnableWideColumns move_target;
      move_target = std::move(result);
      ASSERT_EQ(move_target.columns(), columns);
    }
  };

  // Test with and without fill_cache to cover both the case when pointers are
  // invalidated during PinnableSlice's move and when they are not.
  test_move(/* fill_cache*/ false);
  test_move(/* fill_cache*/ true);
}

TEST_F(DBWideBasicTest, PutEntityWithBlobValue) {
  // Test that PutEntity works and after compaction with blob options,
  // large columns are stored as blobs

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 100;  // Values >= 100 bytes go to blob files

  Reopen(options);

  // Create columns with values larger than min_blob_size
  constexpr char key[] = "blob_entity_key";
  const std::string large_value =
      GenerateLargeValue(200);  // 200 bytes, larger than threshold
  const std::string small_value =
      GenerateSmallValue();  // Less than min_blob_size

  // Columns must be in sorted order by name
  // kDefaultWideColumnName (empty string) < "large_col" < "small_col"
  WideColumns columns{{kDefaultWideColumnName, large_value},
                      {"large_col", large_value},
                      {"small_col", small_value}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));

  // Verify from memtable
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  // Flush to create SST file (and blob file for large values)
  ASSERT_OK(Flush());

  // Verify after flush
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  // Compact to ensure blob files are properly created
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify after compaction - blob resolution should return correct values
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  // Explicit verification: blob files were actually created
  {
    // Verify at least one blob file exists
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_GE(blob_files.size(), 1)
        << "Expected at least one blob file after compaction with large values";

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
  }
}

TEST_F(DBWideBasicTest, GetEntityWithBlobResolution) {
  // Test GetEntity() returns the correct entity with blob values resolved
  // Also verify blob statistics to confirm data is stored in blob files

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 50;
  options.statistics = CreateDBStatistics();

  Reopen(options);

  constexpr char first_key[] = "first";
  constexpr char second_key[] = "second";

  const std::string blob_value1 =
      GenerateLargeValue(100);  // Will be stored as blob
  const std::string blob_value2 =
      GenerateLargeValue(150);                // Will be stored as blob
  const std::string inline_value = "inline";  // Won't be stored as blob

  WideColumns first_columns{{kDefaultWideColumnName, blob_value1},
                            {"attr1", inline_value},
                            {"attr2", blob_value2}};

  WideColumns second_columns{{"col1", blob_value1}, {"col2", inline_value}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           first_key, first_columns));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           second_key, second_columns));

  // Reset statistics before flush/compaction
  options.statistics->Reset();

  // Flush and compact to ensure data goes to blob files
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify blob statistics - confirm data was written to blob files
  {
    uint64_t blob_bytes_written =
        options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_WRITTEN);
    ASSERT_GT(blob_bytes_written, 0)
        << "Expected non-zero bytes written to blob files";

    // The total bytes should account for the large column values
    // We have 3 blob values: blob_value1 (100 bytes) x 2 + blob_value2 (150
    // bytes) Each blob record has header overhead, so actual written bytes
    // will be larger
    const uint64_t expected_min_bytes =
        blob_value1.size() * 2 + blob_value2.size();
    ASSERT_GE(blob_bytes_written, expected_min_bytes)
        << "Blob bytes written should account for all large column values";
  }

  // Verify blob files exist
  {
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_GE(blob_files.size(), 1)
        << "Expected at least one blob file after compaction";
  }

  // Verify GetEntity resolves blob references correctly
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             first_key, &result));
    ASSERT_EQ(result.columns(), first_columns);
  }

  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             second_key, &result));
    ASSERT_EQ(result.columns(), second_columns);
  }

  // Also verify Get returns correct default column value
  {
    PinnableSlice result;
    ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), first_key,
                       &result));
    ASSERT_EQ(result, blob_value1);
  }
}

TEST_F(DBWideBasicTest, IterateEntityWithBlobResolution) {
  // Test that iterating over entities correctly resolves blob columns

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 50;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr char key2[] = "key2";
  constexpr char key3[] = "key3";

  const std::string blob_value =
      GenerateLargeValue(100);  // Will be stored as blob
  const std::string small_value =
      GenerateSmallValue();  // Won't be stored as blob

  WideColumns columns1{{kDefaultWideColumnName, blob_value},
                       {"a", small_value}};
  WideColumns columns2{{"b", blob_value}, {"c", small_value}};
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"d", blob_value}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));

  // Flush and compact to move data to blob files
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Forward iteration
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->columns(), columns1);
    ASSERT_EQ(iter->value(), blob_value);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->columns(), columns2);
    ASSERT_TRUE(iter->value().empty());  // No default column

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key3);
    ASSERT_EQ(iter->columns(), columns3);
    ASSERT_EQ(iter->value(), small_value);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Backward iteration
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key3);
    ASSERT_EQ(iter->columns(), columns3);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->columns(), columns2);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->columns(), columns1);

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

TEST_F(DBWideBasicTest, MultiGetEntityWithBlobResolution) {
  // Test MultiGetEntity with entities that have blob columns

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 50;

  Reopen(options);

  constexpr size_t num_keys = 4;
  constexpr char key1[] = "key1";
  constexpr char key2[] = "key2";
  constexpr char key3[] = "key3";
  constexpr char key4[] = "key4";

  const std::string blob_value = GenerateLargeValue(100);
  const std::string small_value = GenerateSmallValue();

  WideColumns columns1{{kDefaultWideColumnName, blob_value}};
  WideColumns columns2{{"attr", blob_value}, {"attr2", small_value}};
  WideColumns columns3{{kDefaultWideColumnName, small_value}};
  WideColumns columns4{{"x", blob_value}, {"y", blob_value}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key4,
                           columns4));

  // Flush and compact
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Test MultiGetEntity
  {
    std::array<Slice, num_keys> keys{{key1, key2, key3, key4}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), columns1);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(results[1].columns(), columns2);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(results[2].columns(), columns3);

    ASSERT_OK(statuses[3]);
    ASSERT_EQ(results[3].columns(), columns4);
  }

  // Test MultiGet for default column values
  {
    std::array<Slice, num_keys> keys{{key1, key2, key3, key4}};
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], blob_value);

    ASSERT_OK(statuses[1]);
    ASSERT_TRUE(values[1].empty());  // No default column

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], small_value);

    ASSERT_OK(statuses[3]);
    ASSERT_TRUE(values[3].empty());  // No default column
  }
}

void DBWideBasicTest::RunEntityBlobAfterFlush(const Options& options) {
  Reopen(options);

  constexpr char key1[] = "flush_key1";
  constexpr char key2[] = "flush_key2";

  const std::string blob_value = GenerateLargeValue(128);
  const std::string small_value = "tiny";

  WideColumns columns1{{kDefaultWideColumnName, blob_value},
                       {"col1", small_value}};
  WideColumns columns2{{"col2", blob_value}, {"col3", blob_value}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));

  // Verify before flush (from memtable)
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result));
    ASSERT_EQ(result.columns(), columns1);
  }

  // Flush
  ASSERT_OK(Flush());

  // Verify after flush
  {
    PinnableWideColumns result1;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result1));
    ASSERT_EQ(result1.columns(), columns1);

    PinnableWideColumns result2;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                             &result2));
    ASSERT_EQ(result2.columns(), columns2);
  }

  // Verify with iterator
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
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Write more data and flush again
  constexpr char key3[] = "flush_key3";
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"col4", blob_value}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));
  ASSERT_OK(Flush());

  // Verify all three keys via MultiGetEntity
  {
    constexpr size_t num_keys = 3;
    std::array<Slice, num_keys> keys{{key1, key2, key3}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), columns1);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(results[1].columns(), columns2);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(results[2].columns(), columns3);
  }
}

TEST_F(DBWideBasicTest, EntityBlobAfterFlush) {
  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 64;
  RunEntityBlobAfterFlush(options);
}

void DBWideBasicTest::RunEntityBlobAfterCompaction(const Options& options) {
  Reopen(options);

  constexpr char key1[] = "compact_key1";
  constexpr char key2[] = "compact_key2";
  constexpr char key3[] = "compact_key3";

  const std::string blob_value1 = GenerateLargeValue(128);
  const std::string blob_value2 = GenerateLargeValue(256);
  const std::string small_value = GenerateSmallValue();

  WideColumns columns1{{kDefaultWideColumnName, blob_value1},
                       {"attr1", small_value}};
  WideColumns columns2{{"attr2", blob_value1}, {"attr3", blob_value2}};
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"attr4", blob_value2}};

  // Write and flush first batch
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(Flush());

  // Write and flush second batch
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));
  ASSERT_OK(Flush());

  // Verify before compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result));
    ASSERT_EQ(result.columns(), columns1);
  }

  // Compact
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify after compaction - GetEntity
  {
    PinnableWideColumns result1;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result1));
    ASSERT_EQ(result1.columns(), columns1);

    PinnableWideColumns result2;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                             &result2));
    ASSERT_EQ(result2.columns(), columns2);

    PinnableWideColumns result3;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                             &result3));
    ASSERT_EQ(result3.columns(), columns3);
  }

  // Get (for default column)
  {
    PinnableSlice result1;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &result1));
    ASSERT_EQ(result1, blob_value1);

    PinnableSlice result2;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key2, &result2));
    ASSERT_TRUE(result2.empty());  // No default column

    PinnableSlice result3;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key3, &result3));
    ASSERT_EQ(result3, small_value);
  }

  // Iterator
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->columns(), columns1);
    ASSERT_EQ(iter->value(), blob_value1);

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
    ASSERT_EQ(iter->value(), small_value);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // MultiGetEntity
  {
    constexpr size_t num_keys = 3;
    std::array<Slice, num_keys> keys{{key1, key2, key3}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), columns1);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(results[1].columns(), columns2);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(results[2].columns(), columns3);
  }

  // Reopen and verify persistence
  Reopen(options);

  {
    PinnableWideColumns result1;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result1));
    ASSERT_EQ(result1.columns(), columns1);

    PinnableWideColumns result2;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                             &result2));
    ASSERT_EQ(result2.columns(), columns2);

    PinnableWideColumns result3;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                             &result3));
    ASSERT_EQ(result3.columns(), columns3);
  }
}

TEST_F(DBWideBasicTest, EntityBlobAfterCompaction) {
  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 64;
  RunEntityBlobAfterCompaction(options);
}

TEST_F(DBWideBasicTest, SanityChecks) {
  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";
  constexpr size_t num_keys = 2;

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    PinnableWideColumns columns;
    ASSERT_TRUE(db_->GetEntity(ReadOptions(), column_family, foo, &columns)
                    .IsInvalidArgument());
  }

  {
    constexpr PinnableWideColumns* columns = nullptr;
    ASSERT_TRUE(
        db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), foo, columns)
            .IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kGet;

    PinnableWideColumns columns;
    ASSERT_TRUE(
        db_->GetEntity(read_options, db_->DefaultColumnFamily(), foo, &columns)
            .IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), column_family, num_keys, keys.data(),
                        results.data(), statuses.data());

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr Slice* keys = nullptr;
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                        keys, results.data(), statuses.data());

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    std::array<Slice, num_keys> keys{{foo, bar}};
    constexpr PinnableWideColumns* results = nullptr;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                        keys.data(), results, statuses.data());

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kMultiGet;

    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(read_options, db_->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());
    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle** column_families = nullptr;
    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), num_keys, column_families, keys.data(),
                        results.data(), statuses.data());

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    std::array<ColumnFamilyHandle*, num_keys> column_families{
        {db_->DefaultColumnFamily(), db_->DefaultColumnFamily()}};
    constexpr Slice* keys = nullptr;
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), num_keys, column_families.data(), keys,
                        results.data(), statuses.data());

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    std::array<ColumnFamilyHandle*, num_keys> column_families{
        {db_->DefaultColumnFamily(), db_->DefaultColumnFamily()}};
    std::array<Slice, num_keys> keys{{foo, bar}};
    constexpr PinnableWideColumns* results = nullptr;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), num_keys, column_families.data(),
                        keys.data(), results, statuses.data());

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kMultiGet;

    std::array<ColumnFamilyHandle*, num_keys> column_families{
        {db_->DefaultColumnFamily(), db_->DefaultColumnFamily()}};
    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(read_options, num_keys, column_families.data(),
                        keys.data(), results.data(), statuses.data());
    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }
}

// Compaction filter that removes wide-column entities based on TTL threshold.
// Used by CompactionFilterWithBlobGC and its universal compaction variant.
class TTLCompactionFilter : public CompactionFilter {
 public:
  explicit TTLCompactionFilter(std::atomic<int>* ttl_threshold)
      : ttl_threshold_(ttl_threshold) {}

  Decision FilterV4(
      int /* level */, const Slice& /* key */, ValueType value_type,
      const Slice* /* existing_value */, const WideColumns* existing_columns,
      std::string* /* new_value */,
      std::vector<std::pair<std::string, std::string>>* /* new_columns */,
      std::string* /* skip_until */,
      WideColumnBlobResolver* /* blob_resolver */) const override {
    if (value_type != ValueType::kWideColumnEntity || !existing_columns) {
      return Decision::kKeep;
    }

    for (const auto& column : *existing_columns) {
      if (column.name() == "ttl") {
        int ttl_value = std::stoi(column.value().ToString());
        if (ttl_value < ttl_threshold_->load()) {
          return Decision::kRemove;
        }
        break;
      }
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "TTLCompactionFilter"; }

 private:
  std::atomic<int>* ttl_threshold_;
};

void DBWideBasicTest::RunCompactionFilterWithBlobGC(
    const Options& options, std::atomic<int>* ttl_threshold) {
  constexpr int kNumRecords = 100;
  constexpr int kRecordsPerFlush = 10;
  constexpr int kNumFlushes = kNumRecords / kRecordsPerFlush;
  constexpr size_t kBlobValueSize = 1024;  // 1KB

  DestroyAndReopen(options);

  const std::string blob_value1 = GenerateLargeValue(kBlobValueSize, 'x');
  const std::string blob_value2 = GenerateLargeValue(kBlobValueSize, 'y');

  // Write 100 records, flushing every 10 records
  for (int i = 0; i < kNumRecords; ++i) {
    char key_buf[20];
    snprintf(key_buf, sizeof(key_buf), "key_%04d", i);
    std::string key(key_buf);

    std::string ttl_value = std::to_string(i);

    WideColumns columns{
        {"ttl", ttl_value}, {"value1", blob_value1}, {"value2", blob_value2}};

    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                             columns));

    if ((i + 1) % kRecordsPerFlush == 0) {
      ASSERT_OK(Flush());
    }
  }

  // Verify we have kNumFlushes blob files (one per flush)
  {
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_EQ(blob_files.size(), static_cast<size_t>(kNumFlushes))
        << "Expected " << kNumFlushes << " blob files after " << kNumFlushes
        << " flushes";
  }

  // Verify all records exist
  {
    int count = 0;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++count;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, kNumRecords);
  }

  // Incrementally remove records and verify blob files are garbage collected
  for (int round = 1; round <= kNumFlushes; ++round) {
    int threshold = round * kRecordsPerFlush;
    ttl_threshold->store(threshold);

    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    int expected_remaining_records = kNumRecords - threshold;

    // Verify remaining records count and all column values
    {
      int count = 0;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ++count;

        const WideColumns& columns = iter->columns();
        ASSERT_EQ(columns.size(), 3)
            << "Expected 3 columns (ttl, value1, value2)";

        bool found_ttl = false;
        bool found_value1 = false;
        bool found_value2 = false;

        for (const auto& column : columns) {
          if (column.name() == "ttl") {
            int ttl_value = std::stoi(column.value().ToString());
            ASSERT_GE(ttl_value, threshold)
                << "Key with ttl " << ttl_value
                << " should have been removed (threshold=" << threshold << ")";
            found_ttl = true;
          } else if (column.name() == "value1") {
            ASSERT_EQ(column.value().ToString(), blob_value1)
                << "Blob value1 mismatch for key " << iter->key().ToString();
            found_value1 = true;
          } else if (column.name() == "value2") {
            ASSERT_EQ(column.value().ToString(), blob_value2)
                << "Blob value2 mismatch for key " << iter->key().ToString();
            found_value2 = true;
          }
        }

        ASSERT_TRUE(found_ttl)
            << "TTL column not found for key " << iter->key().ToString();
        ASSERT_TRUE(found_value1)
            << "value1 column not found for key " << iter->key().ToString();
        ASSERT_TRUE(found_value2)
            << "value2 column not found for key " << iter->key().ToString();
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(count, expected_remaining_records)
          << "Round " << round << ": expected " << expected_remaining_records
          << " records after removing ttl < " << threshold;
    }

    if (expected_remaining_records == 0) {
      std::vector<uint64_t> blob_files = GetBlobFileNumbers();
      ASSERT_EQ(blob_files.size(), 0)
          << "All blob files should be garbage collected after all records "
             "are removed";
    }
  }

  // Final verification: database should be empty
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid()) << "Database should be empty";
    ASSERT_OK(iter->status());
  }

  // Final verification: no blob files should remain
  {
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_EQ(blob_files.size(), 0) << "All blob files should be removed";
  }
}

TEST_F(DBWideBasicTest, CompactionFilterWithBlobGC) {
  std::atomic<int> ttl_threshold{0};
  TTLCompactionFilter filter(&ttl_threshold);

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 100;
  options.compaction_filter = &filter;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;

  RunCompactionFilterWithBlobGC(options, &ttl_threshold);
}

TEST_F(DBWideBasicTest, EntityBlobAfterFlushUniversal) {
  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 64;
  options.compaction_style = kCompactionStyleUniversal;
  RunEntityBlobAfterFlush(options);
}

TEST_F(DBWideBasicTest, EntityBlobAfterCompactionUniversal) {
  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 64;
  options.compaction_style = kCompactionStyleUniversal;
  RunEntityBlobAfterCompaction(options);
}

TEST_F(DBWideBasicTest, CompactionFilterWithBlobGCUniversal) {
  std::atomic<int> ttl_threshold{0};
  TTLCompactionFilter filter(&ttl_threshold);

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 100;
  options.compaction_filter = &filter;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.0;
  options.compaction_style = kCompactionStyleUniversal;

  RunCompactionFilterWithBlobGC(options, &ttl_threshold);
}

TEST_F(DBWideBasicTest, MergeEntityWithBlobBackwardIteration) {
  // Test that backward iteration (Prev) correctly resolves blob columns
  // in entities that are base values for merge operations. This exercises
  // MergeWithWideColumnBaseValue which must resolve blob references before
  // passing the entity to TimedFullMerge (which only supports V1 format).

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 50;
  const std::string delim(",");
  options.merge_operator = MergeOperators::CreateStringAppendOperator(delim);

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr char key2[] = "key2";

  const std::string large_value = GenerateLargeValue(100);
  const std::string small_value = GenerateSmallValue();

  // PutEntity with a large column value that will be stored as a blob
  WideColumns columns1{{kDefaultWideColumnName, large_value},
                       {"attr", small_value}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  // A simple key for reference
  ASSERT_OK(Put(key2, "plain_value"));

  // Flush and compact to move large values to blob files
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Now apply a merge on top of the entity with blob columns
  ASSERT_OK(
      db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key1, "merged"));
  ASSERT_OK(Flush());

  const std::string expected_default = large_value + delim + "merged";

  // Forward iteration: verify merge result
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->value(), expected_default);
  }

  // Backward iteration: this exercises MergeWithWideColumnBaseValue
  // with blob entity during reverse scan
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->value(), "plain_value");

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->value(), expected_default);

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Also verify via Get and GetEntity
  {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), key1, &value));
    ASSERT_EQ(value, expected_default);
  }

  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result));
    // After merge, the result should have the merged default column
    // and the original non-default columns preserved
    WideColumns expected{{kDefaultWideColumnName, expected_default},
                         {"attr", small_value}};
    ASSERT_EQ(result.columns(), expected);
  }
}

TEST_F(DBWideBasicTest, LazyColumnResolutionIterator) {
  // Test that lazy_column_resolution works correctly with iterators.
  // When enabled, blob columns should remain unresolved until
  // ResolveColumn() is called.

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 50;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr char key2[] = "key2";

  const std::string blob_value = GenerateLargeValue(100);
  const std::string small_value = GenerateSmallValue();

  WideColumns columns1{{kDefaultWideColumnName, blob_value},
                       {"a", small_value}};
  WideColumns columns2{{"b", blob_value}, {"c", small_value}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Forward iteration with lazy resolution
  {
    ReadOptions read_opts;
    read_opts.lazy_column_resolution = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);

    // The default column (blob) should have been eagerly resolved for value()
    ASSERT_EQ(iter->value(), blob_value);

    // Non-blob column "a" should NOT be unresolved
    ASSERT_FALSE(iter->IsUnresolvedColumn(1));
    ASSERT_EQ(iter->columns()[1].value(), small_value);

    // Column 0 (default) was eagerly resolved
    ASSERT_FALSE(iter->IsUnresolvedColumn(0));

    // Should have no unresolved columns since default was auto-resolved and
    // "a" is inline
    ASSERT_FALSE(iter->HasUnresolvedColumns());

    // Move to key2 which has blob column "b" and inline column "c"
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key2);

    // "b" (index 0) is a blob column - should be unresolved
    ASSERT_TRUE(iter->HasUnresolvedColumns());
    ASSERT_TRUE(iter->IsUnresolvedColumn(0));

    // "c" (index 1) is an inline column - should NOT be unresolved
    ASSERT_FALSE(iter->IsUnresolvedColumn(1));
    ASSERT_EQ(iter->columns()[1].value(), small_value);

    // Resolve the blob column
    ASSERT_OK(iter->ResolveColumn(0));
    ASSERT_FALSE(iter->IsUnresolvedColumn(0));
    ASSERT_EQ(iter->columns()[0].value(), blob_value);

    // Now no unresolved columns
    ASSERT_FALSE(iter->HasUnresolvedColumns());

    // Verify the full columns match
    ASSERT_EQ(iter->columns(), columns2);
  }

  // Test ResolveAllColumns
  {
    ReadOptions read_opts;
    read_opts.lazy_column_resolution = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    // Resolve all columns at once
    ASSERT_OK(iter->ResolveAllColumns());
    ASSERT_FALSE(iter->HasUnresolvedColumns());
    ASSERT_EQ(iter->columns(), columns1);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    ASSERT_OK(iter->ResolveAllColumns());
    ASSERT_FALSE(iter->HasUnresolvedColumns());
    ASSERT_EQ(iter->columns(), columns2);
  }

  // Backward iteration with lazy resolution
  {
    ReadOptions read_opts;
    read_opts.lazy_column_resolution = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key2);

    ASSERT_OK(iter->ResolveAllColumns());
    ASSERT_EQ(iter->columns(), columns2);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);

    ASSERT_OK(iter->ResolveAllColumns());
    ASSERT_EQ(iter->columns(), columns1);
  }

  // Without lazy resolution (default behavior), everything works as before
  {
    ReadOptions read_opts;
    read_opts.lazy_column_resolution = false;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_FALSE(iter->HasUnresolvedColumns());
    ASSERT_EQ(iter->columns(), columns1);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_FALSE(iter->HasUnresolvedColumns());
    ASSERT_EQ(iter->columns(), columns2);
  }
}

TEST_F(DBWideBasicTest, LazyColumnResolutionWithMerge) {
  // Test that lazy_column_resolution works correctly when merge operations
  // are involved. Merge should force eager resolution since it needs all
  // column values.

  Options options = GetOptionsForBlobTest();
  options.min_blob_size = 50;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();

  Reopen(options);

  constexpr char key1[] = "key1";

  const std::string blob_value = GenerateLargeValue(100);
  const std::string small_value = GenerateSmallValue();

  WideColumns columns{{kDefaultWideColumnName, blob_value}, {"a", small_value}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // The entity should be readable with lazy resolution
  {
    ReadOptions read_opts;
    read_opts.lazy_column_resolution = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), key1);

    // Resolve all and check
    ASSERT_OK(iter->ResolveAllColumns());
    ASSERT_EQ(iter->columns(), columns);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
