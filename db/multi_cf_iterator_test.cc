//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>

#include "db/db_test_util.h"

namespace ROCKSDB_NAMESPACE {

class MultiCfIteratorTest : public DBTestBase {
 public:
  MultiCfIteratorTest()
      : DBTestBase("multi_cf_iterator_test", /*env_do_fsync=*/true) {}

  // Verify Iteration of MultiCfIterator
  // by SeekToFirst() + Next() and SeekToLast() + Prev()
  void verifyMultiCfIterator(
      const std::vector<ColumnFamilyHandle*>& cfhs,
      const std::vector<Slice>& expected_keys,
      const std::optional<std::vector<Slice>>& expected_values = std::nullopt,
      const std::optional<std::vector<WideColumns>>& expected_wide_columns =
          std::nullopt,
      const std::optional<std::vector<AttributeGroups>>&
          expected_attribute_groups = std::nullopt) {
    int i = 0;
    std::unique_ptr<Iterator> iter =
        db_->NewMultiCfIterator(ReadOptions(), cfhs);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(expected_keys[i], iter->key());
      if (expected_values.has_value()) {
        ASSERT_EQ(expected_values.value()[i], iter->value());
      }
      if (expected_wide_columns.has_value()) {
        ASSERT_EQ(expected_wide_columns.value()[i], iter->columns());
      }
      if (expected_attribute_groups.has_value()) {
        // TODO - Add this back when attribute_groups() API is added
        // ASSERT_EQ(expected_attribute_groups.value()[i],
        //           iter->attribute_groups());
      }
      ++i;
    }
    ASSERT_EQ(expected_keys.size(), i);
    ASSERT_OK(iter->status());

    int rev_i = i - 1;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ASSERT_EQ(expected_keys[rev_i], iter->key());
      if (expected_values.has_value()) {
        ASSERT_EQ(expected_values.value()[rev_i], iter->value());
      }
      if (expected_wide_columns.has_value()) {
        ASSERT_EQ(expected_wide_columns.value()[rev_i], iter->columns());
      }
      if (expected_attribute_groups.has_value()) {
        // TODO - Add this back when attribute_groups() API is added
        // ASSERT_EQ(expected_attribute_groups.value()[rev_i],
        //           iter->attribute_groups());
      }
      rev_i--;
    }
    ASSERT_OK(iter->status());
  }

  void verifyExpectedKeys(ColumnFamilyHandle* cfh,
                          const std::vector<Slice>& expected_keys) {
    int i = 0;
    Iterator* iter = db_->NewIterator(ReadOptions(), cfh);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(expected_keys[i], iter->key());
      ++i;
    }
    ASSERT_EQ(expected_keys.size(), i);
    ASSERT_OK(iter->status());
    delete iter;
  }
};

TEST_F(MultiCfIteratorTest, InvalidArguments) {
  Options options = GetDefaultOptions();
  {
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    // Invalid - No CF is provided
    std::unique_ptr<Iterator> iter_with_no_cf =
        db_->NewMultiCfIterator(ReadOptions(), {});
    ASSERT_NOK(iter_with_no_cf->status());
    ASSERT_TRUE(iter_with_no_cf->status().IsInvalidArgument());
  }
}

TEST_F(MultiCfIteratorTest, SimpleValues) {
  Options options = GetDefaultOptions();
  {
    // Case 1: Unique key per CF
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
    ASSERT_OK(Put(2, "key_3", "key_3_cf_2_val"));
    ASSERT_OK(Put(3, "key_4", "key_4_cf_3_val"));

    std::vector<Slice> expected_keys = {"key_1", "key_2", "key_3", "key_4"};
    std::vector<Slice> expected_values = {"key_1_cf_0_val", "key_2_cf_1_val",
                                          "key_3_cf_2_val", "key_4_cf_3_val"};

    // Test for iteration over CF default->1->2->3
    std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
        handles_[0], handles_[1], handles_[2], handles_[3]};
    verifyMultiCfIterator(cfhs_order_0_1_2_3, expected_keys, expected_values);

    // Test for iteration over CF 3->1->default_cf->2
    std::vector<ColumnFamilyHandle*> cfhs_order_3_1_0_2 = {
        handles_[3], handles_[1], handles_[0], handles_[2]};
    // Iteration order and the return values should be the same since keys are
    // unique per CF
    verifyMultiCfIterator(cfhs_order_3_1_0_2, expected_keys, expected_values);

    // Verify Seek()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewMultiCfIterator(ReadOptions(), cfhs_order_0_1_2_3);
      iter->Seek("");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Seek("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Seek("key_2");
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
      iter->Seek("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
    // Verify SeekForPrev()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewMultiCfIterator(ReadOptions(), cfhs_order_0_1_2_3);
      iter->SeekForPrev("");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      iter->SeekForPrev("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_1_val");
      iter->SeekForPrev("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "key_4->key_4_cf_3_val");
      iter->Prev();
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_2_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_4->key_4_cf_3_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
  }
  {
    // Case 2: Same key in multiple CFs
    options = CurrentOptions(options);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(3, "key_1", "key_1_cf_3_val"));
    ASSERT_OK(Put(1, "key_2", "key_2_cf_1_val"));
    ASSERT_OK(Put(2, "key_2", "key_2_cf_2_val"));
    ASSERT_OK(Put(0, "key_3", "key_3_cf_0_val"));
    ASSERT_OK(Put(1, "key_3", "key_3_cf_1_val"));
    ASSERT_OK(Put(3, "key_3", "key_3_cf_3_val"));

    std::vector<Slice> expected_keys = {"key_1", "key_2", "key_3"};

    // Test for iteration over CFs default->1->2->3
    std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
        handles_[0], handles_[1], handles_[2], handles_[3]};
    std::vector<Slice> expected_values = {"key_1_cf_0_val", "key_2_cf_1_val",
                                          "key_3_cf_0_val"};
    verifyMultiCfIterator(cfhs_order_0_1_2_3, expected_keys, expected_values);

    // Test for iteration over CFs 3->2->default_cf->1
    std::vector<ColumnFamilyHandle*> cfhs_order_3_2_0_1 = {
        handles_[3], handles_[2], handles_[0], handles_[1]};
    expected_values = {"key_1_cf_3_val", "key_2_cf_2_val", "key_3_cf_3_val"};
    verifyMultiCfIterator(cfhs_order_3_2_0_1, expected_keys, expected_values);

    // Verify Seek()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewMultiCfIterator(ReadOptions(), cfhs_order_3_2_0_1);
      iter->Seek("");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_3_val");
      iter->Seek("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_3_val");
      iter->Seek("key_2");
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_2_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_3_val");
      iter->Seek("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
    // Verify SeekForPrev()
    {
      std::unique_ptr<Iterator> iter =
          db_->NewMultiCfIterator(ReadOptions(), cfhs_order_3_2_0_1);
      iter->SeekForPrev("");
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
      iter->SeekForPrev("key_1");
      ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_3_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "key_2->key_2_cf_2_val");
      iter->SeekForPrev("key_x");
      ASSERT_EQ(IterStatus(iter.get()), "key_3->key_3_cf_3_val");
      iter->Next();
      ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    }
  }
}

TEST_F(MultiCfIteratorTest, EmptyCfs) {
  Options options = GetDefaultOptions();
  {
    // Case 1: No keys in any of the CFs
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);
    std::unique_ptr<Iterator> iter =
        db_->NewMultiCfIterator(ReadOptions(), handles_);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->Seek("foo");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekForPrev("foo");
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");

    ASSERT_OK(iter->status());
  }
  {
    // Case 2: A single key exists in only one of the CF. Rest CFs are empty.
    ASSERT_OK(Put(1, "key_1", "key_1_cf_1_val"));
    std::unique_ptr<Iterator> iter =
        db_->NewMultiCfIterator(ReadOptions(), handles_);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_1_val");
    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_1_val");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
  }
  {
    // Case 3: same key exists in all of the CFs except one (cf_2)
    ASSERT_OK(Put(0, "key_1", "key_1_cf_0_val"));
    ASSERT_OK(Put(3, "key_1", "key_1_cf_3_val"));
    // handles_ are in the order of 0->1->2->3. We should expect value from cf_0
    std::unique_ptr<Iterator> iter =
        db_->NewMultiCfIterator(ReadOptions(), handles_);
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
    iter->Next();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter.get()), "key_1->key_1_cf_0_val");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter.get()), "(invalid)");
  }
}

TEST_F(MultiCfIteratorTest, WideColumns) {
  // Set up the DB and Column Families
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  constexpr char key_1[] = "key_1";
  WideColumns key_1_columns_in_cf_2{
      {kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_1"}};
  WideColumns key_1_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_1"},
      {"cf_3_col_name_2", "cf_3_col_val_2_key_1"},
      {"cf_3_col_name_3", "cf_3_col_val_3_key_1"}};

  constexpr char key_2[] = "key_2";
  WideColumns key_2_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_2"}};
  WideColumns key_2_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_2"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_2"}};

  constexpr char key_3[] = "key_3";
  WideColumns key_3_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_3"}};
  WideColumns key_3_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_3"}};

  constexpr char key_4[] = "key_4";
  WideColumns key_4_columns_in_cf_0{
      {"cf_0_col_name_1", "cf_0_col_val_1_key_4"}};
  WideColumns key_4_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_4"}};

  // Use AttributeGroup PutEntity API to insert them together
  AttributeGroups key_1_attribute_groups{
      AttributeGroup(handles_[2], key_1_columns_in_cf_2),
      AttributeGroup(handles_[3], key_1_columns_in_cf_3)};
  AttributeGroups key_2_attribute_groups{
      AttributeGroup(handles_[1], key_2_columns_in_cf_1),
      AttributeGroup(handles_[2], key_2_columns_in_cf_2)};
  AttributeGroups key_3_attribute_groups{
      AttributeGroup(handles_[1], key_3_columns_in_cf_1),
      AttributeGroup(handles_[3], key_3_columns_in_cf_3)};
  AttributeGroups key_4_attribute_groups{
      AttributeGroup(handles_[0], key_4_columns_in_cf_0),
      AttributeGroup(handles_[2], key_4_columns_in_cf_2)};

  ASSERT_OK(db_->PutEntity(WriteOptions(), key_1, key_1_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_2, key_2_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_3, key_3_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_4, key_4_attribute_groups));

  // Test for iteration over CF default->1->2->3
  std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
      handles_[0], handles_[1], handles_[2], handles_[3]};
  std::vector<Slice> expected_keys = {key_1, key_2, key_3, key_4};
  // Pick what DBIter would return for value() in the first CF that key exists
  // Since value for kDefaultWideColumnName only exists for key_1, rest will
  // return empty value
  std::vector<Slice> expected_values = {"cf_2_col_val_0_key_1", "", "", ""};

  // Pick columns from the first CF that the key exists and value is stored as
  // wide column
  std::vector<WideColumns> expected_wide_columns = {
      {{kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
       {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
       {"cf_2_col_name_2", "cf_2_col_val_2_key_1"}},
      {{"cf_1_col_name_1", "cf_1_col_val_1_key_2"}},
      {{"cf_1_col_name_1", "cf_1_col_val_1_key_3"}},
      {{"cf_0_col_name_1", "cf_0_col_val_1_key_4"}}};
  verifyMultiCfIterator(cfhs_order_0_1_2_3, expected_keys, expected_values,
                        expected_wide_columns);
}

TEST_F(MultiCfIteratorTest, DifferentComparatorsInMultiCFs) {
  // This test creates two column families with two different comparators.
  // Attempting to create the MultiCFIterator should fail.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  options.comparator = BytewiseComparator();
  CreateColumnFamilies({"cf_forward"}, options);
  options.comparator = ReverseBytewiseComparator();
  CreateColumnFamilies({"cf_reverse"}, options);

  ASSERT_OK(Put(0, "key_1", "value_1"));
  ASSERT_OK(Put(0, "key_2", "value_2"));
  ASSERT_OK(Put(0, "key_3", "value_3"));
  ASSERT_OK(Put(1, "key_1", "value_1"));
  ASSERT_OK(Put(1, "key_2", "value_2"));
  ASSERT_OK(Put(1, "key_3", "value_3"));

  verifyExpectedKeys(handles_[0], {"key_1", "key_2", "key_3"});
  verifyExpectedKeys(handles_[1], {"key_3", "key_2", "key_1"});

  std::unique_ptr<Iterator> iter =
      db_->NewMultiCfIterator(ReadOptions(), handles_);
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsInvalidArgument());
}

TEST_F(MultiCfIteratorTest, CustomComparatorsInMultiCFs) {
  // This test creates two column families with the same custom test
  // comparators (but instantiated independently). Attempting to create the
  // MultiCFIterator should not fail.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  static auto comparator_1 =
      std::make_unique<test::SimpleSuffixReverseComparator>(
          test::SimpleSuffixReverseComparator());
  static auto comparator_2 =
      std::make_unique<test::SimpleSuffixReverseComparator>(
          test::SimpleSuffixReverseComparator());
  ASSERT_NE(comparator_1, comparator_2);

  options.comparator = comparator_1.get();
  CreateColumnFamilies({"cf_1"}, options);
  options.comparator = comparator_2.get();
  CreateColumnFamilies({"cf_2"}, options);

  ASSERT_OK(Put(0, "key_001_001", "value_0_3"));
  ASSERT_OK(Put(0, "key_001_002", "value_0_2"));
  ASSERT_OK(Put(0, "key_001_003", "value_0_1"));
  ASSERT_OK(Put(0, "key_002_001", "value_0_6"));
  ASSERT_OK(Put(0, "key_002_002", "value_0_5"));
  ASSERT_OK(Put(0, "key_002_003", "value_0_4"));
  ASSERT_OK(Put(1, "key_001_001", "value_1_3"));
  ASSERT_OK(Put(1, "key_001_002", "value_1_2"));
  ASSERT_OK(Put(1, "key_001_003", "value_1_1"));
  ASSERT_OK(Put(1, "key_003_004", "value_1_6"));
  ASSERT_OK(Put(1, "key_003_005", "value_1_5"));
  ASSERT_OK(Put(1, "key_003_006", "value_1_4"));

  verifyExpectedKeys(
      handles_[0], {"key_001_003", "key_001_002", "key_001_001", "key_002_003",
                    "key_002_002", "key_002_001"});
  verifyExpectedKeys(
      handles_[1], {"key_001_003", "key_001_002", "key_001_001", "key_003_006",
                    "key_003_005", "key_003_004"});

  std::vector<Slice> expected_keys = {
      "key_001_003", "key_001_002", "key_001_001", "key_002_003", "key_002_002",
      "key_002_001", "key_003_006", "key_003_005", "key_003_004"};
  std::vector<Slice> expected_values = {"value_0_1", "value_0_2", "value_0_3",
                                        "value_0_4", "value_0_5", "value_0_6",
                                        "value_1_4", "value_1_5", "value_1_6"};
  int i = 0;
  std::unique_ptr<Iterator> iter =
      db_->NewMultiCfIterator(ReadOptions(), handles_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(expected_keys[i], iter->key());
    ASSERT_EQ(expected_values[i], iter->value());
    ++i;
  }
  ASSERT_OK(iter->status());
}

TEST_F(MultiCfIteratorTest, DISABLED_IterateAttributeGroups) {
  // Set up the DB and Column Families
  Options options = GetDefaultOptions();
  CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);

  constexpr char key_1[] = "key_1";
  WideColumns key_1_columns_in_cf_2{
      {kDefaultWideColumnName, "cf_2_col_val_0_key_1"},
      {"cf_2_col_name_1", "cf_2_col_val_1_key_1"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_1"}};
  WideColumns key_1_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_1"},
      {"cf_3_col_name_2", "cf_3_col_val_2_key_1"},
      {"cf_3_col_name_3", "cf_3_col_val_3_key_1"}};

  constexpr char key_2[] = "key_2";
  WideColumns key_2_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_2"}};
  WideColumns key_2_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_2"},
      {"cf_2_col_name_2", "cf_2_col_val_2_key_2"}};

  constexpr char key_3[] = "key_3";
  WideColumns key_3_columns_in_cf_1{
      {"cf_1_col_name_1", "cf_1_col_val_1_key_3"}};
  WideColumns key_3_columns_in_cf_3{
      {"cf_3_col_name_1", "cf_3_col_val_1_key_3"}};

  constexpr char key_4[] = "key_4";
  WideColumns key_4_columns_in_cf_0{
      {"cf_0_col_name_1", "cf_0_col_val_1_key_4"}};
  WideColumns key_4_columns_in_cf_2{
      {"cf_2_col_name_1", "cf_2_col_val_1_key_4"}};

  AttributeGroups key_1_attribute_groups{
      AttributeGroup(handles_[2], key_1_columns_in_cf_2),
      AttributeGroup(handles_[3], key_1_columns_in_cf_3)};
  AttributeGroups key_2_attribute_groups{
      AttributeGroup(handles_[1], key_2_columns_in_cf_1),
      AttributeGroup(handles_[2], key_2_columns_in_cf_2)};
  AttributeGroups key_3_attribute_groups{
      AttributeGroup(handles_[1], key_3_columns_in_cf_1),
      AttributeGroup(handles_[3], key_3_columns_in_cf_3)};
  AttributeGroups key_4_attribute_groups{
      AttributeGroup(handles_[0], key_4_columns_in_cf_0),
      AttributeGroup(handles_[2], key_4_columns_in_cf_2)};

  ASSERT_OK(db_->PutEntity(WriteOptions(), key_1, key_1_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_2, key_2_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_3, key_3_attribute_groups));
  ASSERT_OK(db_->PutEntity(WriteOptions(), key_4, key_4_attribute_groups));

  // Test for iteration over CF default->1->2->3
  std::vector<ColumnFamilyHandle*> cfhs_order_0_1_2_3 = {
      handles_[0], handles_[1], handles_[2], handles_[3]};
  std::vector<Slice> expected_keys = {key_1, key_2, key_3, key_4};
  std::vector<AttributeGroups> expected_attribute_groups = {
      key_1_attribute_groups, key_2_attribute_groups, key_3_attribute_groups,
      key_4_attribute_groups};
  verifyMultiCfIterator(
      cfhs_order_0_1_2_3, expected_keys, std::nullopt /* expected_values */,
      std::nullopt /* expected_wide_columns */, expected_attribute_groups);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
