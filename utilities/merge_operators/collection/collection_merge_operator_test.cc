//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//  @author Adam Retter

#include "collection_merge_operator.h"
#include "collection_merge_operator_test_dsl.h"

#include "port/port.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

const uint16_t DEFAULT_TEST_RECORD_SIZE = 4;

bool test_FullMergeV2(const std::vector<std::string>& str_operand_list,
    OwningMergeOperationOutput owning_merge_out,
    const Slice* existing_value = nullptr,
    const Comparator* comparator = nullptr,
    const UniqueConstraint unique_constraint = UniqueConstraint::kNone,
    const Slice& key = Slice("key1", 4)) {
  // input
  std::vector<Slice> operand_list;
  for (auto it = str_operand_list.begin(); it != str_operand_list.end(); ++it) {
    operand_list.push_back(Slice(*it));
  }
  TestLogger logger;
  MergeOperator::MergeOperationInput merge_in(key, existing_value, operand_list, &logger);

  // TODO(AR) adjust when we support variable record size
  CollectionMergeOperator collection_merge(DEFAULT_TEST_RECORD_SIZE, comparator, unique_constraint);
  const bool result = collection_merge.FullMergeV2(merge_in, &(owning_merge_out.merge_out));
  return result;
}

bool test_PartialMergeMulti(const std::vector<std::string>& str_operand_list,
    std::string* new_value, const Comparator* comparator = nullptr,
    const UniqueConstraint unique_constraint = UniqueConstraint::kNone,
    const Slice& key = Slice("key1", 4)) {

  // input
  std::deque<Slice> operand_list;
  for (auto it = str_operand_list.begin(); it != str_operand_list.end(); ++it) {
    operand_list.push_back(Slice(*it));
  }
  TestLogger logger;

  // TODO(AR) adjust when we support variable record size
  CollectionMergeOperator collection_merge(DEFAULT_TEST_RECORD_SIZE, comparator, unique_constraint);
  const bool result = collection_merge.PartialMergeMulti(key, operand_list, new_value, &logger);
  return result;
}

/** FullMergeV2 TESTS **/

TEST(FullMergeV2, EmptyOperandList) {
 std::vector<std::string> operand_list;
 Slice existing_value;

 auto merge_out = EmptyMergeOut();

 const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

 ASSERT_TRUE(result);
 ASSERT_TRUE(merge_out.new_value.empty());
 ASSERT_EQ(existing_value, merge_out.existing_operand);
}

TEST(FullMergeV2, UnknownOperand) {
  auto operand_list = {
    std::string("Z")  // unknown operand
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_FALSE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

/** FullMergeV2 CollectionOperation::kClear TESTS **/

TEST(FullMergeV2, Clear) {
  auto operand_list = {
    Clear()
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);
  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Clear_ExistingValue) {
  auto operand_list = {
    Clear()
  };
  Slice existing_value = RecordString(DEFAULT_TEST_RECORD_SIZE, 3);

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Clear_ExistingValue_KeepFollowing) {
  auto new_records = {
    RecordString(DEFAULT_TEST_RECORD_SIZE),
    RecordString(DEFAULT_TEST_RECORD_SIZE)
  };
  auto operand_list = {
    Clear(),
    Add(new_records)
  };
  Slice existing_value = RecordString(DEFAULT_TEST_RECORD_SIZE, 10);

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(toString(new_records), merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

/** FullMergeV2 CollectionOperation::kAdd TESTS **/

TEST(FullMergeV2, Add) {
  auto new_record = RecordString(DEFAULT_TEST_RECORD_SIZE);
  auto operand_list = {
    Add({new_record})
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ(new_record, merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_IncompleteRecord) {
  auto new_record = "no";
  auto operand_list = {
    Add({new_record})
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_FALSE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Existing) {
  auto operand_list = {
    Add("1000")
  };
  Slice existing_value("2000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("20001000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Existing_Incomplete) {
  auto operand_list = {
    Add("1000")
  };
  Slice existing_value("no");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_FALSE(result);
  ASSERT_EQ("no", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Ordered) {
  auto operand_list = {
    Add("3000"),
    Add("1000"),
    Add("5000"),
    Add("2000"),
    Add("7000"),
    Add("6000")
  };

  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000500060007000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Ordered_uint32) {
  auto operand_list = {
    Add(3000),
    Add(1000),
    Add(5000),
    Add(2000),
    Add(7000),
    Add(6000)
  };

  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, comparator);

  ASSERT_TRUE(result);
  if (port::kLittleEndian) {
    ASSERT_EQ(Add({7000,6000,5000,3000,2000,1000}).substr(1), merge_out.new_value);
  } else {
    ASSERT_EQ(Add({1000,2000,3000,5000,6000,7000}).substr(1), merge_out.new_value);
  }
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Existing_Ordered) {
  auto operand_list = {
    Add("1000")
  };
  Slice existing_value("2000");
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("10002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Existing_Ordered_Reverse) {
  auto operand_list = {
    Add("1000")
  };
  Slice existing_value("2000");
  auto* comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("20001000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Existing_Duplicate_Constraint_None) {
  auto operand_list = {
    Add("1000")
  };
  Slice existing_value("1000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("10001000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Existing_Duplicate_Constraint_EnforceUnique_NoConflict) {
  auto operand_list = {
    Add("2000")
  };
  Slice existing_value("1000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ("10002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_Existing_Duplicate_Constraint_EnforceUnique_Conflict) {
  auto operand_list = {
    Add("1000")
  };
  Slice existing_value("1000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_FALSE(result);
  ASSERT_EQ("1000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany) {
  auto operand_list = {
    Add({
      "2000",
      "4000",
      "6000",
      "8000",
      "9000"
    })
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("20004000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands) {
  auto operand_list = {
    Add("2000"),
    Add("4000"),
    Add("6000"),
    Add("8000"),
    Add("9000")
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("20004000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_ExistingValue) {
  auto operand_list = {
    Add({
      "2000",
      "4000",
      "6000",
      "8000",
      "9000"
    })
  };
  Slice existing_value("5000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("500020004000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands_ExistingValue) {
  auto operand_list = {
    Add("2000"),
    Add("4000"),
    Add("6000"),
    Add("8000"),
    Add("9000")
  };
  Slice existing_value("5000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("500020004000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Ordered) {
  auto operand_list = {
    Add({
      "9000",
      "4000",
      "8000",
      "6000",
      "2000"
    })
  };
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("20004000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Ordered_Reverse) {
  auto operand_list = {
    Add({
      "9000",
      "4000",
      "8000",
      "6000",
      "2000"
    })
  };
  auto* comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("90008000600040002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands_Ordered) {
  auto operand_list = {
    Add("9000"),
    Add("4000"),
    Add("8000"),
    Add("6000"),
    Add("2000")
  };
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("20004000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands_Ordered_Reverse) {
  auto operand_list = {
    Add("9000"),
    Add("4000"),
    Add("8000"),
    Add("6000"),
    Add("2000")
  };
  auto* comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("90008000600040002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_ExistingValue_Ordered) {
  auto operand_list = {
    Add({
      "8000",
      "4000",
      "2000",
      "6000",
      "9000"
    })
  };
  Slice existing_value("5000");
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("200040005000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_ExistingValue_Ordered_Reverse) {
  auto operand_list = {
    Add({
      "8000",
      "4000",
      "2000",
      "6000",
      "9000"
    })
  };
  Slice existing_value("5000");
  auto* comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("900080006000500040002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands_ExistingValue_Ordered) {
  auto operand_list = {
    Add("8000"),
    Add("4000"),
    Add("2000"),
    Add("6000"),
    Add("9000")
  };
  Slice existing_value("5000");
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("200040005000600080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands_ExistingValue_Ordered_Reverse) {
  auto operand_list = {
    Add("8000"),
    Add("4000"),
    Add("2000"),
    Add("6000"),
    Add("9000")
  };
  Slice existing_value("5000");
  auto* comparator = ROCKSDB_NAMESPACE::ReverseBytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("900080006000500040002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Constraint_MakeUnique_NoConflict) {
  auto operand_list = {
    Add({
      "1000",
      "2000",
      "3000"
    })
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Constraint_MakeUnique_Conflict) {
  auto operand_list = {
    Add({
      "1000",
      "2000",
      "1000",
      "3000"
    })
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands_Constraint_MakeUnique_NoConflict) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000")
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddMany_Operands_Constraint_MakeUnique_Conflict) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000")
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, nullptr, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

/** FullMergeV2 CollectionOperation::kRemove TESTS **/
TEST(FullMergeV2, Remove_NonExisting) {
  auto operand_list = {
    Remove("1000")
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Remove_OnlyRecord) {
  auto operand_list = {
    Remove("1000")
  };
  Slice existing_value("1000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Remove_FirstRecord) {
  auto operand_list = {
    Remove("1000")
  };
  Slice existing_value("100020003000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("20003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Remove_MiddleRecord) {
  auto operand_list = {
    Remove("2000")
  };
  Slice existing_value("100020003000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("10003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Remove_LastRecord) {
  auto operand_list = {
    Remove("3000")
  };
  Slice existing_value("100020003000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("10002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Remove_FirstRecord_Duplicates) {
  auto operand_list = {
    Remove("1000")
  };
  Slice existing_value("1000100020003000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Remove_MiddleRecord_Duplicates) {
  auto operand_list = {
    Remove("2000")
  };
  Slice existing_value("1000200020003000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Remove_LastRecord_Duplicates) {
  auto operand_list = {
    Remove("3000")
  };
  Slice existing_value("1000200030003000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveMany) {
  auto operand_list = {
    Remove({
      "1000",
      "4000"
    })
  };
  Slice existing_value("10002000300040005000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("200030005000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveMany_Operands) {
  auto operand_list = {
    Remove("1000"),
    Remove("4000")
  };
  Slice existing_value("10002000300040005000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("200030005000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveAll) {
  auto operand_list = {
    Remove({
      "1000",
      "2000",
      "3000"
    })
  };
  Slice existing_value("100020003000");
  
  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveAll_Operands) {
  auto operand_list = {
    Remove("1000"),
    Remove("2000"),
    Remove("3000")
  };
  Slice existing_value("100020003000");
  
  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveMany_Unordered) {
  auto operand_list = {
    Remove({
      "4000",
      "1000"
    })
  };
  Slice existing_value("10002000300040005000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("200030005000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveMany_Operands_Unordered) {
  auto operand_list = {
    Remove("4000"),
    Remove("1000")
  };
  Slice existing_value("10002000300040005000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("200030005000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveAll_Unordered) {
  auto operand_list = {
    Remove({
      "4000",
      "1000"
      "2000"
      "5000"
      "3000"
    })
  };
  Slice existing_value("10002000300040005000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, RemoveAll_Operands_Unordered) {
  auto operand_list = {
    Remove("4000"),
    Remove("1000"),
    Remove("2000"),
    Remove("5000"),
    Remove("3000")
  };
  Slice existing_value("10002000300040005000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

/** FullMergeV2 CollectionOperation::kAdd and kRemove TESTS **/

TEST(FullMergeV2, AddAndRemove_SingleRemoveOperand) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Remove({
        "4000",
        "1000"
    }),
    Add("3000"),
    Add("6000"),
    Add("7000"),
    Remove("2000")
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("300060007000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, AddAndRemove_MultiRemoveOperand) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Remove("4000"),
    Remove("1000"),
    Add("3000"),
    Add("6000"),
    Add("7000"),
    Remove("2000")
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("300060007000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}


/** FullMergeV2 CollectionOperation::_kMulti TESTS **/

TEST(FullMergeV2, Multi_ClearAdd) {
  auto operand_list = {
    Multi({
      Clear(),
      Add("1000")
    })
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("1000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_ClearAdd_ExistingValue) {
  auto operand_list = {
    Multi({
      Clear(),
      Add("1000")
    })
  };
  Slice existing_value = RecordString(DEFAULT_TEST_RECORD_SIZE, 3);

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("1000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveAdd) {
  auto operand_list = {
    Multi({
      Remove("1000"),
      Add("1000")
    })
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("1000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveAdd_Existing_Match) {
  auto operand_list = {
    Multi({
      Remove("1000"),
      Add("1000")
    })
  };
  Slice existing_value("1000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("1000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveAdd_Existing_NoMatch) {
  auto operand_list = {
    Multi({
      Remove("1000"),
      Add("1000")
    })
  };
  Slice existing_value("2000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("20001000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveAdd_Existing_NoMatch_Ordered) {
  auto operand_list = {
    Multi({
      Remove("1000"),
      Add("1000")
    })
  };
  Slice existing_value("2000");
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("10002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveRemoveAdd) {
  auto operand_list = {
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add("1000")
    })
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("1000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveRemoveAdd_Existing_Match) {
  auto operand_list = {
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add("1000")
    })
  };
  Slice existing_value("1000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("1000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveRemoveAdd_Existing_NoMatch) {
  auto operand_list = {
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add("1000")
    })
  };
  Slice existing_value("2000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("20001000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_RemoveRemoveAdd_Existing_NoMatch_Ordered) {
  auto operand_list = {
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add("1000")
    })
  };
  Slice existing_value("2000");
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("10002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_Mixed_1) {
  auto operand_list = {
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("20003000400070008000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_Mixed_1_Existing_NoMatch) {
  auto operand_list = {
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
  };
  Slice existing_value("1000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000400070008000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_Mixed_1_Existing_Match_1) {
  auto operand_list = {
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
  };
  Slice existing_value("6000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("20003000400070008000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_Mixed_1_Existing_Match_2) {
  auto operand_list = {
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
  };
  Slice existing_value("10006000");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000400070008000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_Mixed_1_Existing_Match_3) {
  auto operand_list = {
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
  };
  Slice existing_value("9999");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("20003000400070008000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_Mixed_1_Existing_Match_4) {
  auto operand_list = {
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
  };
  Slice existing_value("10009999");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("100020003000400070008000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Multi_Mixed_1_Existing_Match_5_Ordered) {
  auto operand_list = {
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
  };
  Slice existing_value("1000500090009999");
  auto* comparator = ROCKSDB_NAMESPACE::BytewiseComparator();

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value, comparator);

  ASSERT_TRUE(result);
  ASSERT_EQ("10002000300040005000700080009000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_MultiRemoveAdd) {
  auto operand_list = {
    Add({
      "0594",
      "0593",
      "0592"
    }),
    Multi({
      Remove({
       "0592",
       "0593",
       "0594"
      }),
      Add({
        "0594",
        "0592",
        "0593"
      }),
    })
  };
  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("059405920593", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

TEST(FullMergeV2, Add_MultiRemoveAdd_uint32) {
  auto operand_list = {
    Add({
      594,
      593,
      592
    }),
    Multi({
      Remove({
       592,
       593,
       594
      }),
      Add({
        594,
        592,
        593
      }),
    })
  };
  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);

  char buf[sizeof(uint32_t) * 3];
  EncodeFixed32(buf, 594);
  EncodeFixed32(buf + sizeof(uint32_t), 592);
  EncodeFixed32(buf + sizeof(uint32_t) * 2, 593);

  std::string expected(buf, sizeof(uint32_t) * 3);
  ASSERT_EQ(expected, merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

// TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
TEST(FullMergeV2, EmptyOperands) {
  std::string empty1;
  std::string empty2;
  auto operand_list = {
    empty1,
    empty2
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_TRUE(merge_out.new_value.empty());
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

// TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
TEST(FullMergeV2, EmptyOperandsAndAdd) {
  std::string empty1;
  std::string empty2;
  auto operand_list = {
    empty1,
    Add({
      "1000",
      "2000"
    }),
    empty2
  };

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out);

  ASSERT_TRUE(result);
  ASSERT_EQ("10002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

// TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
TEST(FullMergeV2, EmptyOperands_Existing) {
  std::string empty1;
  std::string empty2;
  auto operand_list = {
    empty1,
    empty2
  };
  Slice existing_value("1000500090009999");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("1000500090009999", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

// TODO(AR) temp - see https://github.com/facebook/rocksdb/issues/3655
TEST(FullMergeV2, EmptyOperandsAndAdd_Existing) {
  std::string empty1;
  std::string empty2;
  auto operand_list = {
    empty1,
    Add({
      "1000",
      "2000"
    }),
    empty2
  };
  Slice existing_value("1000500090009999");

  auto merge_out = EmptyMergeOut();

  const bool result = test_FullMergeV2(operand_list, merge_out, &existing_value);

  ASSERT_TRUE(result);
  ASSERT_EQ("100050009000999910002000", merge_out.new_value);
  ASSERT_EQ(0, merge_out.existing_operand.size());
}

/** PartialMergeMulti TESTS **/

/** PartialMergeMulti CollectionOperation::kAdd TESTS **/

TEST(PartialMergeMulti, AddAdd) {
  auto operand_list = {
    Add({"1000", "2000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Operands) {
  auto operand_list = {
    Add({"1000", "3000"}),
    Add({"2000", "4000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  /* NOTE: records for addition do not need to be
       ordered at the PartialMergeMulti stage.
       They will be ordered later by FullMergeV2.
  */
  ASSERT_EQ(
    Add({
      "1000",
      "3000",
      "2000",
      "4000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAddAdd) {
  auto operand_list = {
    Add({"1000", "2000", "3000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000",
      "3000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAddAdd_Operands) {
  auto operand_list = {
    Add({"1000", "2000"}),
    Add("3000"),
    Add({"4000", "5000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000",
      "3000",
      "4000",
      "5000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Constraint_None) {
  auto operand_list = {
    Add({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "1000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Operands_Constraint_None) {
  auto operand_list = {
    Add("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "1000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Constraint_EnforceUnique_NoConflict) {
  auto operand_list = {
    Add({"1000", "2000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Constraint_EnforceUnique_Conflict) {
  auto operand_list = {
    Add({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_FALSE(result);
  ASSERT_TRUE(new_value.empty());
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Operands_Constraint_EnforceUnique_NoConflict) {
  auto operand_list = {
    Add("1000"),
    Add("2000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Operands_Constraint_EnforceUnique_Conflict) {
  auto operand_list = {
    Add("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_FALSE(result);
  ASSERT_TRUE(new_value.empty());
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Constraint_MakeUnique_NoConflict) {
  auto operand_list = {
    Add({"1000", "2000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Constraint_MakeUnique_Conflict) {
  auto operand_list = {
    Add({"1000", "1000"}),
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Operands_Constraint_MakeUnique_NoConflict) {
  auto operand_list = {
    Add("1000"),
    Add("2000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAdd_Duplicate_Operands_Constraint_MakeUnique_Conflict) {
  auto operand_list = {
    Add("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000"
    }),
    new_value);
}

/** PartialMergeMulti CollectionOperation::kClear and kAdd TESTS **/

TEST(PartialMergeMulti, AddClear) {
  auto operand_list = {
    Add("1000"),
    Clear()
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Clear(),
    new_value);
}

TEST(PartialMergeMulti, ClearAdd) {
  auto operand_list = {
    Clear(),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Clear(),
      Add("1000")
    }),
    new_value);
}

TEST(PartialMergeMulti, AddClearAdd) {
  auto operand_list = {
    Add("1000"),
    Clear(),
    Add("3000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Clear(),
      Add("3000")
    }),
    new_value);
}

TEST(PartialMergeMulti, ClearAddAdd) {
  auto operand_list = {
    Clear(),
    Add("1000"),
    Add("3000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Clear(),
      Add({"1000", "3000"})
    }),
    new_value);
}

TEST(PartialMergeMulti, AddAddClear) {
  auto operand_list = {
    Add("1000"),
    Add("3000"),
    Clear()
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Clear(),
    new_value);
}

/** PartialMergeMulti CollectionOperation::kRemove TESTS **/

TEST(PartialMergeMulti, RemoveRemove) {
  auto operand_list = {
    Remove({"1000", "2000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Operands) {
  auto operand_list = {
    Remove("1000"),
    Remove("2000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemoveRemove) {
  auto operand_list = {
    Remove({"1000", "2000", "3000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000",
      "3000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemoveRemove_Operands) {
  auto operand_list = {
    Remove({"1000", "2000"}),
    Remove("3000"),
    Remove({"4000", "5000"}),
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000",
      "3000",
      "4000",
      "5000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Constraint_None) {
  auto operand_list = {
    Remove({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "1000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Operands_Constraint_None) {
  auto operand_list = {
    Remove("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "1000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Constraint_EnforceUnique_NoConflict) {
  auto operand_list = {
    Remove({"1000", "2000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Constraint_EnforceUnique_Conflict) {
  auto operand_list = {
    Remove({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_FALSE(result);
  ASSERT_TRUE(new_value.empty());
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Operands_Constraint_EnforceUnique_NoConflict) {
  auto operand_list = {
    Remove("1000"),
    Remove("2000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Operands_Constraint_EnforceUnique_Conflict) {
  auto operand_list = {
    Remove("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);

  ASSERT_FALSE(result);
  ASSERT_TRUE(new_value.empty());
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Constraint_MakeUnique_NoConflict) {
  auto operand_list = {
    Remove({"1000", "2000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Constraint_MakeUnique_Conflict) {
  auto operand_list = {
    Remove({"1000", "1000"}),
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Operands_Constraint_MakeUnique_NoConflict) {
  auto operand_list = {
    Remove("1000"),
    Remove("2000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000",
      "2000"
    }),
    new_value);
}

TEST(PartialMergeMulti, RemoveRemove_Duplicate_Operands_Constraint_MakeUnique_Conflict) {
  auto operand_list = {
    Remove("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000"
    }),
    new_value);
}

/** PartialMergeMulti CollectionOperation::kAdd and kRemove TESTS **/

// [Add(1000), Remove(1000)] => []
TEST(PartialMergeMulti, AddRemove) {
  auto operand_list = {
    Add("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(new_value.empty());
}

// [Remove(1000), Add(1000)] => Multi(Remove(1000), Add(1000))
TEST(PartialMergeMulti, RemoveAdd) {
  auto operand_list = {
    Remove("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  /* NOTE: Remove(v1) + Add(v1) equals Multi(Remove(v1), Add(v1))
      because Remove(v1) may remove zero-or-one existing v1(s)
      so we must always have both because we don't know if how
      many existing v1s there are until FullMergeV2 */

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
        Remove("1000"),
        Add("1000")
    }),
    new_value);
}

// [Add(1000, 1000), Remove(1000)] => Add(1000)
TEST(PartialMergeMulti, AddAddRemove) {
  auto operand_list = {
    Add({"1000", "1000"}),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000"
    }),
    new_value);
}

// [Add(1000), Add(1000), Remove(1000)] => Add(1000)
TEST(PartialMergeMulti, AddAddRemove_Operands) {
  auto operand_list = {
    Add("1000"),
    Add("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000"
    }),
    new_value);
}

// [Remove(1000, 1000), Add(1000)] => Multi(Remove(1000, 1000), Add(1000))
TEST(PartialMergeMulti, RemoveRemoveAdd) {
  auto operand_list = {
    Remove({"1000", "1000"}),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add("1000")
    }),
    new_value);
}

// [Remove(1000), Remove(1000), Add(1000)] => Multi(Remove(1000, 1000), Add(1000))
TEST(PartialMergeMulti, RemoveRemoveAdd_Operands) {
  auto operand_list = {
    Remove("1000"),
    Remove("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add("1000")
    }),
    new_value);
}

// [Remove(1000), Add(1000), Remove(1000)] => Remove(1000)
TEST(PartialMergeMulti, RemoveAddRemove_Operands) {
  auto operand_list = {
    Remove("1000"),
    Add("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000"
    }),
    new_value);
}

// [Add(1000), Remove(1000), Add(1000)] => Add(1000)
TEST(PartialMergeMulti, AddRemoveAdd_Operands) {
  auto operand_list = {
    Add("1000"),
    Remove("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000"
    }),
    new_value);
}

// [Add(1000), Remove(1000, 1000)] => Remove(1000)
TEST(PartialMergeMulti, AddRemoveRemove) {
  auto operand_list = {
    Add("1000"),
    Remove({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000"
    }),
    new_value);
}

// [Add(1000), Remove(1000), Remove(1000)] => Remove(1000)
TEST(PartialMergeMulti, AddRemoveRemove_Operands) {
  auto operand_list = {
    Add("1000"),
    Remove("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove({
      "1000"
    }),
    new_value);
}

// [Remove(1000), Add(1000, 1000)] => Multi(Remove(1000), Add(1000, 1000))
TEST(PartialMergeMulti, RemoveAddAdd) {
  auto operand_list = {
    Remove("1000"),
    Add({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove("1000"),
      Add({
        "1000",
        "1000"
      })
    }),
    new_value);
}

// [Remove(1000), Add(1000), Add(1000)] => Multi(Remove(1000), Add(1000, 1000))
TEST(PartialMergeMulti, RemoveAddAdd_Operands) {
  auto operand_list = {
    Remove("1000"),
    Add("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove("1000"),
      Add({
        "1000",
        "1000"
      })
    }),
    new_value);
}

// [Remove(1000), Add(1000, 1000), Remove(1000)] => Multi(Remove(1000), Add(1000))
TEST(PartialMergeMulti, RemoveAddAddRemove) {
  auto operand_list = {
    Remove("1000"),
    Add({"1000", "1000"}),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove("1000"),
      Add("1000")
    }),
    new_value);
}

// [Remove(1000), Add(1000), Add(1000), Remove(1000)] => Multi(Remove(1000), Add(1000))
TEST(PartialMergeMulti, RemoveAddAddRemove_Operands) {
  auto operand_list = {
    Remove("1000"),
    Add("1000"),
    Add("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove("1000"),
      Add("1000")
    }),
    new_value);
}

// [Add(1000), Remove(1000, 1000), Add(1000)] => Multi(Remove(1000), Add(1000))
TEST(PartialMergeMulti, AddRemoveRemoveAdd) {
  auto operand_list = {
    Add("1000"),
    Remove({"1000", "1000"}),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove("1000"),
      Add("1000")
    }),
    new_value);
}

// [Add(1000), Remove(1000), Remove(1000), Add(1000)] => Multi(Remove(1000), Add(1000))
TEST(PartialMergeMulti, AddRemoveRemoveAdd_Operands) {
  auto operand_list = {
    Add("1000"),
    Remove("1000"),
    Remove("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove("1000"),
      Add("1000")
    }),
    new_value);
}

// [Add(1000, 1000), Remove(1000, 1000)] => []
TEST(PartialMergeMulti, AddAddRemoveRemove) {
  auto operand_list = {
    Add({"1000", "1000"}),
    Remove({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(new_value.empty());
}

// [Add(1000), Add(1000), Remove(1000), Remove(1000)] => []
TEST(PartialMergeMulti, AddAddRemoveRemove_Operands) {
  auto operand_list = {
    Add("1000"),
    Add("1000"),
    Remove("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_TRUE(new_value.empty());
}

// [Remove(1000, 1000), Add(1000, 1000)] => Multi(Remove(1000, 1000), Add(1000, 1000))
TEST(PartialMergeMulti, RemoveRemoveAddAdd) {
  auto operand_list = {
    Remove({"1000", "1000"}),
    Add({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add({
        "1000",
        "1000"
      })
    }),
    new_value);
}

// [Remove(1000), Remove(1000), Add(1000), Add(1000)] => Multi(Remove(1000, 1000), Add(1000, 1000))
TEST(PartialMergeMulti, RemoveRemoveAddAdd_Operands) {
  auto operand_list = {
    Remove("1000"),
    Remove("1000"),
    Add("1000"),
    Add("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Remove({
        "1000",
        "1000"
      }),
      Add({
        "1000",
        "1000"
      })
    }),
    new_value);
}

// [Add(1000, 2000, 1000, 3000), Remove(1000, 1000)] => [Add(2000, 3000)]
TEST(PartialMergeMulti, AddAddRemoveRemove_Mixed_1) {
  auto operand_list = {
    Add({"1000", "2000", "1000", "3000"}),
    Remove({"1000", "1000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000"
    }),
    new_value);
}

// [Add(1000), Add(2000), Add(1000), Add(3000), Remove(1000), Remove(1000)] => [Add(2000, 3000)]
TEST(PartialMergeMulti, AddAddRemoveRemove_Operands_Mixed_1) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000"),
    Remove("1000"),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000"
    }),
    new_value);
}

// [Add(1000, 2000, 1000, 3000), Add(4000, 5000), Remove(1000, 1000), Remove(4000, 5000)] => [Add(2000, 3000)]
TEST(PartialMergeMulti, AddAddRemoveRemove_Mixed_2) {
  auto operand_list = {
    Add({"1000", "2000", "1000", "3000"}),
    Add({"4000", "5000"}),
    Remove({"1000", "1000"}),
    Remove({"4000", "5000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000"
    }),
    new_value);
}

// [Add(1000), Add(2000), Add(1000), Add(3000), Add(4000), Add(5000), Remove(1000), Remove(1000), Remove(4000), Remove(5000)] => [Add(2000, 3000)]
TEST(PartialMergeMulti, AddAddRemoveRemove_Operands_Mixed_2) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000"),
    Add("4000"),
    Add("5000"),
    Remove("1000"),
    Remove("1000"),
    Remove("4000"),
    Remove("5000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000"
    }),
    new_value);
}

// [Add(1000, 2000, 1000, 3000), Add(4000, 5000), Remove(1000, 1000), Remove(5000)] => [Add(2000, 3000, 4000)]
TEST(PartialMergeMulti, AddAddRemoveRemove_Mixed_3) {
  auto operand_list = {
    Add({"1000", "2000", "1000", "3000"}),
    Add({"4000", "5000"}),
    Remove({"1000", "1000"}),
    Remove("5000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000",
      "4000"
    }),
    new_value);
}

// [Add(1000), Add(2000), Add(1000), Add(3000), Add(4000), Add(5000), Remove(1000), Remove(1000), Remove(5000)] => [Add(2000, 3000, 4000)]
TEST(PartialMergeMulti, AddAddRemoveRemove_Operands_Mixed_3) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000"),
    Add("4000"),
    Add("5000"),
    Remove("1000"),
    Remove("1000"),
    Remove("5000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000",
      "4000"
    }),
    new_value);
}

// [Add(1000, 2000, 1000, 3000), Remove(1000, 1000), Add(4000, 5000), Remove(4000, 5000)] => [Add(2000, 3000)]
TEST(PartialMergeMulti, AddRemoveAddRemove_Mixed_1) {
  auto operand_list = {
    Add({"1000", "2000", "1000", "3000"}),
    Remove({"1000", "1000"}),
    Add({"4000", "5000"}),
    Remove({"4000", "5000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000"
    }),
    new_value);
}

// [Add(1000), Add(2000), Add(1000), Add(3000), Remove(1000), Remove(1000), Add(4000), Add(5000), Remove(4000, 5000)] => [Add(2000, 3000)]
TEST(PartialMergeMulti, AddRemoveAddRemove_Operands_Mixed_1) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000"),
    Remove("1000"),
    Remove("1000"),
    Add("4000"),
    Add("5000"),
    Remove("4000"),
    Remove("5000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000",
    }),
    new_value);
}

// [Add(1000, 2000, 1000, 3000), Remove(1000, 1000), Add(4000, 5000), Remove(5000)] => [Add(2000, 3000, 4000)]
TEST(PartialMergeMulti, AddRemoveAddRemove_Mixed_2) {
  auto operand_list = {
    Add({"1000", "2000", "1000", "3000"}),
    Remove({"1000", "1000"}),
    Add({"4000", "5000"}),
    Remove("5000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000",
      "4000"
    }),
    new_value);
}

// [Add(1000), Add(2000), Add(1000), Add(3000), Remove(1000), Remove(1000), Add(4000), Add(5000), Remove(5000)] => [Add(2000, 3000, 4000)]
TEST(PartialMergeMulti, AddRemoveAddRemove_Operands_Mixed_2) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000"),
    Remove("1000"),
    Remove("1000"),
    Add("4000"),
    Add("5000"),
    Remove("5000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "2000",
      "3000",
      "4000"
    }),
    new_value);
}

// [Add(1000, 2000, 1000, 3000), Remove(1000, 1000), Add(4000, 5000), Remove(5000, 6000), Add(7000, 8000, 9000), Remove(9999, 9000)] => [Multi(Add(2000, 3000, 4000), Remove("6000"), Add("7000", "8000"), Remove("9999"))]
TEST(PartialMergeMulti, AddRemoveAddRemove_Mixed_3) {
  auto operand_list = {
    Add({"1000", "2000", "1000", "3000"}),
    Remove({"1000", "1000"}),
    Add({"4000", "5000"}),
    Remove({"5000", "6000"}),
    Add({"7000", "8000", "9000"}),
    Remove({"9999", "9000"})
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
    new_value);
}

// [Add(1000), Add(2000), Add(1000), Add(3000), Remove(1000), Remove(1000), Add(4000), Add(5000), Remove(5000), Remove(6000), Add(7000, 8000), Add(9000), Remove(9999), Remove(9000)] => [Multi(Add(2000, 3000, 4000), Remove("6000"), Add("7000", "8000"), Remove("9999"))]
TEST(PartialMergeMulti, AddRemoveAddRemove_Operands_Mixed_3) {
  auto operand_list = {
    Add("1000"),
    Add("2000"),
    Add("1000"),
    Add("3000"),
    Remove("1000"),
    Remove("1000"),
    Add("4000"),
    Add("5000"),
    Remove("5000"),
    Remove("6000"),
    Add("7000"),
    Add("8000"),
    Add("9000"),
    Remove("9999"),
    Remove("9000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
      Add({
        "2000",
        "3000",
        "4000"
      }),
      Remove("6000"),
      Add({
        "7000",
        "8000"
      }),
      Remove("9999")
    }),
    new_value);
}

/** PartialMergeMulti CollectionOperation::kMulti TESTS **/

TEST(PartialMergeMulti, Multi_AddAdd) {
  auto operand_list = {
    Multi ({
      Add("1000"),
      Add("2000")
    }),
    Multi ({
      Add("3000"),
      Add("4000")
    }),
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "1000",
      "2000",
      "3000"
      "4000"
    }),
    new_value);
}

// [Multi(Remove(1000)), Multi(Add(1000)), Remove(1000)] => Remove(1000)
TEST(PartialMergeMulti, MultiRemoveAddRemove_1) {
  auto operand_list = {
    Multi({
      Remove("1000"),
    }),
    Multi({
      Add("1000")
    }),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  /* NOTE: Remove(v1) + Add(v1) equals Multi(Remove(v1), Add(v1))
      because Remove(v1) may remove zero-or-one existing v1(s)
      so we must always have both because we don't know if how
      many existing v1s there are until FullMergeV2 */

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove("1000"),
    new_value);
}

// [Multi(Remove(1000, Add(1000)), Remove(1000)] => Remove(1000)
TEST(PartialMergeMulti, MultiRemoveAddRemove_2) {
  auto operand_list = {
    Multi({
      Remove("1000"),
      Add("1000")
    }),
    Remove("1000")
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Remove("1000"),
    new_value);
}

// [Multi(Add(2000), Remove(1000, Add(1000)), Multi(Remove(1000, 2000), Add(3000))] => Multi(Remove(1000), Add(3000))
TEST(PartialMergeMulti, MultiComplex1) {
  auto operand_list = {
    Multi({
      Add("2000"),
      Remove("1000"),
      Add("1000")
    }),
    Multi({
      Remove({"1000", "2000"}),
      Add("3000")
    })
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
        Remove("1000"),
        Add("3000")
    }),
    new_value);
}

// [Add(4000), Remove(9000), Add(5000), Multi(Add(2000), Remove(1000, Add(1000)), Remove(5000), Multi(Remove(1000, 2000), Add(3000))] => Multi(Remove(1000, 9000), Add(3000, 4000))
TEST(PartialMergeMulti, MultiComplex2) {
  auto operand_list = {
    Add("4000"),
    Remove("9000"),
    Add("5000"),
    Multi({
      Add("2000"),
      Remove("1000"),
      Add("1000")
    }),
    Remove("5000"),
    Multi({
      Remove({"1000", "2000"}),
      Add("3000")
    })
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Multi({
        Add("4000"),
        Remove({"9000", "1000"}),
        Add({"3000"})
    }),
    new_value);
}

TEST(PartialMergeMulti, Complex1) {
  auto operand_list = {
    Add({
      "0594",
      "0592",
      "0593"
    }),
    Remove("0593"),
    Remove("0592"),
    Remove("0594"),
    Add({
      "0594",
      "0593",
      "0592"
    }),
    Remove("0592"),
    Remove("0593"),
    Remove("0594"),
    Add({
      "0594",
      "0592",
      "0593"
    }),
    Remove("0593"),
    Remove("0592"),
    Remove("0594"),
    Add({
      "0594",
      "0593",
      "0592"
    }),
    Remove("0592"),
    Remove("0593"),
    Remove("0594"),
    Add({
      "0594",
      "0593",
      "0592"
    }),
    Remove("0592"),
    Remove("0593"),
    Remove("0594"),
    Add({
      "0594",
      "0592",
      "0593"
    }),
    Remove("0593"),
    Remove("0592"),
    Remove("0594"),
    Add({
      "0594",
      "0593",
      "0592"
    }),
    Remove("0592"),
    Remove("0593"),
    Remove("0594"),
    Add({
      "0594",
      "0592",
      "0593"
    }),
    Remove("0593"),
    Remove("0592"),
    Remove("0594"),
    Add({
      "0594",
      "0593",
      "0592"
    })
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);

  ASSERT_TRUE(result);
  ASSERT_EQ(
    Add({
      "0594",
      "0593",
      "0592"
    }),
    new_value);
}

TEST(PartialMergeMulti, Complex1_uint32) {
  auto operand_list = {
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    })
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value);
   ASSERT_EQ(
    Add({
      594,
      593,
      592
    }),
    new_value);

  ASSERT_TRUE(result);
}

TEST(PartialMergeMulti, Complex1_uint32_MakeUnique) {
  auto operand_list = {
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    })
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kMakeUnique);
   ASSERT_EQ(
    Add({
      594,
      593,
      592
    }),
    new_value);

  ASSERT_TRUE(result);
}

TEST(PartialMergeMulti, Complex1_uint32_EnforceUnique) {
  auto operand_list = {
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    }),
    Remove(592),
    Remove(593),
    Remove(594),
    Add({
      594,
      592,
      593
    }),
    Remove(593),
    Remove(592),
    Remove(594),
    Add({
      594,
      593,
      592
    })
  };

  std::string new_value;
  const bool result = test_PartialMergeMulti(operand_list, &new_value, nullptr, UniqueConstraint::kEnforceUnique);
   ASSERT_EQ(
    Add({
      594,
      593,
      592
    }),
    new_value);

  ASSERT_TRUE(result);
}

}  // end ROCKSDB_NAMESPACE namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
