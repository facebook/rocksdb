//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

class UtilMergeOperatorTest : public testing::Test {
 public:
  UtilMergeOperatorTest() {}

  std::string FullMergeV2(std::string existing_value,
                          std::vector<std::string> operands,
                          std::string key = "") {
    std::string result;
    Slice result_operand(nullptr, 0);

    Slice existing_value_slice(existing_value);
    std::vector<Slice> operands_slice(operands.begin(), operands.end());

    const MergeOperator::MergeOperationInput merge_in(
        key, &existing_value_slice, operands_slice, nullptr);
    MergeOperator::MergeOperationOutput merge_out(result, result_operand);
    merge_operator_->FullMergeV2(merge_in, &merge_out);

    if (result_operand.data()) {
      result.assign(result_operand.data(), result_operand.size());
    }
    return result;
  }

  std::string FullMergeV2(std::vector<std::string> operands,
                          std::string key = "") {
    std::string result;
    Slice result_operand(nullptr, 0);

    std::vector<Slice> operands_slice(operands.begin(), operands.end());

    const MergeOperator::MergeOperationInput merge_in(key, nullptr,
                                                      operands_slice, nullptr);
    MergeOperator::MergeOperationOutput merge_out(result, result_operand);
    merge_operator_->FullMergeV2(merge_in, &merge_out);

    if (result_operand.data()) {
      result.assign(result_operand.data(), result_operand.size());
    }
    return result;
  }

  std::string PartialMerge(std::string left, std::string right,
                           std::string key = "") {
    std::string result;

    merge_operator_->PartialMerge(key, left, right, &result, nullptr);
    return result;
  }

  std::string PartialMergeMulti(std::deque<std::string> operands,
                                std::string key = "") {
    std::string result;
    std::deque<Slice> operands_slice(operands.begin(), operands.end());

    merge_operator_->PartialMergeMulti(key, operands_slice, &result, nullptr);
    return result;
  }

 protected:
  std::shared_ptr<MergeOperator> merge_operator_;
};

TEST_F(UtilMergeOperatorTest, MaxMergeOperator) {
  merge_operator_ = MergeOperators::CreateMaxOperator();

  EXPECT_EQ("B", FullMergeV2("B", {"A"}));
  EXPECT_EQ("B", FullMergeV2("A", {"B"}));
  EXPECT_EQ("", FullMergeV2({"", "", ""}));
  EXPECT_EQ("A", FullMergeV2({"A"}));
  EXPECT_EQ("ABC", FullMergeV2({"ABC"}));
  EXPECT_EQ("Z", FullMergeV2({"ABC", "Z", "C", "AXX"}));
  EXPECT_EQ("ZZZ", FullMergeV2({"ABC", "CC", "Z", "ZZZ"}));
  EXPECT_EQ("a", FullMergeV2("a", {"ABC", "CC", "Z", "ZZZ"}));

  EXPECT_EQ("z", PartialMergeMulti({"a", "z", "efqfqwgwew", "aaz", "hhhhh"}));

  EXPECT_EQ("b", PartialMerge("a", "b"));
  EXPECT_EQ("z", PartialMerge("z", "azzz"));
  EXPECT_EQ("a", PartialMerge("a", ""));
}

TEST_F(UtilMergeOperatorTest, CreateMergeOperatorsFromString) {
  merge_operator_ = MergeOperators::CreateFromStringId("Unknown");
  ASSERT_EQ(merge_operator_, nullptr);

  merge_operator_ = MergeOperators::CreateFromStringId("put");
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("PutOperator"));

  merge_operator_ = MergeOperators::CreateFromStringId("put_v1");
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("PutOperator"));

  merge_operator_ = MergeOperators::CreateFromStringId("uint64add");
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("UInt64AddOperator"));

  merge_operator_ = MergeOperators::CreateFromStringId("stringappend");
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("StringAppendOperator"));

  merge_operator_ = MergeOperators::CreateFromStringId("stringappendtest");
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("StringAppendTESTOperator"));

  merge_operator_ = MergeOperators::CreateFromStringId("max");
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("MaxOperator"));

  merge_operator_ = MergeOperators::CreateFromStringId("bytesxor");
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("BytesXOR"));
}
  
TEST_F(UtilMergeOperatorTest, LoadMergeOperators) {

  ASSERT_NOK(MergeOperator::CreateMergeOperator("unknown",  &merge_operator_));
  ASSERT_EQ(merge_operator_, nullptr);

  ASSERT_OK(MergeOperator::CreateMergeOperator("put", &merge_operator_));
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("PutOperator"));

  ASSERT_OK(MergeOperator::CreateMergeOperator("put_v1", &merge_operator_));
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("PutOperator"));

  ASSERT_OK(MergeOperator::CreateMergeOperator("uint64add", &merge_operator_));
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("UInt64AddOperator"));

  ASSERT_OK(MergeOperator::CreateMergeOperator("stringappend", &merge_operator_));
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("StringAppendOperator"));

  ASSERT_OK(MergeOperator::CreateMergeOperator("stringappendtest", &merge_operator_));
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("StringAppendTESTOperator"));

  ASSERT_OK(MergeOperator::CreateMergeOperator("max", &merge_operator_));
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("MaxOperator"));

  ASSERT_OK(MergeOperator::CreateMergeOperator("bytesxor", &merge_operator_));
  ASSERT_NE(merge_operator_, nullptr);
  ASSERT_EQ(merge_operator_->Name(), std::string("BytesXOR"));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
