//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

class UtilMergeOperatorTest : public testing::Test {
 public:
  UtilMergeOperatorTest() {}

  std::string FullMerge(std::string existing_value,
                        std::deque<std::string> operands,
                        std::string key = "") {
    Slice existing_value_slice(existing_value);
    std::string result;

    merge_operator_->FullMerge(key, &existing_value_slice, operands, &result,
                               nullptr);
    return result;
  }

  std::string FullMerge(std::deque<std::string> operands,
                        std::string key = "") {
    std::string result;

    merge_operator_->FullMerge(key, nullptr, operands, &result, nullptr);
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

  EXPECT_EQ("B", FullMerge("B", {"A"}));
  EXPECT_EQ("B", FullMerge("A", {"B"}));
  EXPECT_EQ("", FullMerge({"", "", ""}));
  EXPECT_EQ("A", FullMerge({"A"}));
  EXPECT_EQ("ABC", FullMerge({"ABC"}));
  EXPECT_EQ("Z", FullMerge({"ABC", "Z", "C", "AXX"}));
  EXPECT_EQ("ZZZ", FullMerge({"ABC", "CC", "Z", "ZZZ"}));
  EXPECT_EQ("a", FullMerge("a", {"ABC", "CC", "Z", "ZZZ"}));

  EXPECT_EQ("z", PartialMergeMulti({"a", "z", "efqfqwgwew", "aaz", "hhhhh"}));

  EXPECT_EQ("b", PartialMerge("a", "b"));
  EXPECT_EQ("z", PartialMerge("z", "azzz"));
  EXPECT_EQ("a", PartialMerge("a", ""));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
