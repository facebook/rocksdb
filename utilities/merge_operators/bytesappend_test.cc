//
// Created by Jesse Ma on 2019-05-23.
//

#include "util/testharness.h"
#include "utilities/merge_operators.h"


namespace rocksdb {

  class UtilBytesAppendMergeOperatorTest : public testing::Test {
  public:
    UtilBytesAppendMergeOperatorTest() {
    }

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
  TEST_F(UtilBytesAppendMergeOperatorTest, SubTest1) {
    merge_operator_ = MergeOperators::CreateBytesAppendOperator();

    EXPECT_EQ("BA", FullMergeV2("B", {"A"}));
    EXPECT_EQ("AB", FullMergeV2("A", {"B"}));
    EXPECT_EQ("", FullMergeV2({"", "", ""}));
    EXPECT_EQ("A", FullMergeV2({"A"}));
    EXPECT_EQ("ABC", FullMergeV2({"ABC"}));
    EXPECT_EQ("ABCZCAXX", FullMergeV2({"ABC", "Z", "C", "AXX"}));
    EXPECT_EQ("ABCCCZZZZ", FullMergeV2({"ABC", "CC", "Z", "ZZZ"}));
    EXPECT_EQ("aABCCCZZZZ", FullMergeV2("a", {"ABC", "CC", "Z", "ZZZ"}));

    EXPECT_EQ("azefqfqwgwewaazhhhhh", PartialMergeMulti({"a", "z", "efqfqwgwew", "aaz", "hhhhh"}));
    EXPECT_EQ("ab", PartialMerge("a", "b"));
    EXPECT_EQ("zazzz", PartialMerge("z", "azzz"));
    EXPECT_EQ("a",PartialMerge("a", ""));
  }
}  // namespace rocksdb

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
