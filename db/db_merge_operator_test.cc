//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "db/forward_iterator.h"
#include "port/stack_trace.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// Test merge operator functionality.
class DBMergeOperatorTest : public DBTestBase {
 public:
  DBMergeOperatorTest() : DBTestBase("/db_merge_operator_test") {}
};

// A test merge operator mimics put but also fails if one of merge operands is
// "corrupted".
class TestPutOperator : public MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    if (merge_in.existing_value != nullptr &&
        *(merge_in.existing_value) == "corrupted") {
      return false;
    }
    for (auto value : merge_in.operand_list) {
      if (value == "corrupted") {
        return false;
      }
    }
    merge_out->existing_operand = merge_in.operand_list.back();
    return true;
  }

  virtual const char* Name() const override { return "TestPutOperator"; }
};

TEST_F(DBMergeOperatorTest, MergeErrorOnRead) {
  Options options;
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  Reopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "corrupted"));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsCorruption());
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v1"}});
}

TEST_F(DBMergeOperatorTest, MergeErrorOnWrite) {
  Options options;
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.max_successive_merges = 3;
  Reopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "v2"));
  // Will trigger a merge when hitting max_successive_merges and the merge
  // will fail. The delta will be inserted nevertheless.
  ASSERT_OK(Merge("k1", "corrupted"));
  // Data should stay unmerged after the error.
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v2"}, {"k1", "v1"}});
}

TEST_F(DBMergeOperatorTest, MergeErrorOnIteration) {
  Options options;
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());

  DestroyAndReopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "corrupted"));
  ASSERT_OK(Put("k2", "v2"));
  VerifyDBFromMap({{"k1", ""}, {"k2", "v2"}}, nullptr, false,
                  {{"k1", Status::Corruption()}});
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v1"}, {"k2", "v2"}});

  DestroyAndReopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Merge("k2", "corrupted"));
  VerifyDBFromMap({{"k1", "v1"}, {"k2", ""}}, nullptr, false,
                  {{"k2", Status::Corruption()}});
  VerifyDBInternal({{"k1", "v1"}, {"k2", "corrupted"}, {"k2", "v2"}});
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
