// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/agg_merge.h"

#include <gtest/gtest.h>

#include <memory>

#include "db/db_test_util.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "utilities/agg_merge/agg_merge.h"
#include "utilities/agg_merge/example_agg_merge.h"

namespace ROCKSDB_NAMESPACE {

class AggMergeTest : public DBTestBase {
 public:
  AggMergeTest() : DBTestBase("agg_merge_db_test", /*env_do_fsync=*/true) {}
};

TEST_F(AggMergeTest, TestUsingMergeOperator) {
  AddAggregator("sum", std::make_unique<SumAggregator>());
  AddAggregator("last3", std::make_unique<Last3Aggregator>());

  Options options = CurrentOptions();
  options.merge_operator = GetAggMergeOperator();
  Reopen(options);
  std::string v = EncodeHelper::EncodeFuncAndInt("sum", 10);
  ASSERT_OK(Merge("foo", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 20);
  ASSERT_OK(Merge("foo", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 15);
  ASSERT_OK(Merge("foo", v));

  v = EncodeHelper::EncodeFuncAndList("last3", {"a", "b"});
  ASSERT_OK(Merge("bar", v));
  v = EncodeHelper::EncodeFuncAndList("last3", {"c", "d", "e"});
  ASSERT_OK(Merge("bar", v));
  Flush();
  v = EncodeHelper::EncodeFuncAndList("last3", {"f"});
  ASSERT_OK(Merge("bar", v));

  // Test Put() without aggregation type.
  v = "";
  PutVarsignedint64(&v, 30);
  ASSERT_OK(Put("foo2", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 10);
  ASSERT_OK(Merge("foo2", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 20);
  ASSERT_OK(Merge("foo2", v));

  EXPECT_EQ(EncodeHelper::EncodeFuncAndInt("sum", 45), Get("foo"));
  EXPECT_EQ(EncodeHelper::EncodeFuncAndList("last3", {"f", "c", "d"}),
            Get("bar"));
  EXPECT_EQ(EncodeHelper::EncodeFuncAndInt("sum", 60), Get("foo2"));

  // Test changing aggregation type
  v = EncodeHelper::EncodeFuncAndList("last3", {"a", "b"});
  ASSERT_OK(Merge("bar", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 10);
  ASSERT_OK(Merge("bar", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 20);
  ASSERT_OK(Merge("bar", v));
  std::string aggregated_value = Get("bar");
  Slice func, payload;
  ASSERT_TRUE(ExtractAggFuncAndValue(aggregated_value, func, payload));
  EXPECT_EQ("sum", func);
  int64_t ivalue;
  ASSERT_TRUE(GetVarsignedint64(&payload, &ivalue));
  ASSERT_EQ(30, ivalue);

  // Test unregistered function name
  v = EncodeHelper::EncodeFuncAndInt("non_existing", 10);
  ASSERT_OK(Merge("bar2", v));
  std::string v1 = EncodeHelper::EncodeFuncAndInt("non_existing", 20);
  ASSERT_OK(Merge("bar2", v1));
  EXPECT_EQ(
      EncodeAggFuncAndValue(kErrorFuncName, EncodeHelper::EncodeList({v, v1})),
      Get("bar2"));

  // invalidate input
  v = EncodeAggFuncAndValue("sum", "invalid");
  ASSERT_OK(Merge("bar3", v));
  v1 = EncodeHelper::EncodeFuncAndInt("sum", 20);
  ASSERT_OK(Merge("bar3", v1));
  aggregated_value = Get("bar3");
  ASSERT_TRUE(ExtractAggFuncAndValue(aggregated_value, func, payload));
  EXPECT_EQ(kErrorFuncName, func);
  std::vector<Slice> decoded_list;
  ASSERT_TRUE(ExtractList(payload, decoded_list));
  ASSERT_EQ(2, decoded_list.size());
  ASSERT_EQ(v, decoded_list[0]);
  ASSERT_EQ(v1, decoded_list[1]);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
