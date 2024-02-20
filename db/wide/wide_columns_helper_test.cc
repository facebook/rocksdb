//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_columns_helper.h"

#include "db/wide/wide_column_serialization.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

TEST(WideColumnsHelperTest, DumpWideColumns) {
  WideColumns columns{{"foo", "bar"}, {"hello", "world"}};
  std::ostringstream oss;
  WideColumnsHelper::DumpWideColumns(columns, oss, false /* hex */);
  EXPECT_EQ("foo:bar hello:world", oss.str());
}

TEST(WideColumnsHelperTest, DumpSliceAsWideColumns) {
  WideColumns columns{{"foo", "bar"}, {"hello", "world"}};
  std::string output;
  ASSERT_OK(WideColumnSerialization::Serialize(columns, output));
  Slice input(output);

  std::ostringstream oss;
  ASSERT_OK(
      WideColumnsHelper::DumpSliceAsWideColumns(input, oss, false /* hex */));

  EXPECT_EQ("foo:bar hello:world", oss.str());
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
