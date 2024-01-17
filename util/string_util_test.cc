//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "string_util.h"

#include <gtest/gtest.h>
#include <port/stack_trace.h>

#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

TEST(StringUtilTest, NumberToHumanString) {
  ASSERT_EQ("-9223372036G", NumberToHumanString(INT64_MIN));
  ASSERT_EQ("9223372036G", NumberToHumanString(INT64_MAX));
  ASSERT_EQ("0", NumberToHumanString(0));
  ASSERT_EQ("9999", NumberToHumanString(9999));
  ASSERT_EQ("10K", NumberToHumanString(10000));
  ASSERT_EQ("10M", NumberToHumanString(10000000));
  ASSERT_EQ("10G", NumberToHumanString(10000000000));
  ASSERT_EQ("-9999", NumberToHumanString(-9999));
  ASSERT_EQ("-10K", NumberToHumanString(-10000));
  ASSERT_EQ("-10M", NumberToHumanString(-10000000));
  ASSERT_EQ("-10G", NumberToHumanString(-10000000000));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}