// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/iostats_context.h"

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

TEST(IOStatsContextTest, ToString) {
  get_iostats_context()->Reset();
  get_iostats_context()->bytes_read = 12345;

  std::string zero_included = get_iostats_context()->ToString();
  ASSERT_NE(std::string::npos, zero_included.find("= 0"));
  ASSERT_NE(std::string::npos, zero_included.find("= 12345"));

  std::string zero_excluded = get_iostats_context()->ToString(true);
  ASSERT_EQ(std::string::npos, zero_excluded.find("= 0"));
  ASSERT_NE(std::string::npos, zero_excluded.find("= 12345"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
