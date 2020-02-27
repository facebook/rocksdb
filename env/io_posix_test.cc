// Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "env/io_posix.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

#ifdef OS_LINUX
class LogicalBufferSizeCacheTest : public testing::Test {};

TEST_F(LogicalBufferSizeCacheTest, CacheBehavior) {
  int ncall = 0;
  LogicalBufferSizeCache cache([&](int fd) {
    ncall++;
    return fd * 2;
  });
  ASSERT_EQ(0, ncall);

  ASSERT_EQ(2, cache.size("/db/sst1", 1));
  ASSERT_EQ(1, ncall);
  ASSERT_EQ(4, cache.size("/db/sst2", 2));
  ASSERT_EQ(2, ncall);

  cache.AddCacheDirectories({"/db1", "/db2"});
  ASSERT_EQ(2, cache.size("/db/sst", 1));
  ASSERT_EQ(3, ncall);
  ASSERT_EQ(4, cache.size("/db1/sst1", 2));
  ASSERT_EQ(4, ncall);
  // Buffer size for /db1 is cached.
  ASSERT_EQ(4, cache.size("/db1/sst2", 1));
  ASSERT_EQ(4, ncall);
  ASSERT_EQ(6, cache.size("/db2/sst1", 3));
  ASSERT_EQ(5, ncall);
  // Buffer size for /db2 is cached.
  ASSERT_EQ(6, cache.size("/db2/sst2", 2));
  ASSERT_EQ(5, ncall);

  cache.AddCacheDirectories({"/db"});
  ASSERT_EQ(8, cache.size("/db/sst1", 4));
  ASSERT_EQ(6, ncall);
  // Buffer size for /db is cached.
  ASSERT_EQ(8, cache.size("/db/sst2", 1));
  ASSERT_EQ(6, ncall);
}
#endif

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
