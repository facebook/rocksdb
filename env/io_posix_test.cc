// Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "test_util/testharness.h"

#ifdef ROCKSDB_LIB_IO_POSIX
#include "env/io_posix.h"

namespace ROCKSDB_NAMESPACE {

#ifdef OS_LINUX
class LogicalBufferSizeCacheTest : public testing::Test {};

// Tests the caching behavior.
TEST_F(LogicalBufferSizeCacheTest, Cache) {
  int ncall = 0;
  LogicalBufferSizeCache cache([&](int fd) {
    ncall++;
    return fd;
  });
  ASSERT_EQ(0, ncall);

  ASSERT_EQ(7, cache.GetLogicalBufferSize("/db/sst1", 7));
  ASSERT_EQ(1, ncall);
  ASSERT_EQ(8, cache.GetLogicalBufferSize("/db/sst2", 8));
  ASSERT_EQ(2, ncall);

  cache.RefAndCacheLogicalBufferSize({{"/db1", 1}, {"/db2", 2}});
  ASSERT_EQ(4, ncall);
  // No cached size for /db.
  ASSERT_EQ(7, cache.GetLogicalBufferSize("/db/sst1", 7));
  ASSERT_EQ(5, ncall);
  ASSERT_EQ(8, cache.GetLogicalBufferSize("/db/sst2", 8));
  ASSERT_EQ(6, ncall);
  // Buffer size for /db1 is cached.
  ASSERT_EQ(1, cache.GetLogicalBufferSize("/db1/sst1", 4));
  ASSERT_EQ(6, ncall);
  ASSERT_EQ(1, cache.GetLogicalBufferSize("/db1/sst2", 5));
  ASSERT_EQ(6, ncall);
  // Buffer size for /db2 is cached.
  ASSERT_EQ(2, cache.GetLogicalBufferSize("/db2/sst1", 6));
  ASSERT_EQ(6, ncall);
  ASSERT_EQ(2, cache.GetLogicalBufferSize("/db2/sst2", 7));
  ASSERT_EQ(6, ncall);

  cache.RefAndCacheLogicalBufferSize({{"/db", 3}});
  ASSERT_EQ(7, ncall);
  // Buffer size for /db is cached.
  ASSERT_EQ(3, cache.GetLogicalBufferSize("/db/sst1", 7));
  ASSERT_EQ(7, ncall);
  ASSERT_EQ(3, cache.GetLogicalBufferSize("/db/sst2", 8));
  ASSERT_EQ(7, ncall);
}

// Tests the reference counting behavior.
TEST_F(LogicalBufferSizeCacheTest, Ref) {
  int ncall = 0;
  LogicalBufferSizeCache cache([&](int fd) {
    ncall++;
    return fd;
  });
  ASSERT_EQ(0, ncall);

  ASSERT_EQ(1, cache.GetLogicalBufferSize("/db/sst0", 1));
  ASSERT_EQ(1, ncall);

  cache.RefAndCacheLogicalBufferSize({{"/db", 2}});
  ASSERT_EQ(2, ncall);
  // Buffer size for /db is cached. Ref count = 1.
  ASSERT_EQ(2, cache.GetLogicalBufferSize("/db/sst1", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 2, but won't recompute the cached buffer size.
  cache.RefAndCacheLogicalBufferSize({{"/db", 2}});
  ASSERT_EQ(2, ncall);

  // Ref count = 1.
  cache.UnrefAndTryRemoveCachedLogicalBufferSize({{"/db"}});
  // Buffer size for /db is still cached.
  ASSERT_EQ(2, cache.GetLogicalBufferSize("/db/sst2", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 0 and cached buffer size for /db is removed.
  cache.UnrefAndTryRemoveCachedLogicalBufferSize({{"/db"}});
  ASSERT_EQ(1, cache.GetLogicalBufferSize("/db/sst0", 1));
  ASSERT_EQ(3, ncall);
}
#endif

}  // namespace ROCKSDB_NAMESPACE
#endif

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
