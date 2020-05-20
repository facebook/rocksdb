// Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "test_util/testharness.h"

#ifdef ROCKSDB_LIB_IO_POSIX
#include "env/io_posix.h"

namespace ROCKSDB_NAMESPACE {

#ifdef OS_LINUX
class LogicalBlockSizeCacheTest : public testing::Test {};

// Tests the caching behavior.
TEST_F(LogicalBlockSizeCacheTest, Cache) {
  int ncall = 0;
  auto get_fd_block_size = [&](int fd) {
    ncall++;
    return fd;
  };
  std::map<std::string, int> dir_fds{
      {"/", 0},
      {"/db", 1},
      {"/db1", 2},
      {"/db2", 3},
  };
  auto get_dir_block_size = [&](const std::string& dir, size_t* size) {
    ncall++;
    *size = dir_fds[dir];
    return Status::OK();
  };
  LogicalBlockSizeCache cache(get_fd_block_size, get_dir_block_size);
  ASSERT_EQ(0, ncall);
  ASSERT_EQ(0, cache.Size());

  ASSERT_EQ(6, cache.GetLogicalBlockSize("/sst", 6));
  ASSERT_EQ(1, ncall);
  ASSERT_EQ(7, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(2, ncall);
  ASSERT_EQ(8, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(3, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/", "/db1/", "/db2"}));
  ASSERT_EQ(3, cache.Size());
  ASSERT_TRUE(cache.Contains("/"));
  ASSERT_TRUE(cache.Contains("/db1"));
  ASSERT_TRUE(cache.Contains("/db2"));
  ASSERT_EQ(6, ncall);
  // Block size for / is cached.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/sst", 6));
  ASSERT_EQ(6, ncall);
  // No cached size for /db.
  ASSERT_EQ(7, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(7, ncall);
  ASSERT_EQ(8, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(8, ncall);
  // Block size for /db1 is cached.
  ASSERT_EQ(2, cache.GetLogicalBlockSize("/db1/sst1", 4));
  ASSERT_EQ(8, ncall);
  ASSERT_EQ(2, cache.GetLogicalBlockSize("/db1/sst2", 5));
  ASSERT_EQ(8, ncall);
  // Block size for /db2 is cached.
  ASSERT_EQ(3, cache.GetLogicalBlockSize("/db2/sst1", 6));
  ASSERT_EQ(8, ncall);
  ASSERT_EQ(3, cache.GetLogicalBlockSize("/db2/sst2", 7));
  ASSERT_EQ(8, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(4, cache.Size());
  ASSERT_TRUE(cache.Contains("/"));
  ASSERT_TRUE(cache.Contains("/db1"));
  ASSERT_TRUE(cache.Contains("/db2"));
  ASSERT_TRUE(cache.Contains("/db"));

  ASSERT_EQ(9, ncall);
  // Block size for /db is cached.
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(9, ncall);
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(9, ncall);
}

// Tests the reference counting behavior.
TEST_F(LogicalBlockSizeCacheTest, Ref) {
  int ncall = 0;
  auto get_fd_block_size = [&](int fd) {
    ncall++;
    return fd;
  };
  std::map<std::string, int> dir_fds{
      {"/db", 0},
  };
  auto get_dir_block_size = [&](const std::string& dir, size_t* size) {
    ncall++;
    *size = dir_fds[dir];
    return Status::OK();
  };
  LogicalBlockSizeCache cache(get_fd_block_size, get_dir_block_size);

  ASSERT_EQ(0, ncall);

  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst0", 1));
  ASSERT_EQ(1, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(2, ncall);
  ASSERT_EQ(1, cache.GetRefCount("/db"));
  // Block size for /db is cached. Ref count = 1.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/db/sst1", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 2, but won't recompute the cached buffer size.
  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(2, cache.GetRefCount("/db"));
  ASSERT_EQ(2, ncall);

  // Ref count = 1.
  cache.UnrefAndTryRemoveCachedLogicalBlockSize({"/db"});
  ASSERT_EQ(1, cache.GetRefCount("/db"));
  // Block size for /db is still cached.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/db/sst2", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 0 and cached buffer size for /db is removed.
  cache.UnrefAndTryRemoveCachedLogicalBlockSize({"/db"});
  ASSERT_EQ(0, cache.Size());
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst0", 1));
  ASSERT_EQ(3, ncall);
}
#endif

}  // namespace ROCKSDB_NAMESPACE
#endif

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
