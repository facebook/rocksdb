//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DBBlobDirectWriteTest : public DBTestBase {
 public:
  explicit DBBlobDirectWriteTest()
      : DBTestBase("db_blob_direct_write_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBBlobDirectWriteTest, BasicPutGet) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 10;  // Values >= 10 bytes go to blob files
  options.enable_blob_direct_write = true;
  options.blob_direct_write_partitions = 2;
  options.blob_file_size = 1024 * 1024;  // 1MB

  DestroyAndReopen(options);

  // Write a value that should go to blob file (>= min_blob_size)
  std::string large_value(100, 'x');
  ASSERT_OK(Put("key1", large_value));

  // Write a value that should stay inline (< min_blob_size)
  std::string small_value("tiny");
  ASSERT_OK(Put("key2", small_value));

  // Read back both values
  ASSERT_EQ(Get("key1"), large_value);
  ASSERT_EQ(Get("key2"), small_value);
}

TEST_F(DBBlobDirectWriteTest, MultipleWrites) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 10;
  options.enable_blob_direct_write = true;
  options.blob_direct_write_partitions = 4;

  DestroyAndReopen(options);

  // Write multiple large values
  const int num_keys = 100;
  for (int i = 0; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value(100 + i, 'a' + (i % 26));
    ASSERT_OK(Put(key, value));
  }

  // Read back all values
  for (int i = 0; i < num_keys; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expected(100 + i, 'a' + (i % 26));
    ASSERT_EQ(Get(key), expected);
  }
}

TEST_F(DBBlobDirectWriteTest, FlushAndRead) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 10;
  options.enable_blob_direct_write = true;
  options.blob_direct_write_partitions = 2;

  DestroyAndReopen(options);

  // Write large values
  std::string large_value(200, 'v');
  ASSERT_OK(Put("key1", large_value));
  ASSERT_OK(Put("key2", large_value));

  // Flush to SST
  ASSERT_OK(Flush());

  // Read back after flush
  ASSERT_EQ(Get("key1"), large_value);
  ASSERT_EQ(Get("key2"), large_value);
}

TEST_F(DBBlobDirectWriteTest, DeleteAndRead) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 10;
  options.enable_blob_direct_write = true;
  options.blob_direct_write_partitions = 1;

  DestroyAndReopen(options);

  std::string large_value(100, 'z');
  ASSERT_OK(Put("key1", large_value));
  ASSERT_EQ(Get("key1"), large_value);

  ASSERT_OK(Delete("key1"));
  ASSERT_EQ(Get("key1"), "NOT_FOUND");
}

TEST_F(DBBlobDirectWriteTest, MixedBlobAndInlineValues) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 50;  // Only values >= 50 bytes go to blob
  options.enable_blob_direct_write = true;
  options.blob_direct_write_partitions = 2;

  DestroyAndReopen(options);

  // Write mix of small and large values
  std::string small(10, 's');
  std::string large(100, 'l');
  ASSERT_OK(Put("small1", small));
  ASSERT_OK(Put("large1", large));
  ASSERT_OK(Put("small2", small));
  ASSERT_OK(Put("large2", large));

  ASSERT_EQ(Get("small1"), small);
  ASSERT_EQ(Get("large1"), large);
  ASSERT_EQ(Get("small2"), small);
  ASSERT_EQ(Get("large2"), large);

  // Flush and verify
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("small1"), small);
  ASSERT_EQ(Get("large1"), large);
  ASSERT_EQ(Get("small2"), small);
  ASSERT_EQ(Get("large2"), large);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
