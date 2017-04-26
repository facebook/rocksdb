//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>

#include "db/db_test_util.h"
#include "monitoring/thread_status_util.h"
#include "port/stack_trace.h"
#include "rocksdb/statistics.h"

namespace rocksdb {

class DBStatisticsTest : public DBTestBase {
 public:
  DBStatisticsTest() : DBTestBase("/db_statistics_test") {}
};

#ifndef ROCKSDB_LITE

TEST_F(DBStatisticsTest, GetApproximateMemTableStats) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const int N = 128;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
  }

  uint64_t count;
  uint64_t size;

  std::string start = Key(50);
  std::string end = Key(60);
  Range r(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  ASSERT_GT(count, 0);
  ASSERT_LE(count, N);
  ASSERT_GT(size, 6000);
  ASSERT_LT(size, 204800);

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  ASSERT_EQ(count, 0);
  ASSERT_EQ(size, 0);

  Flush();

  start = Key(50);
  end = Key(60);
  r = Range(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  ASSERT_EQ(count, 0);
  ASSERT_EQ(size, 0);

  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(1000 + i), RandomString(&rnd, 1024)));
  }

  start = Key(100);
  end = Key(1020);
  r = Range(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  ASSERT_GT(count, 20);
  ASSERT_GT(size, 6000);
}

#endif  // ROCKSDB_LITE

TEST_F(DBStatisticsTest, CompressionStatsTest) {
  CompressionType type;

  if (Snappy_Supported()) {
    type = kSnappyCompression;
    fprintf(stderr, "using snappy\n");
  } else if (Zlib_Supported()) {
    type = kZlibCompression;
    fprintf(stderr, "using zlib\n");
  } else if (BZip2_Supported()) {
    type = kBZip2Compression;
    fprintf(stderr, "using bzip2\n");
  } else if (LZ4_Supported()) {
    type = kLZ4Compression;
    fprintf(stderr, "using lz4\n");
  } else if (XPRESS_Supported()) {
    type = kXpressCompression;
    fprintf(stderr, "using xpress\n");
  } else if (ZSTD_Supported()) {
    type = kZSTD;
    fprintf(stderr, "using ZSTD\n");
  } else {
    fprintf(stderr, "skipping test, compression disabled\n");
    return;
  }

  Options options = CurrentOptions();
  options.compression = type;
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics->stats_level_ = StatsLevel::kExceptTimeForMutex;
  DestroyAndReopen(options);

  int kNumKeysWritten = 100000;

  // Check that compressions occur and are counted when compression is turned on
  Random rnd(301);
  for (int i = 0; i < kNumKeysWritten; ++i) {
    // compressible string
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 128) + std::string(128, 'a')));
  }
  ASSERT_OK(Flush());
  ASSERT_GT(options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED), 0);

  for (int i = 0; i < kNumKeysWritten; ++i) {
    auto r = Get(Key(i));
  }
  ASSERT_GT(options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED), 0);

  options.compression = kNoCompression;
  DestroyAndReopen(options);
  uint64_t currentCompressions =
            options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
  uint64_t currentDecompressions =
            options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED);

  // Check that compressions do not occur when turned off
  for (int i = 0; i < kNumKeysWritten; ++i) {
    // compressible string
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 128) + std::string(128, 'a')));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED)
            - currentCompressions, 0);

  for (int i = 0; i < kNumKeysWritten; ++i) {
    auto r = Get(Key(i));
  }
  ASSERT_EQ(options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED)
            - currentDecompressions, 0);
}

TEST_F(DBStatisticsTest, MutexWaitStatsDisabledByDefault) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_EQ(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), 0);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBStatisticsTest, MutexWaitStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics->stats_level_ = StatsLevel::kAll;
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_GE(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), kMutexWaitDelay);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
