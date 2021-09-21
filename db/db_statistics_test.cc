//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>

#include "db/db_test_util.h"
#include "monitoring/thread_status_util.h"
#include "port/stack_trace.h"
#include "rocksdb/statistics.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class DBStatisticsTest : public DBTestBase {
 public:
  DBStatisticsTest()
      : DBTestBase("db_statistics_test", /*env_do_fsync=*/true) {}
};

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
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
  DestroyAndReopen(options);

  int kNumKeysWritten = 100000;

  // Check that compressions occur and are counted when compression is turned on
  Random rnd(301);
  for (int i = 0; i < kNumKeysWritten; ++i) {
    // compressible string
    ASSERT_OK(Put(Key(i), rnd.RandomString(128) + std::string(128, 'a')));
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
    ASSERT_OK(Put(Key(i), rnd.RandomString(128) + std::string(128, 'a')));
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
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
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
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_GE(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), kMutexWaitDelay);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBStatisticsTest, ResetStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  for (int i = 0; i < 2; ++i) {
    // pick arbitrary ticker and histogram. On first iteration they're zero
    // because db is unused. On second iteration they're zero due to Reset().
    ASSERT_EQ(0, TestGetTickerCount(options, NUMBER_KEYS_WRITTEN));
    HistogramData histogram_data;
    options.statistics->histogramData(DB_WRITE, &histogram_data);
    ASSERT_EQ(0.0, histogram_data.max);

    if (i == 0) {
      // The Put() makes some of the ticker/histogram stats nonzero until we
      // Reset().
      ASSERT_OK(Put("hello", "rocksdb"));
      ASSERT_EQ(1, TestGetTickerCount(options, NUMBER_KEYS_WRITTEN));
      options.statistics->histogramData(DB_WRITE, &histogram_data);
      ASSERT_GT(histogram_data.max, 0.0);
      ASSERT_OK(options.statistics->Reset());
    }
  }
}

TEST_F(DBStatisticsTest, ExcludeTickers) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  options.statistics->set_stats_level(StatsLevel::kExceptTickers);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_EQ(0, options.statistics->getTickerCount(BYTES_WRITTEN));
  options.statistics->set_stats_level(StatsLevel::kExceptHistogramOrTimers);
  Reopen(options);
  ASSERT_EQ("value", Get("foo"));
  ASSERT_GT(options.statistics->getTickerCount(BYTES_READ), 0);
}

#ifndef ROCKSDB_LITE

TEST_F(DBStatisticsTest, VerifyChecksumReadStat) {
  Options options = CurrentOptions();
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  Reopen(options);

  // Expected to be populated regardless of `PerfLevel` in user thread
  SetPerfLevel(kDisable);

  {
    // Scenario 0: only WAL data. Not verified so require ticker to be zero.
    ASSERT_OK(Put("foo", "value"));
    ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
    ASSERT_OK(db_->VerifyChecksum());
    ASSERT_EQ(0,
              options.statistics->getTickerCount(VERIFY_CHECKSUM_READ_BYTES));
  }

  // Create one SST.
  ASSERT_OK(Flush());
  std::unordered_map<std::string, uint64_t> table_files;
  uint64_t table_files_size = 0;
  GetAllDataFiles(kTableFile, &table_files, &table_files_size);

  {
    // Scenario 1: Table verified in `VerifyFileChecksums()`. This should read
    // the whole file so we require the ticker stat exactly matches the file
    // size.
    ASSERT_OK(options.statistics->Reset());
    ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
    ASSERT_EQ(table_files_size,
              options.statistics->getTickerCount(VERIFY_CHECKSUM_READ_BYTES));
  }

  {
    // Scenario 2: Table verified in `VerifyChecksum()`. This opens a
    // `TableReader` to verify each block. It can involve duplicate reads of the
    // same data so we set a lower-bound only.
    ASSERT_OK(options.statistics->Reset());
    ASSERT_OK(db_->VerifyChecksum());
    ASSERT_GE(options.statistics->getTickerCount(VERIFY_CHECKSUM_READ_BYTES),
              table_files_size);
  }
}

#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
