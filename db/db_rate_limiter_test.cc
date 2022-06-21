//  Copyright (c) 2022-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/file_checksum_helper.h"

namespace ROCKSDB_NAMESPACE {

class DBRateLimiterOnReadTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  explicit DBRateLimiterOnReadTest()
      : DBTestBase("db_rate_limiter_on_read_test", /*env_do_fsync=*/false),
        use_direct_io_(std::get<0>(GetParam())),
        use_block_cache_(std::get<1>(GetParam())),
        use_readahead_(std::get<2>(GetParam())) {}

  void Init() {
    options_ = GetOptions();
    Reopen(options_);
    for (int i = 0; i < kNumFiles; ++i) {
      for (int j = 0; j < kNumKeysPerFile; ++j) {
        ASSERT_OK(Put(Key(i * kNumKeysPerFile + j), "val"));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(1);
  }

  BlockBasedTableOptions GetTableOptions() {
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = !use_block_cache_;
    return table_options;
  }

  ReadOptions GetReadOptions() {
    ReadOptions read_options;
    read_options.rate_limiter_priority = Env::IO_USER;
    read_options.readahead_size = use_readahead_ ? kReadaheadBytes : 0;
    return read_options;
  }

  Options GetOptions() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.file_checksum_gen_factory.reset(new FileChecksumGenCrc32cFactory());
    options.rate_limiter.reset(NewGenericRateLimiter(
        1 << 20 /* rate_bytes_per_sec */, 100 * 1000 /* refill_period_us */,
        10 /* fairness */, RateLimiter::Mode::kAllIo));
    options.table_factory.reset(NewBlockBasedTableFactory(GetTableOptions()));
    options.use_direct_reads = use_direct_io_;
    return options;
  }

 protected:
  const static int kNumKeysPerFile = 1;
  const static int kNumFiles = 3;
  const static int kReadaheadBytes = 32 << 10;  // 32KB

  Options options_;
  const bool use_direct_io_;
  const bool use_block_cache_;
  const bool use_readahead_;
};

std::string GetTestNameSuffix(
    ::testing::TestParamInfo<std::tuple<bool, bool, bool>> info) {
  std::ostringstream oss;
  if (std::get<0>(info.param)) {
    oss << "DirectIO";
  } else {
    oss << "BufferedIO";
  }
  if (std::get<1>(info.param)) {
    oss << "_BlockCache";
  } else {
    oss << "_NoBlockCache";
  }
  if (std::get<2>(info.param)) {
    oss << "_Readahead";
  } else {
    oss << "_NoReadahead";
  }
  return oss.str();
}

#ifndef ROCKSDB_LITE
INSTANTIATE_TEST_CASE_P(DBRateLimiterOnReadTest, DBRateLimiterOnReadTest,
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool()),
                        GetTestNameSuffix);
#else   // ROCKSDB_LITE
// Cannot use direct I/O in lite mode.
INSTANTIATE_TEST_CASE_P(DBRateLimiterOnReadTest, DBRateLimiterOnReadTest,
                        ::testing::Combine(::testing::Values(false),
                                           ::testing::Bool(),
                                           ::testing::Bool()),
                        GetTestNameSuffix);
#endif  // ROCKSDB_LITE

TEST_P(DBRateLimiterOnReadTest, Get) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  int expected = 0;
  for (int i = 0; i < kNumFiles; ++i) {
    {
      std::string value;
      ASSERT_OK(db_->Get(GetReadOptions(), Key(i * kNumKeysPerFile), &value));
      ++expected;
    }
    ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

    {
      std::string value;
      ASSERT_OK(db_->Get(GetReadOptions(), Key(i * kNumKeysPerFile), &value));
      if (!use_block_cache_) {
        ++expected;
      }
    }
    ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
  }
}

TEST_P(DBRateLimiterOnReadTest, NewMultiGet) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  const int kNumKeys = kNumFiles * kNumKeysPerFile;
  int64_t expected = 0;
  {
    std::vector<std::string> key_bufs;
    key_bufs.reserve(kNumKeys);
    std::vector<Slice> keys;
    keys.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; ++i) {
      key_bufs.emplace_back(Key(i));
      keys.emplace_back(key_bufs[i]);
    }
    std::vector<Status> statuses(kNumKeys);
    std::vector<PinnableSlice> values(kNumKeys);
    const int64_t prev_total_rl_req = options_.rate_limiter->GetTotalRequests();
    db_->MultiGet(GetReadOptions(), dbfull()->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());
    const int64_t cur_total_rl_req = options_.rate_limiter->GetTotalRequests();
    for (int i = 0; i < kNumKeys; ++i) {
      ASSERT_TRUE(statuses[i].ok());
    }
    ASSERT_GT(cur_total_rl_req, prev_total_rl_req);
    ASSERT_EQ(cur_total_rl_req - prev_total_rl_req,
              options_.rate_limiter->GetTotalRequests(Env::IO_USER));
  }
  expected += kNumKeys;
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, OldMultiGet) {
  // The old `vector<Status>`-returning `MultiGet()` APIs use `Read()`, which
  // supports rate limiting.
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  const int kNumKeys = kNumFiles * kNumKeysPerFile;
  int expected = 0;
  {
    std::vector<std::string> key_bufs;
    key_bufs.reserve(kNumKeys);
    std::vector<Slice> keys;
    keys.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; ++i) {
      key_bufs.emplace_back(Key(i));
      keys.emplace_back(key_bufs[i]);
    }
    std::vector<std::string> values;
    std::vector<Status> statuses =
        db_->MultiGet(GetReadOptions(), keys, &values);
    for (int i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
    }
  }
  expected += kNumKeys;
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, Iterator) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  std::unique_ptr<Iterator> iter(db_->NewIterator(GetReadOptions()));
  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ++expected;
    ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
  }

  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    // When `use_block_cache_ == true`, the reverse scan will access the blocks
    // loaded to cache during the above forward scan, in which case no further
    // file reads are expected.
    if (!use_block_cache_) {
      ++expected;
    }
  }
  // Reverse scan does not read evenly (one block per iteration) due to
  // descending seqno ordering, so wait until after the loop to check total.
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

#if !defined(ROCKSDB_LITE)

TEST_P(DBRateLimiterOnReadTest, VerifyChecksum) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  ASSERT_OK(db_->VerifyChecksum(GetReadOptions()));
  // The files are tiny so there should have just been one read per file.
  int expected = kNumFiles;
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, VerifyFileChecksums) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  ASSERT_OK(db_->VerifyFileChecksums(GetReadOptions()));
  // The files are tiny so there should have just been one read per file.
  int expected = kNumFiles;
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

#endif  // !defined(ROCKSDB_LITE)

class DBRateLimiterOnWriteTest : public DBTestBase {
 public:
  explicit DBRateLimiterOnWriteTest()
      : DBTestBase("db_rate_limiter_on_write_test", /*env_do_fsync=*/false) {}

  void Init() {
    options_ = GetOptions();
    ASSERT_OK(TryReopenWithColumnFamilies({"default"}, options_));
    Random rnd(301);
    for (int i = 0; i < kNumFiles; i++) {
      ASSERT_OK(Put(0, kStartKey, rnd.RandomString(2)));
      ASSERT_OK(Put(0, kEndKey, rnd.RandomString(2)));
      ASSERT_OK(Flush(0));
    }
  }

  Options GetOptions() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.rate_limiter.reset(NewGenericRateLimiter(
        1 << 20 /* rate_bytes_per_sec */, 100 * 1000 /* refill_period_us */,
        10 /* fairness */, RateLimiter::Mode::kWritesOnly));
    options.table_factory.reset(
        NewBlockBasedTableFactory(BlockBasedTableOptions()));
    return options;
  }

 protected:
  inline const static int64_t kNumFiles = 3;
  inline const static std::string kStartKey = "a";
  inline const static std::string kEndKey = "b";
  Options options_;
};

TEST_F(DBRateLimiterOnWriteTest, Flush) {
  std::int64_t prev_total_request = 0;

  Init();

  std::int64_t actual_flush_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
      prev_total_request;
  std::int64_t exepcted_flush_request = kNumFiles;
  EXPECT_EQ(actual_flush_request, exepcted_flush_request);
  EXPECT_EQ(actual_flush_request,
            options_.rate_limiter->GetTotalRequests(Env::IO_HIGH));
}

TEST_F(DBRateLimiterOnWriteTest, Compact) {
  Init();

  // Pre-comaction:
  // level-0 : `kNumFiles` SST files overlapping on [kStartKey, kEndKey]
#ifndef ROCKSDB_LITE
  std::string files_per_level_pre_compaction = std::to_string(kNumFiles);
  ASSERT_EQ(files_per_level_pre_compaction, FilesPerLevel(0 /* cf */));
#endif  // !ROCKSDB_LITE

  std::int64_t prev_total_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL);
  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_LOW));

  Compact(kStartKey, kEndKey);

  std::int64_t actual_compaction_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
      prev_total_request;

  // Post-comaction:
  // level-0 : 0 SST file
  // level-1 : 1 SST file
#ifndef ROCKSDB_LITE
  std::string files_per_level_post_compaction = "0,1";
  ASSERT_EQ(files_per_level_post_compaction, FilesPerLevel(0 /* cf */));
#endif  // !ROCKSDB_LITE

  std::int64_t exepcted_compaction_request = 1;
  EXPECT_EQ(actual_compaction_request, exepcted_compaction_request);
  EXPECT_EQ(actual_compaction_request,
            options_.rate_limiter->GetTotalRequests(Env::IO_LOW));
}

class DBRateLimiterOnWriteWALTest
    : public DBRateLimiterOnWriteTest,
      public ::testing::WithParamInterface<std::tuple<
          bool /* WriteOptions::disableWal */,
          bool /* Options::manual_wal_flush */,
          Env::IOPriority /* WriteOptions::rate_limiter_priority */>> {
 public:
  static std::string GetTestNameSuffix(
      ::testing::TestParamInfo<std::tuple<bool, bool, Env::IOPriority>> info) {
    std::ostringstream oss;
    if (std::get<0>(info.param)) {
      oss << "DisableWAL";
    } else {
      oss << "EnableWAL";
    }
    if (std::get<1>(info.param)) {
      oss << "_ManualWALFlush";
    } else {
      oss << "_AutoWALFlush";
    }
    if (std::get<2>(info.param) == Env::IO_USER) {
      oss << "_RateLimitAutoWALFlush";
    } else if (std::get<2>(info.param) == Env::IO_TOTAL) {
      oss << "_NoRateLimitAutoWALFlush";
    } else {
      oss << "_RateLimitAutoWALFlushWithIncorrectPriority";
    }
    return oss.str();
  }

  explicit DBRateLimiterOnWriteWALTest()
      : disable_wal_(std::get<0>(GetParam())),
        manual_wal_flush_(std::get<1>(GetParam())),
        rate_limiter_priority_(std::get<2>(GetParam())) {}

  void Init() {
    options_ = GetOptions();
    options_.manual_wal_flush = manual_wal_flush_;
    Reopen(options_);
  }

  WriteOptions GetWriteOptions() {
    WriteOptions write_options;
    write_options.disableWAL = disable_wal_;
    write_options.rate_limiter_priority = rate_limiter_priority_;
    return write_options;
  }

 protected:
  bool disable_wal_;
  bool manual_wal_flush_;
  Env::IOPriority rate_limiter_priority_;
};

INSTANTIATE_TEST_CASE_P(
    DBRateLimiterOnWriteWALTest, DBRateLimiterOnWriteWALTest,
    ::testing::Values(std::make_tuple(false, false, Env::IO_TOTAL),
                      std::make_tuple(false, false, Env::IO_USER),
                      std::make_tuple(false, false, Env::IO_HIGH),
                      std::make_tuple(false, true, Env::IO_USER),
                      std::make_tuple(true, false, Env::IO_USER)),
    DBRateLimiterOnWriteWALTest::GetTestNameSuffix);

TEST_P(DBRateLimiterOnWriteWALTest, AutoWalFlush) {
  Init();

  const bool no_rate_limit_auto_wal_flush =
      (rate_limiter_priority_ == Env::IO_TOTAL);
  const bool valid_arg = (rate_limiter_priority_ == Env::IO_USER &&
                          !disable_wal_ && !manual_wal_flush_);

  std::int64_t prev_total_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL);
  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  Status s = Put("foo", "v1", GetWriteOptions());

  if (no_rate_limit_auto_wal_flush || valid_arg) {
    EXPECT_TRUE(s.ok());
  } else {
    EXPECT_TRUE(s.IsInvalidArgument());
    EXPECT_TRUE(s.ToString().find("WriteOptions::rate_limiter_priority") !=
                std::string::npos);
  }

  std::int64_t actual_auto_wal_flush_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
      prev_total_request;
  std::int64_t expected_auto_wal_flush_request = valid_arg ? 1 : 0;

  EXPECT_EQ(actual_auto_wal_flush_request, expected_auto_wal_flush_request);
  EXPECT_EQ(actual_auto_wal_flush_request,
            options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
