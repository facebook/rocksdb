//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <limits>
#include <string>
#include <unordered_map>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/rate_limiter.h"
#include "util/random.h"
#include "util/sync_point.h"
#include "util/testutil.h"

namespace rocksdb {

class DBOptionsTest : public DBTestBase {
 public:
  DBOptionsTest() : DBTestBase("/db_options_test") {}

#ifndef ROCKSDB_LITE
  std::unordered_map<std::string, std::string> GetMutableDBOptionsMap(
      const DBOptions& options) {
    std::string options_str;
    GetStringFromDBOptions(&options_str, options);
    std::unordered_map<std::string, std::string> options_map;
    StringToMap(options_str, &options_map);
    std::unordered_map<std::string, std::string> mutable_map;
    for (const auto opt : db_options_type_info) {
      if (opt.second.is_mutable &&
          opt.second.verification != OptionVerificationType::kDeprecated) {
        mutable_map[opt.first] = options_map[opt.first];
      }
    }
    return mutable_map;
  }

  std::unordered_map<std::string, std::string> GetMutableCFOptionsMap(
      const ColumnFamilyOptions& options) {
    std::string options_str;
    GetStringFromColumnFamilyOptions(&options_str, options);
    std::unordered_map<std::string, std::string> options_map;
    StringToMap(options_str, &options_map);
    std::unordered_map<std::string, std::string> mutable_map;
    for (const auto opt : cf_options_type_info) {
      if (opt.second.is_mutable &&
          opt.second.verification != OptionVerificationType::kDeprecated) {
        mutable_map[opt.first] = options_map[opt.first];
      }
    }
    return mutable_map;
  }

  std::unordered_map<std::string, std::string> GetRandomizedMutableCFOptionsMap(
      Random* rnd) {
    Options options;
    options.env = env_;
    ImmutableDBOptions db_options(options);
    test::RandomInitCFOptions(&options, rnd);
    auto sanitized_options = SanitizeOptions(db_options, options);
    auto opt_map = GetMutableCFOptionsMap(sanitized_options);
    delete options.compaction_filter;
    return opt_map;
  }

  std::unordered_map<std::string, std::string> GetRandomizedMutableDBOptionsMap(
      Random* rnd) {
    DBOptions db_options;
    test::RandomInitDBOptions(&db_options, rnd);
    auto sanitized_options = SanitizeOptions(dbname_, db_options);
    return GetMutableDBOptionsMap(sanitized_options);
  }
#endif  // ROCKSDB_LITE
};

// RocksDB lite don't support dynamic options.
#ifndef ROCKSDB_LITE

TEST_F(DBOptionsTest, GetLatestDBOptions) {
  // GetOptions should be able to get latest option changed by SetOptions.
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  Random rnd(228);
  Reopen(options);
  auto new_options = GetRandomizedMutableDBOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetDBOptions(new_options));
  ASSERT_EQ(new_options, GetMutableDBOptionsMap(dbfull()->GetDBOptions()));
}

TEST_F(DBOptionsTest, GetLatestCFOptions) {
  // GetOptions should be able to get latest option changed by SetOptions.
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  Random rnd(228);
  Reopen(options);
  CreateColumnFamilies({"foo"}, options);
  ReopenWithColumnFamilies({"default", "foo"}, options);
  auto options_default = GetRandomizedMutableCFOptionsMap(&rnd);
  auto options_foo = GetRandomizedMutableCFOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetOptions(handles_[0], options_default));
  ASSERT_OK(dbfull()->SetOptions(handles_[1], options_foo));
  ASSERT_EQ(options_default,
            GetMutableCFOptionsMap(dbfull()->GetOptions(handles_[0])));
  ASSERT_EQ(options_foo,
            GetMutableCFOptionsMap(dbfull()->GetOptions(handles_[1])));
}

TEST_F(DBOptionsTest, SetOptionsAndReopen) {
  Random rnd(1044);
  auto rand_opts = GetRandomizedMutableCFOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetOptions(rand_opts));
  // Verify if DB can be reopen after setting options.
  Options options;
  options.env = env_;
  ASSERT_OK(TryReopen(options));
}

TEST_F(DBOptionsTest, EnableAutoCompactionAndTriggerStall) {
  const std::string kValue(1024, 'v');
  for (int method_type = 0; method_type < 2; method_type++) {
    for (int option_type = 0; option_type < 4; option_type++) {
      Options options;
      options.create_if_missing = true;
      options.disable_auto_compactions = true;
      options.write_buffer_size = 1024 * 1024 * 10;
      options.compression = CompressionType::kNoCompression;
      options.level0_file_num_compaction_trigger = 1;
      options.level0_stop_writes_trigger = std::numeric_limits<int>::max();
      options.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
      options.hard_pending_compaction_bytes_limit =
          std::numeric_limits<uint64_t>::max();
      options.soft_pending_compaction_bytes_limit =
          std::numeric_limits<uint64_t>::max();
      options.env = env_;

      DestroyAndReopen(options);
      int i = 0;
      for (; i < 1024; i++) {
        Put(Key(i), kValue);
      }
      Flush();
      for (; i < 1024 * 2; i++) {
        Put(Key(i), kValue);
      }
      Flush();
      dbfull()->TEST_WaitForFlushMemTable();
      ASSERT_EQ(2, NumTableFilesAtLevel(0));
      uint64_t l0_size = SizeAtLevel(0);

      switch (option_type) {
        case 0:
          // test with level0_stop_writes_trigger
          options.level0_stop_writes_trigger = 2;
          options.level0_slowdown_writes_trigger = 2;
          break;
        case 1:
          options.level0_slowdown_writes_trigger = 2;
          break;
        case 2:
          options.hard_pending_compaction_bytes_limit = l0_size;
          options.soft_pending_compaction_bytes_limit = l0_size;
          break;
        case 3:
          options.soft_pending_compaction_bytes_limit = l0_size;
          break;
      }
      Reopen(options);
      dbfull()->TEST_WaitForCompact();
      ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
      ASSERT_FALSE(dbfull()->TEST_write_controler().NeedsDelay());

      SyncPoint::GetInstance()->LoadDependency(
          {{"DBOptionsTest::EnableAutoCompactionAndTriggerStall:1",
            "BackgroundCallCompaction:0"},
           {"DBImpl::BackgroundCompaction():BeforePickCompaction",
            "DBOptionsTest::EnableAutoCompactionAndTriggerStall:2"},
           {"DBOptionsTest::EnableAutoCompactionAndTriggerStall:3",
            "DBImpl::BackgroundCompaction():AfterPickCompaction"}});
      // Block background compaction.
      SyncPoint::GetInstance()->EnableProcessing();

      switch (method_type) {
        case 0:
          ASSERT_OK(
              dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
          break;
        case 1:
          ASSERT_OK(dbfull()->EnableAutoCompaction(
              {dbfull()->DefaultColumnFamily()}));
          break;
      }
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:1");
      // Wait for stall condition recalculate.
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:2");

      switch (option_type) {
        case 0:
          ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
          break;
        case 1:
          ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
          ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
          break;
        case 2:
          ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
          break;
        case 3:
          ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
          ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
          break;
      }
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:3");

      // Background compaction executed.
      dbfull()->TEST_WaitForCompact();
      ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
      ASSERT_FALSE(dbfull()->TEST_write_controler().NeedsDelay());
    }
  }
}

TEST_F(DBOptionsTest, SetOptionsMayTriggerCompaction) {
  Options options;
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 1000;
  options.env = env_;
  Reopen(options);
  for (int i = 0; i < 3; i++) {
    // Need to insert two keys to avoid trivial move.
    ASSERT_OK(Put("foo", ToString(i)));
    ASSERT_OK(Put("bar", ToString(i)));
    Flush();
  }
  ASSERT_EQ("3", FilesPerLevel());
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "3"}}));
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ("0,1", FilesPerLevel());
}

TEST_F(DBOptionsTest, SetBackgroundCompactionThreads) {
  Options options;
  options.create_if_missing = true;
  options.max_background_compactions = 1;   // default value
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_compactions", "3"}}));
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  auto stop_token = dbfull()->TEST_write_controler().GetStopToken();
  ASSERT_EQ(3, dbfull()->TEST_BGCompactionsAllowed());
}

TEST_F(DBOptionsTest, SetBackgroundJobs) {
  Options options;
  options.create_if_missing = true;
  options.max_background_jobs = 8;
  options.env = env_;
  Reopen(options);

  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      options.max_background_jobs = 12;
      ASSERT_OK(dbfull()->SetDBOptions(
          {{"max_background_jobs",
            std::to_string(options.max_background_jobs)}}));
    }

    ASSERT_EQ(options.max_background_jobs / 4,
              dbfull()->TEST_BGFlushesAllowed());
    ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

    auto stop_token = dbfull()->TEST_write_controler().GetStopToken();

    ASSERT_EQ(options.max_background_jobs / 4,
              dbfull()->TEST_BGFlushesAllowed());
    ASSERT_EQ(3 * options.max_background_jobs / 4,
              dbfull()->TEST_BGCompactionsAllowed());
  }
}

TEST_F(DBOptionsTest, AvoidFlushDuringShutdown) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  WriteOptions write_without_wal;
  write_without_wal.disableWAL = true;

  ASSERT_FALSE(options.avoid_flush_during_shutdown);
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1", write_without_wal));
  Reopen(options);
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_EQ("1", FilesPerLevel());

  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v2", write_without_wal));
  ASSERT_OK(dbfull()->SetDBOptions({{"avoid_flush_during_shutdown", "true"}}));
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ("", FilesPerLevel());
}

TEST_F(DBOptionsTest, SetDelayedWriteRateOption) {
  Options options;
  options.create_if_missing = true;
  options.delayed_write_rate = 2 * 1024U * 1024U;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(2 * 1024U * 1024U, dbfull()->TEST_write_controler().max_delayed_write_rate());

  ASSERT_OK(dbfull()->SetDBOptions({{"delayed_write_rate", "20000"}}));
  ASSERT_EQ(20000, dbfull()->TEST_write_controler().max_delayed_write_rate());
}

TEST_F(DBOptionsTest, MaxTotalWalSizeChange) {
  Random rnd(1044);
  const auto value_size = size_t(1024);
  std::string value;
  test::RandomString(&rnd, value_size, &value);

  Options options;
  options.create_if_missing = true;
  options.env = env_;
  CreateColumnFamilies({"1", "2", "3"}, options);
  ReopenWithColumnFamilies({"default", "1", "2", "3"}, options);

  WriteOptions write_options;

  const int key_count = 100;
  for (int i = 0; i < key_count; ++i) {
    for (size_t cf = 0; cf < handles_.size(); ++cf) {
      ASSERT_OK(Put(static_cast<int>(cf), Key(i), value));
    }
  }
  ASSERT_OK(dbfull()->SetDBOptions({{"max_total_wal_size", "10"}}));

  for (size_t cf = 0; cf < handles_.size(); ++cf) {
    dbfull()->TEST_WaitForFlushMemTable(handles_[cf]);
    ASSERT_EQ("1", FilesPerLevel(static_cast<int>(cf)));
  }
}

TEST_F(DBOptionsTest, SetStatsDumpPeriodSec) {
  Options options;
  options.create_if_missing = true;
  options.stats_dump_period_sec = 5;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(5, dbfull()->GetDBOptions().stats_dump_period_sec);

  for (int i = 0; i < 20; i++) {
    int num = rand() % 5000 + 1;
    ASSERT_OK(dbfull()->SetDBOptions(
        {{"stats_dump_period_sec", std::to_string(num)}}));
    ASSERT_EQ(num, dbfull()->GetDBOptions().stats_dump_period_sec);
  }
}

static void assert_candidate_files_empty(DBImpl* dbfull, const bool empty) {
  dbfull->TEST_LockMutex();
  JobContext job_context(0);
  dbfull->FindObsoleteFiles(&job_context, false);
  ASSERT_EQ(empty, job_context.full_scan_candidate_files.empty());
  job_context.Clean();
  dbfull->TEST_UnlockMutex();
}

TEST_F(DBOptionsTest, DeleteObsoleteFilesPeriodChange) {
  SpecialEnv env(env_);
  env.time_elapse_only_sleep_ = true;
  Options options;
  options.env = &env;
  options.create_if_missing = true;
  ASSERT_OK(TryReopen(options));

  // Verify that candidate files set is empty when no full scan requested.
  assert_candidate_files_empty(dbfull(), true);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"delete_obsolete_files_period_micros", "0"}}));

  // After delete_obsolete_files_period_micros updated to 0, the next call
  // to FindObsoleteFiles should make a full scan
  assert_candidate_files_empty(dbfull(), false);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"delete_obsolete_files_period_micros", "20"}}));

  assert_candidate_files_empty(dbfull(), true);

  env.addon_time_.store(20);
  assert_candidate_files_empty(dbfull(), true);

  env.addon_time_.store(21);
  assert_candidate_files_empty(dbfull(), false);

  Close();
}

TEST_F(DBOptionsTest, MaxOpenFilesChange) {
  SpecialEnv env(env_);
  Options options;
  options.env = CurrentOptions().env;
  options.max_open_files = -1;

  Reopen(options);

  Cache* tc = dbfull()->TEST_table_cache();

  ASSERT_EQ(-1, dbfull()->GetDBOptions().max_open_files);
  ASSERT_LT(2000, tc->GetCapacity());
  ASSERT_OK(dbfull()->SetDBOptions({{"max_open_files", "1024"}}));
  ASSERT_EQ(1024, dbfull()->GetDBOptions().max_open_files);
  // examine the table cache (actual size should be 1014)
  ASSERT_GT(1500, tc->GetCapacity());
  Close();
}

TEST_F(DBOptionsTest, SanitizeDelayedWriteRate) {
  Options options;
  options.delayed_write_rate = 0;
  Reopen(options);
  ASSERT_EQ(16 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);

  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);
  ASSERT_EQ(31 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
