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
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "monitoring/persistent_stats_history.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/stats_history.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class StatsHistoryTest : public DBTestBase {
 public:
  StatsHistoryTest() : DBTestBase("/stats_history_test") {}
};
#ifndef ROCKSDB_LITE

TEST_F(StatsHistoryTest, RunStatsDumpPeriodSec) {
  Options options;
  options.create_if_missing = true;
  options.stats_dump_period_sec = 5;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  int counter = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
#if defined(OS_MACOSX) && !defined(NDEBUG)
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        uint64_t time_us = *reinterpret_cast<uint64_t*>(arg);
        if (time_us < mock_env->RealNowMicros()) {
          *reinterpret_cast<uint64_t*>(arg) = mock_env->RealNowMicros() + 1000;
        }
      });
#endif  // OS_MACOSX && !NDEBUG
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DumpStats:1", [&](void* /*arg*/) { counter++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_dump_period_sec);
  dbfull()->TEST_WaitForDumpStatsRun([&] { mock_env->set_current_time(5); });
  ASSERT_GE(counter, 1);

  // Test cacel job through SetOptions
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_dump_period_sec", "0"}}));
  int old_val = counter;
  for (int i = 6; i < 20; ++i) {
    dbfull()->TEST_WaitForDumpStatsRun([&] { mock_env->set_current_time(i); });
  }
  ASSERT_EQ(counter, old_val);
  Close();
}

// Test persistent stats background thread scheduling and cancelling
TEST_F(StatsHistoryTest, StatsPersistScheduling) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
#if defined(OS_MACOSX) && !defined(NDEBUG)
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        uint64_t time_us = *reinterpret_cast<uint64_t*>(arg);
        if (time_us < mock_env->RealNowMicros()) {
          *reinterpret_cast<uint64_t*>(arg) = mock_env->RealNowMicros() + 1000;
        }
      });
#endif  // OS_MACOSX && !NDEBUG
  int counter = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistStats:Entry", [&](void* /*arg*/) { counter++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_persist_period_sec);
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  ASSERT_GE(counter, 1);

  // Test cacel job through SetOptions
  ASSERT_TRUE(dbfull()->TEST_IsPersistentStatsEnabled());
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "0"}}));
  ASSERT_FALSE(dbfull()->TEST_IsPersistentStatsEnabled());
  Close();
}

// Test enabling persistent stats for the first time
TEST_F(StatsHistoryTest, PersistentStatsFreshInstall) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 0;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
#if defined(OS_MACOSX) && !defined(NDEBUG)
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        uint64_t time_us = *reinterpret_cast<uint64_t*>(arg);
        if (time_us < mock_env->RealNowMicros()) {
          *reinterpret_cast<uint64_t*>(arg) = mock_env->RealNowMicros() + 1000;
        }
      });
#endif  // OS_MACOSX && !NDEBUG
  int counter = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistStats:Entry", [&](void* /*arg*/) { counter++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "5"}}));
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_persist_period_sec);
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  ASSERT_GE(counter, 1);
  Close();
}

// TODO(Zhongyi): Move persistent stats related tests to a separate file
TEST_F(StatsHistoryTest, GetStatsHistoryInMemory) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
#if defined(OS_MACOSX) && !defined(NDEBUG)
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        uint64_t time_us = *reinterpret_cast<uint64_t*>(arg);
        if (time_us < mock_env->RealNowMicros()) {
          *reinterpret_cast<uint64_t*>(arg) = mock_env->RealNowMicros() + 1000;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
#endif  // OS_MACOSX && !NDEBUG

  CreateColumnFamilies({"pikachu"}, options);
  ASSERT_OK(Put("foo", "bar"));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  int mock_time = 1;
  // Wait for stats persist to finish
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  db_->GetStatsHistory(0 /*start_time*/, 6 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  // disabled stats snapshots
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "0"}}));
  size_t stats_count = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    auto stats_map = stats_iter->GetStatsMap();
    ASSERT_EQ(stats_iter->GetStatsTime(), 5);
    stats_count += stats_map.size();
  }
  ASSERT_GT(stats_count, 0);
  // Wait a bit and verify no more stats are found
  for (mock_time = 6; mock_time < 20; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }
  db_->GetStatsHistory(0 /*start_time*/, 20 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count_new = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    stats_count_new += stats_iter->GetStatsMap().size();
  }
  ASSERT_EQ(stats_count_new, stats_count);
  Close();
}

TEST_F(StatsHistoryTest, InMemoryStatsHistoryPurging) {
  Options options;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.stats_persist_period_sec = 1;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
#if defined(OS_MACOSX) && !defined(NDEBUG)
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        uint64_t time_us = *reinterpret_cast<uint64_t*>(arg);
        if (time_us < mock_env->RealNowMicros()) {
          *reinterpret_cast<uint64_t*>(arg) = mock_env->RealNowMicros() + 1000;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
#endif  // OS_MACOSX && !NDEBUG

  CreateColumnFamilies({"pikachu"}, options);
  ASSERT_OK(Put("foo", "bar"));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  // some random operation to populate statistics
  ASSERT_OK(Delete("foo"));
  ASSERT_OK(Put("sol", "sol"));
  ASSERT_OK(Put("epic", "epic"));
  ASSERT_OK(Put("ltd", "ltd"));
  ASSERT_EQ("sol", Get("sol"));
  ASSERT_EQ("epic", Get("epic"));
  ASSERT_EQ("ltd", Get("ltd"));
  Iterator* iterator = db_->NewIterator(ReadOptions());
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    ASSERT_TRUE(iterator->key() == iterator->value());
  }
  delete iterator;
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("sol"));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  int mock_time = 1;
  // Wait for stats persist to finish
  for (; mock_time < 5; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }

  // second round of ops
  ASSERT_OK(Put("saigon", "saigon"));
  ASSERT_OK(Put("noodle talk", "noodle talk"));
  ASSERT_OK(Put("ping bistro", "ping bistro"));
  iterator = db_->NewIterator(ReadOptions());
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    ASSERT_TRUE(iterator->key() == iterator->value());
  }
  delete iterator;
  ASSERT_OK(Flush());
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  for (; mock_time < 10; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  db_->GetStatsHistory(0 /*start_time*/, 10 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count = 0;
  int slice_count = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    slice_count++;
    auto stats_map = stats_iter->GetStatsMap();
    stats_count += stats_map.size();
  }
  size_t stats_history_size = dbfull()->TEST_EstimateInMemoryStatsHistorySize();
  ASSERT_GE(slice_count, 9);
  ASSERT_GE(stats_history_size, 12000);
  // capping memory cost at 12000 bytes since one slice is around 10000~12000
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_history_buffer_size", "12000"}}));
  ASSERT_EQ(12000, dbfull()->GetDBOptions().stats_history_buffer_size);
  // Wait for stats persist to finish
  for (; mock_time < 20; ++mock_time) {
    dbfull()->TEST_WaitForPersistStatsRun(
        [&] { mock_env->set_current_time(mock_time); });
  }
  db_->GetStatsHistory(0 /*start_time*/, 20 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count_reopen = 0;
  slice_count = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    slice_count++;
    auto stats_map = stats_iter->GetStatsMap();
    stats_count_reopen += stats_map.size();
  }
  size_t stats_history_size_reopen =
      dbfull()->TEST_EstimateInMemoryStatsHistorySize();
  // only one slice can fit under the new stats_history_buffer_size
  ASSERT_LT(slice_count, 2);
  ASSERT_TRUE(stats_history_size_reopen < 12000 &&
              stats_history_size_reopen > 0);
  ASSERT_TRUE(stats_count_reopen < stats_count && stats_count_reopen > 0);
  Close();
  // TODO: may also want to verify stats timestamp to make sure we are purging
  // the correct stats snapshot
}

int countkeys(Iterator* iter) {
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    count++;
  }
  return count;
}

TEST_F(StatsHistoryTest, GetStatsHistoryFromDisk) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.persist_stats_to_disk = true;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  CreateColumnFamilies({"pikachu"}, options);
  ASSERT_OK(Put("foo", "bar"));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(Get("foo"), "bar");

  // Wait for stats persist to finish
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  auto iter =
      db_->NewIterator(ReadOptions(), dbfull()->PersistentStatsColumnFamily());
  int key_count1 = countkeys(iter);
  delete iter;
  dbfull()->TEST_WaitForPersistStatsRun(
      [&] { mock_env->set_current_time(10); });
  iter =
      db_->NewIterator(ReadOptions(), dbfull()->PersistentStatsColumnFamily());
  int key_count2 = countkeys(iter);
  delete iter;
  dbfull()->TEST_WaitForPersistStatsRun(
      [&] { mock_env->set_current_time(15); });
  iter =
      db_->NewIterator(ReadOptions(), dbfull()->PersistentStatsColumnFamily());
  int key_count3 = countkeys(iter);
  delete iter;
  ASSERT_GE(key_count2, key_count1);
  ASSERT_GE(key_count3, key_count2);
  ASSERT_EQ(key_count3 - key_count2, key_count2 - key_count1);
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  db_->GetStatsHistory(0 /*start_time*/, 16 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count = 0;
  int slice_count = 0;
  int non_zero_count = 0;
  for (int i = 1; stats_iter->Valid(); stats_iter->Next(), i++) {
    slice_count++;
    auto stats_map = stats_iter->GetStatsMap();
    ASSERT_EQ(stats_iter->GetStatsTime(), 5 * i);
    for (auto& stat : stats_map) {
      if (stat.second != 0) {
        non_zero_count++;
      }
    }
    stats_count += stats_map.size();
  }
  ASSERT_EQ(slice_count, 3);
  // 2 extra keys for format version
  ASSERT_EQ(stats_count, key_count3 - 2);
  // verify reopen will not cause data loss
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  db_->GetStatsHistory(0 /*start_time*/, 16 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  size_t stats_count_reopen = 0;
  int slice_count_reopen = 0;
  int non_zero_count_recover = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    slice_count_reopen++;
    auto stats_map = stats_iter->GetStatsMap();
    for (auto& stat : stats_map) {
      if (stat.second != 0) {
        non_zero_count_recover++;
      }
    }
    stats_count_reopen += stats_map.size();
  }
  ASSERT_EQ(non_zero_count, non_zero_count_recover);
  ASSERT_EQ(slice_count, slice_count_reopen);
  ASSERT_EQ(stats_count, stats_count_reopen);
  Close();
}

// Test persisted stats matches the value found in options.statistics and
// the stats value retains after DB reopen
TEST_F(StatsHistoryTest, PersitentStatsVerifyValue) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.persist_stats_to_disk = true;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  std::map<std::string, uint64_t> stats_map_before;
  ASSERT_TRUE(options.statistics->getTickerMap(&stats_map_before));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  CreateColumnFamilies({"pikachu"}, options);
  ASSERT_OK(Put("foo", "bar"));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(Get("foo"), "bar");

  // Wait for stats persist to finish
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  auto iter =
      db_->NewIterator(ReadOptions(), dbfull()->PersistentStatsColumnFamily());
  countkeys(iter);
  delete iter;
  dbfull()->TEST_WaitForPersistStatsRun(
      [&] { mock_env->set_current_time(10); });
  iter =
      db_->NewIterator(ReadOptions(), dbfull()->PersistentStatsColumnFamily());
  countkeys(iter);
  delete iter;
  dbfull()->TEST_WaitForPersistStatsRun(
      [&] { mock_env->set_current_time(15); });
  iter =
      db_->NewIterator(ReadOptions(), dbfull()->PersistentStatsColumnFamily());
  countkeys(iter);
  delete iter;
  dbfull()->TEST_WaitForPersistStatsRun(
      [&] { mock_env->set_current_time(20); });

  std::map<std::string, uint64_t> stats_map_after;
  ASSERT_TRUE(options.statistics->getTickerMap(&stats_map_after));
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  db_->GetStatsHistory(0 /*start_time*/, 21 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  std::string sample = "rocksdb.num.iterator.deleted";
  uint64_t recovered_value = 0;
  for (int i = 1; stats_iter->Valid(); stats_iter->Next(), ++i) {
    auto stats_map = stats_iter->GetStatsMap();
    ASSERT_EQ(stats_iter->GetStatsTime(), 5 * i);
    for (const auto& stat : stats_map) {
      if (sample.compare(stat.first) == 0) {
        recovered_value += stat.second;
      }
    }
  }
  ASSERT_EQ(recovered_value, stats_map_after[sample]);

  // test stats value retains after recovery
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  db_->GetStatsHistory(0 /*start_time*/, 21 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  uint64_t new_recovered_value = 0;
  for (int i = 1; stats_iter->Valid(); stats_iter->Next(), i++) {
    auto stats_map = stats_iter->GetStatsMap();
    ASSERT_EQ(stats_iter->GetStatsTime(), 5 * i);
    for (const auto& stat : stats_map) {
      if (sample.compare(stat.first) == 0) {
        new_recovered_value += stat.second;
      }
    }
  }
  ASSERT_EQ(recovered_value, new_recovered_value);

  // TODO(Zhongyi): also add test to read raw values from disk and verify
  // correctness
  Close();
}

// TODO(Zhongyi): add test for different format versions

TEST_F(StatsHistoryTest, PersistentStatsCreateColumnFamilies) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.persist_stats_to_disk = true;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  ASSERT_OK(TryReopen(options));
  CreateColumnFamilies({"one", "two", "three"}, options);
  ASSERT_OK(Put(1, "foo", "bar"));
  ReopenWithColumnFamilies({"default", "one", "two", "three"}, options);
  ASSERT_EQ(Get(2, "foo"), "bar");
  CreateColumnFamilies({"four"}, options);
  ReopenWithColumnFamilies({"default", "one", "two", "three", "four"}, options);
  ASSERT_EQ(Get(2, "foo"), "bar");
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  auto iter =
      db_->NewIterator(ReadOptions(), dbfull()->PersistentStatsColumnFamily());
  int key_count = countkeys(iter);
  delete iter;
  ASSERT_GE(key_count, 0);
  uint64_t num_write_wal = 0;
  std::string sample = "rocksdb.write.wal";
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  db_->GetStatsHistory(0 /*start_time*/, 5 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  for (; stats_iter->Valid(); stats_iter->Next()) {
    auto stats_map = stats_iter->GetStatsMap();
    for (const auto& stat : stats_map) {
      if (sample.compare(stat.first) == 0) {
        num_write_wal += stat.second;
      }
    }
  }
  stats_iter.reset();
  ASSERT_EQ(num_write_wal, 2);

  options.persist_stats_to_disk = false;
  ReopenWithColumnFamilies({"default", "one", "two", "three", "four"}, options);
  int cf_count = 0;
  for (auto cfd : *dbfull()->versions_->GetColumnFamilySet()) {
    (void)cfd;
    cf_count++;
  }
  // persistent stats cf will be implicitly opened even if
  // persist_stats_to_disk is false
  ASSERT_EQ(cf_count, 6);
  ASSERT_EQ(Get(2, "foo"), "bar");

  // attempt to create column family using same name, should fail
  ColumnFamilyOptions cf_opts(options);
  ColumnFamilyHandle* handle;
  ASSERT_NOK(db_->CreateColumnFamily(cf_opts, kPersistentStatsColumnFamilyName,
                                     &handle));

  options.persist_stats_to_disk = true;
  ReopenWithColumnFamilies({"default", "one", "two", "three", "four"}, options);
  ASSERT_NOK(db_->CreateColumnFamily(cf_opts, kPersistentStatsColumnFamilyName,
                                     &handle));
  // verify stats is not affected by prior failed CF creation
  db_->GetStatsHistory(0 /*start_time*/, 5 /*end_time*/, &stats_iter);
  ASSERT_TRUE(stats_iter != nullptr);
  num_write_wal = 0;
  for (; stats_iter->Valid(); stats_iter->Next()) {
    auto stats_map = stats_iter->GetStatsMap();
    for (const auto& stat : stats_map) {
      if (sample.compare(stat.first) == 0) {
        num_write_wal += stat.second;
      }
    }
  }
  ASSERT_EQ(num_write_wal, 2);

  Close();
  Destroy(options);
}

TEST_F(StatsHistoryTest, PersistentStatsReadOnly) {
  ASSERT_OK(Put("bar", "v2"));
  Close();

  auto options = CurrentOptions();
  options.stats_persist_period_sec = 5;
  options.persist_stats_to_disk = true;
  assert(options.env == env_);
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v2", Get("bar"));
  Close();

  // Reopen and flush memtable.
  ASSERT_OK(TryReopen(options));
  Flush();
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(options));
}

TEST_F(StatsHistoryTest, ForceManualFlushStatsCF) {
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 1024 * 1024 * 10;  // 10 Mb
  options.stats_persist_period_sec = 5;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.persist_stats_to_disk = true;
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env;
  mock_env.reset(new ROCKSDB_NAMESPACE::MockTimeEnv(env_));
  mock_env->set_current_time(0);  // in seconds
  options.env = mock_env.get();
  CreateColumnFamilies({"pikachu"}, options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ColumnFamilyData* cfd_default =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily())
          ->cfd();
  ColumnFamilyData* cfd_stats = static_cast<ColumnFamilyHandleImpl*>(
                                    dbfull()->PersistentStatsColumnFamily())
                                    ->cfd();
  ColumnFamilyData* cfd_test =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();

  ASSERT_OK(Put("foo", "v0"));
  ASSERT_OK(Put("bar", "v0"));
  ASSERT_EQ("v0", Get("bar"));
  ASSERT_EQ("v0", Get("foo"));
  ASSERT_OK(Put(1, "Eevee", "v0"));
  ASSERT_EQ("v0", Get(1, "Eevee"));
  dbfull()->TEST_WaitForPersistStatsRun([&] { mock_env->set_current_time(5); });
  // writing to all three cf, flush default cf
  // LogNumbers: default: 14, stats: 4, pikachu: 4
  ASSERT_OK(Flush());
  ASSERT_EQ(cfd_stats->GetLogNumber(), cfd_test->GetLogNumber());
  ASSERT_LT(cfd_stats->GetLogNumber(), cfd_default->GetLogNumber());

  ASSERT_OK(Put("foo1", "v1"));
  ASSERT_OK(Put("bar1", "v1"));
  ASSERT_EQ("v1", Get("bar1"));
  ASSERT_EQ("v1", Get("foo1"));
  ASSERT_OK(Put(1, "Vaporeon", "v1"));
  ASSERT_EQ("v1", Get(1, "Vaporeon"));
  // writing to default and test cf, flush test cf
  // LogNumbers: default: 14, stats: 16, pikachu: 16
  ASSERT_OK(Flush(1));
  ASSERT_EQ(cfd_stats->GetLogNumber(), cfd_test->GetLogNumber());
  ASSERT_GT(cfd_stats->GetLogNumber(), cfd_default->GetLogNumber());

  ASSERT_OK(Put("foo2", "v2"));
  ASSERT_OK(Put("bar2", "v2"));
  ASSERT_EQ("v2", Get("bar2"));
  ASSERT_EQ("v2", Get("foo2"));
  dbfull()->TEST_WaitForPersistStatsRun(
      [&] { mock_env->set_current_time(10); });
  // writing to default and stats cf, flushing default cf
  // LogNumbers: default: 19, stats: 19, pikachu: 19
  ASSERT_OK(Flush());
  ASSERT_EQ(cfd_stats->GetLogNumber(), cfd_test->GetLogNumber());
  ASSERT_EQ(cfd_stats->GetLogNumber(), cfd_default->GetLogNumber());

  ASSERT_OK(Put("foo3", "v3"));
  ASSERT_OK(Put("bar3", "v3"));
  ASSERT_EQ("v3", Get("bar3"));
  ASSERT_EQ("v3", Get("foo3"));
  ASSERT_OK(Put(1, "Jolteon", "v3"));
  ASSERT_EQ("v3", Get(1, "Jolteon"));
  dbfull()->TEST_WaitForPersistStatsRun(
      [&] { mock_env->set_current_time(15); });
  // writing to all three cf, flushing test cf
  // LogNumbers: default: 19, stats: 19, pikachu: 22
  ASSERT_OK(Flush(1));
  ASSERT_LT(cfd_stats->GetLogNumber(), cfd_test->GetLogNumber());
  ASSERT_EQ(cfd_stats->GetLogNumber(), cfd_default->GetLogNumber());
  Close();
}

#endif  // !ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
