//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_work_scheduler.h"

#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "test_util/mock_time_env.h"

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE
class PeriodicWorkSchedulerTest : public DBTestBase {
 public:
  PeriodicWorkSchedulerTest()
      : DBTestBase("periodic_work_scheduler_test", /*env_do_fsync=*/true) {
    mock_clock_ = std::make_shared<MockSystemClock>(env_->GetSystemClock());
    mock_env_.reset(new CompositeEnvWrapper(env_, mock_clock_));
  }

 protected:
  std::unique_ptr<Env> mock_env_;
  std::shared_ptr<MockSystemClock> mock_clock_;

  void SetUp() override {
    mock_clock_->InstallTimedWaitFixCallback();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::StartPeriodicWorkScheduler:Init", [&](void* arg) {
          auto* periodic_work_scheduler_ptr =
              reinterpret_cast<PeriodicWorkScheduler**>(arg);
          *periodic_work_scheduler_ptr =
              PeriodicWorkTestScheduler::Default(mock_clock_);
        });
  }
};

TEST_F(PeriodicWorkSchedulerTest, Basic) {
  constexpr unsigned int kPeriodSec =
      PeriodicWorkScheduler::kDefaultFlushInfoLogPeriodSec;
  Close();
  Options options;
  options.stats_dump_period_sec = kPeriodSec;
  options.stats_persist_period_sec = kPeriodSec;
  options.create_if_missing = true;
  options.env = mock_env_.get();

  int dump_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::DumpStats:StartRunning",
                                        [&](void*) { dump_st_counter++; });

  int pst_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::PersistStats:StartRunning",
                                        [&](void*) { pst_st_counter++; });

  int flush_info_log_counter = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushInfoLog:StartRunning",
      [&](void*) { flush_info_log_counter++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  ASSERT_EQ(kPeriodSec, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(kPeriodSec, dbfull()->GetDBOptions().stats_persist_period_sec);

  ASSERT_GT(kPeriodSec, 1u);
  dbfull()->TEST_WaitForStatsDumpRun([&] {
    mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec) - 1);
  });

  auto scheduler = dbfull()->TEST_GetPeriodicWorkScheduler();
  ASSERT_NE(nullptr, scheduler);
  ASSERT_EQ(3, scheduler->TEST_GetValidTaskNum());

  ASSERT_EQ(1, dump_st_counter);
  ASSERT_EQ(1, pst_st_counter);
  ASSERT_EQ(1, flush_info_log_counter);

  dbfull()->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });

  ASSERT_EQ(2, dump_st_counter);
  ASSERT_EQ(2, pst_st_counter);
  ASSERT_EQ(2, flush_info_log_counter);

  dbfull()->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });

  ASSERT_EQ(3, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(3, flush_info_log_counter);

  // Disable scheduler with SetOption
  ASSERT_OK(dbfull()->SetDBOptions(
      {{"stats_dump_period_sec", "0"}, {"stats_persist_period_sec", "0"}}));
  ASSERT_EQ(0u, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(0u, dbfull()->GetDBOptions().stats_persist_period_sec);

  // Info log flush should still run.
  dbfull()->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(3, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(4, flush_info_log_counter);

  scheduler = dbfull()->TEST_GetPeriodicWorkScheduler();
  ASSERT_EQ(1u, scheduler->TEST_GetValidTaskNum());

  // Re-enable one task
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_dump_period_sec", "5"}}));
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(0u, dbfull()->GetDBOptions().stats_persist_period_sec);

  scheduler = dbfull()->TEST_GetPeriodicWorkScheduler();
  ASSERT_NE(nullptr, scheduler);
  ASSERT_EQ(2, scheduler->TEST_GetValidTaskNum());

  dbfull()->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(4, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(5, flush_info_log_counter);

  Close();
}

TEST_F(PeriodicWorkSchedulerTest, MultiInstances) {
  constexpr int kPeriodSec = 5;
  const int kInstanceNum = 10;

  Close();
  Options options;
  options.stats_dump_period_sec = kPeriodSec;
  options.stats_persist_period_sec = kPeriodSec;
  options.create_if_missing = true;
  options.env = mock_env_.get();

  int dump_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::DumpStats:2",
                                        [&](void*) { dump_st_counter++; });

  int pst_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::PersistStats:StartRunning",
                                        [&](void*) { pst_st_counter++; });
  SyncPoint::GetInstance()->EnableProcessing();

  auto dbs = std::vector<DB*>(kInstanceNum);
  for (int i = 0; i < kInstanceNum; i++) {
    ASSERT_OK(
        DB::Open(options, test::PerThreadDBPath(std::to_string(i)), &(dbs[i])));
  }

  auto dbi = static_cast_with_check<DBImpl>(dbs[kInstanceNum - 1]);
  auto scheduler = dbi->TEST_GetPeriodicWorkScheduler();
  ASSERT_EQ(kInstanceNum * 3, scheduler->TEST_GetValidTaskNum());

  int expected_run = kInstanceNum;
  dbi->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec - 1); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  expected_run += kInstanceNum;
  dbi->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  expected_run += kInstanceNum;
  dbi->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  int half = kInstanceNum / 2;
  for (int i = 0; i < half; i++) {
    delete dbs[i];
  }

  expected_run += (kInstanceNum - half) * 2;

  dbi->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  dbi->TEST_WaitForStatsDumpRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  for (int i = half; i < kInstanceNum; i++) {
    ASSERT_OK(dbs[i]->Close());
    delete dbs[i];
  }
}

TEST_F(PeriodicWorkSchedulerTest, MultiEnv) {
  constexpr int kDumpPeriodSec = 5;
  constexpr int kPersistPeriodSec = 10;
  Close();
  Options options1;
  options1.stats_dump_period_sec = kDumpPeriodSec;
  options1.stats_persist_period_sec = kPersistPeriodSec;
  options1.create_if_missing = true;
  options1.env = mock_env_.get();

  Reopen(options1);

  std::unique_ptr<Env> mock_env2(
      new CompositeEnvWrapper(Env::Default(), mock_clock_));
  Options options2;
  options2.stats_dump_period_sec = kDumpPeriodSec;
  options2.stats_persist_period_sec = kPersistPeriodSec;
  options2.create_if_missing = true;
  options1.env = mock_env2.get();

  std::string dbname = test::PerThreadDBPath("multi_env_test");
  DB* db;
  ASSERT_OK(DB::Open(options2, dbname, &db));
  DBImpl* dbi = static_cast_with_check<DBImpl>(db);

  ASSERT_EQ(dbi->TEST_GetPeriodicWorkScheduler(),
            dbfull()->TEST_GetPeriodicWorkScheduler());

  ASSERT_OK(db->Close());
  delete db;
  Close();
}
#endif  // !ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
