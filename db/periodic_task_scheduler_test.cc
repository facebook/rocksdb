//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_task_scheduler.h"

#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "test_util/mock_time_env.h"

namespace ROCKSDB_NAMESPACE {

class PeriodicTaskSchedulerTest : public DBTestBase {
 public:
  PeriodicTaskSchedulerTest()
      : DBTestBase("periodic_task_scheduler_test", /*env_do_fsync=*/true) {
    mock_clock_ = std::make_shared<MockSystemClock>(env_->GetSystemClock());
    mock_env_.reset(new CompositeEnvWrapper(env_, mock_clock_));
  }

 protected:
  std::unique_ptr<Env> mock_env_;
  std::shared_ptr<MockSystemClock> mock_clock_;

  void SetUp() override {
    mock_clock_->InstallTimedWaitFixCallback();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::StartPeriodicTaskScheduler:Init", [&](void* arg) {
          auto periodic_task_scheduler_ptr =
              static_cast<PeriodicTaskScheduler*>(arg);
          periodic_task_scheduler_ptr->TEST_OverrideTimer(mock_clock_.get());
        });
  }
};

TEST_F(PeriodicTaskSchedulerTest, Basic) {
  constexpr unsigned int kPeriodSec = 10;
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

  int trigger_compaction_counter = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::TriggerPeriodicCompaction:StartRunning",
      [&](void*) { trigger_compaction_counter++; });

  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  ASSERT_EQ(kPeriodSec, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(kPeriodSec, dbfull()->GetDBOptions().stats_persist_period_sec);

  ASSERT_GT(kPeriodSec, 1u);
  dbfull()->TEST_WaitForPeriodicTaskRun([&] {
    mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec) - 1);
  });

  const PeriodicTaskScheduler& scheduler =
      dbfull()->TEST_GetPeriodicTaskScheduler();
  ASSERT_EQ((int)PeriodicTaskType::kMax - 1, scheduler.TEST_GetValidTaskNum());

  ASSERT_EQ(1, dump_st_counter);
  ASSERT_EQ(1, pst_st_counter);
  ASSERT_EQ(1, flush_info_log_counter);

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });

  ASSERT_EQ(2, dump_st_counter);
  ASSERT_EQ(2, pst_st_counter);
  ASSERT_EQ(2, flush_info_log_counter);

  dbfull()->TEST_WaitForPeriodicTaskRun(
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
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(3, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(4, flush_info_log_counter);

  ASSERT_EQ(2u, scheduler.TEST_GetValidTaskNum());

  // Re-enable one task
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_dump_period_sec", "5"}}));
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(0u, dbfull()->GetDBOptions().stats_persist_period_sec);

  ASSERT_EQ(3, scheduler.TEST_GetValidTaskNum());

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(4, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(5, flush_info_log_counter);

  // With the new ComputeTriggerCompactionPeriod() logic, since
  // stats_dump_period_sec = stats_persist_period_sec = kPeriodSec, the trigger
  // compaction period is also kPeriodSec. Unlike other periodic tasks,
  // kTriggerCompaction uses run_immediately=false, so it doesn't fire on
  // initial registration. It fires after each period elapses.
  // By now we've had: initial 9s + 10s + 10s + 10s + 10s = 49s elapsed.
  // kTriggerCompaction fires at t=10, 20, 30, 40 = 4 times.
  ASSERT_EQ(4, trigger_compaction_counter);

  // Sleep one more period and verify it increments
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(5, trigger_compaction_counter);

  Close();
}

TEST_F(PeriodicTaskSchedulerTest, TriggerCompactionPeriodComputation) {
  // Test that ComputeTriggerCompactionPeriod() returns the expected values
  // for different configurations.
  Close();

  // Helper to capture the trigger compaction period on Reopen
  uint64_t trigger_compaction_period = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "PeriodicTaskScheduler::Register:TaskRegistered", [&](void* arg) {
        auto* task_info =
            static_cast<std::pair<PeriodicTaskType, uint64_t>*>(arg);
        if (task_info->first == PeriodicTaskType::kTriggerCompaction) {
          trigger_compaction_period = task_info->second;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto test_period = [&](Options& options, uint64_t expected_period) {
    options.create_if_missing = true;
    options.env = mock_env_.get();
    trigger_compaction_period = 0;
    Reopen(options);
    ASSERT_EQ(expected_period, trigger_compaction_period);
    Close();
  };

  // Case 1: 12-hour cap when stats_dump_period is longer
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;  // 24 hours
    options.stats_persist_period_sec = 0;
    test_period(options, 12 * 60 * 60);  // Expect 12 hours (cap)
  }

  // Case 2: periodic_compaction_seconds / kTriggerDivisor
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;  // 24 hours
    options.stats_persist_period_sec = 0;
    options.periodic_compaction_seconds = 500;  // 500 / 5 = 100
    test_period(options, 100);
  }
}

TEST_F(PeriodicTaskSchedulerTest, MultiInstances) {
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

  auto dbs = std::vector<std::unique_ptr<DB>>(kInstanceNum);
  for (int i = 0; i < kInstanceNum; i++) {
    ASSERT_OK(
        DB::Open(options, test::PerThreadDBPath(std::to_string(i)), &(dbs[i])));
  }

  auto dbi = static_cast_with_check<DBImpl>(dbs[kInstanceNum - 1].get());

  const PeriodicTaskScheduler& scheduler = dbi->TEST_GetPeriodicTaskScheduler();
  // kRecordSeqnoTime is not registered since the feature is not enabled
  ASSERT_EQ(kInstanceNum * ((int)PeriodicTaskType::kMax - 1),
            scheduler.TEST_GetValidTaskNum());

  int expected_run = kInstanceNum;
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec - 1); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  expected_run += kInstanceNum;
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  expected_run += kInstanceNum;
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  int half = kInstanceNum / 2;
  for (int i = 0; i < half; i++) {
    dbs[i].reset();
  }

  expected_run += (kInstanceNum - half) * 2;

  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  for (int i = half; i < kInstanceNum; i++) {
    ASSERT_OK(dbs[i]->Close());
    dbs[i].reset();
  }
}

TEST_F(PeriodicTaskSchedulerTest, MultiEnv) {
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
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options2, dbname, &db));

  ASSERT_OK(db->Close());
  db.reset();
  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
