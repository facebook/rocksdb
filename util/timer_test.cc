//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/timer.h"

#include "db/db_test_util.h"

namespace ROCKSDB_NAMESPACE {

class TimerTest : public testing::Test {
 public:
  TimerTest() : mock_env_(new MockTimeEnv(Env::Default())) {}

 protected:
  std::unique_ptr<MockTimeEnv> mock_env_;

  void SetUp() override { mock_env_->InstallTimedWaitFixCallback(); }

  const int kUsPerSec = 1000000;
};

TEST_F(TimerTest, SingleScheduleOnce) {
  const int kInitDelayUs = 1 * kUsPerSec;
  Timer timer(mock_env_.get());

  int count = 0;
  timer.Add([&] { count++; }, "fn_sch_test", kInitDelayUs, 0);

  ASSERT_TRUE(timer.Start());

  ASSERT_EQ(0, count);
  // Wait for execution to finish
  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kInitDelayUs); });
  ASSERT_EQ(1, count);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, MultipleScheduleOnce) {
  const int kInitDelay1Us = 1 * kUsPerSec;
  const int kInitDelay2Us = 3 * kUsPerSec;
  Timer timer(mock_env_.get());

  int count1 = 0;
  timer.Add([&] { count1++; }, "fn_sch_test1", kInitDelay1Us, 0);

  int count2 = 0;
  timer.Add([&] { count2++; }, "fn_sch_test2", kInitDelay2Us, 0);

  ASSERT_TRUE(timer.Start());
  ASSERT_EQ(0, count1);
  ASSERT_EQ(0, count2);

  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kInitDelay1Us); });

  ASSERT_EQ(1, count1);
  ASSERT_EQ(0, count2);

  timer.TEST_WaitForRun([&] {
    mock_env_->MockSleepForMicroseconds(kInitDelay2Us - kInitDelay1Us);
  });

  ASSERT_EQ(1, count1);
  ASSERT_EQ(1, count2);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, SingleScheduleRepeatedly) {
  const int kIterations = 5;
  const int kInitDelayUs = 1 * kUsPerSec;
  const int kRepeatUs = 1 * kUsPerSec;

  Timer timer(mock_env_.get());
  int count = 0;
  timer.Add([&] { count++; }, "fn_sch_test", kInitDelayUs, kRepeatUs);

  ASSERT_TRUE(timer.Start());
  ASSERT_EQ(0, count);

  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kInitDelayUs); });

  ASSERT_EQ(1, count);

  // Wait for execution to finish
  for (int i = 1; i < kIterations; i++) {
    timer.TEST_WaitForRun(
        [&] { mock_env_->MockSleepForMicroseconds(kRepeatUs); });
  }
  ASSERT_EQ(kIterations, count);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, MultipleScheduleRepeatedly) {
  const int kIterations = 5;
  const int kInitDelay1Us = 0 * kUsPerSec;
  const int kInitDelay2Us = 1 * kUsPerSec;
  const int kInitDelay3Us = 0 * kUsPerSec;
  const int kRepeatUs = 2 * kUsPerSec;
  const int kLargeRepeatUs = 100 * kUsPerSec;

  Timer timer(mock_env_.get());

  int count1 = 0;
  timer.Add([&] { count1++; }, "fn_sch_test1", kInitDelay1Us, kRepeatUs);

  int count2 = 0;
  timer.Add([&] { count2++; }, "fn_sch_test2", kInitDelay2Us, kRepeatUs);

  // Add a function with relatively large repeat interval
  int count3 = 0;
  timer.Add([&] { count3++; }, "fn_sch_test3", kInitDelay3Us, kLargeRepeatUs);

  ASSERT_TRUE(timer.Start());

  ASSERT_EQ(0, count2);
  // Wait for execution to finish
  for (int i = 1; i < kIterations * (kRepeatUs / kUsPerSec); i++) {
    timer.TEST_WaitForRun(
        [&] { mock_env_->MockSleepForMicroseconds(1 * kUsPerSec); });
    ASSERT_EQ((i + 2) / (kRepeatUs / kUsPerSec), count1);
    ASSERT_EQ((i + 1) / (kRepeatUs / kUsPerSec), count2);

    // large interval function should only run once (the first one).
    ASSERT_EQ(1, count3);
  }

  timer.Cancel("fn_sch_test1");

  // Wait for execution to finish
  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(1 * kUsPerSec); });
  ASSERT_EQ(kIterations, count1);
  ASSERT_EQ(kIterations, count2);
  ASSERT_EQ(1, count3);

  timer.Cancel("fn_sch_test2");

  ASSERT_EQ(kIterations, count1);
  ASSERT_EQ(kIterations, count2);

  // execute the long interval one
  timer.TEST_WaitForRun([&] {
    mock_env_->MockSleepForMicroseconds(
        kLargeRepeatUs - static_cast<int>(mock_env_->NowMicros()));
  });
  ASSERT_EQ(2, count3);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, AddAfterStartTest) {
  const int kIterations = 5;
  const int kInitDelayUs = 1 * kUsPerSec;
  const int kRepeatUs = 1 * kUsPerSec;

  // wait timer to run and then add a new job
  SyncPoint::GetInstance()->LoadDependency(
      {{"Timer::Run::Waiting", "TimerTest:AddAfterStartTest:1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Timer timer(mock_env_.get());

  ASSERT_TRUE(timer.Start());

  TEST_SYNC_POINT("TimerTest:AddAfterStartTest:1");
  int count = 0;
  timer.Add([&] { count++; }, "fn_sch_test", kInitDelayUs, kRepeatUs);
  ASSERT_EQ(0, count);
  // Wait for execution to finish
  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kInitDelayUs); });
  ASSERT_EQ(1, count);

  for (int i = 1; i < kIterations; i++) {
    timer.TEST_WaitForRun(
        [&] { mock_env_->MockSleepForMicroseconds(kRepeatUs); });
  }
  ASSERT_EQ(kIterations, count);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, CancelRunningTask) {
  const int kRepeatUs = 1 * kUsPerSec;
  constexpr char kTestFuncName[] = "test_func";
  Timer timer(mock_env_.get());
  ASSERT_TRUE(timer.Start());
  int* value = new int;
  *value = 0;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"TimerTest::CancelRunningTask:test_func:0",
       "TimerTest::CancelRunningTask:BeforeCancel"},
      {"Timer::WaitForTaskCompleteIfNecessary:TaskExecuting",
       "TimerTest::CancelRunningTask:test_func:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  timer.Add(
      [&]() {
        *value = 1;
        TEST_SYNC_POINT("TimerTest::CancelRunningTask:test_func:0");
        TEST_SYNC_POINT("TimerTest::CancelRunningTask:test_func:1");
      },
      kTestFuncName, 0, kRepeatUs);
  port::Thread control_thr([&]() {
    TEST_SYNC_POINT("TimerTest::CancelRunningTask:BeforeCancel");
    timer.Cancel(kTestFuncName);
    // Verify that *value has been set to 1.
    ASSERT_EQ(1, *value);
    delete value;
    value = nullptr;
  });
  mock_env_->MockSleepForMicroseconds(kRepeatUs);
  control_thr.join();
  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, ShutdownRunningTask) {
  const int kRepeatUs = 1 * kUsPerSec;
  constexpr char kTestFunc1Name[] = "test_func1";
  constexpr char kTestFunc2Name[] = "test_func2";
  Timer timer(mock_env_.get());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"TimerTest::ShutdownRunningTest:test_func:0",
       "TimerTest::ShutdownRunningTest:BeforeShutdown"},
      {"Timer::WaitForTaskCompleteIfNecessary:TaskExecuting",
       "TimerTest::ShutdownRunningTest:test_func:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(timer.Start());

  int* value = new int;
  *value = 0;
  timer.Add(
      [&]() {
        TEST_SYNC_POINT("TimerTest::ShutdownRunningTest:test_func:0");
        *value = 1;
        TEST_SYNC_POINT("TimerTest::ShutdownRunningTest:test_func:1");
      },
      kTestFunc1Name, 0, kRepeatUs);

  timer.Add([&]() { ++(*value); }, kTestFunc2Name, 0, kRepeatUs);

  port::Thread control_thr([&]() {
    TEST_SYNC_POINT("TimerTest::ShutdownRunningTest:BeforeShutdown");
    timer.Shutdown();
  });
  mock_env_->MockSleepForMicroseconds(kRepeatUs);
  control_thr.join();
  delete value;
}

TEST_F(TimerTest, AddSameFuncName) {
  const int kInitDelayUs = 1 * kUsPerSec;
  const int kRepeat1Us = 5 * kUsPerSec;
  const int kRepeat2Us = 4 * kUsPerSec;

  Timer timer(mock_env_.get());
  ASSERT_TRUE(timer.Start());

  int func_counter1 = 0;
  timer.Add([&] { func_counter1++; }, "duplicated_func", kInitDelayUs,
            kRepeat1Us);

  int func2_counter = 0;
  timer.Add([&] { func2_counter++; }, "func2", kInitDelayUs, kRepeat2Us);

  // New function with the same name should override the existing one
  int func_counter2 = 0;
  timer.Add([&] { func_counter2++; }, "duplicated_func", kInitDelayUs,
            kRepeat1Us);

  ASSERT_EQ(0, func_counter1);
  ASSERT_EQ(0, func2_counter);
  ASSERT_EQ(0, func_counter2);

  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kInitDelayUs); });

  ASSERT_EQ(0, func_counter1);
  ASSERT_EQ(1, func2_counter);
  ASSERT_EQ(1, func_counter2);

  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kRepeat1Us); });

  ASSERT_EQ(0, func_counter1);
  ASSERT_EQ(2, func2_counter);
  ASSERT_EQ(2, func_counter2);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, RepeatIntervalWithFuncRunningTime) {
  const int kInitDelayUs = 1 * kUsPerSec;
  const int kRepeatUs = 5 * kUsPerSec;
  const int kFuncRunningTimeUs = 1 * kUsPerSec;

  Timer timer(mock_env_.get());
  ASSERT_TRUE(timer.Start());

  int func_counter = 0;
  timer.Add(
      [&] {
        mock_env_->MockSleepForMicroseconds(kFuncRunningTimeUs);
        func_counter++;
      },
      "func", kInitDelayUs, kRepeatUs);

  ASSERT_EQ(0, func_counter);
  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kInitDelayUs); });
  ASSERT_EQ(1, func_counter);
  ASSERT_EQ(kInitDelayUs + kFuncRunningTimeUs, mock_env_->NowMicros());

  // After repeat interval time, the function is not executed, as running
  // the function takes some time (`kFuncRunningTimeSec`). The repeat interval
  // is the time between ending time of the last call and starting time of the
  // next call.
  uint64_t next_abs_interval_time_us = kInitDelayUs + kRepeatUs;
  timer.TEST_WaitForRun([&] {
    mock_env_->set_current_time(next_abs_interval_time_us / kUsPerSec);
  });
  ASSERT_EQ(1, func_counter);

  // After the function running time, it's executed again
  timer.TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kFuncRunningTimeUs); });
  ASSERT_EQ(2, func_counter);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, DestroyRunningTimer) {
  const int kInitDelayUs = 1 * kUsPerSec;
  const int kRepeatUs = 1 * kUsPerSec;

  auto timer_ptr = new Timer(mock_env_.get());

  int count = 0;
  timer_ptr->Add([&] { count++; }, "fn_sch_test", kInitDelayUs, kRepeatUs);
  ASSERT_TRUE(timer_ptr->Start());

  timer_ptr->TEST_WaitForRun(
      [&] { mock_env_->MockSleepForMicroseconds(kInitDelayUs); });

  // delete a running timer should not cause any exception
  delete timer_ptr;
}

TEST_F(TimerTest, DestroyTimerWithRunningFunc) {
  const int kRepeatUs = 1 * kUsPerSec;
  auto timer_ptr = new Timer(mock_env_.get());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"TimerTest::DestroyTimerWithRunningFunc:test_func:0",
       "TimerTest::DestroyTimerWithRunningFunc:BeforeDelete"},
      {"Timer::WaitForTaskCompleteIfNecessary:TaskExecuting",
       "TimerTest::DestroyTimerWithRunningFunc:test_func:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(timer_ptr->Start());

  int count = 0;
  timer_ptr->Add(
      [&]() {
        TEST_SYNC_POINT("TimerTest::DestroyTimerWithRunningFunc:test_func:0");
        count++;
        TEST_SYNC_POINT("TimerTest::DestroyTimerWithRunningFunc:test_func:1");
      },
      "fn_running_test", 0, kRepeatUs);

  port::Thread control_thr([&] {
    TEST_SYNC_POINT("TimerTest::DestroyTimerWithRunningFunc:BeforeDelete");
    delete timer_ptr;
  });
  mock_env_->MockSleepForMicroseconds(kRepeatUs);
  control_thr.join();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
