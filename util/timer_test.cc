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
  const uint64_t kSecond = 1000000;  // 1sec = 1000000us

  void SetUp() override { mock_env_->InstallTimedWaitFixCallback(); }
};

TEST_F(TimerTest, SingleScheduleOnceTest) {
  const int kInitDelaySec = 1;
  int mock_time_sec = 0;
  mock_env_->set_current_time(mock_time_sec);
  Timer timer(mock_env_.get());

  int count = 0;
  timer.Add([&] { count++; }, "fn_sch_test", kInitDelaySec * kSecond, 0);

  ASSERT_TRUE(timer.Start());

  ASSERT_EQ(0, count);
  // Wait for execution to finish
  mock_time_sec += kInitDelaySec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  ASSERT_EQ(1, count);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, MultipleScheduleOnceTest) {
  const int kInitDelay1Sec = 1;
  const int kInitDelay2Sec = 3;
  int mock_time_sec = 0;
  mock_env_->set_current_time(mock_time_sec);
  Timer timer(mock_env_.get());

  int count1 = 0;
  timer.Add([&] { count1++; }, "fn_sch_test1", kInitDelay1Sec * kSecond, 0);

  int count2 = 0;
  timer.Add([&] { count2++; }, "fn_sch_test2", kInitDelay2Sec * kSecond, 0);

  ASSERT_TRUE(timer.Start());
  ASSERT_EQ(0, count1);
  ASSERT_EQ(0, count2);

  mock_time_sec = kInitDelay1Sec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });

  ASSERT_EQ(1, count1);
  ASSERT_EQ(0, count2);

  mock_time_sec = kInitDelay2Sec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });

  ASSERT_EQ(1, count1);
  ASSERT_EQ(1, count2);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, SingleScheduleRepeatedlyTest) {
  const int kIterations = 5;
  const int kInitDelaySec = 1;
  const int kRepeatSec = 1;
  int mock_time_sec = 0;
  mock_env_->set_current_time(mock_time_sec);

  Timer timer(mock_env_.get());
  int count = 0;
  timer.Add([&] { count++; }, "fn_sch_test", kInitDelaySec * kSecond,
            kRepeatSec * kSecond);

  ASSERT_TRUE(timer.Start());
  ASSERT_EQ(0, count);

  mock_time_sec += kInitDelaySec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });

  ASSERT_EQ(1, count);

  // Wait for execution to finish
  for (int i = 1; i < kIterations; i++) {
    mock_time_sec += kRepeatSec;
    timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  }
  ASSERT_EQ(kIterations, count);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, MultipleScheduleRepeatedlyTest) {
  const int kInitDelay1Sec = 0;
  const int kInitDelay2Sec = 1;
  const int kInitDelay3Sec = 0;
  const int kRepeatSec = 2;
  const int kLargeRepeatSec = 100;
  const int kIterations = 5;

  int mock_time_sec = 0;
  mock_env_->set_current_time(mock_time_sec);
  Timer timer(mock_env_.get());

  int count1 = 0;
  timer.Add([&] { count1++; }, "fn_sch_test1", kInitDelay1Sec * kSecond,
            kRepeatSec * kSecond);

  int count2 = 0;
  timer.Add([&] { count2++; }, "fn_sch_test2", kInitDelay2Sec * kSecond,
            kRepeatSec * kSecond);

  // Add a function with relatively large repeat interval
  int count3 = 0;
  timer.Add([&] { count3++; }, "fn_sch_test3", kInitDelay3Sec * kSecond,
            kLargeRepeatSec * kSecond);

  ASSERT_TRUE(timer.Start());

  ASSERT_EQ(0, count2);
  ASSERT_EQ(0, count3);
  // Wait for execution to finish
  for (; count1 < kIterations; mock_time_sec++) {
    timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
    ASSERT_EQ((mock_time_sec + 2) / kRepeatSec, count1);
    ASSERT_EQ((mock_time_sec + 1) / kRepeatSec, count2);

    // large interval function should only run once (the first one).
    ASSERT_EQ(1, count3);
  }

  timer.Cancel("fn_sch_test1");

  // Wait for execution to finish
  mock_time_sec++;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  ASSERT_EQ(kIterations, count1);
  ASSERT_EQ(kIterations, count2);
  ASSERT_EQ(1, count3);

  timer.Cancel("fn_sch_test2");

  ASSERT_EQ(kIterations, count1);
  ASSERT_EQ(kIterations, count2);

  // execute the long interval one
  mock_time_sec = kLargeRepeatSec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  ASSERT_EQ(2, count3);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, AddAfterStartTest) {
  const int kIterations = 5;
  const int kInitDelaySec = 1;
  const int kRepeatSec = 1;

  // wait timer to run and then add a new job
  SyncPoint::GetInstance()->LoadDependency(
      {{"Timer::Run::Waiting", "TimerTest:AddAfterStartTest:1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  int mock_time_sec = 0;
  mock_env_->set_current_time(mock_time_sec);
  Timer timer(mock_env_.get());

  ASSERT_TRUE(timer.Start());

  TEST_SYNC_POINT("TimerTest:AddAfterStartTest:1");
  int count = 0;
  timer.Add([&] { count++; }, "fn_sch_test", kInitDelaySec * kSecond,
            kRepeatSec * kSecond);
  ASSERT_EQ(0, count);
  // Wait for execution to finish
  mock_time_sec += kInitDelaySec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  ASSERT_EQ(1, count);

  for (int i = 1; i < kIterations; i++) {
    mock_time_sec += kRepeatSec;
    timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  }
  ASSERT_EQ(kIterations, count);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, CancelRunningTask) {
  constexpr char kTestFuncName[] = "test_func";
  mock_env_->set_current_time(0);
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
      kTestFuncName, 0, 1 * kSecond);
  port::Thread control_thr([&]() {
    TEST_SYNC_POINT("TimerTest::CancelRunningTask:BeforeCancel");
    timer.Cancel(kTestFuncName);
    // Verify that *value has been set to 1.
    ASSERT_EQ(1, *value);
    delete value;
    value = nullptr;
  });
  mock_env_->set_current_time(1);
  control_thr.join();
  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, ShutdownRunningTask) {
  constexpr char kTestFunc1Name[] = "test_func1";
  constexpr char kTestFunc2Name[] = "test_func2";
  mock_env_->set_current_time(0);
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
      kTestFunc1Name, 0, 1 * kSecond);

  timer.Add([&]() { ++(*value); }, kTestFunc2Name, 0, 1 * kSecond);

  port::Thread control_thr([&]() {
    TEST_SYNC_POINT("TimerTest::ShutdownRunningTest:BeforeShutdown");
    timer.Shutdown();
  });
  mock_env_->set_current_time(1);
  control_thr.join();
  delete value;
}

TEST_F(TimerTest, AddSameFuncName) {
  const int kInitDelaySec = 1;
  const int kRepeat1Sec = 5;
  const int kRepeat2Sec = 4;

  int mock_time_sec = 0;
  mock_env_->set_current_time(mock_time_sec);
  Timer timer(mock_env_.get());

  ASSERT_TRUE(timer.Start());

  int func_counter1 = 0;
  timer.Add([&] { func_counter1++; }, "duplicated_func",
            kInitDelaySec * kSecond, kRepeat1Sec * kSecond);

  int func2_counter = 0;
  timer.Add([&] { func2_counter++; }, "func2", kInitDelaySec * kSecond,
            kRepeat2Sec * kSecond);

  // New function with the same name should override the existing one
  int func_counter2 = 0;
  timer.Add([&] { func_counter2++; }, "duplicated_func",
            kInitDelaySec * kSecond, kRepeat1Sec * kSecond);

  ASSERT_EQ(0, func_counter1);
  ASSERT_EQ(0, func2_counter);
  ASSERT_EQ(0, func_counter2);

  mock_time_sec += kInitDelaySec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });

  ASSERT_EQ(0, func_counter1);
  ASSERT_EQ(1, func2_counter);
  ASSERT_EQ(1, func_counter2);

  mock_time_sec += kRepeat1Sec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });

  ASSERT_EQ(0, func_counter1);
  ASSERT_EQ(2, func2_counter);
  ASSERT_EQ(2, func_counter2);

  ASSERT_TRUE(timer.Shutdown());
}

TEST_F(TimerTest, RepeatIntervalWithFuncRunningTime) {
  const int kInitDelaySec = 1;
  const int kRepeatSec = 5;
  const int kFuncRunningTimeSec = 1;

  int mock_time_sec = 0;
  mock_env_->set_current_time(mock_time_sec);
  Timer timer(mock_env_.get());

  ASSERT_TRUE(timer.Start());

  int func_counter = 0;
  timer.Add(
      [&] {
        mock_env_->set_current_time(mock_time_sec + kFuncRunningTimeSec);
        func_counter++;
      },
      "func", kInitDelaySec * kSecond, kRepeatSec * kSecond);

  ASSERT_EQ(0, func_counter);
  mock_time_sec += kInitDelaySec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  ASSERT_EQ(1, func_counter);

  // After repeat interval time, the function is not executed, as running
  // the function takes some time (`kFuncRunningTimeSec`). The repeat interval
  // is the time between ending time of the last call and starting time of the
  // next call.
  mock_time_sec += kRepeatSec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  ASSERT_EQ(1, func_counter);

  mock_time_sec += kFuncRunningTimeSec;
  timer.TEST_WaitForRun([&] { mock_env_->set_current_time(mock_time_sec); });
  ASSERT_EQ(2, func_counter);

  ASSERT_TRUE(timer.Shutdown());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
