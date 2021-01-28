// Copyright (c) 2017 Rockset

#include "cloud/cloud_scheduler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class CloudSchedulerTest : public testing::Test {
 public:
  CloudSchedulerTest() { scheduler_ = CloudScheduler::Get(); }
  ~CloudSchedulerTest() {}

  std::shared_ptr<CloudScheduler> scheduler_;
};

// This test tests basic scheduling function. There are 2 jobs, job2 is
// scheduled before job1. We check that job2 is actually run before job1.
TEST_F(CloudSchedulerTest, TestSchedule) {
  std::chrono::steady_clock::time_point p1;
  std::chrono::steady_clock::time_point p2;

  auto job1 = [&p1](void *) { p1 = std::chrono::steady_clock::now(); };
  auto job2 = [&p2](void *) { p2 = std::chrono::steady_clock::now(); };

  scheduler_->ScheduleJob(std::chrono::milliseconds(300), job1, nullptr);
  scheduler_->ScheduleJob(std::chrono::milliseconds(100), job2, nullptr);

  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  ASSERT_LT(p2, p1);
}

TEST_F(CloudSchedulerTest, TestCancel) {
  static int job1 = 1;
  static int job2 = 0;
  auto doJob = [](void *arg) { (*(reinterpret_cast<int *>(arg)))++; };

  auto handle1 =
      scheduler_->ScheduleJob(std::chrono::microseconds(100), doJob, &job1);
  auto handle2 =
      scheduler_->ScheduleJob(std::chrono::microseconds(200), doJob, &job2);
  ASSERT_TRUE(scheduler_->CancelJob(handle2));
  usleep(300);
  ASSERT_EQ(job1, 2);
  ASSERT_EQ(job2, 0);
  ASSERT_FALSE(scheduler_->CancelJob(handle1));
  ASSERT_FALSE(scheduler_->CancelJob(handle2));
}

TEST_F(CloudSchedulerTest, TestRecurring) {
  int job1 = 1;
  int job2 = 1;
  auto doJob1 = [&job1](void *) { job1++; };
  auto doJob2 = [&job2](void *) { job2++; };

  auto handle1 = scheduler_->ScheduleRecurringJob(std::chrono::microseconds(10),
                                                  std::chrono::microseconds(50),
                                                  doJob1, nullptr);
  scheduler_->ScheduleRecurringJob(std::chrono::microseconds(120),
                                   std::chrono::microseconds(100), doJob2,
                                   nullptr);
  usleep(700);
  ASSERT_GE(job2, 4);
  ASSERT_GT(job1, job2);
  ASSERT_TRUE(scheduler_->CancelJob(handle1));
  auto old1 = job1;
  auto old2 = job2;
  usleep(200);
  ASSERT_EQ(job1, old1);
  ASSERT_GT(job2, old2);
}

TEST_F(CloudSchedulerTest, TestMultipleSchedulers) {
  auto scheduler2 = CloudScheduler::Get();

  int job1 = 1;
  int job2 = 1;
  auto doJob1 = [&job1](void *) { job1++; };
  auto doJob2 = [&job2](void *) { job2++; };

  auto handle1 =
      scheduler_->ScheduleJob(std::chrono::microseconds(120), doJob1, nullptr);
  auto handle2 =
      scheduler2->ScheduleJob(std::chrono::microseconds(120), doJob2, nullptr);
  ASSERT_FALSE(scheduler_->CancelJob(handle2));
  ASSERT_FALSE(scheduler2->CancelJob(handle1));
  ASSERT_TRUE(scheduler2->CancelJob(handle2));
  ASSERT_FALSE(scheduler2->CancelJob(handle2));
  usleep(200);
  ASSERT_EQ(job1, 2);
  ASSERT_EQ(job2, 1);

  scheduler_->ScheduleRecurringJob(std::chrono::microseconds(40),
                                   std::chrono::microseconds(20), doJob1,
                                   nullptr);
  scheduler2->ScheduleRecurringJob(std::chrono::microseconds(50),
                                   std::chrono::microseconds(20), doJob2,
                                   nullptr);
  scheduler2.reset();
  auto old1 = job1;
  auto old2 = job2;
  usleep(200);
  ASSERT_EQ(job2, old2);
  ASSERT_GT(job1, old1);
}

// This test tests the scenario where a job is started before we attempt to
// cancel it. It should wait for the job to finish.
TEST_F(CloudSchedulerTest, TestLongRunningJobCancel) {
  enum class JobStatus {
    NOT_STARTED = 0,
    STARTED = 1,
    FINISHED = 2,
  };

  std::mutex statusLock;
  std::atomic<JobStatus> status{JobStatus::NOT_STARTED};
  std::condition_variable statusVar;

  auto doJob = [&](void *) {
    {
      std::unique_lock<std::mutex> lk(statusLock);
      status.store(JobStatus::STARTED);
      statusVar.notify_all();
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    {
      std::unique_lock<std::mutex> lk(statusLock);
      status.store(JobStatus::FINISHED);
    }
  };

  auto handle =
      scheduler_->ScheduleJob(std::chrono::microseconds(0), doJob, nullptr);

  {
    std::unique_lock<std::mutex> lg(statusLock);
    statusVar.wait(lg, [&]() { return status == JobStatus::STARTED; });
  }

  scheduler_->CancelJob(handle);
  ASSERT_EQ(status.load(), JobStatus::FINISHED);
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
