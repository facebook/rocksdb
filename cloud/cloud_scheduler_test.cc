// Copyright (c) 2017 Rockset

#include "cloud/cloud_scheduler.h"

#include <chrono>
#include <unordered_set>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class CloudSchedulerTest : public testing::Test {
 public:
  CloudSchedulerTest() { scheduler_ = CloudScheduler::Get(); }
  ~CloudSchedulerTest() {}

  std::shared_ptr<CloudScheduler> scheduler_;
};

TEST_F(CloudSchedulerTest, TestSchedule) {
  static int job1 = 1;
  static int job2 = 1;
  auto doJob = [](void *arg) { (*(reinterpret_cast<int *>(arg)))++; };

  scheduler_->ScheduleJob(std::chrono::microseconds(100), doJob, &job1);
  scheduler_->ScheduleJob(std::chrono::microseconds(300), doJob, &job2);

  usleep(200);
  ASSERT_EQ(job2, 1);
  ASSERT_EQ(job1, 2);
  usleep(200);
  ASSERT_EQ(job2, 2);
  ASSERT_EQ(job1, 2);

  scheduler_->ScheduleJob(std::chrono::microseconds(300), doJob, &job1);
  scheduler_->ScheduleJob(std::chrono::microseconds(100), doJob, &job2);
  usleep(200);
  ASSERT_EQ(job1, 2);
  ASSERT_EQ(job2, 3);
  usleep(200);
  ASSERT_EQ(job1, 3);
  ASSERT_EQ(job2, 3);
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

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
