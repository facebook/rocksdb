// Copyright (c) 2017 Rockset

#include "cloud/cloud_scheduler.h"

#include <chrono>
#include <unordered_set>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class CloudSchedulerTest : public testing::Test {
 public:
  CloudSchedulerTest() { scheduler_ = CloudScheduler::Get(); }
  ~CloudSchedulerTest() {
    for (auto h : handles_) {
      scheduler_->CancelJob(h);
    }
  }

  void CancelAllJobs() {
    for (auto h : handles_) {
      scheduler_->CancelJob(h);
    }
    handles_.clear();
  }

  bool CancelJob(int h) {
    bool cancelled = scheduler_->CancelJob(h);
    handles_.erase(h);
    return cancelled;
  }

  int ScheduleJob(int us, std::function<void(void *)> job, void *arg) {
    auto h = scheduler_->ScheduleJob(std::chrono::microseconds(us), job, arg);
    handles_.insert(h);
    return h;
  }

  int ScheduleRecurringJob(int us, int freq, std::function<void(void *)> job,
                           void *arg) {
    auto h = scheduler_->ScheduleRecurringJob(std::chrono::microseconds(us),
                                              std::chrono::microseconds(freq),
                                              job, arg);
    handles_.insert(h);
    return h;
  }

 private:
  std::unordered_set<int> handles_;
  std::shared_ptr<CloudScheduler> scheduler_;
};

TEST_F(CloudSchedulerTest, TestSchedule) {
  static int job1 = 1;
  static int job2 = 1;
  auto doJob = [](void *arg) { (*(reinterpret_cast<int *>(arg)))++; };

  ScheduleJob(50, doJob, &job1);
  ScheduleJob(150, doJob, &job2);

  usleep(80);
  ASSERT_EQ(job2, 1);
  ASSERT_EQ(job1, 2);
  usleep(80);
  ASSERT_EQ(job2, 2);
  ASSERT_EQ(job1, 2);

  ScheduleJob(150, doJob, &job1);
  ScheduleJob(50, doJob, &job2);
  usleep(80);
  ASSERT_EQ(job1, 2);
  ASSERT_EQ(job2, 3);
  usleep(80);
  ASSERT_EQ(job1, 3);
  ASSERT_EQ(job2, 3);
}

TEST_F(CloudSchedulerTest, TestCancel) {
  static int job1 = 1;
  static int job2 = 0;
  auto doJob = [](void *arg) { (*(reinterpret_cast<int *>(arg)))++; };

  auto handle1 = ScheduleJob(50, doJob, &job1);
  auto handle2 = ScheduleJob(80, doJob, &job2);
  ASSERT_TRUE(CancelJob(handle2));
  usleep(120);
  ASSERT_EQ(job1, 2);
  ASSERT_EQ(job2, 0);
  ASSERT_FALSE(CancelJob(handle1));
  ASSERT_FALSE(CancelJob(handle2));
}

TEST_F(CloudSchedulerTest, TestRecurring) {
  int job1 = 1;
  int job2 = 1;
  auto doJob = [](void *arg) { (*(reinterpret_cast<int *>(arg)))++; };

  auto handle1 = ScheduleRecurringJob(10, 40, doJob, &job1);
  ScheduleRecurringJob(20, 80, doJob, &job2);
  usleep(200);
  ASSERT_GE(job1, 5);
  ASSERT_GE(job2, 3);
  ASSERT_TRUE(CancelJob(handle1));
  usleep(120);
  ASSERT_GE(job1, 5);
  ASSERT_GE(job2, 4);
}
}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
