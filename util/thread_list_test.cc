//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <mutex>
#include <condition_variable>

#include "util/thread_status_impl.h"
#include "util/testharness.h"
#include "rocksdb/db.h"

#if ROCKSDB_USING_THREAD_STATUS

namespace rocksdb {

class SleepingBackgroundTask {
 public:
  SleepingBackgroundTask(const void* db_key, const std::string& db_name,
                         const void* cf_key, const std::string& cf_name)
      : db_key_(db_key), db_name_(db_name),
        cf_key_(cf_key), cf_name_(cf_name),
        should_sleep_(true), sleeping_count_(0) {
    ThreadStatusImpl::NewColumnFamilyInfo(
        db_key_, db_name_, cf_key_, cf_name_);
  }

  ~SleepingBackgroundTask() {
    ThreadStatusImpl::EraseDatabaseInfo(db_key_);
  }

  void DoSleep() {
    thread_local_status.SetColumnFamilyInfoKey(cf_key_);
    std::unique_lock<std::mutex> l(mutex_);
    sleeping_count_++;
    while (should_sleep_) {
      bg_cv_.wait(l);
    }
    sleeping_count_--;
    bg_cv_.notify_all();
    thread_local_status.SetColumnFamilyInfoKey(0);
  }
  void WakeUp() {
    std::unique_lock<std::mutex> l(mutex_);
    should_sleep_ = false;
    bg_cv_.notify_all();
  }
  void WaitUntilDone() {
    std::unique_lock<std::mutex> l(mutex_);
    while (sleeping_count_ > 0) {
      bg_cv_.wait(l);
    }
  }

  static void DoSleepTask(void* arg) {
    reinterpret_cast<SleepingBackgroundTask*>(arg)->DoSleep();
  }

 private:
  const void* db_key_;
  const std::string db_name_;
  const void* cf_key_;
  const std::string cf_name_;
  std::mutex mutex_;
  std::condition_variable bg_cv_;
  bool should_sleep_;
  std::atomic<int> sleeping_count_;
};

class ThreadListTest {
 public:
  ThreadListTest() {
  }
};

TEST(ThreadListTest, SimpleColumnFamilyInfoTest) {
  Env* env = Env::Default();
  const int kHighPriorityThreads = 3;
  const int kLowPriorityThreads = 5;
  const int kSleepingHighPriThreads = kHighPriorityThreads - 1;
  const int kSleepingLowPriThreads = kLowPriorityThreads / 3;
  env->SetBackgroundThreads(kHighPriorityThreads, Env::HIGH);
  env->SetBackgroundThreads(kLowPriorityThreads, Env::LOW);

  SleepingBackgroundTask sleeping_task(
      reinterpret_cast<void*>(1234), "sleeping",
      reinterpret_cast<void*>(5678), "pikachu");

  for (int test = 0; test < kSleepingHighPriThreads; ++test) {
    env->Schedule(&SleepingBackgroundTask::DoSleepTask,
        &sleeping_task, Env::Priority::HIGH);
  }
  for (int test = 0; test < kSleepingLowPriThreads; ++test) {
    env->Schedule(&SleepingBackgroundTask::DoSleepTask,
        &sleeping_task, Env::Priority::LOW);
  }

  // make sure everything is scheduled.
  env->SleepForMicroseconds(10000);

  std::vector<ThreadStatus> thread_list;

  // Verify the number of sleeping threads in each pool.
  GetThreadList(&thread_list);
  int sleeping_count[ThreadStatus::ThreadType::TOTAL] = {0};
  for (auto thread_status : thread_list) {
    if (thread_status.cf_name == "pikachu" &&
        thread_status.db_name == "sleeping") {
      sleeping_count[thread_status.thread_type]++;
    }
  }
  ASSERT_EQ(
      sleeping_count[ThreadStatus::ThreadType::ROCKSDB_HIGH_PRIORITY],
      kSleepingHighPriThreads);
  ASSERT_EQ(
      sleeping_count[ThreadStatus::ThreadType::ROCKSDB_LOW_PRIORITY],
      kSleepingLowPriThreads);
  ASSERT_EQ(
      sleeping_count[ThreadStatus::ThreadType::USER_THREAD], 0);

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();

  // Verify none of the threads are sleeping
  GetThreadList(&thread_list);
  for (int i = 0; i < ThreadStatus::ThreadType::TOTAL; ++i) {
    sleeping_count[i] = 0;
  }

  for (auto thread_status : thread_list) {
    if (thread_status.cf_name == "pikachu" &&
        thread_status.db_name == "sleeping") {
      sleeping_count[thread_status.thread_type]++;
    }
  }
  ASSERT_EQ(
      sleeping_count[ThreadStatus::ThreadType::ROCKSDB_HIGH_PRIORITY], 0);
  ASSERT_EQ(
      sleeping_count[ThreadStatus::ThreadType::ROCKSDB_LOW_PRIORITY], 0);
  ASSERT_EQ(
      sleeping_count[ThreadStatus::ThreadType::USER_THREAD], 0);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}

#else

int main(int argc, char** argv) {
  return 0;
}

#endif  // ROCKSDB_USING_THREAD_STATUS
