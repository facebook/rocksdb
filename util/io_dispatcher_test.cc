//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/io_dispatcher.h"

#include <atomic>
#include <chrono>
#include <thread>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class IODispatcherTest : public testing::Test {
 public:
  IODispatcherTest() = default;
  ~IODispatcherTest() override = default;
};

TEST_F(IODispatcherTest, SingleJob) {
  IODispatcher* dispatcher = NewIODispatcher(1);
  std::atomic<int> counter(0);

  dispatcher->SubmitJob([&counter]() { counter++; });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ASSERT_EQ(1, counter.load());
  delete dispatcher;
}

TEST_F(IODispatcherTest, MultipleJobs) {
  IODispatcher* dispatcher = NewIODispatcher(4);
  std::atomic<int> counter(0);
  const int num_jobs = 10;

  for (int i = 0; i < num_jobs; i++) {
    dispatcher->SubmitJob([&counter]() { counter++; });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  ASSERT_EQ(num_jobs, counter.load());
  delete dispatcher;
}

TEST_F(IODispatcherTest, QueueLength) {
  IODispatcher* dispatcher = NewIODispatcher(1);

  std::atomic<bool> job_started(false);
  std::atomic<bool> job_continue(false);

  dispatcher->SubmitJob([&job_started, &job_continue]() {
    job_started = true;
    while (!job_continue.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });

  while (!job_started.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  dispatcher->SubmitJob([]() {});
  dispatcher->SubmitJob([]() {});

  ASSERT_GE(dispatcher->GetQueueLen(), 1u);

  job_continue = true;
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  delete dispatcher;
}

TEST_F(IODispatcherTest, ConcurrentJobExecution) {
  IODispatcher* dispatcher = NewIODispatcher(4);
  std::atomic<int> concurrent_count(0);
  std::atomic<int> max_concurrent(0);
  std::atomic<int> completed_count(0);
  const int num_jobs = 8;

  for (int i = 0; i < num_jobs; i++) {
    dispatcher->SubmitJob(
        [&concurrent_count, &max_concurrent, &completed_count]() {
          int current = concurrent_count.fetch_add(1) + 1;

          int expected_max = max_concurrent.load();
          while (current > expected_max &&
                 !max_concurrent.compare_exchange_weak(expected_max, current)) {
          }

          std::this_thread::sleep_for(std::chrono::milliseconds(50));

          concurrent_count.fetch_sub(1);
          completed_count.fetch_add(1);
        });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_EQ(num_jobs, completed_count.load());
  ASSERT_LE(max_concurrent.load(), 4);
  ASSERT_GE(max_concurrent.load(), 1);

  delete dispatcher;
}

TEST_F(IODispatcherTest, MoveSemantics) {
  IODispatcher* dispatcher = NewIODispatcher(2);
  std::atomic<int> counter(0);

  auto lambda = [&counter]() { counter++; };

  dispatcher->SubmitJob(std::move(lambda));

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ASSERT_EQ(1, counter.load());

  delete dispatcher;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
