//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <memory>

#include "db/db_test_util.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/repeatable_thread.h"

class RepeatableThreadTest : public testing::Test {
 public:
  RepeatableThreadTest()
      : mock_env_(new ROCKSDB_NAMESPACE::MockTimeEnv(
            ROCKSDB_NAMESPACE::Env::Default())) {}

 protected:
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env_;
};

TEST_F(RepeatableThreadTest, TimedTest) {
  constexpr uint64_t kSecond = 1000000;  // 1s = 1000000us
  constexpr int kIteration = 3;
  ROCKSDB_NAMESPACE::Env* env = ROCKSDB_NAMESPACE::Env::Default();
  ROCKSDB_NAMESPACE::port::Mutex mutex;
  ROCKSDB_NAMESPACE::port::CondVar test_cv(&mutex);
  int count = 0;
  uint64_t prev_time = env->NowMicros();
  ROCKSDB_NAMESPACE::RepeatableThread thread(
      [&] {
        ROCKSDB_NAMESPACE::MutexLock l(&mutex);
        count++;
        uint64_t now = env->NowMicros();
        assert(count == 1 || prev_time + 1 * kSecond <= now);
        prev_time = now;
        if (count >= kIteration) {
          test_cv.SignalAll();
        }
      },
      "rt_test", env, 1 * kSecond);
  // Wait for execution finish.
  {
    ROCKSDB_NAMESPACE::MutexLock l(&mutex);
    while (count < kIteration) {
      test_cv.Wait();
    }
  }

  // Test cancel
  thread.cancel();
}

TEST_F(RepeatableThreadTest, MockEnvTest) {
  constexpr uint64_t kSecond = 1000000;  // 1s = 1000000us
  constexpr int kIteration = 3;
  mock_env_->set_current_time(0);  // in seconds
  std::atomic<int> count{0};

#if defined(OS_MACOSX) && !defined(NDEBUG)
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        // Obtain the current (real) time in seconds and add 1000 extra seconds
        // to ensure that RepeatableThread::wait invokes TimedWait with a time
        // greater than (real) current time. This is to prevent the TimedWait
        // function from returning immediately without sleeping and releasing
        // the mutex on certain platforms, e.g. OS X. If TimedWait returns
        // immediately, the mutex will not be released, and
        // RepeatableThread::TEST_WaitForRun never has a chance to execute the
        // callback which, in this case, updates the result returned by
        // mock_env->NowMicros. Consequently, RepeatableThread::wait cannot
        // break out of the loop, causing test to hang. The extra 1000 seconds
        // is a best-effort approach because there seems no reliable and
        // deterministic way to provide the aforementioned guarantee. By the
        // time RepeatableThread::wait is called, it is no guarantee that the
        // delay + mock_env->NowMicros will be greater than the current real
        // time. However, 1000 seconds should be sufficient in most cases.
        uint64_t time_us = *reinterpret_cast<uint64_t*>(arg);
        if (time_us < mock_env_->RealNowMicros()) {
          *reinterpret_cast<uint64_t*>(arg) = mock_env_->RealNowMicros() + 1000;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
#endif  // OS_MACOSX && !NDEBUG

  ROCKSDB_NAMESPACE::RepeatableThread thread(
      [&] { count++; }, "rt_test", mock_env_.get(), 1 * kSecond, 1 * kSecond);
  for (int i = 1; i <= kIteration; i++) {
    // Bump current time
    thread.TEST_WaitForRun([&] { mock_env_->set_current_time(i); });
  }
  // Test function should be exectued exactly kIteraion times.
  ASSERT_EQ(kIteration, count.load());

  // Test cancel
  thread.cancel();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
