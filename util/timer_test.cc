//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/timer.h"

#include "db/db_test_util.h"

class TimerTest : public testing::Test {
 public:
  TimerTest()
      : mock_env_(new ROCKSDB_NAMESPACE::MockTimeEnv(
          ROCKSDB_NAMESPACE::Env::Default())) {}

 protected:
  std::unique_ptr<ROCKSDB_NAMESPACE::MockTimeEnv> mock_env_;
};

TEST_F(TimerTest, SingleScheduleOnceTest) {
    const uint64_t kSecond = 1000000;  // 1sec = 1000000us
    const int kIteration = 1;
    ROCKSDB_NAMESPACE::Env* env = ROCKSDB_NAMESPACE::Env::Default();
    ROCKSDB_NAMESPACE::port::Mutex mutex;
    ROCKSDB_NAMESPACE::port::CondVar test_cv(&mutex);
    ROCKSDB_NAMESPACE::Timer timer(env);
    int count = 0;
    timer.Add(
        [&] {
          ROCKSDB_NAMESPACE::MutexLock l(&mutex);
          count++;
          if (count >= kIteration) {
            test_cv.SignalAll();
          }
        },
        "fn_sch_test",
        1 * kSecond,
        0);

    ASSERT_TRUE(timer.Start());

    // Wait for execution to finish
    {
      ROCKSDB_NAMESPACE::MutexLock l(&mutex);
      while(count < kIteration) {
        test_cv.Wait();
      }
    }

    ASSERT_TRUE(timer.Shutdown());

    ASSERT_EQ(1, count);
}

TEST_F(TimerTest, MultipleScheduleOnceTest) {
    const uint64_t kSecond = 1000000;  // 1sec = 1000000us
    const int kIteration = 1;
    ROCKSDB_NAMESPACE::Env* env = ROCKSDB_NAMESPACE::Env::Default();
    ROCKSDB_NAMESPACE::Timer timer(env);

    ROCKSDB_NAMESPACE::port::Mutex mutex1;
    ROCKSDB_NAMESPACE::port::CondVar test_cv1(&mutex1);
    int count1 = 0;
    timer.Add(
        [&] {
            ROCKSDB_NAMESPACE::MutexLock l(&mutex1);
            count1++;
            if (count1 >= kIteration) {
            test_cv1.SignalAll();
            }
        },
        "fn_sch_test1",
        1 * kSecond,
        0);

    ROCKSDB_NAMESPACE::port::Mutex mutex2;
    ROCKSDB_NAMESPACE::port::CondVar test_cv2(&mutex2);
    int count2 = 0;
    timer.Add(
        [&] {
            ROCKSDB_NAMESPACE::MutexLock l(&mutex2);
            count2 += 5;
            if (count2 >= kIteration) {
            test_cv2.SignalAll();
            }
        },
        "fn_sch_test2",
        3 * kSecond,
        0);

    ASSERT_TRUE(timer.Start());

    // Wait for execution to finish
    {
        ROCKSDB_NAMESPACE::MutexLock l(&mutex1);
        while(count1 < kIteration) {
          test_cv1.Wait();
        }
    }

    ASSERT_EQ(1, count1);
    ASSERT_EQ(0, count2);

    // Wait for execution to finish
    {
        ROCKSDB_NAMESPACE::MutexLock l(&mutex2);
        while(count2 < kIteration) {
          test_cv2.Wait();
        }
    }

    ASSERT_TRUE(timer.Shutdown());

    ASSERT_EQ(1, count1);
    ASSERT_EQ(5, count2);
}

TEST_F(TimerTest, SingleScheduleRepeatedlyTest) {
    const uint64_t kSecond = 1000000;  // 1sec = 1000000us
    const int kIteration = 5;
    ROCKSDB_NAMESPACE::Env* env = ROCKSDB_NAMESPACE::Env::Default();
    ROCKSDB_NAMESPACE::port::Mutex mutex;
    ROCKSDB_NAMESPACE::port::CondVar test_cv(&mutex);
    ROCKSDB_NAMESPACE::Timer timer(env);
    int count = 0;
    timer.Add(
        [&] {
          ROCKSDB_NAMESPACE::MutexLock l(&mutex);
          count++;
          fprintf(stderr, "%d\n", count);
          if (count >= kIteration) {
            test_cv.SignalAll();
          }
        },
        "fn_sch_test",
        1 * kSecond,
        1 * kSecond);

    ASSERT_TRUE(timer.Start());

    // Wait for execution to finish
    {
      ROCKSDB_NAMESPACE::MutexLock l(&mutex);
      while(count < kIteration) {
        test_cv.Wait();
      }
    }

    ASSERT_TRUE(timer.Shutdown());

    ASSERT_EQ(5, count);
}

TEST_F(TimerTest, MultipleScheduleRepeatedlyTest) {
    const uint64_t kSecond = 1000000;  // 1sec = 1000000us
    ROCKSDB_NAMESPACE::Env* env = ROCKSDB_NAMESPACE::Env::Default();
    ROCKSDB_NAMESPACE::Timer timer(env);

    ROCKSDB_NAMESPACE::port::Mutex mutex1;
    ROCKSDB_NAMESPACE::port::CondVar test_cv1(&mutex1);
    const int kIteration1 = 5;
    int count1 = 0;
    timer.Add(
        [&] {
            ROCKSDB_NAMESPACE::MutexLock l(&mutex1);
            count1++;
            fprintf(stderr, "hello\n");
            if (count1 >= kIteration1) {
            test_cv1.SignalAll();
            }
        },
        "fn_sch_test1",
        0,
        2 * kSecond);

    ROCKSDB_NAMESPACE::port::Mutex mutex2;
    ROCKSDB_NAMESPACE::port::CondVar test_cv2(&mutex2);
    const int kIteration2 = 3;
    int count2 = 0;
    timer.Add(
        [&] {
            ROCKSDB_NAMESPACE::MutexLock l(&mutex2);
            count2 += 5;
            fprintf(stderr, "world\n");
            if (count2 >= kIteration2) {
            test_cv2.SignalAll();
            }
        },
        "fn_sch_test2",
        1 * kSecond,
        5 * kSecond);

    ASSERT_TRUE(timer.Start());

    // Wait for execution to finish
    {
        ROCKSDB_NAMESPACE::MutexLock l(&mutex1);
        while(count1 < kIteration1) {
          test_cv1.Wait();
        }
    }

    ASSERT_EQ(5, count1);
    ASSERT_EQ(10, count2);

    // Wait for execution to finish
    {
        ROCKSDB_NAMESPACE::MutexLock l(&mutex2);
        while(count2 < kIteration2) {
          test_cv2.Wait();
        }
    }

    ASSERT_TRUE(timer.Shutdown());

    ASSERT_EQ(5, count1);
    ASSERT_EQ(10, count2);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
