//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <folly/synchronization/DistributedMutex.h>
#include <folly/container/Array.h>
#include <folly/synchronization/Baton.h>

#ifdef OS_AIX
#include "gtest/gtest.h"
#else
#include <gtest/gtest.h>
#endif

#ifndef ROCKSDB_LITE

#include <chrono>
#include <cmath>
#include <thread>

namespace folly {
namespace test {
template <template <typename> class Atomic>
using TestDistributedMutex =
    folly::detail::distributed_mutex::DistributedMutex<Atomic, false>;
} // namespace test

namespace {
constexpr auto kStressFactor = 1000;
constexpr auto kStressTestSeconds = 2;
constexpr auto kForever = std::chrono::hours{100};

int sum(int n) {
  return (n * (n + 1)) / 2;
}

template <template <typename> class Atom = std::atomic>
void basicNThreads(int numThreads, int iterations = kStressFactor) {
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& barrier = std::atomic<int>{0};
  auto&& threads = std::vector<std::thread>{};
  auto&& result = std::vector<int>{};

  auto&& function = [&](int id) {
    return [&, id] {
      for (auto j = 0; j < iterations; ++j) {
        auto lck = std::unique_lock<_t<std::decay<decltype(mutex)>>>{mutex};
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        std::this_thread::yield();
        result.push_back(id);
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
      }
    };
  };

  for (auto i = 1; i <= numThreads; ++i) {
    threads.push_back(std::thread(function(i)));
  }
  for (auto& thread : threads) {
    thread.join();
  }

  auto total = 0;
  for (auto value : result) {
    total += value;
  }
  EXPECT_EQ(total, sum(numThreads) * iterations);
}

template <template <typename> class Atom = std::atomic>
void lockWithTryAndTimedNThreads(
    int numThreads,
    std::chrono::seconds duration) {
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& barrier = std::atomic<int>{0};
  auto&& threads = std::vector<std::thread>{};
  auto&& stop = std::atomic<bool>{false};

  auto&& lockUnlockFunction = [&]() {
    while (!stop.load()) {
      auto lck = std::unique_lock<_t<std::decay<decltype(mutex)>>>{mutex};
      EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
      std::this_thread::yield();
      EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
    }
  };

  auto tryLockFunction = [&]() {
    while (!stop.load()) {
      using Mutex = _t<std::decay<decltype(mutex)>>;
      auto lck = std::unique_lock<Mutex>{mutex, std::defer_lock};
      if (lck.try_lock()) {
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        std::this_thread::yield();
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
      }
    }
  };

  auto timedLockFunction = [&]() {
    while (!stop.load()) {
      using Mutex = _t<std::decay<decltype(mutex)>>;
      auto lck = std::unique_lock<Mutex>{mutex, std::defer_lock};
      if (lck.try_lock_for(kForever)) {
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        std::this_thread::yield();
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
      }
    }
  };

  for (auto i = 0; i < (numThreads / 3); ++i) {
    threads.push_back(std::thread(lockUnlockFunction));
  }
  for (auto i = 0; i < (numThreads / 3); ++i) {
    threads.push_back(std::thread(tryLockFunction));
  }
  for (auto i = 0; i < (numThreads / 3); ++i) {
    threads.push_back(std::thread(timedLockFunction));
  }

  /* sleep override */
  std::this_thread::sleep_for(duration);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}

template <template <typename> class Atom = std::atomic>
void combineNThreads(int numThreads, std::chrono::seconds duration) {
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& barrier = std::atomic<int>{0};
  auto&& threads = std::vector<std::thread>{};
  auto&& stop = std::atomic<bool>{false};

  auto&& function = [&]() {
    return [&] {
      auto&& expected = std::uint64_t{0};
      auto&& local = std::atomic<std::uint64_t>{0};
      auto&& result = std::atomic<std::uint64_t>{0};
      while (!stop.load()) {
        ++expected;
        auto current = mutex.lock_combine([&]() {
          result.fetch_add(1);
          EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
          EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 1);
          std::this_thread::yield();
          SCOPE_EXIT {
            EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
          };
          EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 2);
          return local.fetch_add(1);
        });
        EXPECT_EQ(current, expected - 1);
      }

      EXPECT_EQ(expected, result.load());
    };
  };

  for (auto i = 1; i <= numThreads; ++i) {
    threads.push_back(std::thread(function()));
  }

  /* sleep override */
  std::this_thread::sleep_for(duration);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}

template <template <typename> class Atom = std::atomic>
void combineWithLockNThreads(int numThreads, std::chrono::seconds duration) {
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& barrier = std::atomic<int>{0};
  auto&& threads = std::vector<std::thread>{};
  auto&& stop = std::atomic<bool>{false};

  auto&& lockUnlockFunction = [&]() {
    while (!stop.load()) {
      auto lck = std::unique_lock<_t<std::decay<decltype(mutex)>>>{mutex};
      EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
      std::this_thread::yield();
      EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
    }
  };

  auto&& combineFunction = [&]() {
    auto&& expected = std::uint64_t{0};
    auto&& total = std::atomic<std::uint64_t>{0};

    while (!stop.load()) {
      ++expected;
      auto current = mutex.lock_combine([&]() {
        auto iteration = total.fetch_add(1);
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 1);
        std::this_thread::yield();
        SCOPE_EXIT {
          EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
        };
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 2);
        return iteration;
      });

      EXPECT_EQ(expected, current + 1);
    }

    EXPECT_EQ(expected, total.load());
  };

  for (auto i = 1; i < (numThreads / 2); ++i) {
    threads.push_back(std::thread(combineFunction));
  }
  for (auto i = 0; i < (numThreads / 2); ++i) {
    threads.push_back(std::thread(lockUnlockFunction));
  }

  /* sleep override */
  std::this_thread::sleep_for(duration);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}

template <template <typename> class Atom = std::atomic>
void combineWithTryLockNThreads(int numThreads, std::chrono::seconds duration) {
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& barrier = std::atomic<int>{0};
  auto&& threads = std::vector<std::thread>{};
  auto&& stop = std::atomic<bool>{false};

  auto&& lockUnlockFunction = [&]() {
    while (!stop.load()) {
      auto lck = std::unique_lock<_t<std::decay<decltype(mutex)>>>{mutex};
      EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
      std::this_thread::yield();
      EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
    }
  };

  auto&& combineFunction = [&]() {
    auto&& expected = std::uint64_t{0};
    auto&& total = std::atomic<std::uint64_t>{0};

    while (!stop.load()) {
      ++expected;
      auto current = mutex.lock_combine([&]() {
        auto iteration = total.fetch_add(1);
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 1);
        std::this_thread::yield();
        SCOPE_EXIT {
          EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
        };
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 2);
        return iteration;
      });

      EXPECT_EQ(expected, current + 1);
    }

    EXPECT_EQ(expected, total.load());
  };

  auto tryLockFunction = [&]() {
    while (!stop.load()) {
      using Mutex = _t<std::decay<decltype(mutex)>>;
      auto lck = std::unique_lock<Mutex>{mutex, std::defer_lock};
      if (lck.try_lock()) {
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        std::this_thread::yield();
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
      }
    }
  };

  for (auto i = 0; i < (numThreads / 3); ++i) {
    threads.push_back(std::thread(lockUnlockFunction));
  }
  for (auto i = 0; i < (numThreads / 3); ++i) {
    threads.push_back(std::thread(combineFunction));
  }
  for (auto i = 0; i < (numThreads / 3); ++i) {
    threads.push_back(std::thread(tryLockFunction));
  }

  /* sleep override */
  std::this_thread::sleep_for(duration);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}

template <template <typename> class Atom = std::atomic>
void combineWithLockTryAndTimedNThreads(
    int numThreads,
    std::chrono::seconds duration) {
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& barrier = std::atomic<int>{0};
  auto&& threads = std::vector<std::thread>{};
  auto&& stop = std::atomic<bool>{false};

  auto&& lockUnlockFunction = [&]() {
    while (!stop.load()) {
      auto lck = std::unique_lock<_t<std::decay<decltype(mutex)>>>{mutex};
      EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
      std::this_thread::yield();
      EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
    }
  };

  auto&& combineFunction = [&]() {
    auto&& expected = std::uint64_t{0};
    auto&& total = std::atomic<std::uint64_t>{0};

    while (!stop.load()) {
      ++expected;
      auto current = mutex.lock_combine([&]() {
        auto iteration = total.fetch_add(1);
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 1);
        std::this_thread::yield();
        SCOPE_EXIT {
          EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
        };
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 2);

        // return a non-trivially-copyable object that occupies all the
        // storage we use to coalesce returns to test that codepath
        return folly::make_array(
            iteration,
            iteration + 1,
            iteration + 2,
            iteration + 3,
            iteration + 4,
            iteration + 5);
      });

      EXPECT_EQ(expected, current[0] + 1);
      EXPECT_EQ(expected, current[1]);
      EXPECT_EQ(expected, current[2] - 1);
      EXPECT_EQ(expected, current[3] - 2);
      EXPECT_EQ(expected, current[4] - 3);
      EXPECT_EQ(expected, current[5] - 4);
    }

    EXPECT_EQ(expected, total.load());
  };

  auto tryLockFunction = [&]() {
    while (!stop.load()) {
      using Mutex = _t<std::decay<decltype(mutex)>>;
      auto lck = std::unique_lock<Mutex>{mutex, std::defer_lock};
      if (lck.try_lock()) {
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        std::this_thread::yield();
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
      }
    }
  };

  auto timedLockFunction = [&]() {
    while (!stop.load()) {
      using Mutex = _t<std::decay<decltype(mutex)>>;
      auto lck = std::unique_lock<Mutex>{mutex, std::defer_lock};
      if (lck.try_lock_for(kForever)) {
        EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
        std::this_thread::yield();
        EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
      }
    }
  };

  for (auto i = 0; i < (numThreads / 4); ++i) {
    threads.push_back(std::thread(lockUnlockFunction));
  }
  for (auto i = 0; i < (numThreads / 4); ++i) {
    threads.push_back(std::thread(combineFunction));
  }
  for (auto i = 0; i < (numThreads / 4); ++i) {
    threads.push_back(std::thread(tryLockFunction));
  }
  for (auto i = 0; i < (numThreads / 4); ++i) {
    threads.push_back(std::thread(timedLockFunction));
  }

  /* sleep override */
  std::this_thread::sleep_for(duration);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace

TEST(DistributedMutex, InternalDetailTestOne) {
  auto value = 0;
  auto ptr = reinterpret_cast<std::uintptr_t>(&value);
  EXPECT_EQ(folly::detail::distributed_mutex::extractPtr<int>(ptr), &value);
  ptr = ptr | 0b1;
  EXPECT_EQ(folly::detail::distributed_mutex::extractPtr<int>(ptr), &value);
}

TEST(DistributedMutex, Basic) {
  auto&& mutex = folly::DistributedMutex{};
  auto state = mutex.lock();
  mutex.unlock(std::move(state));
}

TEST(DistributedMutex, BasicTryLock) {
  auto&& mutex = folly::DistributedMutex{};

  while (true) {
    auto state = mutex.try_lock();
    if (state) {
      mutex.unlock(std::move(state));
      break;
    }
  }
}

TEST(DistributedMutex, StressTwoThreads) {
  basicNThreads(2);
}
TEST(DistributedMutex, StressThreeThreads) {
  basicNThreads(3);
}
TEST(DistributedMutex, StressFourThreads) {
  basicNThreads(4);
}
TEST(DistributedMutex, StressFiveThreads) {
  basicNThreads(5);
}
TEST(DistributedMutex, StressSixThreads) {
  basicNThreads(6);
}
TEST(DistributedMutex, StressSevenThreads) {
  basicNThreads(7);
}
TEST(DistributedMutex, StressEightThreads) {
  basicNThreads(8);
}
TEST(DistributedMutex, StressSixteenThreads) {
  basicNThreads(16);
}
TEST(DistributedMutex, StressThirtyTwoThreads) {
  basicNThreads(32);
}
TEST(DistributedMutex, StressSixtyFourThreads) {
  basicNThreads(64);
}
TEST(DistributedMutex, StressHundredThreads) {
  basicNThreads(100);
}
TEST(DistributedMutex, StressHardwareConcurrencyThreads) {
  basicNThreads(std::thread::hardware_concurrency());
}

TEST(DistributedMutex, StressThreeThreadsLockTryAndTimed) {
  lockWithTryAndTimedNThreads(3, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixThreadsLockTryAndTimed) {
  lockWithTryAndTimedNThreads(6, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressTwelveThreadsLockTryAndTimed) {
  lockWithTryAndTimedNThreads(12, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressTwentyFourThreadsLockTryAndTimed) {
  lockWithTryAndTimedNThreads(24, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressFourtyEightThreadsLockTryAndTimed) {
  lockWithTryAndTimedNThreads(48, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixtyFourThreadsLockTryAndTimed) {
  lockWithTryAndTimedNThreads(64, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressHwConcThreadsLockTryAndTimed) {
  lockWithTryAndTimedNThreads(
      std::thread::hardware_concurrency(),
      std::chrono::seconds{kStressTestSeconds});
}

TEST(DistributedMutex, StressTwoThreadsCombine) {
  combineNThreads(2, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressThreeThreadsCombine) {
  combineNThreads(3, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressFourThreadsCombine) {
  combineNThreads(4, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressFiveThreadsCombine) {
  combineNThreads(5, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixThreadsCombine) {
  combineNThreads(6, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSevenThreadsCombine) {
  combineNThreads(7, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressEightThreadsCombine) {
  combineNThreads(8, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixteenThreadsCombine) {
  combineNThreads(16, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressThirtyTwoThreadsCombine) {
  combineNThreads(32, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixtyFourThreadsCombine) {
  combineNThreads(64, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressHundredThreadsCombine) {
  combineNThreads(100, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressHardwareConcurrencyThreadsCombine) {
  combineNThreads(
      std::thread::hardware_concurrency(),
      std::chrono::seconds{kStressTestSeconds});
}

TEST(DistributedMutex, StressTwoThreadsCombineAndLock) {
  combineWithLockNThreads(2, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressFourThreadsCombineAndLock) {
  combineWithLockNThreads(4, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressEightThreadsCombineAndLock) {
  combineWithLockNThreads(8, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixteenThreadsCombineAndLock) {
  combineWithLockNThreads(16, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressThirtyTwoThreadsCombineAndLock) {
  combineWithLockNThreads(32, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixtyFourThreadsCombineAndLock) {
  combineWithLockNThreads(64, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressHardwareConcurrencyThreadsCombineAndLock) {
  combineWithLockNThreads(
      std::thread::hardware_concurrency(),
      std::chrono::seconds{kStressTestSeconds});
}

TEST(DistributedMutex, StressThreeThreadsCombineTryLockAndLock) {
  combineWithTryLockNThreads(3, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixThreadsCombineTryLockAndLock) {
  combineWithTryLockNThreads(6, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressTwelveThreadsCombineTryLockAndLock) {
  combineWithTryLockNThreads(12, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressTwentyFourThreadsCombineTryLockAndLock) {
  combineWithTryLockNThreads(24, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressFourtyEightThreadsCombineTryLockAndLock) {
  combineWithTryLockNThreads(48, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixtyFourThreadsCombineTryLockAndLock) {
  combineWithTryLockNThreads(64, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressHardwareConcurrencyThreadsCombineTryLockAndLock) {
  combineWithTryLockNThreads(
      std::thread::hardware_concurrency(),
      std::chrono::seconds{kStressTestSeconds});
}

TEST(DistributedMutex, StressThreeThreadsCombineTryLockLockAndTimed) {
  combineWithLockTryAndTimedNThreads(
      3, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixThreadsCombineTryLockLockAndTimed) {
  combineWithLockTryAndTimedNThreads(
      6, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressTwelveThreadsCombineTryLockLockAndTimed) {
  combineWithLockTryAndTimedNThreads(
      12, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressTwentyFourThreadsCombineTryLockLockAndTimed) {
  combineWithLockTryAndTimedNThreads(
      24, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressFourtyEightThreadsCombineTryLockLockAndTimed) {
  combineWithLockTryAndTimedNThreads(
      48, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressSixtyFourThreadsCombineTryLockLockAndTimed) {
  combineWithLockTryAndTimedNThreads(
      64, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressHwConcurrencyThreadsCombineTryLockLockAndTimed) {
  combineWithLockTryAndTimedNThreads(
      std::thread::hardware_concurrency(),
      std::chrono::seconds{kStressTestSeconds});
}

TEST(DistributedMutex, StressTryLock) {
  auto&& mutex = folly::DistributedMutex{};

  for (auto i = 0; i < kStressFactor; ++i) {
    while (true) {
      auto state = mutex.try_lock();
      if (state) {
        mutex.unlock(std::move(state));
        break;
      }
    }
  }
}

TEST(DistributedMutex, TimedLockTimeout) {
  auto&& mutex = folly::DistributedMutex{};
  auto&& start = folly::Baton<>{};
  auto&& done = folly::Baton<>{};

  auto thread = std::thread{[&]() {
    auto state = mutex.lock();
    start.post();
    done.wait();
    mutex.unlock(std::move(state));
  }};

  start.wait();
  auto result = mutex.try_lock_for(std::chrono::milliseconds{10});
  EXPECT_FALSE(result);
  done.post();
  thread.join();
}

TEST(DistributedMutex, TimedLockAcquireAfterUnlock) {
  auto&& mutex = folly::DistributedMutex{};
  auto&& start = folly::Baton<>{};

  auto thread = std::thread{[&]() {
    auto state = mutex.lock();
    start.post();
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    mutex.unlock(std::move(state));
  }};

  start.wait();
  auto result = mutex.try_lock_for(kForever);
  EXPECT_TRUE(result);
  thread.join();
}

namespace {
template <template <typename> class Atom = std::atomic>
void stressTryLockWithConcurrentLocks(
    int numThreads,
    int iterations = kStressFactor) {
  auto&& threads = std::vector<std::thread>{};
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& atomic = std::atomic<std::uint64_t>{0};

  for (auto i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([&] {
      for (auto j = 0; j < iterations; ++j) {
        auto state = mutex.lock();
        EXPECT_EQ(atomic.fetch_add(1, std::memory_order_relaxed), 0);
        EXPECT_EQ(atomic.fetch_sub(1, std::memory_order_relaxed), 1);
        mutex.unlock(std::move(state));
      }
    }));
  }

  for (auto i = 0; i < iterations; ++i) {
    if (auto state = mutex.try_lock()) {
      EXPECT_EQ(atomic.fetch_add(1, std::memory_order_relaxed), 0);
      EXPECT_EQ(atomic.fetch_sub(1, std::memory_order_relaxed), 1);
      mutex.unlock(std::move(state));
    }
  }

  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace

TEST(DistributedMutex, StressTryLockWithConcurrentLocksTwoThreads) {
  stressTryLockWithConcurrentLocks(2);
}
TEST(DistributedMutex, StressTryLockWithConcurrentLocksFourThreads) {
  stressTryLockWithConcurrentLocks(4);
}
TEST(DistributedMutex, StressTryLockWithConcurrentLocksEightThreads) {
  stressTryLockWithConcurrentLocks(8);
}
TEST(DistributedMutex, StressTryLockWithConcurrentLocksSixteenThreads) {
  stressTryLockWithConcurrentLocks(16);
}
TEST(DistributedMutex, StressTryLockWithConcurrentLocksThirtyTwoThreads) {
  stressTryLockWithConcurrentLocks(32);
}
TEST(DistributedMutex, StressTryLockWithConcurrentLocksSixtyFourThreads) {
  stressTryLockWithConcurrentLocks(64);
}

namespace {
template <template <typename> class Atom = std::atomic>
void concurrentTryLocks(int numThreads, int iterations = kStressFactor) {
  auto&& threads = std::vector<std::thread>{};
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& atomic = std::atomic<std::uint64_t>{0};

  for (auto i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([&] {
      for (auto j = 0; j < iterations; ++j) {
        if (auto state = mutex.try_lock()) {
          EXPECT_EQ(atomic.fetch_add(1, std::memory_order_relaxed), 0);
          EXPECT_EQ(atomic.fetch_sub(1, std::memory_order_relaxed), 1);
          mutex.unlock(std::move(state));
        }
      }
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace

TEST(DistributedMutex, StressTryLockWithTwoThreads) {
  concurrentTryLocks(2);
}
TEST(DistributedMutex, StressTryLockFourThreads) {
  concurrentTryLocks(4);
}
TEST(DistributedMutex, StressTryLockEightThreads) {
  concurrentTryLocks(8);
}
TEST(DistributedMutex, StressTryLockSixteenThreads) {
  concurrentTryLocks(16);
}
TEST(DistributedMutex, StressTryLockThirtyTwoThreads) {
  concurrentTryLocks(32);
}
TEST(DistributedMutex, StressTryLockSixtyFourThreads) {
  concurrentTryLocks(64);
}

namespace {
class TestConstruction {
 public:
  TestConstruction() = delete;
  explicit TestConstruction(int) {
    defaultConstructs().fetch_add(1, std::memory_order_relaxed);
  }
  TestConstruction(TestConstruction&&) noexcept {
    moveConstructs().fetch_add(1, std::memory_order_relaxed);
  }
  TestConstruction(const TestConstruction&) {
    copyConstructs().fetch_add(1, std::memory_order_relaxed);
  }
  TestConstruction& operator=(const TestConstruction&) {
    copyAssigns().fetch_add(1, std::memory_order_relaxed);
    return *this;
  }
  TestConstruction& operator=(TestConstruction&&) {
    moveAssigns().fetch_add(1, std::memory_order_relaxed);
    return *this;
  }
  ~TestConstruction() {
    destructs().fetch_add(1, std::memory_order_relaxed);
  }

  static std::atomic<std::uint64_t>& defaultConstructs() {
    static auto&& atomic = std::atomic<std::uint64_t>{0};
    return atomic;
  }
  static std::atomic<std::uint64_t>& moveConstructs() {
    static auto&& atomic = std::atomic<std::uint64_t>{0};
    return atomic;
  }
  static std::atomic<std::uint64_t>& copyConstructs() {
    static auto&& atomic = std::atomic<std::uint64_t>{0};
    return atomic;
  }
  static std::atomic<std::uint64_t>& moveAssigns() {
    static auto&& atomic = std::atomic<std::uint64_t>{0};
    return atomic;
  }
  static std::atomic<std::uint64_t>& copyAssigns() {
    static auto&& atomic = std::atomic<std::uint64_t>{0};
    return atomic;
  }
  static std::atomic<std::uint64_t>& destructs() {
    static auto&& atomic = std::atomic<std::uint64_t>{0};
    return atomic;
  }

  static void reset() {
    defaultConstructs().store(0);
    moveConstructs().store(0);
    copyConstructs().store(0);
    copyAssigns().store(0);
    destructs().store(0);
  }
};
} // namespace

TEST(DistributedMutex, TestAppropriateDestructionAndConstructionWithCombine) {
  auto&& mutex = folly::DistributedMutex{};
  auto&& stop = std::atomic<bool>{false};

  // test the simple return path to make sure that in the absence of
  // contention, we get the right number of constructs and destructs
  mutex.lock_combine([]() { return TestConstruction{1}; });
  auto moves = TestConstruction::moveConstructs().load();
  auto defaults = TestConstruction::defaultConstructs().load();
  EXPECT_EQ(TestConstruction::defaultConstructs().load(), 1);
  EXPECT_TRUE(moves == 0 || moves == 1);
  EXPECT_EQ(TestConstruction::destructs().load(), moves + defaults);

  // loop and make sure we were able to test the path where the critical
  // section of the thread gets combined, and assert that we see the expected
  // number of constructions and destructions
  //
  // this implements a timed backoff to test the combined path, so we use the
  // smallest possible delay in tests
  auto thread = std::thread{[&]() {
    auto&& duration = std::chrono::milliseconds{10};
    while (!stop.load()) {
      TestConstruction::reset();
      auto&& ready = folly::Baton<>{};
      auto&& release = folly::Baton<>{};

      // make one thread start it's critical section, signal and wait for
      // another thread to enqueue, to test the
      auto innerThread = std::thread{[&]() {
        mutex.lock_combine([&]() {
          ready.post();
          release.wait();
          /* sleep override */
          std::this_thread::sleep_for(duration);
        });
      }};

      // wait for the thread to get in its critical section, then tell it to go
      ready.wait();
      release.post();
      mutex.lock_combine([&]() { return TestConstruction{1}; });

      innerThread.join();

      // at this point we should have only one default construct, either 3
      // or 4 move constructs the same number of destructions as
      // constructions
      auto innerDefaults = TestConstruction::defaultConstructs().load();
      auto innerMoves = TestConstruction::moveConstructs().load();
      auto destructs = TestConstruction::destructs().load();
      EXPECT_EQ(innerDefaults, 1);
      EXPECT_TRUE(innerMoves == 3 || innerMoves == 4 || innerMoves == 1);
      EXPECT_EQ(destructs, innerMoves + innerDefaults);
      EXPECT_EQ(TestConstruction::moveAssigns().load(), 0);
      EXPECT_EQ(TestConstruction::copyAssigns().load(), 0);

      // increase duration by 100ms each iteration
      duration = duration + std::chrono::milliseconds{100};
    }
  }};

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::seconds{kStressTestSeconds});
  stop.store(true);
  thread.join();
}

namespace {
template <template <typename> class Atom = std::atomic>
void concurrentLocksManyMutexes(int numThreads, std::chrono::seconds duration) {
  using DMutex = folly::detail::distributed_mutex::DistributedMutex<Atom>;
  const auto&& kNumMutexes = 10;
  auto&& threads = std::vector<std::thread>{};
  auto&& mutexes = std::vector<DMutex>(kNumMutexes);
  auto&& barriers = std::vector<std::atomic<std::uint64_t>>(kNumMutexes);
  auto&& stop = std::atomic<bool>{false};

  for (auto i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([&] {
      auto&& total = std::atomic<std::uint64_t>{0};
      auto&& expected = std::uint64_t{0};

      for (auto j = 0; !stop.load(std::memory_order_relaxed); ++j) {
        auto& mutex = mutexes[j % kNumMutexes];
        auto& barrier = barriers[j % kNumMutexes];

        ++expected;
        auto result = mutex.lock_combine([&]() {
          EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
          EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 1);
          std::this_thread::yield();
          SCOPE_EXIT {
            EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
          };
          EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 2);
          return total.fetch_add(1, std::memory_order_relaxed);
        });
        EXPECT_EQ(result, expected - 1);
      }

      EXPECT_EQ(total.load(), expected);
    }));
  }

  /* sleep override */
  std::this_thread::sleep_for(duration);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace

TEST(DistributedMutex, StressWithManyMutexesAlternatingTwoThreads) {
  concurrentLocksManyMutexes(2, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressWithManyMutexesAlternatingFourThreads) {
  concurrentLocksManyMutexes(4, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressWithManyMutexesAlternatingEightThreads) {
  concurrentLocksManyMutexes(8, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressWithManyMutexesAlternatingSixteenThreads) {
  concurrentLocksManyMutexes(16, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressWithManyMutexesAlternatingThirtyTwoThreads) {
  concurrentLocksManyMutexes(32, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressWithManyMutexesAlternatingSixtyFourThreads) {
  concurrentLocksManyMutexes(64, std::chrono::seconds{kStressTestSeconds});
}

namespace {
class ExceptionWithConstructionTrack : public std::exception {
 public:
  explicit ExceptionWithConstructionTrack(int id)
      : id_{std::to_string(id)}, constructionTrack_{id} {}

  const char* what() const noexcept override {
    return id_.c_str();
  }

 private:
  std::string id_;
  TestConstruction constructionTrack_;
};
} // namespace

TEST(DistributedMutex, TestExceptionPropagationUncontended) {
  TestConstruction::reset();
  auto&& mutex = folly::DistributedMutex{};
  auto&& thread = std::thread{[&]() {
    try {
      mutex.lock_combine([&]() { throw ExceptionWithConstructionTrack{46}; });
    } catch (std::exception& exc) {
      auto integer = std::stoi(exc.what());
      EXPECT_EQ(integer, 46);
      EXPECT_GT(TestConstruction::defaultConstructs(), 0);
    }
    EXPECT_EQ(
        TestConstruction::defaultConstructs(), TestConstruction::destructs());
  }};
  thread.join();
}

namespace {
template <template <typename> class Atom = std::atomic>
void concurrentExceptionPropagationStress(
    int numThreads,
    std::chrono::milliseconds t) {
  // this test fails under with a false negative under older versions of TSAN
  // for some reason so disable it when TSAN is enabled
  if (folly::kIsSanitizeThread) {
    return;
  }

  TestConstruction::reset();
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& threads = std::vector<std::thread>{};
  auto&& stop = std::atomic<bool>{false};
  auto&& barrier = std::atomic<std::uint64_t>{0};

  for (auto i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([&]() {
      for (auto j = 0; !stop.load(); ++j) {
        auto value = int{0};
        try {
          value = mutex.lock_combine([&]() {
            EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
            EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 1);
            std::this_thread::yield();
            SCOPE_EXIT {
              EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
            };
            EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 2);

            // we only throw an exception once every 3 times
            if (!(j % 3)) {
              throw ExceptionWithConstructionTrack{j};
            }

            return j;
          });
        } catch (std::exception& exc) {
          value = std::stoi(exc.what());
        }

        EXPECT_EQ(value, j);
      }
    }));
  }

  /* sleep override */
  std::this_thread::sleep_for(t);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace

TEST(DistributedMutex, TestExceptionPropagationStressTwoThreads) {
  concurrentExceptionPropagationStress(
      2, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, TestExceptionPropagationStressFourThreads) {
  concurrentExceptionPropagationStress(
      4, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, TestExceptionPropagationStressEightThreads) {
  concurrentExceptionPropagationStress(
      8, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, TestExceptionPropagationStressSixteenThreads) {
  concurrentExceptionPropagationStress(
      16, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, TestExceptionPropagationStressThirtyTwoThreads) {
  concurrentExceptionPropagationStress(
      32, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, TestExceptionPropagationStressSixtyFourThreads) {
  concurrentExceptionPropagationStress(
      64, std::chrono::seconds{kStressTestSeconds});
}

namespace {
std::array<std::uint64_t, 8> makeMonotonicArray(int start) {
  auto array = std::array<std::uint64_t, 8>{};
  for (auto& element : array) { element = start++; }
  return array;
}

template <template <typename> class Atom = std::atomic>
void concurrentBigValueReturnStress(
    int numThreads,
    std::chrono::milliseconds t) {
  auto&& mutex = folly::detail::distributed_mutex::DistributedMutex<Atom>{};
  auto&& threads = std::vector<std::thread>{};
  auto&& stop = std::atomic<bool>{false};
  auto&& barrier = std::atomic<std::uint64_t>{0};

  for (auto i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([&]() {
      auto&& value = std::atomic<std::uint64_t>{0};

      for (auto j = 0; !stop.load(); ++j) {
        auto returned = mutex.lock_combine([&]() {
          EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 0);
          EXPECT_EQ(barrier.fetch_add(1, std::memory_order_relaxed), 1);
          std::this_thread::yield();
          // return an entire cacheline worth of data
          auto current = value.fetch_add(1, std::memory_order_relaxed);
          SCOPE_EXIT {
            EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 1);
          };
          EXPECT_EQ(barrier.fetch_sub(1, std::memory_order_relaxed), 2);
          return makeMonotonicArray(static_cast<int>(current));
        });

        auto expected = value.load() - 1;
        for (auto& element : returned) {
          EXPECT_EQ(element, expected++);
        }
      }
    }));
  }

  /* sleep override */
  std::this_thread::sleep_for(t);
  stop.store(true);
  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace

TEST(DistributedMutex, StressBigValueReturnTwoThreads) {
  concurrentBigValueReturnStress(2, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressBigValueReturnFourThreads) {
  concurrentBigValueReturnStress(4, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressBigValueReturnEightThreads) {
  concurrentBigValueReturnStress(8, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressBigValueReturnSixteenThreads) {
  concurrentBigValueReturnStress(16, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressBigValueReturnThirtyTwoThreads) {
  concurrentBigValueReturnStress(32, std::chrono::seconds{kStressTestSeconds});
}
TEST(DistributedMutex, StressBigValueReturnSixtyFourThreads) {
  concurrentBigValueReturnStress(64, std::chrono::seconds{kStressTestSeconds});
}

} // namespace folly
#endif  // ROCKSDB_LITE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
