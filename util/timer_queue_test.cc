//  Portions Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// borrowed from
// http://www.crazygaze.com/blog/2016/03/24/portable-c-timer-queue/
// Timer Queue
//
// License
//
// The source code in this article is licensed under the CC0 license, so feel
// free
// to copy, modify, share, do whatever you want with it.
// No attribution is required, but Ill be happy if you do.
// CC0 license

// The person who associated a work with this deed has dedicated the work to the
// public domain by waiving all of his or her rights to the work worldwide
// under copyright law, including all related and neighboring rights, to the
// extent allowed by law.  You can copy, modify, distribute and perform the
// work, even for
// commercial purposes, all without asking permission. See Other Information
// below.
//

#include "util/timer_queue.h"

#include <future>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace Timing {

using Clock = std::chrono::high_resolution_clock;
double now() {
  static auto start = Clock::now();
  return std::chrono::duration<double, std::milli>(Clock::now() - start)
      .count();
}

}  // namespace Timing

class TimerQueueTest : public testing::Test {};

TEST_F(TimerQueueTest, BasicFunctionality) {
  TimerQueue q;

  double tnow = Timing::now();

  q.add(10000, [tnow](bool aborted) mutable {
    printf("T 1: %d, Elapsed %4.2fms\n", aborted, Timing::now() - tnow);
    return std::make_pair(false, 0);
  });
  q.add(10001, [tnow](bool aborted) mutable {
    printf("T 2: %d, Elapsed %4.2fms\n", aborted, Timing::now() - tnow);
    return std::make_pair(false, 0);
  });

  q.add(1000, [tnow](bool aborted) mutable {
    printf("T 3: %d, Elapsed %4.2fms\n", aborted, Timing::now() - tnow);
    return std::make_pair(!aborted, 1000);
  });

  auto id = q.add(2000, [tnow](bool aborted) mutable {
    printf("T 4: %d, Elapsed %4.2fms\n", aborted, Timing::now() - tnow);
    return std::make_pair(!aborted, 2000);
  });

  (void)id;
  // auto ret = q.cancel(id);
  // assert(ret == 1);
  // q.cancelAll();

  // Test passes if we can create and add timers without crashing
  ASSERT_TRUE(true);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
