//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// And a small example just adding and cancelling timers

//////////////////////////////////////////
#include "util/timer_queue.h"
#include <future>

namespace Timing {

using Clock = std::chrono::high_resolution_clock;
static thread_local Clock::time_point ms_previous;
double now() {
  static auto start = Clock::now();
  return std::chrono::duration<double, std::milli>(Clock::now() - start)
      .count();
}

}  // namespace Timing

int main() {
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

  return 0;
}
//////////////////////////////////////////
