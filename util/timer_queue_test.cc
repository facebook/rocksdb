//////////////////////////////////////////
// And a small example just adding and cancelling timers…

//////////////////////////////////////////
#include "timer_queue.h"
#include <future>

namespace Timing {

using Clock = std::chrono::high_resolution_clock;
static thread_local Clock::time_point ms_previous;
double now() {
  static auto start = Clock::now();
  return std::chrono::duration<double, std::milli>(Clock::now() - start)
      .count();
}

void sleep(unsigned ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

}  // namespace Timing

int main() {
  TimerQueue q;

  q.start();
  double tnow = Timing::now();

  q.add(10000, [tnow](bool aborted) mutable {
    printf("T 1: %d, Elapsed %4.2fms\n", (int)aborted, Timing::now() - tnow);
    return std::make_pair(false, 0);
  });
  q.add(10001, [tnow](bool aborted) mutable {
    printf("T 2: %d, Elapsed %4.2fms\n", (int)aborted, Timing::now() - tnow);
    return std::make_pair(false, 0);
  });

  q.add(1000, [tnow](bool aborted) mutable {
    printf("T 3: %d, Elapsed %4.2fms\n", (int)aborted, Timing::now() - tnow);
    return std::make_pair(!aborted, 1000);
  });

  auto id = q.add(2000, [tnow](bool aborted) mutable {
    printf("T 4: %d, Elapsed %4.2fms\n", (int)aborted, Timing::now() - tnow);
    return std::make_pair(!aborted, 2000);
  });

  (void)id;
  // auto ret = q.cancel(id);
  // assert(ret == 1);

  Timing::sleep(20000);
  q.cancelAll();
  Timing::sleep(10000);

  return 0;
}
//////////////////////////////////////////
