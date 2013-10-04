#ifndef PERF_CONTEXT_IMP_H
#define PERF_CONTEXT_IMP_H

#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace rocksdb {

extern enum PerfLevel perf_level;

inline void StartPerfTimer(StopWatchNano* timer) {
  if (perf_level >= PerfLevel::kEnableTime) {
    timer->Start();
  }
}

inline void BumpPerfCount(uint64_t* count, uint64_t delta = 1) {
  if (perf_level >= PerfLevel::kEnableCount) {
    *count += delta;
  }
}

inline void BumpPerfTime(uint64_t* time,
                         StopWatchNano* timer,
                         bool reset = true) {
  if (perf_level >= PerfLevel::kEnableTime) {
    *time += timer->ElapsedNanos(reset);
  }
}

}

#endif
