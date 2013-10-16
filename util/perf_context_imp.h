//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
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
