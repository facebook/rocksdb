//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace rocksdb {

#if defined(NPERF_CONTEXT) || defined(IOS_CROSS_COMPILE)

#define PERF_TIMER_DECLARE()
#define PERF_TIMER_START(metric)
#define PERF_TIMER_AUTO(metric)
#define PERF_TIMER_MEASURE(metric)
#define PERF_TIMER_STOP(metric)
#define PERF_COUNTER_ADD(metric, value)

#else

extern __thread PerfLevel perf_level;

class PerfStepTimer {
 public:
  PerfStepTimer()
    : enabled_(perf_level >= PerfLevel::kEnableTime),
      env_(enabled_ ? Env::Default() : nullptr),
      start_(0) {
  }

  void Start() {
    if (enabled_) {
      start_ = env_->NowNanos();
    }
  }

  void Measure(uint64_t* metric) {
    if (start_) {
      uint64_t now = env_->NowNanos();
      *metric += now - start_;
      start_ = now;
    }
  }

  void Stop(uint64_t* metric) {
    if (start_) {
      *metric += env_->NowNanos() - start_;
      start_ = 0;
    }
  }

 private:
  const bool enabled_;
  Env* const env_;
  uint64_t start_;
};

// Declare the local timer object to be used later on
#define PERF_TIMER_DECLARE()           \
  PerfStepTimer perf_step_timer;

// Set start time of the timer
#define PERF_TIMER_START(metric)          \
  perf_step_timer.Start();

// Declare and set start time of the timer
#define PERF_TIMER_AUTO(metric)           \
  PerfStepTimer perf_step_timer;          \
  perf_step_timer.Start();

// Update metric with time elapsed since last START. start time is reset
// to current timestamp.
#define PERF_TIMER_MEASURE(metric)        \
  perf_step_timer.Measure(&(perf_context.metric));

// Update metric with time elapsed since last START. But start time is not set.
#define PERF_TIMER_STOP(metric)        \
  perf_step_timer.Stop(&(perf_context.metric));

// Increase metric value
#define PERF_COUNTER_ADD(metric, value)     \
  perf_context.metric += value;

#endif

}
