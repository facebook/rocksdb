//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace rocksdb {

#if defined(NPERF_CONTEXT)

#define PERF_TIMER_GUARD(metric)
#define PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(metric, condition)
#define PERF_TIMER_MEASURE(metric)
#define PERF_TIMER_STOP(metric)
#define PERF_TIMER_START(metric)
#define PERF_COUNTER_ADD(metric, value)

#else

// Stop the timer and update the metric
#define PERF_TIMER_STOP(metric) perf_step_timer_##metric.Stop();

#define PERF_TIMER_START(metric) perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_TIMER_GUARD(metric)                                       \
  PerfStepTimer perf_step_timer_##metric(&(get_perf_context()->metric)); \
  perf_step_timer_##metric.Start();

#define PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(metric, condition)            \
  PerfStepTimer perf_step_timer_##metric(&(get_perf_context()->metric), true); \
  if ((condition)) {                                                         \
    perf_step_timer_##metric.Start();                                        \
  }

// Update metric with time elapsed since last START. start time is reset
// to current timestamp.
#define PERF_TIMER_MEASURE(metric) perf_step_timer_##metric.Measure();

// Increase metric value
#define PERF_COUNTER_ADD(metric, value) get_perf_context()->metric += value;

#endif

}
