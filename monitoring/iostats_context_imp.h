//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/iostats_context.h"

#ifndef IOS_CROSS_COMPILE

// increment a specific counter by the specified value
#define IOSTATS_ADD(metric, value)     \
  (iostats_context.metric += value)

// Increase metric value only when it is positive
#define IOSTATS_ADD_IF_POSITIVE(metric, value)   \
  if (value > 0) { IOSTATS_ADD(metric, value); }

// reset a specific counter to zero
#define IOSTATS_RESET(metric)          \
  (iostats_context.metric = 0)

// reset all counters to zero
#define IOSTATS_RESET_ALL()                        \
  (iostats_context.Reset())

#define IOSTATS_SET_THREAD_POOL_ID(value)      \
  (iostats_context.thread_pool_id = value)

#define IOSTATS_THREAD_POOL_ID()               \
  (iostats_context.thread_pool_id)

#define IOSTATS(metric)                        \
  (iostats_context.metric)

// Declare and set start time of the timer
#define IOSTATS_TIMER_GUARD(metric)                                       \
  PerfStepTimer iostats_step_timer_ ## metric(&(iostats_context.metric));  \
  iostats_step_timer_ ## metric.Start();

// Declare timer as a member of a class
#define IOSTATS_TIMER_DECL(metric)               \
  PerfStepTimer iostats_step_timer_ ## metric

// Init timer in the __ctor init list
#define IOSTATS_TIMER_INIT(metric)               \
  iostats_step_timer_ ## metric(&(iostats_context.metric))

// Start the timer
#define IOSTATS_TIMER_START(metric)               \
  iostats_step_timer_ ## metric.Start();

// Stop the timer and update the metric
#define IOSTATS_TIMER_STOP(metric)               \
  iostats_step_timer_ ## metric.Stop();

/// Async begin

// The following  METER macros operate on
// raw values instead of using PerfStepTimer
// This is because in async world we should
// capture the thread-local of one thread and
// charge it from another thread on io completion.
// Thus we capture the start time into the raw value
// and on completion we charge the completing thread

// Declares a raw value for measuring perf
#define IOSTATS_METER_DECL(metric)           \
  PerfMeter iostats_meter_ ## metric

// Inits raw value for as a member of the class
#define IOSTATS_METER_INIT(metric)           \
  iostats_meter_ ## metric()

#define IOSTATS_METER_START(metric)          \
  iostats_meter_ ## metric.Start()

#define IOSTATS_METER_MEASURE(metric)       \
  iostats_meter_ ## metric.Measure(&(iostats_context.metric))

#define IOSTATS_METER_STOP(metric)       \
  iostats_meter_ ## metric.Stop(&(iostats_context.metric))

/// Async end

#else  // IOS_CROSS_COMPILE

#define IOSTATS_ADD(metric, value)
#define IOSTATS_ADD_IF_POSITIVE(metric, value)
#define IOSTATS_RESET(metric)
#define IOSTATS_RESET_ALL()
#define IOSTATS_SET_THREAD_POOL_ID(value)
#define IOSTATS_THREAD_POOL_ID()
#define IOSTATS(metric) 0

#define IOSTATS_TIMER_GUARD(metric)
#define IOSTATS_TIMER_DECL(metric)
#define IOSTATS_TIMER_INIT(metric)
#define IOSTATS_TIMER_START(metric)
#define IOSTATS_TIMER_STOP(metric)

#define IOSTATS_METER_DECL(metric)
#define IOSTATS_METER_INIT(metric)
#define IOSTATS_METER_START(metric)
#define IOSTATS_METER_MEASURE(metric)
#define IOSTATS_METER_STOP(metric)

#endif  // IOS_CROSS_COMPILE
