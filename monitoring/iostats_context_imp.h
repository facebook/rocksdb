//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "rocksdb/iostats_context.h"

#if !defined(NIOSTATS_CONTEXT)
namespace ROCKSDB_NAMESPACE {
extern thread_local IOStatsContext iostats_context;
// It is not consistent that whether iostats follows PerfLevel.Timer counters
// follows it but BackupEngine relies on counter metrics to always be there.
// Here we create a backdoor option to disable some counters, so that some
// existing statsare not polluted by file operations, such as logging, by
// turning this off.
extern thread_local bool enable_iostats;
}  // namespace ROCKSDB_NAMESPACE

// increment a specific counter by the specified value
#define IOSTATS_ADD(metric, value)   \
  if (enable_iostats) {              \
    iostats_context.metric += value; \
  }

// reset a specific counter to zero
#define IOSTATS_RESET(metric) (iostats_context.metric = 0)

// reset all counters to zero
#define IOSTATS_RESET_ALL() (iostats_context.Reset())

#define IOSTATS_SET_THREAD_POOL_ID(value) \
  (iostats_context.thread_pool_id = value)

#define IOSTATS_THREAD_POOL_ID() (iostats_context.thread_pool_id)

#define IOSTATS(metric) (iostats_context.metric)

// Declare and set start time of the timer
#define IOSTATS_TIMER_GUARD(metric)                                     \
  PerfStepTimer iostats_step_timer_##metric(&(iostats_context.metric)); \
  iostats_step_timer_##metric.Start();

// Declare and set start time of the timer
#define IOSTATS_CPU_TIMER_GUARD(metric, clock)         \
  PerfStepTimer iostats_step_timer_##metric(           \
      &(iostats_context.metric), clock, true,          \
      PerfLevel::kEnableTimeAndCPUTimeExceptForMutex); \
  iostats_step_timer_##metric.Start();

#else  // !NIOSTATS_CONTEXT

#define IOSTATS_ADD(metric, value)
#define IOSTATS_ADD_IF_POSITIVE(metric, value)
#define IOSTATS_RESET(metric)
#define IOSTATS_RESET_ALL()
#define IOSTATS_SET_THREAD_POOL_ID(value)
#define IOSTATS_THREAD_POOL_ID()
#define IOSTATS(metric) 0

#define IOSTATS_TIMER_GUARD(metric)
#define IOSTATS_CPU_TIMER_GUARD(metric, clock) static_cast<void>(clock)

#endif  // !NIOSTATS_CONTEXT
