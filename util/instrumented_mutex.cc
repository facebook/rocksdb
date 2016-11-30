//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "util/instrumented_mutex.h"
#include "util/perf_context_imp.h"
#include "util/sync_point.h"
#include "util/thread_status_util.h"

namespace rocksdb {
namespace {
bool ShouldReportToStats(Env* env, Statistics* stats) {
  return env != nullptr && stats != nullptr &&
          stats->stats_level_ > kExceptTimeForMutex;
}
}  // namespace

void InstrumentedMutex::Lock() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(db_mutex_lock_nanos,
                                         stats_code_ == DB_MUTEX_WAIT_MICROS);
  uint64_t wait_time_micros = 0;
  if (ShouldReportToStats(env_, stats_)) {
    {
      StopWatch sw(env_, nullptr, 0, &wait_time_micros);
      LockInternal();
    }
    RecordTick(stats_, stats_code_, wait_time_micros);
  } else {
    LockInternal();
  }
}

void InstrumentedMutex::LockInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  mutex_.Lock();
}

void InstrumentedCondVar::Wait() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(db_condition_wait_nanos,
                                         stats_code_ == DB_MUTEX_WAIT_MICROS);
  uint64_t wait_time_micros = 0;
  if (ShouldReportToStats(env_, stats_)) {
    {
      StopWatch sw(env_, nullptr, 0, &wait_time_micros);
      WaitInternal();
    }
    RecordTick(stats_, stats_code_, wait_time_micros);
  } else {
    WaitInternal();
  }
}

void InstrumentedCondVar::WaitInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  cond_.Wait();
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us) {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(db_condition_wait_nanos,
                                         stats_code_ == DB_MUTEX_WAIT_MICROS);
  uint64_t wait_time_micros = 0;
  bool result = false;
  if (ShouldReportToStats(env_, stats_)) {
    {
      StopWatch sw(env_, nullptr, 0, &wait_time_micros);
      result = TimedWaitInternal(abs_time_us);
    }
    RecordTick(stats_, stats_code_, wait_time_micros);
  } else {
    result = TimedWaitInternal(abs_time_us);
  }
  return result;
}

bool InstrumentedCondVar::TimedWaitInternal(uint64_t abs_time_us) {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif

  TEST_SYNC_POINT_CALLBACK("InstrumentedCondVar::TimedWaitInternal",
                           &abs_time_us);

  return cond_.TimedWait(abs_time_us);
}

}  // namespace rocksdb
