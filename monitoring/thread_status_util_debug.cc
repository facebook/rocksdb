// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>

#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NDEBUG
// the delay for debugging purpose.
static std::atomic<int> states_delay[ThreadStatus::NUM_STATE_TYPES];

void ThreadStatusUtil::TEST_SetStateDelay(const ThreadStatus::StateType state,
                                          int micro) {
  states_delay[state].store(micro, std::memory_order_relaxed);
}

void ThreadStatusUtil::TEST_StateDelay(const ThreadStatus::StateType state) {
  auto delay = states_delay[state].load(std::memory_order_relaxed);
  if (delay > 0) {
    SystemClock::Default()->SleepForMicroseconds(delay);
  }
}

Env::IOActivity ThreadStatusUtil::TEST_GetExpectedIOActivity(
    ThreadStatus::OperationType thread_op) {
  switch (thread_op) {
    case ThreadStatus::OperationType::OP_FLUSH:
      return Env::IOActivity::kFlush;
    case ThreadStatus::OperationType::OP_COMPACTION:
      return Env::IOActivity::kCompaction;
    case ThreadStatus::OperationType::OP_DBOPEN:
      return Env::IOActivity::kDBOpen;
    default:
      return Env::IOActivity::kUnknown;
  }
}

#endif  // !NDEBUG

}  // namespace ROCKSDB_NAMESPACE
