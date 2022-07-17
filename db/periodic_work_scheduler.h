//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "util/timer.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;

using PeriodicTaskFunc = std::function<void()>;

constexpr uint64_t kInvalidPeriodSec = 0;

enum class PeriodicTaskType : uint8_t {
  kDumpStats = 0,
  kPersistStats,
  kFlushInfoLog,
  kRecordSeqnoTime,
  kMax,
};

// PeriodicTaskScheduler is a singleton object, which is scheduling/running
// DumpStats(), PersistStats(), and FlushInfoLog() for all DB instances. All DB
// instances use the same object from `Default()`.
//
// Internally, it uses a single threaded timer object to run the periodic work
// functions. Timer thread will always be started since the info log flushing
// cannot be disabled.
class PeriodicTaskScheduler {
 public:
  explicit PeriodicTaskScheduler(Env& env) : env_(env) {}

  Status Register(PeriodicTaskType task_type, const PeriodicTaskFunc& fn);

  Status Register(PeriodicTaskType task_type, const PeriodicTaskFunc& fn,
                  uint64_t repeat_period_seconds);

  Status Unregister(PeriodicTaskType task_type);

  void TEST_OverrideTimer(SystemClock* clock);

  void TEST_WaitForRun(std::function<void()> callback) const {
    if (timer_ != nullptr) {
      timer_->TEST_WaitForRun(callback);
    }
  }

  size_t TEST_GetValidTaskNum() const {
    if (timer_ != nullptr) {
      return timer_->TEST_GetPendingTaskNum();
    }
    return 0;
  }

  bool TEST_HasTask(PeriodicTaskType task_type) const {
    auto it = tasks_map_.find(task_type);
    return it != tasks_map_.end();
  }

  // Periodically flush info log out of application buffer at a low frequency.
  // This improves debuggability in case of RocksDB hanging since it ensures the
  // log messages leading up to the hang will eventually become visible in the
  // log.

 private:
  Timer* Default() {
    static Timer timer(SystemClock::Default().get());
    return &timer;
  }

  struct TaskInfo {
    TaskInfo(std::string _name, uint64_t _repeat_every_sec)
        : name(std::move(_name)), repeat_every_sec(_repeat_every_sec) {}
    std::string name;
    uint64_t repeat_every_sec;
  };

  std::map<PeriodicTaskType, TaskInfo> tasks_map_;

  Timer* timer_ = Default();

  Env& env_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
