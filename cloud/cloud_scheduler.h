//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <chrono>
#include <functional>
#include <memory>
#include <set>
#include <utility>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
// Class for scheduling jobs to run on a separate thread
class CloudScheduler {
 public:
  virtual ~CloudScheduler() {}

  // Schedules a job to run after "when" microseconds have elapsed,
  // invoking the specified callback with the specified arg
  // Returns a handle to the scheduled job so that it may be canceled
  virtual long ScheduleJob(std::chrono::microseconds when,
                           std::function<void(void *)> callback, void *arg) = 0;

  // Schedules a job to run after "when" microseconds have elapsed,
  // invoking the specified callback with the specified arg
  // Returns a handle to the scheduled job so that it may be canceled.
  // The callback will be invoked every frequency microseconds until
  // the job is canceled or the scheduler is shutdown
  virtual long ScheduleRecurringJob(std::chrono::microseconds when,
                                    std::chrono::microseconds frequency,
                                    std::function<void(void *)> callback,
                                    void *arg) = 0;

  // Cancels the job represented by handle.  Returns true if the job
  // was canceled, false otherwise.
  virtual bool CancelJob(long handle) = 0;

  // Returns true if the job represented by handle has not completed,
  // meaning that it is either currently schedule or actively running
  virtual bool IsScheduled(long handle) = 0;
  // Returns a new instance of a cloud scheduler.  The caller is responsible
  // for freeing the scheduler when it is no longer required.
  static std::shared_ptr<CloudScheduler> Get();
};
#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
