//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <queue>

#include "db/write_ticket.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;

namespace log {
class Writer;
}  // namespace log

struct WalLane {
  WalLane() : work_available_cv_(&mutex_) {}

  WalLane(const WalLane&) = delete;
  WalLane& operator=(const WalLane&) = delete;

  Status Enqueue(WriteTicket* ticket) {
    assert(ticket != nullptr);
    InstrumentedMutexLock lock(&mutex_);
    if (shutting_down_) {
      return Status::ShutdownInProgress("WAL lane is shutting down");
    }
    ticket->MarkLaneAdmitted();
    pending_.push(ticket);
    work_available_cv_.Signal();
    return Status::OK();
  }

  WriteTicket* WaitForWork() {
    InstrumentedMutexLock lock(&mutex_);
    while (current_ticket_ != nullptr || pending_.empty()) {
      if (shutting_down_ && current_ticket_ == nullptr && pending_.empty()) {
        return nullptr;
      }
      work_available_cv_.Wait();
    }
    current_ticket_ = pending_.front();
    pending_.pop();
    return current_ticket_;
  }

  void FinishWork(WriteTicket* ticket) {
    InstrumentedMutexLock lock(&mutex_);
    assert(current_ticket_ == ticket);
    current_ticket_ = nullptr;
    work_available_cv_.SignalAll();
  }

  void Shutdown() {
    InstrumentedMutexLock lock(&mutex_);
    shutting_down_ = true;
    work_available_cv_.SignalAll();
  }

  // Reserved for the later phase where each lane owns a distinct WAL.
  WritableFileWriter* file = nullptr;
  log::Writer* log_writer = nullptr;

 private:
  InstrumentedMutex mutex_;
  InstrumentedCondVar work_available_cv_;
  WriteTicket* current_ticket_ = nullptr;
  std::queue<WriteTicket*> pending_;
  bool shutting_down_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
