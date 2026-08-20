//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>

#include "db/write_thread.h"
#include "rocksdb/io_status.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

class WalWriteRequest {
 public:
  virtual ~WalWriteRequest() = default;
  virtual IOStatus Run() = 0;
};

enum class TicketState : uint8_t {
  kQueued,
  kWalWriting,
  kWalFinished,
  kRetiring,
  kAcknowledged,
  kCancelled,
};

struct WriteTicket {
  WriteTicket(WriteBatch* write_batch, WriteThread::Writer* write_thread_writer,
              WalWriteRequest* wal_write_request)
      : batch(write_batch),
        writer(write_thread_writer),
        wal_write_request_(wal_write_request) {}

  WriteTicket(const WriteTicket&) = delete;
  WriteTicket& operator=(const WriteTicket&) = delete;

  ~WriteTicket() {
    wal_status.PermitUncheckedError();
    final_status_.PermitUncheckedError();
  }

  IOStatus RunWalWrite() {
    assert(wal_write_request_ != nullptr);
    assert(state.load(std::memory_order_acquire) == TicketState::kQueued);
    state.store(TicketState::kWalWriting, std::memory_order_release);
    return wal_write_request_->Run();
  }

  void MarkLaneAdmitted() {
    lane_admitted_.store(true, std::memory_order_release);
  }

  bool WasLaneAdmitted() const {
    return lane_admitted_.load(std::memory_order_acquire);
  }

  void SetWalResult(IOStatus status) {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    const TicketState current = state.load(std::memory_order_acquire);
    assert(current == TicketState::kQueued ||
           current == TicketState::kWalWriting);
    wal_status = std::move(status);
    state.store(TicketState::kWalFinished, std::memory_order_release);
  }

  IOStatus WalResult() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    assert(state.load(std::memory_order_acquire) == TicketState::kRetiring);
    return wal_status;
  }

  bool TryMarkRetiring() {
    TicketState expected = TicketState::kWalFinished;
    return state.compare_exchange_strong(expected, TicketState::kRetiring,
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire);
  }

  void SetFinalResult(IOStatus status) {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    assert(state.load(std::memory_order_acquire) == TicketState::kRetiring);
    final_status_ = std::move(status);
    state.store(TicketState::kAcknowledged, std::memory_order_release);
    // Notify while holding the mutex so the stack-owned ticket cannot be
    // destroyed by its waiter before notify_one() completes.
    wait_cv_.notify_one();
  }

  void PrepareCancelledResult(const Status& status) {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    final_status_ = status_to_io_status(Status(status));
  }

  void MarkCancelled() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    assert(state.load(std::memory_order_acquire) != TicketState::kAcknowledged);
    state.store(TicketState::kCancelled, std::memory_order_release);
    // See SetFinalResult() for why notification happens under the lock.
    wait_cv_.notify_one();
  }

  void WaitUntilAcknowledged() {
    std::unique_lock<std::mutex> lock(wait_mutex_);
    wait_cv_.wait(lock, [this] {
      const TicketState current = state.load(std::memory_order_acquire);
      return current == TicketState::kAcknowledged ||
             current == TicketState::kCancelled;
    });
  }

  IOStatus FinalStatus() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    const TicketState current = state.load(std::memory_order_acquire);
    assert(current == TicketState::kAcknowledged ||
           current == TicketState::kCancelled);
    return final_status_;
  }

  WriteBatch* batch;  // Or an owned immutable WAL payload

  // Information needed to wake the blocked DB::Write() caller.
  WriteThread::Writer* writer;

  std::atomic<TicketState> state{TicketState::kQueued};

  // Written by the WAL lane before publishing kWalFinished.
  IOStatus wal_status;

 private:
  // The request and the public pointer fields are owned by the blocked caller
  // and remain valid until WaitUntilAcknowledged() returns.
  WalWriteRequest* wal_write_request_;
  IOStatus final_status_;
  std::atomic<bool> lane_admitted_{false};
  std::mutex wait_mutex_;
  std::condition_variable wait_cv_;
};

}  // namespace ROCKSDB_NAMESPACE
