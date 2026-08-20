//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstddef>
#include <deque>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "db/write_ticket.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct WalLane;

class ParallelWalAckQueue {
 public:
  struct Entry {
    WriteTicket* ticket = nullptr;
    WalLane* lane = nullptr;
    size_t charged_bytes = 0;
  };

  // A zero limit disables that admission bound.
  ParallelWalAckQueue(size_t max_inflight_batches, size_t max_inflight_bytes)
      : progress_cv_(&mutex_),
        space_available_cv_(&mutex_),
        max_inflight_batches_(max_inflight_batches),
        max_inflight_bytes_(max_inflight_bytes) {}

  // Called during admission, before the lane can start the ticket.
  Status Enqueue(Entry entry) {
    if (entry.ticket == nullptr || entry.lane == nullptr) {
      return Status::InvalidArgument(
          "WAL acknowledgement entry requires a ticket and lane");
    }

    InstrumentedMutexLock lock(&mutex_);
    while (!shutting_down_ && !CanEnqueue(entry.charged_bytes)) {
      space_available_cv_.Wait();
    }
    if (shutting_down_) {
      return Status::ShutdownInProgress(
          "WAL acknowledgement queue is shutting down");
    }
    if (entry.charged_bytes >
        std::numeric_limits<size_t>::max() - inflight_bytes_) {
      return Status::InvalidArgument(
          "WAL acknowledgement byte accounting overflow");
    }

    entries_.push_back(entry);
    ++inflight_batches_;
    inflight_bytes_ += entry.charged_bytes;
    return Status::OK();
  }

  // Called by a lane after append/sync finishes.
  void MarkWalFinished(WriteTicket* ticket, IOStatus status) {
    assert(ticket != nullptr);
    InstrumentedMutexLock lock(&mutex_);
    ticket->SetWalResult(std::move(status));
    progress_cv_.SignalAll();
  }

  // Called only by the acknowledgement worker.
  bool WaitForRetirableHead() {
    InstrumentedMutexLock lock(&mutex_);
    while (true) {
      if (aborting_) {
        return false;
      }
      if (!entries_.empty() && IsWalFinished(entries_.front().ticket)) {
        return true;
      }
      if (shutting_down_ && entries_.empty()) {
        return false;
      }
      progress_cv_.Wait();
    }
  }

  std::optional<Entry> TryClaimFront() {
    InstrumentedMutexLock lock(&mutex_);
    if (aborting_ || claimed_ticket_ != nullptr || entries_.empty()) {
      return std::nullopt;
    }

    WriteTicket* ticket = entries_.front().ticket;
    if (!ticket->TryMarkRetiring()) {
      return std::nullopt;
    }
    claimed_ticket_ = ticket;
    return entries_.front();
  }

  void PopClaimedFront(WriteTicket* ticket) {
    InstrumentedMutexLock lock(&mutex_);
    assert(ticket != nullptr);
    assert(claimed_ticket_ == ticket);
    assert(!entries_.empty());
    assert(entries_.front().ticket == ticket);
    assert(inflight_batches_ > 0);
    assert(inflight_bytes_ >= entries_.front().charged_bytes);

    --inflight_batches_;
    inflight_bytes_ -= entries_.front().charged_bytes;
    entries_.pop_front();
    claimed_ticket_ = nullptr;

    space_available_cv_.SignalAll();
    progress_cv_.SignalAll();
  }

  // Error and DB shutdown handling.
  //
  // AbortAll() stops admission and removes all unclaimed entries. It records
  // their final status but deliberately does not wake their callers: the
  // caller must first quiesce every lane that can still reference the returned
  // tickets, then invoke WriteTicket::MarkCancelled() for each entry.
  std::vector<Entry> AbortAll(const Status& status) {
    InstrumentedMutexLock lock(&mutex_);
    shutting_down_ = true;
    aborting_ = true;
    progress_cv_.SignalAll();
    space_available_cv_.SignalAll();

    // Do not race an entry that the acknowledgement worker has already
    // claimed. It will finish normally before the remaining queue is aborted.
    while (claimed_ticket_ != nullptr) {
      progress_cv_.Wait();
    }

    std::vector<Entry> aborted(entries_.begin(), entries_.end());
    for (const Entry& entry : aborted) {
      entry.ticket->PrepareCancelledResult(status);
    }
    entries_.clear();
    inflight_batches_ = 0;
    inflight_bytes_ = 0;

    progress_cv_.SignalAll();
    space_available_cv_.SignalAll();
    return aborted;
  }

  // Graceful shutdown: reject new entries while allowing admitted entries to
  // become WAL-finished and retire in order.
  void Shutdown() {
    InstrumentedMutexLock lock(&mutex_);
    shutting_down_ = true;
    progress_cv_.SignalAll();
    space_available_cv_.SignalAll();
  }

 private:
  bool CanEnqueue(size_t charged_bytes) const {
    // An oversized ticket is allowed when it is the only entry. Otherwise it
    // could wait forever for a byte limit it can never satisfy.
    if (entries_.empty()) {
      return true;
    }
    if (max_inflight_batches_ != 0 &&
        inflight_batches_ >= max_inflight_batches_) {
      return false;
    }
    if (max_inflight_bytes_ != 0 &&
        (charged_bytes > max_inflight_bytes_ ||
         inflight_bytes_ > max_inflight_bytes_ - charged_bytes)) {
      return false;
    }
    return true;
  }

  static bool IsWalFinished(WriteTicket* ticket) {
    return ticket->state.load(std::memory_order_acquire) ==
           TicketState::kWalFinished;
  }

  InstrumentedMutex mutex_;
  InstrumentedCondVar progress_cv_;
  InstrumentedCondVar space_available_cv_;

  std::deque<Entry> entries_;

  size_t inflight_batches_ = 0;
  size_t inflight_bytes_ = 0;

  const size_t max_inflight_batches_;
  const size_t max_inflight_bytes_;

  WriteTicket* claimed_ticket_ = nullptr;
  bool shutting_down_ = false;
  bool aborting_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
