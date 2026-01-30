//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Message passed between writer threads and the visibility tracker.
// A writer thread creates a message with the sequence range it's inserting,
// enqueues it, performs the memtable insertion, then marks
// memtable_insertion_completed. The visibility tracker dequeues messages, waits
// for completion, then marks sequence_visible to notify the writer.
struct MemtableInsertionMessage {
  SequenceNumber first_sequence;
  SequenceNumber last_sequence;
  std::atomic<bool> memtable_insertion_completed{false};
  std::atomic<bool> sequence_visible{false};
  std::mutex mutex;
  std::condition_variable cv;

  MemtableInsertionMessage() = default;
  MemtableInsertionMessage(SequenceNumber first, SequenceNumber last)
      : first_sequence(first), last_sequence(last) {}

  // Non-copyable and non-movable due to mutex and condition_variable
  MemtableInsertionMessage(const MemtableInsertionMessage&) = delete;
  MemtableInsertionMessage& operator=(const MemtableInsertionMessage&) = delete;
  MemtableInsertionMessage(MemtableInsertionMessage&&) = delete;
  MemtableInsertionMessage& operator=(MemtableInsertionMessage&&) = delete;
};

// Thread-safe queue for tracking memtable insertion and visibility signaling.
// Used in the partitioned WAL pipeline to coordinate between:
// 1. Writer threads that enqueue messages and wait for visibility
// 2. The visibility tracker thread that processes completions in order
class MemtableInsertionCompletionQueue {
 public:
  MemtableInsertionCompletionQueue() = default;
  ~MemtableInsertionCompletionQueue() = default;

  // Non-copyable and non-movable
  MemtableInsertionCompletionQueue(const MemtableInsertionCompletionQueue&) =
      delete;
  MemtableInsertionCompletionQueue& operator=(
      const MemtableInsertionCompletionQueue&) = delete;

  // Enqueue a message to the queue. Thread-safe.
  void Enqueue(std::shared_ptr<MemtableInsertionMessage> msg);

  // Dequeue the head of the queue. Blocks if the queue is empty.
  // Returns nullptr if the queue has been shutdown.
  std::shared_ptr<MemtableInsertionMessage> DequeueHead();

  // Wait for the given message's sequence_visible flag to become true.
  // Returns immediately if already visible or if shutdown has been called.
  void WaitForVisibility(std::shared_ptr<MemtableInsertionMessage> msg);

  // Shutdown the queue, unblocking all waiters.
  // After this call, DequeueHead returns nullptr and WaitForVisibility returns.
  void Shutdown();

  // Check if the queue has been shutdown.
  bool IsShutdown() const;

 private:
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::deque<std::shared_ptr<MemtableInsertionMessage>> queue_;
  std::atomic<bool> shutdown_{false};
};

}  // namespace ROCKSDB_NAMESPACE
