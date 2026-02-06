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
#include <memory>

#include "db/memtable_insertion_completion_queue.h"
#include "port/port.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class VersionSet;

// Dedicated background thread that processes the
// MemtableInsertionCompletionQueue and updates the global visible sequence
// number.
//
// The tracker dequeues messages from the queue in order, waits for each
// message's memtable insertion to complete, then updates the visible sequence
// number in VersionSet and signals the writer thread that the sequence is
// now visible.
//
// This ensures that sequences become visible in strict order, even when
// memtable insertions complete out of order in the partitioned WAL pipeline.
class SequenceVisibilityTracker {
 public:
  // Creates a SequenceVisibilityTracker.
  // @param versions: VersionSet to update with visible sequence numbers.
  //                  Can be nullptr for testing without VersionSet.
  // @param queue: The MemtableInsertionCompletionQueue to dequeue from.
  SequenceVisibilityTracker(VersionSet* versions,
                            MemtableInsertionCompletionQueue* queue);
  ~SequenceVisibilityTracker();

  // Non-copyable and non-movable
  SequenceVisibilityTracker(const SequenceVisibilityTracker&) = delete;
  SequenceVisibilityTracker& operator=(const SequenceVisibilityTracker&) =
      delete;
  SequenceVisibilityTracker(SequenceVisibilityTracker&&) = delete;
  SequenceVisibilityTracker& operator=(SequenceVisibilityTracker&&) = delete;

  // Start the background thread that processes the queue.
  // Returns OK on success, or an error if the thread is already running.
  Status Start();

  // Gracefully shutdown the tracker.
  // This will shutdown the queue and wait for the background thread to exit.
  void Stop();

  // Returns the current visible sequence number.
  // This is thread-safe and can be called from any thread.
  SequenceNumber GetVisibleSequence() const;

  // Returns true if the background thread is running.
  bool IsRunning() const;

 private:
  // Background thread function that continuously processes the queue.
  void BackgroundThreadFunc();

  // Wait for a message's memtable insertion to complete.
  // Uses spinning with yield for efficiency.
  void WaitForMemtableCompletion(
      const std::shared_ptr<MemtableInsertionMessage>& msg);

  VersionSet* versions_;
  MemtableInsertionCompletionQueue* queue_;
  std::unique_ptr<port::Thread> bg_thread_;
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> running_{false};
  std::atomic<SequenceNumber> visible_sequence_{0};
};

}  // namespace ROCKSDB_NAMESPACE
