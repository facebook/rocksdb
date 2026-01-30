//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/sequence_visibility_tracker.h"

#include <thread>

#include "db/version_set.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

SequenceVisibilityTracker::SequenceVisibilityTracker(
    VersionSet* versions, MemtableInsertionCompletionQueue* queue)
    : versions_(versions), queue_(queue) {
  assert(queue_ != nullptr);
}

SequenceVisibilityTracker::~SequenceVisibilityTracker() { Stop(); }

Status SequenceVisibilityTracker::Start() {
  TEST_SYNC_POINT("SequenceVisibilityTracker::Start:Begin");

  if (running_.load()) {
    return Status::InvalidArgument("Tracker is already running");
  }

  shutdown_.store(false);
  running_.store(true);

  bg_thread_.reset(
      new port::Thread(&SequenceVisibilityTracker::BackgroundThreadFunc, this));

  TEST_SYNC_POINT("SequenceVisibilityTracker::Start:End");
  return Status::OK();
}

void SequenceVisibilityTracker::Stop() {
  TEST_SYNC_POINT("SequenceVisibilityTracker::Stop:Begin");

  if (!running_.load()) {
    return;
  }

  shutdown_.store(true);

  // Shutdown the queue to unblock the background thread
  if (queue_ != nullptr) {
    queue_->Shutdown();
  }

  // Wait for background thread to exit
  if (bg_thread_ != nullptr && bg_thread_->joinable()) {
    bg_thread_->join();
  }

  running_.store(false);

  TEST_SYNC_POINT("SequenceVisibilityTracker::Stop:End");
}

SequenceNumber SequenceVisibilityTracker::GetVisibleSequence() const {
  return visible_sequence_.load(std::memory_order_acquire);
}

bool SequenceVisibilityTracker::IsRunning() const { return running_.load(); }

void SequenceVisibilityTracker::BackgroundThreadFunc() {
  TEST_SYNC_POINT("SequenceVisibilityTracker::BackgroundThreadFunc:Start");

  while (!shutdown_.load()) {
    TEST_SYNC_POINT(
        "SequenceVisibilityTracker::BackgroundThreadFunc:BeforeDequeue");

    // Step 1: Dequeue message (blocks if queue is empty)
    auto msg = queue_->DequeueHead();

    if (msg == nullptr) {
      // Queue was shutdown or returned nullptr
      TEST_SYNC_POINT(
          "SequenceVisibilityTracker::BackgroundThreadFunc:NullMessage");
      break;
    }

    TEST_SYNC_POINT(
        "SequenceVisibilityTracker::BackgroundThreadFunc:AfterDequeue");

    // Step 2: Wait for memtable insertion to complete
    WaitForMemtableCompletion(msg);

    TEST_SYNC_POINT(
        "SequenceVisibilityTracker::BackgroundThreadFunc:"
        "AfterWaitForCompletion");

    if (shutdown_.load()) {
      break;
    }

    // Step 3: Update versions_->SetLastSequence if versions_ is available
    // Only update if the new sequence is greater than the current one
    // to avoid assertion failures in SetLastSequence
    if (versions_ != nullptr) {
      SequenceNumber current_seq = versions_->LastSequence();
      if (msg->last_sequence > current_seq) {
        versions_->SetLastSequence(msg->last_sequence);
      }
    }

    // Step 4: Update visible_sequence_ to msg->last_sequence
    visible_sequence_.store(msg->last_sequence, std::memory_order_release);

    TEST_SYNC_POINT(
        "SequenceVisibilityTracker::BackgroundThreadFunc:AfterUpdateSequence");

    // Step 5: Set msg->sequence_visible = true and signal msg->cv
    {
      std::lock_guard<std::mutex> lock(msg->mutex);
      msg->sequence_visible.store(true);
      msg->cv.notify_all();
    }

    TEST_SYNC_POINT(
        "SequenceVisibilityTracker::BackgroundThreadFunc:"
        "AfterSignalVisibility");
  }

  TEST_SYNC_POINT("SequenceVisibilityTracker::BackgroundThreadFunc:End");
}

void SequenceVisibilityTracker::WaitForMemtableCompletion(
    const std::shared_ptr<MemtableInsertionMessage>& msg) {
  TEST_SYNC_POINT("SequenceVisibilityTracker::WaitForMemtableCompletion:Start");

  // Spin-wait with yield for memtable insertion to complete
  while (!msg->memtable_insertion_completed.load() && !shutdown_.load()) {
    std::this_thread::yield();
  }

  TEST_SYNC_POINT("SequenceVisibilityTracker::WaitForMemtableCompletion:End");
}

}  // namespace ROCKSDB_NAMESPACE
