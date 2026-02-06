//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/memtable_insertion_completion_queue.h"

#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

void MemtableInsertionCompletionQueue::Enqueue(
    std::shared_ptr<MemtableInsertionMessage> msg) {
  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::Enqueue:Start");
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    queue_.push_back(std::move(msg));
  }
  queue_cv_.notify_one();
  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::Enqueue:End");
}

std::shared_ptr<MemtableInsertionMessage>
MemtableInsertionCompletionQueue::DequeueHead() {
  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::DequeueHead:Start");
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait until queue is non-empty or shutdown is requested
  queue_cv_.wait(lock,
                 [this]() { return !queue_.empty() || shutdown_.load(); });

  if (shutdown_.load() && queue_.empty()) {
    TEST_SYNC_POINT("MemtableInsertionCompletionQueue::DequeueHead:Shutdown");
    return nullptr;
  }

  auto msg = queue_.front();
  queue_.pop_front();
  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::DequeueHead:End");
  return msg;
}

void MemtableInsertionCompletionQueue::WaitForVisibility(
    std::shared_ptr<MemtableInsertionMessage> msg) {
  if (!msg) {
    return;
  }

  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::WaitForVisibility:Start");

  std::unique_lock<std::mutex> lock(msg->mutex);
  msg->cv.wait(lock, [this, &msg]() {
    return msg->sequence_visible.load() || shutdown_.load();
  });

  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::WaitForVisibility:End");
}

void MemtableInsertionCompletionQueue::Shutdown() {
  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::Shutdown:Start");
  shutdown_.store(true);

  // Notify all threads waiting on the queue
  queue_cv_.notify_all();

  // Also notify any threads waiting for visibility on existing messages
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    for (auto& msg : queue_) {
      std::lock_guard<std::mutex> msg_lock(msg->mutex);
      msg->cv.notify_all();
    }
  }
  TEST_SYNC_POINT("MemtableInsertionCompletionQueue::Shutdown:End");
}

bool MemtableInsertionCompletionQueue::IsShutdown() const {
  return shutdown_.load();
}

}  // namespace ROCKSDB_NAMESPACE
