//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/memtable_insertion_completion_queue.h"

#include <atomic>
#include <thread>
#include <vector>

#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class MemtableInsertionCompletionQueueTest : public testing::Test {
 public:
  void SetUp() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
};

// Test basic enqueue and dequeue functionality
TEST_F(MemtableInsertionCompletionQueueTest, EnqueueDequeue) {
  MemtableInsertionCompletionQueue queue;

  auto msg1 = std::make_shared<MemtableInsertionMessage>(1, 5);
  auto msg2 = std::make_shared<MemtableInsertionMessage>(6, 10);

  queue.Enqueue(msg1);
  queue.Enqueue(msg2);

  auto dequeued1 = queue.DequeueHead();
  ASSERT_NE(dequeued1, nullptr);
  EXPECT_EQ(dequeued1->first_sequence, 1);
  EXPECT_EQ(dequeued1->last_sequence, 5);

  auto dequeued2 = queue.DequeueHead();
  ASSERT_NE(dequeued2, nullptr);
  EXPECT_EQ(dequeued2->first_sequence, 6);
  EXPECT_EQ(dequeued2->last_sequence, 10);
}

// Test that messages are dequeued in FIFO order
TEST_F(MemtableInsertionCompletionQueueTest, OrderPreserved) {
  MemtableInsertionCompletionQueue queue;

  const int kNumMessages = 100;
  for (int i = 0; i < kNumMessages; i++) {
    auto msg = std::make_shared<MemtableInsertionMessage>(
        static_cast<SequenceNumber>(i * 10),
        static_cast<SequenceNumber>(i * 10 + 5));
    queue.Enqueue(msg);
  }

  // Dequeue and verify FIFO order
  for (int i = 0; i < kNumMessages; i++) {
    auto msg = queue.DequeueHead();
    ASSERT_NE(msg, nullptr);
    EXPECT_EQ(msg->first_sequence, static_cast<SequenceNumber>(i * 10));
    EXPECT_EQ(msg->last_sequence, static_cast<SequenceNumber>(i * 10 + 5));
  }
}

// Test WaitForVisibility using sync points
TEST_F(MemtableInsertionCompletionQueueTest, WaitForVisibilityWithSyncPoint) {
  MemtableInsertionCompletionQueue queue;

  auto msg = std::make_shared<MemtableInsertionMessage>(1, 10);
  queue.Enqueue(msg);

  std::atomic<bool> wait_started{false};
  std::atomic<bool> visibility_set{false};
  std::atomic<bool> wait_completed{false};

  // Use sync point to know when WaitForVisibility has started waiting
  SyncPoint::GetInstance()->SetCallBack(
      "MemtableInsertionCompletionQueue::WaitForVisibility:Start",
      [&](void* /*arg*/) { wait_started.store(true); });

  SyncPoint::GetInstance()->SetCallBack(
      "MemtableInsertionCompletionQueue::WaitForVisibility:End",
      [&](void* /*arg*/) { wait_completed.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  // Thread that waits for visibility
  std::thread waiter([&]() { queue.WaitForVisibility(msg); });

  // Wait for the waiter thread to enter WaitForVisibility
  while (!wait_started.load()) {
    std::this_thread::yield();
  }

  // Verify wait has not completed yet
  EXPECT_FALSE(wait_completed.load());

  // Now set visibility and notify
  {
    std::lock_guard<std::mutex> lock(msg->mutex);
    msg->sequence_visible.store(true);
    visibility_set.store(true);
    msg->cv.notify_all();
  }

  waiter.join();

  // Verify wait completed after visibility was set
  EXPECT_TRUE(wait_completed.load());
  EXPECT_TRUE(visibility_set.load());
}

// Test that shutdown unblocks all waiters
TEST_F(MemtableInsertionCompletionQueueTest, ShutdownUnblocksWaiters) {
  MemtableInsertionCompletionQueue queue;

  std::atomic<int> dequeue_started_count{0};
  std::atomic<int> dequeue_returned_null_count{0};
  std::atomic<int> visibility_wait_started_count{0};
  std::atomic<int> visibility_wait_ended_count{0};

  // Track dequeue attempts
  SyncPoint::GetInstance()->SetCallBack(
      "MemtableInsertionCompletionQueue::DequeueHead:Start",
      [&](void* /*arg*/) { dequeue_started_count.fetch_add(1); });

  SyncPoint::GetInstance()->SetCallBack(
      "MemtableInsertionCompletionQueue::DequeueHead:Shutdown",
      [&](void* /*arg*/) { dequeue_returned_null_count.fetch_add(1); });

  // Track visibility waits
  SyncPoint::GetInstance()->SetCallBack(
      "MemtableInsertionCompletionQueue::WaitForVisibility:Start",
      [&](void* /*arg*/) { visibility_wait_started_count.fetch_add(1); });

  SyncPoint::GetInstance()->SetCallBack(
      "MemtableInsertionCompletionQueue::WaitForVisibility:End",
      [&](void* /*arg*/) { visibility_wait_ended_count.fetch_add(1); });

  SyncPoint::GetInstance()->EnableProcessing();

  // Create a message that will be waiting for visibility
  auto msg = std::make_shared<MemtableInsertionMessage>(1, 10);
  queue.Enqueue(msg);

  // Dequeue the message first
  auto dequeued = queue.DequeueHead();
  ASSERT_NE(dequeued, nullptr);

  // Start threads that will block
  std::thread dequeuer([&]() {
    auto result = queue.DequeueHead();
    EXPECT_EQ(result, nullptr);  // Should return nullptr after shutdown
  });

  std::thread visibility_waiter([&]() { queue.WaitForVisibility(msg); });

  // Wait for threads to start blocking
  while (dequeue_started_count.load() < 2 ||
         visibility_wait_started_count.load() < 1) {
    std::this_thread::yield();
  }

  // Shutdown should unblock all
  queue.Shutdown();

  dequeuer.join();
  visibility_waiter.join();

  EXPECT_TRUE(queue.IsShutdown());
  EXPECT_GE(dequeue_returned_null_count.load(), 1);
  EXPECT_GE(visibility_wait_ended_count.load(), 1);
}

// Test concurrent enqueue and dequeue operations
TEST_F(MemtableInsertionCompletionQueueTest, ConcurrentEnqueueDequeue) {
  MemtableInsertionCompletionQueue queue;

  const int kNumProducers = 4;
  const int kNumMessagesPerProducer = 25;
  const int kTotalMessages = kNumProducers * kNumMessagesPerProducer;
  std::atomic<int> total_enqueued{0};
  std::atomic<int> total_dequeued{0};
  std::atomic<bool> producers_done{false};

  std::vector<std::thread> producers;
  for (int p = 0; p < kNumProducers; p++) {
    producers.emplace_back([&, p]() {
      for (int i = 0; i < kNumMessagesPerProducer; i++) {
        auto msg = std::make_shared<MemtableInsertionMessage>(
            static_cast<SequenceNumber>(p * 1000 + i),
            static_cast<SequenceNumber>(p * 1000 + i + 1));
        queue.Enqueue(msg);
        total_enqueued.fetch_add(1);
      }
    });
  }

  // Wait for all producers to finish first
  for (auto& t : producers) {
    t.join();
  }
  producers_done.store(true);

  // Now consume all messages (queue has all messages, no blocking needed)
  while (total_dequeued.load() < kTotalMessages) {
    auto msg = queue.DequeueHead();
    if (msg == nullptr) {
      break;
    }
    total_dequeued.fetch_add(1);
  }

  queue.Shutdown();

  EXPECT_EQ(total_enqueued.load(), kTotalMessages);
  EXPECT_EQ(total_dequeued.load(), kTotalMessages);
}

// Test message completion and visibility flow
TEST_F(MemtableInsertionCompletionQueueTest, CompletionAndVisibilityFlow) {
  MemtableInsertionCompletionQueue queue;

  auto msg = std::make_shared<MemtableInsertionMessage>(100, 110);

  // Initially, insertion is not complete and not visible
  EXPECT_FALSE(msg->memtable_insertion_completed.load());
  EXPECT_FALSE(msg->sequence_visible.load());

  queue.Enqueue(msg);

  // Writer thread: enqueues, does "insertion", marks complete
  std::thread writer([&]() {
    // Simulate memtable insertion
    msg->memtable_insertion_completed.store(true);

    // Wait for visibility
    queue.WaitForVisibility(msg);

    // At this point, should be visible
    EXPECT_TRUE(msg->sequence_visible.load());
  });

  // Visibility tracker thread: dequeues, waits for completion, marks visible
  std::thread tracker([&]() {
    auto dequeued = queue.DequeueHead();
    ASSERT_NE(dequeued, nullptr);

    // Wait for memtable insertion to complete
    while (!dequeued->memtable_insertion_completed.load()) {
      std::this_thread::yield();
    }

    // Mark sequence as visible and notify
    {
      std::lock_guard<std::mutex> lock(dequeued->mutex);
      dequeued->sequence_visible.store(true);
      dequeued->cv.notify_all();
    }
  });

  writer.join();
  tracker.join();

  EXPECT_TRUE(msg->memtable_insertion_completed.load());
  EXPECT_TRUE(msg->sequence_visible.load());
}

// Test blocking dequeue on empty queue using sync point dependency
TEST_F(MemtableInsertionCompletionQueueTest, BlockingDequeueOnEmptyQueue) {
  MemtableInsertionCompletionQueue queue;

  std::atomic<bool> dequeue_started{false};
  std::shared_ptr<MemtableInsertionMessage> dequeued_msg;

  SyncPoint::GetInstance()->SetCallBack(
      "MemtableInsertionCompletionQueue::DequeueHead:Start",
      [&](void* /*arg*/) { dequeue_started.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  // Start a thread that will block on dequeue (queue is empty)
  std::thread dequeuer([&]() { dequeued_msg = queue.DequeueHead(); });

  // Wait for dequeue to start blocking
  while (!dequeue_started.load()) {
    std::this_thread::yield();
  }

  // Verify dequeue has started but not completed (dequeued_msg should still be
  // nullptr because the thread is blocked waiting)
  EXPECT_TRUE(dequeue_started.load());

  // Enqueue a message to unblock the dequeuer
  auto msg = std::make_shared<MemtableInsertionMessage>(1, 5);
  queue.Enqueue(msg);

  dequeuer.join();

  ASSERT_NE(dequeued_msg, nullptr);
  EXPECT_EQ(dequeued_msg->first_sequence, 1);
  EXPECT_EQ(dequeued_msg->last_sequence, 5);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
