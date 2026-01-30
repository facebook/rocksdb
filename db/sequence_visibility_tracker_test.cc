//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/sequence_visibility_tracker.h"

#include <atomic>
#include <thread>
#include <vector>

#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class SequenceVisibilityTrackerTest : public testing::Test {
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

// Test that the tracker updates visible sequence when messages are processed
TEST_F(SequenceVisibilityTrackerTest, UpdatesVisibleSequence) {
  MemtableInsertionCompletionQueue queue;

  // Create tracker without VersionSet (nullptr is valid for testing)
  SequenceVisibilityTracker tracker(nullptr, &queue);

  // Initial visible sequence should be 0
  EXPECT_EQ(tracker.GetVisibleSequence(), 0);

  // Start the tracker
  ASSERT_OK(tracker.Start());
  EXPECT_TRUE(tracker.IsRunning());

  // Create and enqueue a message
  auto msg1 = std::make_shared<MemtableInsertionMessage>(1, 10);
  queue.Enqueue(msg1);

  // Mark memtable insertion as complete
  msg1->memtable_insertion_completed.store(true);

  // Wait for visibility
  queue.WaitForVisibility(msg1);

  // Verify visible sequence was updated
  EXPECT_EQ(tracker.GetVisibleSequence(), 10);
  EXPECT_TRUE(msg1->sequence_visible.load());

  // Test with a second message
  auto msg2 = std::make_shared<MemtableInsertionMessage>(11, 20);
  queue.Enqueue(msg2);
  msg2->memtable_insertion_completed.store(true);
  queue.WaitForVisibility(msg2);

  EXPECT_EQ(tracker.GetVisibleSequence(), 20);
  EXPECT_TRUE(msg2->sequence_visible.load());

  // Stop the tracker
  tracker.Stop();
  EXPECT_FALSE(tracker.IsRunning());
}

// Test that the tracker waits for memtable completion before updating sequence
TEST_F(SequenceVisibilityTrackerTest, WaitsForMemtableCompletion) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  std::atomic<bool> tracker_waiting_for_completion{false};
  std::atomic<bool> completion_signaled{false};

  // Set up sync point to detect when tracker is waiting for memtable completion
  SyncPoint::GetInstance()->SetCallBack(
      "SequenceVisibilityTracker::WaitForMemtableCompletion:Start",
      [&](void* /*arg*/) { tracker_waiting_for_completion.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(tracker.Start());

  // Create a message but don't mark memtable insertion complete
  auto msg = std::make_shared<MemtableInsertionMessage>(1, 100);
  queue.Enqueue(msg);

  // Wait for tracker to start waiting for completion
  while (!tracker_waiting_for_completion.load()) {
    std::this_thread::yield();
  }

  // At this point, tracker should be waiting; sequence should not be updated
  EXPECT_EQ(tracker.GetVisibleSequence(), 0);
  EXPECT_FALSE(msg->sequence_visible.load());

  // Now mark memtable insertion as complete
  msg->memtable_insertion_completed.store(true);
  completion_signaled.store(true);

  // Wait for visibility
  queue.WaitForVisibility(msg);

  // Now sequence should be updated
  EXPECT_EQ(tracker.GetVisibleSequence(), 100);
  EXPECT_TRUE(msg->sequence_visible.load());

  tracker.Stop();
}

// Test that messages are processed in order, maintaining sequence order
TEST_F(SequenceVisibilityTrackerTest, InOrderProcessing) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  const int kNumMessages = 10;
  std::vector<std::shared_ptr<MemtableInsertionMessage>> messages;

  ASSERT_OK(tracker.Start());

  // Create and enqueue all messages
  for (int i = 0; i < kNumMessages; i++) {
    auto msg = std::make_shared<MemtableInsertionMessage>(
        static_cast<SequenceNumber>(i * 10 + 1),
        static_cast<SequenceNumber>((i + 1) * 10));
    messages.push_back(msg);
    queue.Enqueue(msg);
  }

  // Mark all messages as insertion complete at once
  for (int i = 0; i < kNumMessages; i++) {
    messages[i]->memtable_insertion_completed.store(true);
  }

  // Wait for all to become visible
  for (int i = 0; i < kNumMessages; i++) {
    queue.WaitForVisibility(messages[i]);
  }

  // Verify all messages became visible
  for (int i = 0; i < kNumMessages; i++) {
    EXPECT_TRUE(messages[i]->sequence_visible.load())
        << "Message " << i << " not visible";
  }

  // Verify final visible sequence
  EXPECT_EQ(tracker.GetVisibleSequence(),
            static_cast<SequenceNumber>(kNumMessages * 10));

  tracker.Stop();
}

// Test that even when later messages complete before earlier ones,
// visibility is still signaled in order
TEST_F(SequenceVisibilityTrackerTest, OutOfOrderCompletionInOrderVisibility) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  std::vector<SequenceNumber> update_order;
  std::mutex order_mutex;

  // Track sequence updates via sync point
  SyncPoint::GetInstance()->SetCallBack(
      "SequenceVisibilityTracker::BackgroundThreadFunc:AfterUpdateSequence",
      [&](void* /*arg*/) {
        std::lock_guard<std::mutex> lock(order_mutex);
        update_order.push_back(tracker.GetVisibleSequence());
      });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(tracker.Start());

  // Create and enqueue messages
  auto msg0 = std::make_shared<MemtableInsertionMessage>(1, 10);
  auto msg1 = std::make_shared<MemtableInsertionMessage>(11, 20);
  auto msg2 = std::make_shared<MemtableInsertionMessage>(21, 30);

  queue.Enqueue(msg0);
  queue.Enqueue(msg1);
  queue.Enqueue(msg2);

  // Complete all messages - msg1 and msg2 complete before msg0
  // but visibility should still be signaled in order
  msg2->memtable_insertion_completed.store(true);
  msg1->memtable_insertion_completed.store(true);
  msg0->memtable_insertion_completed.store(true);

  // Wait for all to become visible
  queue.WaitForVisibility(msg0);
  queue.WaitForVisibility(msg1);
  queue.WaitForVisibility(msg2);

  // All should be visible now
  EXPECT_TRUE(msg0->sequence_visible.load());
  EXPECT_TRUE(msg1->sequence_visible.load());
  EXPECT_TRUE(msg2->sequence_visible.load());

  // Verify updates happened in order (10, 20, 30)
  {
    std::lock_guard<std::mutex> lock(order_mutex);
    ASSERT_EQ(update_order.size(), 3);
    EXPECT_EQ(update_order[0], 10);
    EXPECT_EQ(update_order[1], 20);
    EXPECT_EQ(update_order[2], 30);
  }

  tracker.Stop();
}

// Test graceful shutdown
TEST_F(SequenceVisibilityTrackerTest, GracefulShutdown) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  std::atomic<bool> bg_thread_started{false};
  std::atomic<bool> bg_thread_ended{false};

  SyncPoint::GetInstance()->SetCallBack(
      "SequenceVisibilityTracker::BackgroundThreadFunc:Start",
      [&](void* /*arg*/) { bg_thread_started.store(true); });

  SyncPoint::GetInstance()->SetCallBack(
      "SequenceVisibilityTracker::BackgroundThreadFunc:End",
      [&](void* /*arg*/) { bg_thread_ended.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(tracker.Start());
  EXPECT_TRUE(tracker.IsRunning());

  // Wait for background thread to start
  while (!bg_thread_started.load()) {
    std::this_thread::yield();
  }

  // Stop should be graceful
  tracker.Stop();

  // Verify thread has ended
  EXPECT_TRUE(bg_thread_ended.load());
  EXPECT_FALSE(tracker.IsRunning());
  EXPECT_TRUE(queue.IsShutdown());
}

// Test that Stop() can be called multiple times safely
TEST_F(SequenceVisibilityTrackerTest, MultipleStopCalls) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  ASSERT_OK(tracker.Start());
  EXPECT_TRUE(tracker.IsRunning());

  // First stop
  tracker.Stop();
  EXPECT_FALSE(tracker.IsRunning());

  // Second stop should be safe
  tracker.Stop();
  EXPECT_FALSE(tracker.IsRunning());

  // Third stop should also be safe
  tracker.Stop();
  EXPECT_FALSE(tracker.IsRunning());
}

// Test that Start() returns error if already running
TEST_F(SequenceVisibilityTrackerTest, StartWhileRunning) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  ASSERT_OK(tracker.Start());
  EXPECT_TRUE(tracker.IsRunning());

  // Starting again should fail
  Status s = tracker.Start();
  EXPECT_TRUE(s.IsInvalidArgument());
  EXPECT_TRUE(tracker.IsRunning());

  tracker.Stop();
}

// Test shutdown during pending messages processing
TEST_F(SequenceVisibilityTrackerTest, ShutdownDuringProcessing) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  std::atomic<bool> waiting_for_completion{false};

  SyncPoint::GetInstance()->SetCallBack(
      "SequenceVisibilityTracker::WaitForMemtableCompletion:Start",
      [&](void* /*arg*/) { waiting_for_completion.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(tracker.Start());

  // Enqueue a message but don't complete it
  auto msg = std::make_shared<MemtableInsertionMessage>(1, 10);
  queue.Enqueue(msg);

  // Wait for tracker to start waiting for completion
  while (!waiting_for_completion.load()) {
    std::this_thread::yield();
  }

  // Shutdown while tracker is waiting
  tracker.Stop();

  // Should have shut down gracefully
  EXPECT_FALSE(tracker.IsRunning());
}

// Test that destructor calls Stop() automatically
TEST_F(SequenceVisibilityTrackerTest, DestructorCallsStop) {
  MemtableInsertionCompletionQueue queue;

  std::atomic<bool> bg_thread_ended{false};

  SyncPoint::GetInstance()->SetCallBack(
      "SequenceVisibilityTracker::BackgroundThreadFunc:End",
      [&](void* /*arg*/) { bg_thread_ended.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  {
    SequenceVisibilityTracker tracker(nullptr, &queue);
    ASSERT_OK(tracker.Start());
    // Destructor should call Stop()
  }

  // Background thread should have ended
  EXPECT_TRUE(bg_thread_ended.load());
}

// Test processing with sync point dependency for precise ordering control
TEST_F(SequenceVisibilityTrackerTest, SyncPointDependencyOrdering) {
  MemtableInsertionCompletionQueue queue;
  SequenceVisibilityTracker tracker(nullptr, &queue);

  SyncPoint::GetInstance()->LoadDependency({
      {"SequenceVisibilityTrackerTest::SyncPointDependency:AfterEnqueue",
       "SequenceVisibilityTrackerTest::SyncPointDependency:BeforeComplete"},
  });

  std::atomic<bool> sequence_updated{false};

  SyncPoint::GetInstance()->SetCallBack(
      "SequenceVisibilityTracker::BackgroundThreadFunc:AfterUpdateSequence",
      [&](void* /*arg*/) { sequence_updated.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(tracker.Start());

  // Create and enqueue message
  auto msg = std::make_shared<MemtableInsertionMessage>(1, 50);

  std::thread producer([&]() {
    queue.Enqueue(msg);
    TEST_SYNC_POINT(
        "SequenceVisibilityTrackerTest::SyncPointDependency:AfterEnqueue");
  });

  std::thread completer([&]() {
    TEST_SYNC_POINT(
        "SequenceVisibilityTrackerTest::SyncPointDependency:BeforeComplete");
    msg->memtable_insertion_completed.store(true);
  });

  producer.join();
  completer.join();

  // Wait for visibility
  queue.WaitForVisibility(msg);

  EXPECT_TRUE(sequence_updated.load());
  EXPECT_EQ(tracker.GetVisibleSequence(), 50);

  tracker.Stop();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
