//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Tests for PartitionedWALSyncThread.

#include "db/partitioned_wal_sync_thread.h"

#include <atomic>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "db/log_writer.h"
#include "db/partitioned_wal_manager.h"
#include "env/mock_env.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class PartitionedWALSyncThreadTest : public testing::Test {
 protected:
  std::string test_dir_;
  std::unique_ptr<Env> env_;
  std::shared_ptr<FileSystem> fs_;
  DBOptions db_options_;
  std::unique_ptr<ImmutableDBOptions> immutable_db_options_;

  void SetUp() override {
    env_.reset(MockEnv::Create(Env::Default()));
    fs_ = env_->GetFileSystem();
    test_dir_ = test::PerThreadDBPath("partitioned_wal_sync_thread_test");
    ASSERT_OK(env_->CreateDirIfMissing(test_dir_));

    db_options_.env = env_.get();
    db_options_.wal_dir = test_dir_;
    db_options_.create_if_missing = true;
    immutable_db_options_ = std::make_unique<ImmutableDBOptions>(db_options_);
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->ClearTrace();

    // Clean up test directory
    std::vector<std::string> files;
    if (env_->GetChildren(test_dir_, &files).ok()) {
      for (const auto& f : files) {
        env_->DeleteFile(test_dir_ + "/" + f).PermitUncheckedError();
      }
    }
    env_->DeleteDir(test_dir_).PermitUncheckedError();
  }

  std::unique_ptr<PartitionedWALManager> CreateManager(
      uint32_t num_partitions) {
    return std::make_unique<PartitionedWALManager>(
        fs_.get(), *immutable_db_options_, test_dir_, num_partitions);
  }
};

TEST_F(PartitionedWALSyncThreadTest, SyncsAtConfiguredInterval) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Write some data to partitions
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    std::string body = "test data for partition " + std::to_string(i);
    log::PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
  }

  // Use sync points to track sync operations
  std::atomic<int> sync_count{0};

  SyncPoint::GetInstance()->SetCallBack(
      "PartitionedWALSyncThread::BackgroundThreadFunc:AfterSync",
      [&](void* /*arg*/) {
        sync_count.fetch_add(1, std::memory_order_relaxed);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Create sync thread with 50ms interval
  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 50);
  ASSERT_OK(sync_thread.Start());
  EXPECT_TRUE(sync_thread.IsRunning());

  // Wait for at least 2 syncs to occur
  int max_wait_iterations = 100;  // 100 * 20ms = 2 seconds max wait
  while (sync_count.load(std::memory_order_relaxed) < 2 &&
         max_wait_iterations > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    max_wait_iterations--;
  }

  EXPECT_GE(sync_count.load(std::memory_order_relaxed), 2);

  // Stop the thread
  sync_thread.Stop();
  EXPECT_FALSE(sync_thread.IsRunning());

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, SyncNowForcesImmediateSync) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Write some data
  for (uint32_t i = 0; i < kNumPartitions; i++) {
    auto* writer = manager->GetWriter(i);
    std::string body = "test data " + std::to_string(i);
    log::PartitionedLogWriter::WriteResult result;
    ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));
  }

  // Use sync points to verify sync happens
  std::atomic<int> sync_count{0};

  SyncPoint::GetInstance()->SetCallBack(
      "PartitionedWALSyncThread::BackgroundThreadFunc:AfterSync",
      [&](void* /*arg*/) {
        sync_count.fetch_add(1, std::memory_order_relaxed);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Create sync thread with very long interval (10 seconds)
  // so we can verify SyncNow forces immediate sync
  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 10000);
  ASSERT_OK(sync_thread.Start());

  // Initial sync count should be 0 since interval hasn't passed
  EXPECT_EQ(sync_count.load(std::memory_order_relaxed), 0);

  // Force immediate sync
  IOStatus s = sync_thread.SyncNow();
  ASSERT_OK(s);

  // Sync should have occurred
  EXPECT_GE(sync_count.load(std::memory_order_relaxed), 1);

  // Force another sync
  s = sync_thread.SyncNow();
  ASSERT_OK(s);

  EXPECT_GE(sync_count.load(std::memory_order_relaxed), 2);

  sync_thread.Stop();
  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, GracefulShutdown) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Track thread lifecycle with sync points
  std::atomic<bool> thread_started{false};
  std::atomic<bool> thread_exited{false};

  SyncPoint::GetInstance()->SetCallBack(
      "PartitionedWALSyncThread::BackgroundThreadFunc:Start",
      [&](void* /*arg*/) {
        thread_started.store(true, std::memory_order_release);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "PartitionedWALSyncThread::BackgroundThreadFunc:Exit",
      [&](void* /*arg*/) {
        thread_exited.store(true, std::memory_order_release);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Create and start sync thread
  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 1000);
  ASSERT_OK(sync_thread.Start());

  // Wait for thread to start
  int max_wait = 100;
  while (!thread_started.load(std::memory_order_acquire) && max_wait > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    max_wait--;
  }
  EXPECT_TRUE(thread_started.load(std::memory_order_acquire));
  EXPECT_TRUE(sync_thread.IsRunning());

  // Stop should complete gracefully
  sync_thread.Stop();

  // Thread should have exited
  EXPECT_TRUE(thread_exited.load(std::memory_order_acquire));
  EXPECT_FALSE(sync_thread.IsRunning());

  // Double stop should be safe
  sync_thread.Stop();
  EXPECT_FALSE(sync_thread.IsRunning());

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, ZeroIntervalSyncsEveryTime) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Track sync count
  std::atomic<int> sync_count{0};

  SyncPoint::GetInstance()->SetCallBack(
      "PartitionedWALSyncThread::BackgroundThreadFunc:AfterSync",
      [&](void* /*arg*/) {
        sync_count.fetch_add(1, std::memory_order_relaxed);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Create sync thread with zero interval
  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 0);
  ASSERT_OK(sync_thread.Start());

  // Wait a short time - with zero interval, syncs should happen rapidly
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Should have many syncs in this time
  int count = sync_count.load(std::memory_order_relaxed);
  EXPECT_GT(count, 5) << "Expected many syncs with zero interval, got "
                      << count;

  sync_thread.Stop();
  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, SyncWithMainWALWriter) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Create a main WAL file
  std::string main_wal_path = test_dir_ + "/main_wal.log";
  DBOptions db_opts;
  FileOptions file_opts;
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(fs_->NewWritableFile(main_wal_path, file_opts, &file, nullptr));

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), main_wal_path, file_opts, env_->GetSystemClock().get()));

  std::unique_ptr<log::Writer> main_writer(
      new log::Writer(std::move(file_writer), 1, false));

  // Write to main WAL
  ASSERT_OK(main_writer->AddRecord(WriteOptions(), Slice("test record")));

  // Create sync thread with main WAL writer
  PartitionedWALSyncThread sync_thread(manager.get(), main_writer.get(), 100);
  ASSERT_OK(sync_thread.Start());

  // Force sync which should sync both partitioned and main WAL
  IOStatus s = sync_thread.SyncNow();
  ASSERT_OK(s);

  sync_thread.Stop();

  // Clean up
  ASSERT_OK(main_writer->Close(WriteOptions()));
  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, SetMainWALWriterAfterStart) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Create sync thread without main WAL writer
  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 100);
  ASSERT_OK(sync_thread.Start());

  // First sync without main writer
  IOStatus s = sync_thread.SyncNow();
  ASSERT_OK(s);

  // Create main WAL writer
  std::string main_wal_path = test_dir_ + "/main_wal2.log";
  FileOptions file_opts;
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(fs_->NewWritableFile(main_wal_path, file_opts, &file, nullptr));

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), main_wal_path, file_opts, env_->GetSystemClock().get()));

  std::unique_ptr<log::Writer> main_writer(
      new log::Writer(std::move(file_writer), 2, false));

  // Set main WAL writer after start
  sync_thread.SetMainWALWriter(main_writer.get());

  // Write to main WAL
  ASSERT_OK(main_writer->AddRecord(WriteOptions(), Slice("test record 2")));

  // Sync should now include main WAL
  s = sync_thread.SyncNow();
  ASSERT_OK(s);

  sync_thread.Stop();

  // Clean up
  ASSERT_OK(main_writer->Close(WriteOptions()));
  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, StartAfterStop) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 100);

  // Start, stop, start cycle
  ASSERT_OK(sync_thread.Start());
  EXPECT_TRUE(sync_thread.IsRunning());

  sync_thread.Stop();
  EXPECT_FALSE(sync_thread.IsRunning());

  // Should be able to start again
  ASSERT_OK(sync_thread.Start());
  EXPECT_TRUE(sync_thread.IsRunning());

  sync_thread.Stop();
  EXPECT_FALSE(sync_thread.IsRunning());

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, SyncNowWhenNotRunning) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  // Write some data
  auto* writer = manager->GetWriter(0);
  std::string body = "test data";
  log::PartitionedLogWriter::WriteResult result;
  ASSERT_OK(writer->WriteBody(WriteOptions(), Slice(body), 1, &result));

  // Create sync thread but don't start it
  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 100);

  // SyncNow should still work even when thread is not running
  IOStatus s = sync_thread.SyncNow();
  ASSERT_OK(s);

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, DoubleStartFails) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 100);

  ASSERT_OK(sync_thread.Start());
  EXPECT_TRUE(sync_thread.IsRunning());

  // Double start should fail
  Status s = sync_thread.Start();
  EXPECT_TRUE(s.IsInvalidArgument());
  EXPECT_TRUE(sync_thread.IsRunning());

  sync_thread.Stop();
  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

TEST_F(PartitionedWALSyncThreadTest, ConcurrentSyncNow) {
  const uint32_t kNumPartitions = 2;
  auto manager = CreateManager(kNumPartitions);
  ASSERT_OK(manager->Open(1));

  PartitionedWALSyncThread sync_thread(manager.get(), nullptr, 1000);
  ASSERT_OK(sync_thread.Start());

  // Launch multiple threads calling SyncNow concurrently
  const int kNumThreads = 10;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  std::atomic<int> error_count{0};

  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&]() {
      for (int j = 0; j < 5; j++) {
        IOStatus s = sync_thread.SyncNow();
        if (s.ok()) {
          success_count.fetch_add(1, std::memory_order_relaxed);
        } else {
          error_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  sync_thread.Stop();

  // All syncs should have succeeded
  EXPECT_EQ(success_count.load(), kNumThreads * 5);
  EXPECT_EQ(error_count.load(), 0);

  ASSERT_OK(manager->CloseAll(WriteOptions()));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
