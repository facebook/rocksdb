//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Implementation of PartitionedWALSyncThread.

#include "db/partitioned_wal_sync_thread.h"

#include "db/log_writer.h"
#include "db/partitioned_wal_manager.h"
#include "file/writable_file_writer.h"
#include "monitoring/statistics_impl.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

PartitionedWALSyncThread::PartitionedWALSyncThread(
    PartitionedWALManager* partitioned_wal_manager,
    log::Writer* main_wal_writer, uint64_t sync_interval_ms, SystemClock* clock,
    Statistics* stats)
    : partitioned_wal_manager_(partitioned_wal_manager),
      main_wal_writer_(main_wal_writer),
      sync_interval_ms_(sync_interval_ms),
      clock_(clock),
      stats_(stats) {}

PartitionedWALSyncThread::~PartitionedWALSyncThread() { Stop(); }

Status PartitionedWALSyncThread::Start() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (running_.load(std::memory_order_acquire)) {
    return Status::InvalidArgument("PartitionedWALSyncThread already running");
  }

  shutdown_.store(false, std::memory_order_release);
  running_.store(true, std::memory_order_release);

  bg_thread_ =
      std::make_unique<port::Thread>([this]() { BackgroundThreadFunc(); });

  return Status::OK();
}

void PartitionedWALSyncThread::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load(std::memory_order_acquire)) {
      return;
    }
    shutdown_.store(true, std::memory_order_release);
    cv_.notify_all();
  }

  if (bg_thread_ && bg_thread_->joinable()) {
    bg_thread_->join();
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    running_.store(false, std::memory_order_release);
    bg_thread_.reset();
  }
}

IOStatus PartitionedWALSyncThread::SyncNow() {
  std::unique_lock<std::mutex> lock(mutex_);

  if (!running_.load(std::memory_order_acquire)) {
    // If not running, perform sync directly
    return DoSync();
  }

  // Request a sync and wake up the background thread
  sync_requested_.store(true, std::memory_order_release);
  cv_.notify_all();

  // Wait for the sync to complete
  sync_complete_cv_.wait(lock, [this]() {
    return !sync_requested_.load(std::memory_order_acquire) ||
           shutdown_.load(std::memory_order_acquire);
  });

  return last_sync_status_;
}

void PartitionedWALSyncThread::SetMainWALWriter(log::Writer* writer) {
  main_wal_writer_.store(writer, std::memory_order_release);
}

void PartitionedWALSyncThread::BackgroundThreadFunc() {
  TEST_SYNC_POINT("PartitionedWALSyncThread::BackgroundThreadFunc:Start");

  while (!shutdown_.load(std::memory_order_acquire)) {
    TEST_SYNC_POINT(
        "PartitionedWALSyncThread::BackgroundThreadFunc:BeforeWait");

    {
      std::unique_lock<std::mutex> lock(mutex_);

      if (sync_interval_ms_ > 0) {
        // Wait for the specified interval or until signaled
        cv_.wait_for(lock, std::chrono::milliseconds(sync_interval_ms_),
                     [this]() {
                       return shutdown_.load(std::memory_order_acquire) ||
                              sync_requested_.load(std::memory_order_acquire);
                     });
      } else {
        // Zero interval: check for shutdown or sync request without blocking
        // but still yield to avoid busy spinning in tests
        if (!sync_requested_.load(std::memory_order_acquire) &&
            !shutdown_.load(std::memory_order_acquire)) {
          // Brief wait to prevent busy spinning, but still sync frequently
          cv_.wait_for(lock, std::chrono::microseconds(100), [this]() {
            return shutdown_.load(std::memory_order_acquire) ||
                   sync_requested_.load(std::memory_order_acquire);
          });
        }
      }
    }

    if (shutdown_.load(std::memory_order_acquire)) {
      break;
    }

    TEST_SYNC_POINT(
        "PartitionedWALSyncThread::BackgroundThreadFunc:BeforeSync");

    // Perform the sync
    IOStatus sync_status = DoSync();

#ifndef NDEBUG
    sync_count_.fetch_add(1, std::memory_order_relaxed);
#endif

    TEST_SYNC_POINT("PartitionedWALSyncThread::BackgroundThreadFunc:AfterSync");

    // If this was a requested sync, notify waiters
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (sync_requested_.load(std::memory_order_acquire)) {
        last_sync_status_ = sync_status;
        sync_requested_.store(false, std::memory_order_release);
        sync_complete_cv_.notify_all();
      }
    }
  }

  // Handle any pending sync request on shutdown
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (sync_requested_.load(std::memory_order_acquire)) {
      last_sync_status_ = IOStatus::Aborted("Shutdown in progress");
      sync_requested_.store(false, std::memory_order_release);
      sync_complete_cv_.notify_all();
    }
  }

  TEST_SYNC_POINT("PartitionedWALSyncThread::BackgroundThreadFunc:Exit");
}

IOStatus PartitionedWALSyncThread::DoSync() {
  // Start timing for sync latency
  uint64_t sync_start = 0;
  if (stats_ && clock_) {
    sync_start = clock_->NowMicros();
  }

  IOStatus s = IOStatus::OK();

  // Sync partitioned WAL files
  if (partitioned_wal_manager_ != nullptr &&
      partitioned_wal_manager_->IsOpen()) {
    WriteOptions write_opts;
    s = partitioned_wal_manager_->SyncAll(write_opts);
    if (!s.ok()) {
      return s;
    }
  }

  // Sync main WAL writer if set
  log::Writer* main_writer = main_wal_writer_.load(std::memory_order_acquire);
  if (main_writer != nullptr) {
    WritableFileWriter* file_writer = main_writer->file();
    if (file_writer != nullptr) {
      s = file_writer->Sync(IOOptions(), false /* use_fsync */);
    }
  }

  // Record sync statistics
  if (s.ok()) {
    RecordTick(stats_, PARTITIONED_WAL_SYNCS);
    if (stats_ && clock_) {
      uint64_t sync_elapsed = clock_->NowMicros() - sync_start;
      RecordInHistogram(stats_, PARTITIONED_WAL_SYNC_LATENCY, sync_elapsed);
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
