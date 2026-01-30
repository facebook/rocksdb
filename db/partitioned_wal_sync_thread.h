//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Background thread that periodically syncs all WAL files based on
// configurable interval.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

#include "port/port.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class PartitionedWALManager;

namespace log {
class Writer;
}  // namespace log

// PartitionedWALSyncThread provides a background thread that periodically
// syncs all WAL files (both partitioned and main WAL) based on a configurable
// interval.
//
// Thread safety:
// - Start() and Stop() should be called from a single thread
// - SyncNow() is thread-safe and can be called from any thread
// - SetMainWALWriter() is thread-safe
class PartitionedWALSyncThread {
 public:
  // Create a sync thread.
  //
  // Parameters:
  //   partitioned_wal_manager: Manager for partitioned WAL files (not owned)
  //   main_wal_writer: Main WAL writer, can be nullptr initially (not owned)
  //   sync_interval_ms: Interval between syncs in milliseconds
  //                     If 0, syncs continuously without delay
  //   clock: System clock for timing (not owned, can be nullptr)
  //   stats: Statistics object for recording metrics (not owned, can be
  //   nullptr)
  PartitionedWALSyncThread(PartitionedWALManager* partitioned_wal_manager,
                           log::Writer* main_wal_writer,
                           uint64_t sync_interval_ms,
                           SystemClock* clock = nullptr,
                           Statistics* stats = nullptr);

  ~PartitionedWALSyncThread();

  // No copying allowed
  PartitionedWALSyncThread(const PartitionedWALSyncThread&) = delete;
  PartitionedWALSyncThread& operator=(const PartitionedWALSyncThread&) = delete;

  // Start the background sync thread.
  // Returns error if already started.
  Status Start();

  // Stop the background thread gracefully.
  // Blocks until the thread has exited.
  void Stop();

  // Force an immediate sync of all WAL files.
  // This wakes up the background thread to perform a sync immediately.
  // Returns the result of the sync operation.
  IOStatus SyncNow();

  // Set the main WAL writer. Can be called after construction.
  // Thread-safe.
  void SetMainWALWriter(log::Writer* writer);

  // Check if the thread is running.
  bool IsRunning() const { return running_.load(std::memory_order_acquire); }

#ifndef NDEBUG
  // For testing: get the number of sync operations performed
  uint64_t TEST_GetSyncCount() const {
    return sync_count_.load(std::memory_order_acquire);
  }
#endif

 private:
  // Background thread function
  void BackgroundThreadFunc();

  // Perform the actual sync operation
  IOStatus DoSync();

  PartitionedWALManager* partitioned_wal_manager_;
  std::atomic<log::Writer*> main_wal_writer_;
  uint64_t sync_interval_ms_;
  SystemClock* clock_;
  Statistics* stats_;

  std::atomic<bool> shutdown_{false};
  std::atomic<bool> running_{false};
  std::atomic<bool> sync_requested_{false};

  std::unique_ptr<port::Thread> bg_thread_;
  std::mutex mutex_;
  std::condition_variable cv_;

  // Result of the last sync operation (for SyncNow)
  IOStatus last_sync_status_;
  std::condition_variable sync_complete_cv_;

#ifndef NDEBUG
  std::atomic<uint64_t> sync_count_{0};
#endif
};

}  // namespace ROCKSDB_NAMESPACE
