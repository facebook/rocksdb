//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Manager class for coordinating multiple partitioned WAL writers.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "db/partitioned_log_writer.h"
#include "options/db_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;

// PartitionedWALManager coordinates multiple partitioned WAL writers.
// It manages the lifecycle of partition files, handles rotation of WAL files,
// and provides round-robin partition selection for writes.
//
// Partition files are named: PARTITIONED_XXXXX_P0.log, PARTITIONED_XXXXX_P1.log
// where XXXXX is the zero-padded log number.
//
// Thread safety:
// - writers_ array access is safe after Open() completes
// - SelectPartition() uses atomic round-robin counter
// - RotateWALFiles() and CloseAll() require external synchronization
class PartitionedWALManager {
 public:
  // Create a manager for partitioned WAL files.
  //
  // Parameters:
  //   fs: FileSystem to use for file operations (not owned)
  //   db_options: Database options containing configuration
  //   db_path: Path to the database directory (used to determine WAL dir)
  //   num_partitions: Number of partition files to create
  PartitionedWALManager(FileSystem* fs, const ImmutableDBOptions& db_options,
                        const std::string& db_path, uint32_t num_partitions);

  ~PartitionedWALManager();

  // Open creates partition files and writers.
  // Must be called before any other operations.
  Status Open(uint64_t starting_log_number);

  // Get writer for a partition (0 to num_partitions-1).
  // Returns nullptr if partition_id is out of range or manager not opened.
  // IMPORTANT: Caller must hold shared lock via LockShared()/UnlockShared()
  // or use std::shared_lock on GetSharedMutex() during the entire write
  // operation to prevent the writer from being invalidated during rotation.
  log::PartitionedLogWriter* GetWriter(uint32_t partition_id);

  // Lock for shared access (multiple writers can proceed concurrently).
  // Call before GetWriter() and hold until write is complete.
  void LockShared() { rw_mutex_.lock_shared(); }
  void UnlockShared() { rw_mutex_.unlock_shared(); }

  // Get the shared mutex for use with std::shared_lock
  std::shared_mutex& GetSharedMutex() { return rw_mutex_; }

  // Select partition for next write using round-robin.
  // Thread-safe via atomic counter.
  uint32_t SelectPartition();

  // Rotate WAL files - close current files and create new ones.
  // This creates new partition files with the given log number.
  // The old files are left in place (caller should delete them when safe).
  Status RotateWALFiles(uint64_t new_log_number);

  // Sync all partition files to disk.
  IOStatus SyncAll(const WriteOptions& opts);

  // Close all partition files.
  IOStatus CloseAll(const WriteOptions& opts);

  // Delete obsolete partition files older than min_log_number.
  // Scans the WAL directory for partition files and deletes those
  // with log numbers less than min_log_number.
  Status DeleteObsoleteFiles(uint64_t min_log_number);

  // Get the current log number.
  uint64_t GetCurrentLogNumber() const;

  // Get the number of partitions.
  uint32_t GetNumPartitions() const { return num_partitions_; }

  // Check if manager is open.
  bool IsOpen() const { return is_open_; }

  // Get the maximum file size across all partition files.
  // Returns 0 if manager is not open or no writers exist.
  uint64_t GetMaxFileSize() const;

  // Get the total size of all partition files for the current log number.
  // Returns 0 if manager is not open or no writers exist.
  uint64_t GetTotalFileSize() const;

  // Generate the file name for a partition file.
  // Format: PARTITIONED_XXXXX_PY.log where XXXXX is log number, Y is partition
  static std::string PartitionFileName(const std::string& wal_dir,
                                       uint64_t log_number,
                                       uint32_t partition_id);

  // Parse a partition file name to extract log number and partition id.
  // Returns true if the file name matches the partition file pattern.
  static bool ParsePartitionFileName(const std::string& filename,
                                     uint64_t* log_number,
                                     uint32_t* partition_id);

 private:
  // Create writer for a single partition file.
  Status CreateWriter(uint64_t log_number, uint32_t partition_id,
                      std::unique_ptr<log::PartitionedLogWriter>* writer);

  // Close all current writers.
  IOStatus CloseWriters(const WriteOptions& opts);

  FileSystem* fs_;
  const ImmutableDBOptions& db_options_;
  std::string wal_dir_;
  uint32_t num_partitions_;

  // Current log number for partition files
  std::atomic<uint64_t> current_log_number_{0};

  // Round-robin counter for partition selection
  std::atomic<uint32_t> next_partition_{0};

  // Writers for each partition (size = num_partitions_)
  std::vector<std::unique_ptr<log::PartitionedLogWriter>> writers_;

  // Protect writers_ during rotation
  mutable std::mutex mutex_;

  // Reader-writer lock for writer access.
  // - Writes hold shared lock during the entire write operation
  // - Rotation holds exclusive lock to safely close and replace writers
  mutable std::shared_mutex rw_mutex_;

  // Whether the manager has been opened
  bool is_open_{false};
};

}  // namespace ROCKSDB_NAMESPACE
