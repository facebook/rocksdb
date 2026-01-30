//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Implementation of PartitionedWALManager.

#include "db/partitioned_wal_manager.h"

#include <cinttypes>
#include <cstdio>
#include <regex>

#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/options_helper.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

PartitionedWALManager::PartitionedWALManager(
    FileSystem* fs, const ImmutableDBOptions& db_options,
    const std::string& db_path, uint32_t num_partitions)
    : fs_(fs),
      db_options_(db_options),
      wal_dir_(db_options.GetWalDir(db_path)),
      num_partitions_(num_partitions) {
  writers_.resize(num_partitions_);
}

PartitionedWALManager::~PartitionedWALManager() {
  if (is_open_) {
    WriteOptions opts;
    CloseWriters(opts).PermitUncheckedError();
  }
}

Status PartitionedWALManager::Open(uint64_t starting_log_number) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (is_open_) {
    return Status::InvalidArgument("PartitionedWALManager already open");
  }

  if (num_partitions_ == 0) {
    return Status::InvalidArgument("num_partitions must be > 0");
  }

  current_log_number_.store(starting_log_number, std::memory_order_release);

  // Create writers for each partition
  for (uint32_t i = 0; i < num_partitions_; i++) {
    Status s = CreateWriter(starting_log_number, i, &writers_[i]);
    if (!s.ok()) {
      // Clean up any writers we already created
      WriteOptions opts;
      for (uint32_t j = 0; j < i; j++) {
        if (writers_[j]) {
          writers_[j]->Close(opts).PermitUncheckedError();
          writers_[j].reset();
        }
      }
      return s;
    }
  }

  is_open_ = true;
  ROCKS_LOG_INFO(db_options_.logger,
                 "PartitionedWALManager opened with %" PRIu32
                 " partitions, log number %" PRIu64,
                 num_partitions_, starting_log_number);
  return Status::OK();
}

Status PartitionedWALManager::CreateWriter(
    uint64_t log_number, uint32_t partition_id,
    std::unique_ptr<log::PartitionedLogWriter>* writer) {
  std::string fname = PartitionFileName(wal_dir_, log_number, partition_id);

  // Set up file options for WAL
  DBOptions db_opts = BuildDBOptions(db_options_, MutableDBOptions());
  FileOptions file_opts =
      fs_->OptimizeForLogWrite(FileOptions(db_opts), db_opts);

  // Set temperature if specified
  if (db_options_.wal_write_temperature != Temperature::kUnknown) {
    file_opts.temperature = db_options_.wal_write_temperature;
  }

  // Create the writable file
  std::unique_ptr<FSWritableFile> file;
  IOStatus io_s = fs_->NewWritableFile(fname, file_opts, &file, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }

  // Create the file writer
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), fname, file_opts, db_options_.clock,
      nullptr /* io_tracer */, nullptr /* stats */,
      Histograms::HISTOGRAM_ENUM_MAX, db_options_.listeners, nullptr,
      false /* checksum_handoff */, false /* checksum_handoff_func */));

  // Create the partitioned log writer
  *writer = std::make_unique<log::PartitionedLogWriter>(
      std::move(file_writer), log_number, partition_id,
      db_options_.recycle_log_file_num > 0);

  return Status::OK();
}

log::PartitionedLogWriter* PartitionedWALManager::GetWriter(
    uint32_t partition_id) {
  if (!is_open_ || partition_id >= num_partitions_) {
    return nullptr;
  }
  return writers_[partition_id].get();
}

uint32_t PartitionedWALManager::SelectPartition() {
  // Atomic fetch-and-increment with wrap-around
  uint32_t partition = next_partition_.fetch_add(1, std::memory_order_relaxed);
  return partition % num_partitions_;
}

uint64_t PartitionedWALManager::GetMaxFileSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!is_open_) {
    return 0;
  }

  uint64_t max_size = 0;
  for (uint32_t i = 0; i < num_partitions_; i++) {
    if (writers_[i]) {
      uint64_t size = writers_[i]->GetFileSize();
      if (size > max_size) {
        max_size = size;
      }
    }
  }
  return max_size;
}

uint64_t PartitionedWALManager::GetTotalFileSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!is_open_) {
    return 0;
  }

  uint64_t total_size = 0;
  for (uint32_t i = 0; i < num_partitions_; i++) {
    if (writers_[i]) {
      total_size += writers_[i]->GetFileSize();
    }
  }
  return total_size;
}

Status PartitionedWALManager::RotateWALFiles(uint64_t new_log_number) {
  // Acquire exclusive lock to ensure no writes are in progress.
  // This blocks until all shared lock holders (active writes) complete.
  std::unique_lock<std::shared_mutex> rw_lock(rw_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);

  if (!is_open_) {
    return Status::InvalidArgument("PartitionedWALManager not open");
  }

  // Close current writers
  WriteOptions opts;
  IOStatus io_s = CloseWriters(opts);
  if (!io_s.ok()) {
    return io_s;
  }

  // Create new writers with the new log number
  // NOTE: Only update current_log_number_ AFTER all writers are successfully
  // created to maintain consistency in case of partial failure.
  for (uint32_t i = 0; i < num_partitions_; i++) {
    Status s = CreateWriter(new_log_number, i, &writers_[i]);
    if (!s.ok()) {
      // Clean up any writers we created
      for (uint32_t j = 0; j < i; j++) {
        if (writers_[j]) {
          writers_[j]->Close(opts).PermitUncheckedError();
          writers_[j].reset();
        }
      }
      is_open_ = false;
      return s;
    }
  }

  // Only update log number after all writers are successfully created
  current_log_number_.store(new_log_number, std::memory_order_release);

  ROCKS_LOG_INFO(db_options_.logger,
                 "PartitionedWALManager rotated to log number %" PRIu64,
                 new_log_number);
  return Status::OK();
}

IOStatus PartitionedWALManager::SyncAll(const WriteOptions& opts) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!is_open_) {
    return IOStatus::InvalidArgument("PartitionedWALManager not open");
  }

  for (uint32_t i = 0; i < num_partitions_; i++) {
    if (writers_[i]) {
      IOStatus s = writers_[i]->Sync(opts);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return IOStatus::OK();
}

IOStatus PartitionedWALManager::CloseWriters(const WriteOptions& opts) {
  IOStatus result = IOStatus::OK();
  for (uint32_t i = 0; i < num_partitions_; i++) {
    if (writers_[i]) {
      IOStatus s = writers_[i]->Close(opts);
      if (!s.ok() && result.ok()) {
        result = s;
      }
      writers_[i].reset();
    }
  }
  return result;
}

IOStatus PartitionedWALManager::CloseAll(const WriteOptions& opts) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!is_open_) {
    return IOStatus::OK();
  }

  IOStatus s = CloseWriters(opts);
  is_open_ = false;
  return s;
}

Status PartitionedWALManager::DeleteObsoleteFiles(uint64_t min_log_number) {
  // Get list of files in WAL directory
  std::vector<std::string> files;
  IOOptions io_opts;
  IOStatus io_s = fs_->GetChildren(wal_dir_, io_opts, &files, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }

  uint64_t current_log = current_log_number_.load(std::memory_order_acquire);
  uint64_t partition_delete_threshold =
      (min_log_number > 1) ? min_log_number - 1 : 0;
  ROCKS_LOG_INFO(db_options_.logger,
                 "DeleteObsoleteFiles: min_log_number=%" PRIu64
                 ", partition_delete_threshold=%" PRIu64
                 ", current_log_number=%" PRIu64,
                 min_log_number, partition_delete_threshold, current_log);

  Status result = Status::OK();
  for (const auto& filename : files) {
    uint64_t log_number;
    uint32_t partition_id;

    if (ParsePartitionFileName(filename, &log_number, &partition_id)) {
      // Delete files with log numbers older than partition_delete_threshold.
      // We use min_log_number - 1 because during WAL rotation, a main WAL
      // file with log number N can contain completion records that reference
      // partition files from log number N-1 (if the write started before
      // rotation but the completion record was written after). So we must
      // keep partition files with log_number >= min_log_number - 1.
      // min_log_number is computed from versions and memtables, ensuring
      // we never delete files still needed for recovery.
      if (log_number < partition_delete_threshold) {
        std::string full_path = wal_dir_ + "/" + filename;
        io_s = fs_->DeleteFile(full_path, io_opts, nullptr);
        if (!io_s.ok() && result.ok()) {
          result = io_s;
        } else {
          ROCKS_LOG_INFO(
              db_options_.logger,
              "Deleted obsolete partition file: %s (log_number=%" PRIu64 ")",
              full_path.c_str(), log_number);
        }
      }
    }
  }
  return result;
}

uint64_t PartitionedWALManager::GetCurrentLogNumber() const {
  return current_log_number_.load(std::memory_order_acquire);
}

// static
std::string PartitionedWALManager::PartitionFileName(const std::string& wal_dir,
                                                     uint64_t log_number,
                                                     uint32_t partition_id) {
  char buf[64];
  snprintf(buf, sizeof(buf), "PARTITIONED_%06" PRIu64 "_P%" PRIu32 ".log",
           log_number, partition_id);
  return wal_dir + "/" + buf;
}

// static
bool PartitionedWALManager::ParsePartitionFileName(const std::string& filename,
                                                   uint64_t* log_number,
                                                   uint32_t* partition_id) {
  // Pattern: PARTITIONED_XXXXXX_PY.log
  // XXXXXX is the log number (6 digits, but could be more)
  // Y is the partition id (could be multiple digits)
  uint64_t num = 0;
  uint32_t part = 0;

  // Use sscanf for parsing
  if (sscanf(filename.c_str(), "PARTITIONED_%" SCNu64 "_P%" SCNu32 ".log", &num,
             &part) == 2) {
    if (log_number) {
      *log_number = num;
    }
    if (partition_id) {
      *partition_id = part;
    }
    return true;
  }
  return false;
}

}  // namespace ROCKSDB_NAMESPACE
