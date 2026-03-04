//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_log_format.h"
#include "port/port.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobLogWriter;
class SystemClock;
class WritableFileWriter;
struct FileOptions;
struct ImmutableDBOptions;

// Default round-robin partition strategy.
class RoundRobinPartitionStrategy : public BlobFilePartitionStrategy {
 public:
  uint32_t SelectPartition(uint32_t num_partitions, uint32_t /*column_family_id*/,
                           const Slice& /*key*/, const Slice& /*value*/) override {
    return static_cast<uint32_t>(
        next_index_.fetch_add(1, std::memory_order_relaxed) % num_partitions);
  }

 private:
  std::atomic<uint64_t> next_index_{0};
};

// Manages partitioned blob files for the write-path blob direct write feature.
// Each partition has its own BlobLogWriter and mutex, allowing concurrent
// blob writes from multiple writer threads.
class BlobFilePartitionManager {
 public:
  // Type alias for file number allocator function.
  using FileNumberAllocator = std::function<uint64_t()>;

  BlobFilePartitionManager(
      uint32_t num_partitions,
      std::shared_ptr<BlobFilePartitionStrategy> strategy,
      FileNumberAllocator file_number_allocator,
      FileSystem* fs,
      SystemClock* clock,
      Statistics* statistics,
      const FileOptions& file_options,
      const std::string& db_path,
      uint64_t blob_file_size,
      bool use_fsync);

  ~BlobFilePartitionManager();

  // Write a blob value to a partition. The partition is selected by the
  // configured strategy. Returns the blob file number, offset, and size
  // for constructing a BlobIndex.
  // Thread-safe: multiple writers can call this concurrently.
  Status WriteBlob(const WriteOptions& write_options,
                   uint32_t column_family_id,
                   CompressionType compression,
                   const Slice& key,
                   const Slice& value,
                   uint64_t* blob_file_number,
                   uint64_t* blob_offset,
                   uint64_t* blob_size);

  // Seal all open partitions by writing footers. Collects BlobFileAddition
  // entries for all completed and currently-open files.
  // Called during DB shutdown and before recovery sealing.
  Status SealAllPartitions(const WriteOptions& write_options,
                           std::vector<BlobFileAddition>* additions);

  // Collect completed (rotated) blob file additions and clear them.
  // Called during flush to register sealed files in the MANIFEST.
  void TakeCompletedBlobFileAdditions(
      std::vector<BlobFileAddition>* additions);

  // Sync all open blob files. Called when write_options.sync = true
  // to ensure blob data is durable before WAL write.
  Status SyncAllOpenFiles(const WriteOptions& write_options);

 private:
  struct Partition {
    port::Mutex mutex;
    std::unique_ptr<BlobLogWriter> writer;
    uint64_t file_number = 0;
    uint64_t file_size = 0;
    uint64_t blob_count = 0;
    uint64_t total_blob_bytes = 0;
    uint32_t column_family_id = 0;
    CompressionType compression = kNoCompression;
    std::vector<BlobFileAddition> completed_files;
  };

  Status OpenNewBlobFile(Partition* partition,
                         uint32_t column_family_id,
                         CompressionType compression);
  Status CloseBlobFile(Partition* partition);

  const uint32_t num_partitions_;
  std::shared_ptr<BlobFilePartitionStrategy> strategy_;
  FileNumberAllocator file_number_allocator_;
  FileSystem* fs_;
  SystemClock* clock_;
  Statistics* statistics_;
  FileOptions file_options_;
  std::string db_path_;
  uint64_t blob_file_size_;
  bool use_fsync_;

  std::vector<std::unique_ptr<Partition>> partitions_;

  // Mutex protecting completed_files_ across all partitions during
  // TakeCompletedBlobFileAdditions.
  port::Mutex completed_files_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
