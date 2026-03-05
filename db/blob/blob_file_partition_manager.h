//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <list>
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
#include "rocksdb/slice.h"
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
  uint32_t SelectPartition(uint32_t num_partitions,
                           uint32_t /*column_family_id*/, const Slice& /*key*/,
                           const Slice& /*value*/) override {
    return static_cast<uint32_t>(
        next_index_.fetch_add(1, std::memory_order_relaxed) % num_partitions);
  }

 private:
  std::atomic<uint64_t> next_index_{0};
};

// Manages partitioned blob files for the write-path blob direct write feature.
//
// Supports a zero-copy deferred flush model (when buffer_size > 0):
// - WriteBlob() pre-calculates offsets and stores Slice references pointing
//   directly into the caller's WriteBatch buffer (no memcpy)
// - After TransformBatch, the caller moves the WriteBatch rep_ into shared
//   ownership via AdoptBatchBuffer() so Slices remain valid
// - FlushPendingRecords() writes to disk in batch and releases buffer refs
// - Backpressure via atomic pending_bytes with stall watermark
// - Read path checks pending records for unflushed data
class BlobFilePartitionManager {
 public:
  using FileNumberAllocator = std::function<uint64_t()>;

  BlobFilePartitionManager(
      uint32_t num_partitions,
      std::shared_ptr<BlobFilePartitionStrategy> strategy,
      FileNumberAllocator file_number_allocator, FileSystem* fs,
      SystemClock* clock, Statistics* statistics,
      const FileOptions& file_options, const std::string& db_path,
      uint64_t blob_file_size, bool use_fsync, uint64_t buffer_size = 0);

  ~BlobFilePartitionManager();

  // Write a blob value to a partition. Returns blob file number, offset, size.
  // In deferred mode (buffer_size > 0): stores zero-copy Slice references
  // into the WriteBatch buffer. Caller MUST call AdoptBatchBuffer() after
  // TransformBatch completes to transfer buffer ownership.
  // Thread-safe: multiple writers can call this concurrently.
  Status WriteBlob(const WriteOptions& write_options,
                   uint32_t column_family_id, CompressionType compression,
                   const Slice& key, const Slice& value,
                   uint64_t* blob_file_number, uint64_t* blob_offset,
                   uint64_t* blob_size);


  // Set rep_owner on all pending records that have an empty rep_owner.
  // Called after TransformBatch + std::move to transfer buffer ownership.
  // Look up an unflushed blob value by file number and offset.
  // Returns true if found (copies into *value), false if not pending.
  bool GetPendingBlobValue(uint64_t file_number, uint64_t offset,
                           std::string* value) const;

  // Seal all open partitions. Flushes pending records first.
  Status SealAllPartitions(const WriteOptions& write_options,
                           std::vector<BlobFileAddition>* additions);

  void TakeCompletedBlobFileAdditions(
      std::vector<BlobFileAddition>* additions);

  // Sync all open blob files. Flushes pending records first.
  Status SyncAllOpenFiles(const WriteOptions& write_options);

  // Flush buffered data in all open blob files to OS.
  Status FlushAllOpenFiles(const WriteOptions& write_options);

  // Returns true if deferred flush mode is active.
  bool IsDeferredFlushMode() const { return buffer_size_ > 0; }

  // Dump per-operation timing breakdown to stderr (for benchmarking).
  void DumpTimingStats() const;

 private:
  // A pending blob record waiting to be flushed to disk.
  // key/value Slices point into rep_owner's buffer (zero-copy from WriteBatch).
  struct PendingRecord {
    std::string key;
    std::string value;
    uint64_t file_number;
    uint64_t blob_offset;
  };

  struct Partition {
    port::Mutex mutex;
    port::CondVar pending_cv;
    std::unique_ptr<BlobLogWriter> writer;
    uint64_t file_number = 0;
    uint64_t file_size = 0;
    uint64_t blob_count = 0;
    uint64_t total_blob_bytes = 0;
    uint32_t column_family_id = 0;
    CompressionType compression = kNoCompression;
    uint64_t unflushed_bytes = 0;

    // Deferred flush state
    std::list<PendingRecord> pending_records;
    std::atomic<uint64_t> pending_bytes{0};
    uint64_t next_write_offset = 0;

    std::vector<BlobFileAddition> completed_files;

    Partition();
    ~Partition();
  };

  Status OpenNewBlobFile(Partition* partition, uint32_t column_family_id,
                         CompressionType compression);
  Status CloseBlobFile(Partition* partition);
  Status FlushPendingRecords(Partition* partition);

  // Synchronous write path (when buffer_size_ == 0).
  Status WriteBlobSync(Partition* partition, const Slice& key,
                       const Slice& value, uint64_t* blob_offset);

  // Deferred write path (when buffer_size_ > 0).
  Status WriteBlobDeferred(Partition* partition, const Slice& key,
                           const Slice& value, uint64_t* blob_offset,
                           std::string key_copy, std::string value_copy);

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
  uint64_t buffer_size_;
  uint64_t high_water_mark_;


  std::vector<std::unique_ptr<Partition>> partitions_;
  port::Mutex completed_files_mutex_;

  // Timing instrumentation (atomic relaxed for low overhead).
  mutable std::atomic<uint64_t> deferred_offset_nanos_{0};
  mutable std::atomic<uint64_t> deferred_list_nanos_{0};
  mutable std::atomic<uint64_t> deferred_count_{0};
  mutable std::atomic<uint64_t> sync_addrecord_nanos_{0};
  mutable std::atomic<uint64_t> sync_count_{0};
  mutable std::atomic<uint64_t> mutex_wait_nanos_{0};
  mutable std::atomic<uint64_t> flush_nanos_{0};
  mutable std::atomic<uint64_t> flush_count_{0};
};

}  // namespace ROCKSDB_NAMESPACE
