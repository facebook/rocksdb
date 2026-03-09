//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_write_batch_transformer.h"
#include "port/port.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileCompletionCallback;
class BlobLogWriter;
class Compressor;
class Decompressor;
class IOTracer;
class Logger;
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
      uint64_t blob_file_size, bool use_fsync,
      CompressionType blob_compression_type = kNoCompression,
      uint64_t buffer_size = 0, bool use_direct_io = false,
      uint64_t flush_interval_ms = 0,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      FileChecksumGenFactory* file_checksum_gen_factory = nullptr,
      const FileTypeSet& checksum_handoff_file_types = {},
      BlobFileCompletionCallback* blob_callback = nullptr,
      const std::string& db_id = "", const std::string& db_session_id = "",
      Logger* info_log = nullptr);

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

  // Returns the set of blob file numbers currently open (unsealed) by the
  // partition manager. Used by compaction to skip GC on files still being
  // written to.
  std::unordered_set<uint64_t> GetUnsealedBlobFileNumbers() const;

  // Returns true if deferred flush mode is active.
  bool IsDeferredFlushMode() const { return buffer_size_ > 0; }

  // Get cached blob direct write settings for a column family.
  // Avoids SuperVersion lookup on every Put.
  BlobDirectWriteSettings GetCachedSettings(uint32_t cf_id) const {
    settings_mutex_.Lock();
    auto it = cached_settings_.find(cf_id);
    BlobDirectWriteSettings result;
    if (it != cached_settings_.end()) {
      result = it->second;
    }
    settings_mutex_.Unlock();
    return result;
  }

  // Update cached settings for a column family.
  // Called during DB open and SetOptions.
  void UpdateCachedSettings(uint32_t cf_id,
                            const BlobDirectWriteSettings& settings) {
    settings_mutex_.Lock();
    cached_settings_[cf_id] = settings;
    settings_mutex_.Unlock();
  }

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

  // State captured under the mutex for deferred sealing outside the mutex.
  struct DeferredSeal {
    std::unique_ptr<BlobLogWriter> writer;
    std::vector<PendingRecord> records;
    uint64_t file_number = 0;
    uint64_t blob_count = 0;
    uint64_t total_blob_bytes = 0;
  };

  // Background I/O thread pool for seal and flush operations.
  struct BGWorkItem {
    enum Type { kSeal, kFlush };
    Type type;
    DeferredSeal seal;  // Only used for kSeal.
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

    // Per-partition compressor working area for thread safety.
    Compressor::ManagedWorkingArea compressor_wa;

    // Deferred flush state
    std::vector<PendingRecord> pending_records;
    // Records currently being flushed to disk by the BG thread.
    // Readable by GetPendingBlobValue to avoid read-after-write visibility gap.
    std::vector<PendingRecord> in_flight_records;
    std::atomic<uint64_t> pending_bytes{0};
    uint64_t next_write_offset = 0;

    std::vector<BlobFileAddition> completed_files;

    // Per-partition background work queue (protected by bg_mutex_).
    std::deque<BGWorkItem> bg_queue;
    bool bg_in_flight = false;  // A thread is currently processing this partition.

    Partition();
    ~Partition();
  };

  Status OpenNewBlobFile(Partition* partition, uint32_t column_family_id,
                         CompressionType compression);
  Status CloseBlobFile(Partition* partition);
  Status FlushPendingRecords(Partition* partition);

  // Prepare a file rollover under the mutex: captures old state into
  // DeferredSeal and opens a new file. Writers can immediately continue
  // on the new file after the mutex is released.
  Status PrepareFileRollover(Partition* partition, uint32_t column_family_id,
                             CompressionType compression,
                             DeferredSeal* deferred);

  // Seal a previously-prepared old file outside the mutex: flushes pending
  // records, writes footer, records BlobFileAddition.
  Status SealDeferredFile(Partition* partition, DeferredSeal* deferred);

  // Background I/O thread: processes seal and flush work items.
  void BackgroundIOLoop();

  // Submit a deferred seal to the background thread.
  void SubmitSeal(Partition* partition, DeferredSeal&& seal);

  // Submit a flush request to the background thread.
  void SubmitFlush(Partition* partition);

  // Drain all pending background work. Called before SealAllPartitions.
  void DrainBackgroundWork();

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
  uint64_t flush_interval_us_;  // Periodic flush interval in microseconds.

  std::shared_ptr<Compressor> compressor_;      // null for kNoCompression
  std::shared_ptr<Decompressor> decompressor_;  // for GetPendingBlobValue reads
  CompressionType blob_compression_type_;

  std::shared_ptr<IOTracer> io_tracer_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  FileChecksumGenFactory* file_checksum_gen_factory_;
  FileTypeSet checksum_handoff_file_types_;
  BlobFileCompletionCallback* blob_callback_;
  std::string db_id_;
  std::string db_session_id_;
  Logger* info_log_;

  std::vector<std::unique_ptr<Partition>> partitions_;
  mutable port::Mutex settings_mutex_;
  std::unordered_map<uint32_t, BlobDirectWriteSettings> cached_settings_;

  port::Mutex completed_files_mutex_;

  port::Mutex bg_mutex_;
  port::CondVar bg_cv_;
  port::CondVar bg_drain_cv_;  // Signaled when all queues become empty.
  std::vector<std::thread> bg_threads_;
  bool bg_stop_{false};
  Status bg_status_;  // First error from background thread.
};

}  // namespace ROCKSDB_NAMESPACE
