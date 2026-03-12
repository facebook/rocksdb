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
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_write_batch_transformer.h"
#include "port/port.h"
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

class BlobFileCache;
class BlobFileCompletionCallback;
class BlobIndex;
class BlobLogWriter;
class Decompressor;
class IOTracer;
class Logger;
class PinnableSlice;
class SystemClock;
class Version;
class WritableFileWriter;
struct FileOptions;
struct ImmutableDBOptions;
struct ReadOptions;

// Default round-robin partition strategy.
class RoundRobinPartitionStrategy : public BlobFilePartitionStrategy {
 public:
  uint32_t SelectPartition(uint32_t num_partitions,
                           uint32_t /*column_family_id*/, const Slice& /*key*/,
                           const Slice& /*value*/) const override {
    return static_cast<uint32_t>(
        next_index_.fetch_add(1, std::memory_order_relaxed) % num_partitions);
  }

 private:
  mutable std::atomic<uint64_t> next_index_{0};
};

// Manages partitioned blob files for the write-path blob direct write feature.
//
// ARCHITECTURE NOTE: This manager is instantiated once per DB and shared
// across all column families. Per-CF partition managers would be more
// consistent with RocksDB's per-CF blob file model and would eliminate
// settings aggregation issues, CF-switching file churn, and the need for
// CF-aware orphan recovery. Consider migrating to per-CF managers in a
// future iteration.
//
// FILE NUMBER ALLOCATION: File numbers are allocated during Put() via
// VersionSet::NewFileNumber(), potentially many versions before the blob
// file is registered in the MANIFEST. After crashes, orphan recovery in
// db_impl_open.cc reconciles unregistered blob files. This creates file
// number gaps and relies entirely on orphan recovery for crash consistency.
// Consider registering blob files in the MANIFEST at creation time with
// a "pending" state to eliminate the need for orphan recovery.
//
// Supports a zero-copy deferred flush model (when buffer_size > 0):
// - WriteBlob() pre-calculates offsets and stores Slice references pointing
//   directly into the caller's WriteBatch buffer (no memcpy)
// - After TransformBatch, the caller moves the WriteBatch rep_ into shared
//   ownership via AdoptBatchBuffer() so Slices remain valid
// - FlushPendingRecords() writes to disk in batch and releases buffer refs
// - Backpressure via atomic pending_bytes with stall watermark
// - Read path checks pending records for unflushed data
//
// The deferred flush model (~500+ lines) provides significant syscall
// reduction for small values (e.g., 250x for 4KB values) but adds
// complexity: BG thread pool, pending/in-flight record tracking, 4-tier
// read fallback, and backpressure logic. For large values (64KB+), the
// per-record syscall overhead is proportionally small. The sync-only path
// (buffer_size=0) is approximately 15 lines.
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
  // If caller already has the settings, pass them to avoid a redundant lookup.
  Status WriteBlob(const WriteOptions& write_options,
                   uint32_t column_family_id, CompressionType compression,
                   const Slice& key, const Slice& value,
                   uint64_t* blob_file_number, uint64_t* blob_offset,
                   uint64_t* blob_size,
                   const BlobDirectWriteSettings* settings = nullptr);

  // Look up an unflushed blob value by file number and offset.
  // Returns OK if found (value populated), NotFound if not pending,
  // or an error Status on decompression failure.
  Status GetPendingBlobValue(uint64_t file_number, uint64_t offset,
                             std::string* value) const;

  // Seal all open partitions. Flushes pending records first.
  // Returns OK immediately if no blobs have been written since the last seal.
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

  // Collect blob file numbers currently open for writing. Used by
  // FindObsoleteFiles to prevent DeleteObsoleteFiles from deleting
  // active blob files that haven't been registered in the MANIFEST yet.
  void GetActiveBlobFileNumbers(
      std::unordered_set<uint64_t>* file_numbers) const;

  using SettingsMap = std::unordered_map<uint32_t, BlobDirectWriteSettings>;

  // Get cached blob direct write settings for a column family.
  // Lock-free read: loads a raw pointer to an immutable map snapshot.
  // Zero atomic ref-count operations (just one pointer load).
  BlobDirectWriteSettings GetCachedSettings(uint32_t cf_id) const {
    const SettingsMap* map = cached_settings_.load(std::memory_order_acquire);
    if (map) {
      auto it = map->find(cf_id);
      if (it != map->end()) {
        return it->second;
      }
    }
    return BlobDirectWriteSettings{};
  }

  // Update cached settings for a column family.
  // Called only during DB open (single-threaded, no concurrent readers).
  // Creates a new immutable snapshot via copy-on-write; old snapshots are
  // retired and freed at destruction.
  // NOTE: Dynamic option changes via SetOptions() are NOT reflected here.
  // If enable_blob_direct_write, min_blob_size, or compression settings
  // change at runtime, the cached settings become stale. This is acceptable
  // because enable_blob_direct_write requires DB reopen to change.
  void UpdateCachedSettings(uint32_t cf_id,
                            const BlobDirectWriteSettings& settings) {
    std::lock_guard<std::mutex> lock(settings_write_mutex_);
    const SettingsMap* old_map =
        cached_settings_.load(std::memory_order_relaxed);
    auto* new_map = old_map ? new SettingsMap(*old_map) : new SettingsMap();
    (*new_map)[cf_id] = settings;
    cached_settings_.store(new_map, std::memory_order_release);
    if (old_map) {
      retired_settings_.push_back(old_map);
    }
  }

  // Resolve a blob index from the write path using 4-tier fallback:
  //   1. Pending records (unflushed deferred data in partition manager)
  //   2. Version::GetBlob (standard path for registered blob files)
  //   3. BlobFileCache (direct read for unregistered files, with
  //      evict-and-retry for stale cached readers)
  //   4. Retry pending records (safety net for BG flush race)
  // The BlobIndex must be pre-decoded by the caller.
  static Status ResolveBlobDirectWriteIndex(
      const ReadOptions& read_options, const Slice& user_key,
      const BlobIndex& blob_idx, const Version* version,
      BlobFileCache* blob_file_cache, BlobFilePartitionManager* partition_mgr,
      PinnableSlice* blob_value);

  // Dump per-operation timing breakdown to stderr (for benchmarking).
  void DumpTimingStats() const;

 private:
  // A pending blob record waiting to be flushed to disk.
  // Owns the key and value data. Use std::list so that pointers into
  // elements remain stable across insertions (pending_index stores raw
  // pointers into these records).
  struct PendingRecord {
    std::string key;
    std::string value;
    uint64_t file_number;
    uint64_t blob_offset;
  };

  // Key for the per-partition pending blob index (O(1) lookup by file+offset).
  struct PendingBlobKey {
    uint64_t file_number;
    uint64_t blob_offset;
    bool operator==(const PendingBlobKey& o) const {
      return file_number == o.file_number && blob_offset == o.blob_offset;
    }
  };
  struct PendingBlobKeyHash {
    size_t operator()(const PendingBlobKey& k) const {
      return std::hash<uint64_t>()(k.file_number) * 0x9e3779b97f4a7c15ULL +
             std::hash<uint64_t>()(k.blob_offset);
    }
  };

  struct PendingBlobValueEntry {
    const std::string* data;  // Non-owning pointer into PendingRecord::value
    CompressionType compression;
  };

  // State captured under the mutex for deferred sealing outside the mutex.
  struct DeferredSeal {
    std::unique_ptr<BlobLogWriter> writer;
    std::deque<PendingRecord> records;
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

    // Deferred flush state. Uses std::deque so that push_back does not
    // invalidate pointers to existing elements (pending_index stores raw
    // pointers into PendingRecord::value).
    std::deque<PendingRecord> pending_records;
    std::atomic<uint64_t> pending_bytes{0};
    uint64_t next_write_offset = 0;

    // Per-partition pending blob index for O(1) read-path lookup by
    // (file_number, blob_offset). Protected by this partition's mutex,
    // eliminating the global serialization point that a shared index would
    // create across all partitions.
    std::unordered_map<PendingBlobKey, PendingBlobValueEntry,
                       PendingBlobKeyHash>
        pending_index;

    std::vector<BlobFileAddition> completed_files;

    // Per-partition background work queue (protected by bg_mutex_).
    std::deque<BGWorkItem> bg_queue;
    bool bg_in_flight = false;
    std::atomic<bool> flush_queued{false};

    Partition();
    ~Partition();
  };

  void RemoveFromPendingIndex(Partition* partition,
                              const std::deque<PendingRecord>& records);
  void RemoveFromPendingIndexLocked(Partition* partition,
                                    const std::deque<PendingRecord>& records);

  void AddFilePartitionMapping(uint64_t file_number, uint32_t partition_idx);
  void RemoveFilePartitionMapping(uint64_t file_number);

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

  // Flush deferred records to a BlobLogWriter. Returns the number of
  // successfully written records via *records_written and decrements
  // pending_bytes for all records (written or not).
  Status FlushRecordsToDisk(BlobLogWriter* writer, Partition* partition,
                            std::deque<PendingRecord>& records,
                            size_t* records_written);

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
  // RCU-based settings cache: readers load the raw pointer (zero overhead),
  // writers use settings_write_mutex_ + copy-on-write + atomic store.
  // Old snapshots are retired into retired_settings_ and freed at destruction.
  std::atomic<const SettingsMap*> cached_settings_{nullptr};
  mutable std::mutex settings_write_mutex_;
  std::vector<const SettingsMap*> retired_settings_;

  // Maps active blob file numbers to their owning partition index.
  // Used by GetPendingBlobValue to direct lookups to the correct
  // partition's pending_index without scanning all partitions.
  // Write-light (only on file open/close), read-moderate (each
  // GetPendingBlobValue). RCU-based: readers use atomic_load (lock-free),
  // writers use file_partition_write_mutex_ + copy-on-write + atomic_store.
  using FilePartitionMap = std::unordered_map<uint64_t, uint32_t>;
  std::shared_ptr<FilePartitionMap> file_to_partition_;
  mutable std::mutex file_partition_write_mutex_;

  port::Mutex completed_files_mutex_;

  port::Mutex bg_mutex_;
  port::CondVar bg_cv_;
  port::CondVar bg_drain_cv_;
  // Ready queue for O(1) work dispatch: partitions with pending BG work
  // are enqueued here instead of requiring a linear scan of all partitions.
  std::deque<Partition*> ready_queue_;
  std::vector<std::thread> bg_threads_;
  bool bg_stop_{false};
  bool bg_seal_in_progress_{false};  // Prevents BG threads from picking up new
                                     // work during SealAllPartitions.
  port::CondVar bg_seal_done_cv_;    // Signaled when seal completes.
  Status bg_status_;                 // First error from background thread.
  std::atomic<bool> bg_has_error_{false};  // Lock-free check for bg_status_.

  // Tracks whether any blobs have been written since the last
  // SealAllPartitions call. Enables fast-path skip in SealAllPartitions
  // when no blob writes occurred (common when flush fires for non-blob CFs).
  std::atomic<uint64_t> blobs_written_since_seal_{0};
};

}  // namespace ROCKSDB_NAMESPACE
