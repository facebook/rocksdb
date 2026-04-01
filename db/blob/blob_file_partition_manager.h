//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
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
class Env;
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
// BLOB FILE LIFECYCLE INVARIANT
//
// Each blob file maps to exactly one memtable generation (epoch) and
// consequently to exactly one SST after flush. This invariant is enforced
// by rotating blob files at every SwitchMemtable:
//
//   Epoch 1: M0 writes to F1-F4.  Flush M0 -> SST S0 references F1-F4.
//   Epoch 2: M1 writes to F5-F8.  Flush M1 -> SST S1 references F5-F8.
//   Epoch 3: M2 writes to F9-F12. Flush M2 -> SST S2 references F9-F12.
//
// Why this matters:
//
// 1. GC correctness: total_blob_bytes (set at seal time) equals exactly
//    the garbage that will accumulate when the one referencing SST is
//    compacted away. No orphan bytes that permanently block GC.
//
// 2. Crash recovery: if a memtable is lost (e.g., crash without WAL),
//    only that memtable's blob files contain unreachable data. Those files
//    are either orphans (cleaned up by OrphanBlobFileResolver) or their
//    total_blob_bytes matches the committed SST's references exactly.
//    No phantom bytes that prevent file collection.
//
// 3. SaveBlobFilesTo: every BlobFileAddition has a corresponding SST
//    that links to it, so files are never dropped from the version.
//
// The invariant is enforced by:
// - RotateAllPartitions at SwitchMemtable (epoch boundary)
// - Epoch check in write group leader (rejects cross-epoch writes)
// - Epoch-tagged deferred seal batches (flush finds its own batch)
//
// ARCHITECTURE NOTE: Each column family with enable_blob_direct_write=true
// gets its own BlobFilePartitionManager with its own settings. The manager
// is stored in ColumnFamilyData and created during DB::Open. This ensures
// each CF uses its own partition count, buffer size, blob file size, etc.
// without any cross-CF aggregation.
//
// FILE NUMBER ALLOCATION: File numbers are allocated during Put() via
// VersionSet::NewFileNumber(), potentially many versions before the blob
// file is registered in the MANIFEST. After crashes, orphan recovery in
// db_impl_open.cc reconciles unregistered blob files. This creates file
// number gaps and relies entirely on orphan recovery for crash consistency.
//
// Supports a pre-copy deferred flush model (when buffer_size > 0):
// - WriteBlob() copies key/value into std::string-backed PendingRecords
//   and pre-calculates offsets (one memcpy per Put)
// - PendingRecords are queued and flushed to disk via Env::Schedule
// - Backpressure via atomic pending_bytes with stall watermark
// - Read path checks pending records for unflushed data
//
// The deferred flush model (~500+ lines) provides significant syscall
// reduction for small values but adds
// complexity: Env::Schedule callbacks, pending/in-flight record tracking,
// 4-tier read fallback, and backpressure logic. For large values (64KB+), the
// per-record syscall overhead is proportionally small. The sync-only path
// (buffer_size=0) is significantly simpler.
class BlobFilePartitionManager {
 public:
  using FileNumberAllocator = std::function<uint64_t()>;

  BlobFilePartitionManager(
      uint32_t num_partitions,
      std::shared_ptr<BlobFilePartitionStrategy> strategy,
      FileNumberAllocator file_number_allocator, Env* env, FileSystem* fs,
      SystemClock* clock, Statistics* statistics,
      const FileOptions& file_options, const std::string& db_path,
      uint64_t blob_file_size, bool use_fsync,
      CompressionType blob_compression_type, uint64_t buffer_size,
      bool use_direct_io, uint64_t flush_interval_ms,
      const std::shared_ptr<IOTracer>& io_tracer,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      FileChecksumGenFactory* file_checksum_gen_factory,
      const FileTypeSet& checksum_handoff_file_types,
      BlobFileCache* blob_file_cache, BlobFileCompletionCallback* blob_callback,
      const std::string& db_id, const std::string& db_session_id,
      Logger* info_log);

  ~BlobFilePartitionManager();

  // Write a blob value to a partition. Returns blob file number, offset, size.
  // In deferred mode (buffer_size > 0): copies key/value into PendingRecords
  // for later BG flush. In sync mode (buffer_size == 0): writes directly.
  // Thread-safe: multiple writers can call this concurrently.
  // If caller already has the settings, pass them to avoid a redundant lookup.
  Status WriteBlob(const WriteOptions& write_options, uint32_t column_family_id,
                   CompressionType compression, const Slice& key,
                   const Slice& value, uint64_t* blob_file_number,
                   uint64_t* blob_offset, uint64_t* blob_size,
                   const BlobDirectWriteSettings* settings = nullptr);

  // Look up an unflushed blob value by file number and offset.
  // Returns OK if found (value populated), NotFound if not pending,
  // or an error Status on decompression failure.
  Status GetPendingBlobValue(uint64_t file_number, uint64_t offset,
                             std::string* value) const;

  // Seal all open partitions. Flushes pending records first.
  // Returns OK immediately if no blobs have been written since the last seal.
  // If seal_all is true, seals both rotation deferred files AND active files
  // (used during DB shutdown). Otherwise, seals only rotation deferred files
  // (normal flush path) or active files (no rotation happened).
  //
  // epochs: the blob_write_epochs of the memtables being flushed. Used to find
  // the correct deferred batches in the rotation queue (epoch-tagged matching
  // instead of FIFO pop). Pass empty to seal active partition files (no
  // rotation happened, e.g., manual flush before memtable is full). When
  // multiple memtables are flushed together, pass all their epochs.
  Status SealAllPartitions(
      const WriteOptions& write_options,
      std::vector<BlobFileAddition>* additions, bool seal_all = false,
      const std::vector<uint64_t>& epochs = std::vector<uint64_t>());

  // Collect completed (sealed) blob file additions from all partitions.
  // Called during flush to gather BlobFileAddition metadata for the
  // VersionEdit. Additions are moved out of the partition state, so
  // each addition is returned exactly once.
  void TakeCompletedBlobFileAdditions(std::vector<BlobFileAddition>* additions);

  // Return sealed blob file additions that were not consumed (e.g., because
  // the flush was switched to mempurge). The additions are pushed back into
  // partition 0's completed_files so they will be picked up by the next flush.
  void ReturnUnconsumedAdditions(std::vector<BlobFileAddition>&& additions);

  // Ensure blob files referenced by WALs up to a durability boundary are
  // durable before WAL durability advances. This always syncs
  // rotation_deferred_seals_ without sealing them so the eventual flush can
  // still append the footer and register the file in MANIFEST. When
  // `sync_open_files` is true, it also syncs the currently open files for this
  // CF since they may still contain records referenced by the WALs being
  // durably advanced.
  Status SyncWalRelevantFiles(const WriteOptions& write_options,
                              bool sync_open_files);

  // Sync all open blob files. Flushes pending records first.
  Status SyncAllOpenFiles(const WriteOptions& write_options);

  // Flush buffered data in all open blob files to the OS. In deferred mode,
  // same-partition writers are blocked until the active pending snapshot has
  // been drained, so callers can publish BlobIndex offsets only after the
  // referenced bytes are disk-readable.
  Status FlushAllOpenFiles(const WriteOptions& write_options);

  // Returns true if deferred flush mode is active.
  bool IsDeferredFlushMode() const { return buffer_size_ > 0; }

  // Collect blob file numbers managed by this partition manager. This
  // includes files being written, files being sealed (I/O in progress),
  // and sealed files awaiting MANIFEST commit. The file_to_partition_
  // mapping is retained until the flush caller commits the file to MANIFEST
  // and calls RemoveFilePartitionMappings(). Used by FindObsoleteFiles to
  // prevent PurgeObsoleteFiles from deleting files not yet in blob_live_set.
  void GetActiveBlobFileNumbers(
      std::unordered_set<uint64_t>* file_numbers) const;

  // Remove multiple file_number mappings. Called by the flush path after
  // sealed blob files have been committed to the MANIFEST, so
  // PurgeObsoleteFiles will find them in blob_live_set instead.
  void RemoveFilePartitionMappings(const std::vector<uint64_t>& file_numbers);

  // Get cached blob direct write settings for this manager's column family.
  // Lock-free read via acquire load on the settings pointer.
  BlobDirectWriteSettings GetCachedSettings(uint32_t /*cf_id*/) const {
    const BlobDirectWriteSettings* s =
        cached_settings_.load(std::memory_order_acquire);
    return s ? *s : BlobDirectWriteSettings{};
  }

  // Update cached settings for this manager's column family.
  // Called during DB open and by SetOptions() when min_blob_size or
  // blob_compression_type change. Uses copy-on-write: allocates a new
  // settings snapshot and retires the old one (freed at destruction).
  // Thread-safe: concurrent readers see either the old or new snapshot.
  void UpdateCachedSettings(uint32_t cf_id,
                            const BlobDirectWriteSettings& settings) {
    (void)cf_id;
    std::lock_guard<std::mutex> lock(settings_write_mutex_);
    const BlobDirectWriteSettings* old =
        cached_settings_.load(std::memory_order_relaxed);
    auto* new_settings = new BlobDirectWriteSettings(settings);
    cached_settings_.store(new_settings, std::memory_order_release);
    if (old) {
      retired_settings_.push_back(old);
    }
  }

  // Resolve a blob index from the write path using 4-tier fallback:
  //   1. Version::GetBlob (standard path for registered blob files)
  //   2. Pending records (unflushed deferred data in partition manager)
  //   3. BlobFileCache (direct read for unregistered files, with
  //      evict-and-uncached-retry for stale cached readers)
  //   4. Retry pending records — covers the race window where the BG
  //      thread removed a record from pending_index (so tier 1 missed)
  //      but the data is not yet readable on disk (file not synced/sealed,
  //      or BlobFileReader has stale file_size_)
  // The BlobIndex must be pre-decoded by the caller.
  static Status ResolveBlobDirectWriteIndex(
      const ReadOptions& read_options, const Slice& user_key,
      const BlobIndex& blob_idx, const Version* version,
      BlobFileCache* blob_file_cache, BlobFilePartitionManager* partition_mgr,
      PinnableSlice* blob_value);

  // Dump per-operation timing breakdown to stderr (for benchmarking).
  void DumpTimingStats() const;

  // Subtract uncommitted bytes from the manager's tracking. Called when
  // a WriteBatch that was already transformed (blobs written to files)
  // fails to commit. The bytes are accumulated in uncommitted_bytes_ and
  // subtracted during the next seal to keep total_blob_bytes accurate
  // for GC calculations.
  void SubtractUncommittedBytes(uint64_t bytes, uint64_t file_number);

  // ====================================================================
  // EPOCH-BASED ROTATION
  // ====================================================================
  //
  // Rotate blob files at SwitchMemtable time so each blob file maps to
  // exactly one memtable. Writers snapshot the epoch before WriteBlob
  // and the write group leader checks it after PreprocessWrite. Stale
  // writers are rejected with TryAgain and retry from WriteBlob.
  //
  // PROTOCOL:
  //   Writer: epoch = GetRotationEpoch() → WriteBlob → WriteImpl
  //   Leader: PreprocessWrite (may SwitchMemtable → RotateAllPartitions)
  //           → check each writer's epoch → reject mismatches
  //
  // LOCK ORDERING with rotation:
  //   db_mutex_ → bg_mutex_ → partition->mutex
  //   Writer path: partition->mutex → RELEASE → write group
  //   No circular dependency → deadlock-free.

  // Returns the current rotation epoch (acquire ordering).
  uint64_t GetRotationEpoch() const {
    return rotation_epoch_.load(std::memory_order_acquire);
  }

  // Rotate all partitions: capture old files into DeferredSeals, open
  // new files, bump the rotation epoch. Called from SwitchMemtable
  // under db_mutex_. The captured DeferredSeals are stored internally
  // and sealed later by SealAllPartitions during the flush path.
  //
  // Does NOT do I/O for sealing (no footer write). Only opens new files
  // (creates file + writes header, which is fast).
  Status RotateAllPartitions();

 private:
  // ====================================================================
  // SYNCHRONIZATION OVERVIEW
  // ====================================================================
  //
  // LOCKS (ordered from outermost to innermost):
  //
  //   bg_mutex_         Protects bg_seal_in_progress_, bg_status_.
  //                     Never held during I/O.
  //
  //   partition->mutex  Per-partition lock. Protects writer, file_number,
  //                     file_size, blob_count, total_blob_bytes,
  //                     pending_records, pending_index, completed_files,
  //                     next_write_offset, column_family_id, compression.
  //                     Held briefly during state capture; released
  //                     before I/O in BG flush/seal paths.
  //
  //   file_partition_mutex_  RW-lock protecting file_to_partition_ map.
  //                     Write-locked on file open/close (rare).
  //                     Read-locked on each GetPendingBlobValue (read path).
  //
  //   settings_write_mutex_  Protects cached_settings_ writes (rare;
  //                     only during SetOptions). Readers are lock-free
  //                     via atomic load.
  //
  // LOCK ORDERING: bg_mutex_ -> partition->mutex -> file_partition_mutex_
  //   (no path acquires them in reverse order)
  //
  // LOCK-FREE ATOMICS:
  //   pending_bytes     Per-partition; updated on write (add) and
  //                     flush (sub). Read without lock for backpressure.
  //   bg_in_flight_     Counts outstanding Env::Schedule callbacks.
  //   bg_has_error_     Fast check for bg_status_ errors.
  //   bg_timer_stop_    Shutdown signal for the periodic flush timer.
  //   bg_timer_running_ True while the periodic timer thread is running.
  //   blobs_written_since_seal_  Fast-path skip in SealAllPartitions.
  //   flush_queued      Per-partition; prevents duplicate flush scheduling.
  //
  // THREE OPERATION FLOWS:
  //
  //   WRITE (WriteBlob):
  //     1. Select partition via strategy
  //     2. Backpressure: stall if pending_bytes >= buffer_size_
  //     3. Compress value outside mutex
  //     4. Lock partition->mutex
  //     5. Open file if needed; write (sync) or enqueue (deferred)
  //     6. If file full: PrepareFileRollover -> SubmitSeal
  //     7. If pending_bytes >= high_water_mark_: SubmitFlush
  //     8. Unlock, prepopulate blob cache
  //
  //   BG FLUSH (via Env::Schedule -> BGFlushWrapper):
  //     1. Lock partition->mutex, move pending_records to local deque
  //     2. Unlock, write records to BlobLogWriter, flush to OS
  //     3. Lock partition->mutex, remove from pending_index, signal CV
  //     4. Clear flush_queued (after I/O, not before, to prevent
  //        concurrent flushes on the same partition)
  //
  //   BG SEAL (via Env::Schedule -> BGSealWrapper):
  //     1. Write deferred records to old BlobLogWriter
  //     2. Flush to OS, write footer
  //     3. Evict any cached pre-seal BlobFileReader for that file
  //     4. Lock partition->mutex, add to completed_files
  //     5. Remove from pending_index, keep file_partition mapping until
  //        MANIFEST commit
  //
  // ====================================================================
  // A pending blob record waiting to be flushed to disk.
  // Owns the key and value data.
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
    // True once records have been appended and flushed to the file. The
    // records remain in-memory until final seal so reads can still use the
    // pending-index fallback.
    bool records_flushed = false;
    // True once the file body (header + records) has been synced as part of
    // inactive-WAL durability advancement. Final seal still appends the
    // footer and syncs again before close.
    bool closed_wal_synced = false;
  };

  struct Partition {
    port::Mutex mutex;
    port::CondVar pending_cv;
    std::unique_ptr<BlobLogWriter> writer;
    uint64_t file_number = 0;
    uint64_t file_size = 0;
    uint64_t blob_count = 0;
    uint64_t total_blob_bytes = 0;
    // True once records have been appended to this file and not yet synced.
    // Protected by this partition's mutex.
    bool sync_required = false;
    uint32_t column_family_id = 0;
    CompressionType compression = kNoCompression;
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
    //
    // LIFECYCLE: An entry is created under the partition mutex when a
    // deferred write appends a PendingRecord to pending_records. The
    // PendingBlobValueEntry::data pointer points into the PendingRecord's
    // std::string value, which lives in a std::deque<PendingRecord>.
    // std::deque guarantees that move-construction preserves element
    // addresses (C++11 [deque.modifiers]), so the pointer remains valid
    // when pending_records is moved into a DeferredSeal or into a local
    // deque for BG flush. The BG flush callback writes the records to disk
    // and then calls RemoveFromPendingIndex (under the partition mutex)
    // to erase the entries. Once removed, the PendingRecord strings are
    // freed with the deque.
    //
    // Readers (GetPendingBlobValue) must copy the string under the
    // partition mutex because the BG thread may free the backing
    // PendingRecord immediately after the mutex is released.
    //
    // RACE NOTE (Tier 4): There is a brief window after
    // RemoveFromPendingIndex removes an entry but before the data is
    // readable on disk (file may not be synced/sealed yet). The Tier 4
    // retry in ResolveBlobDirectWriteIndex covers this gap.
    std::unordered_map<PendingBlobKey, PendingBlobValueEntry,
                       PendingBlobKeyHash>
        pending_index;

    std::vector<BlobFileAddition> completed_files;

    // Deduplication flag for BG flush submissions. If true, a flush
    // is already scheduled via Env::Schedule; no need to submit another.
    std::atomic<bool> flush_queued{false};

    // True while an open-file drain is serializing the active writer with a
    // fixed snapshot of pending records. Writers, rotations, active-file
    // seals, and other open-file drains wait on pending_cv while this barrier
    // is active so the writer cannot move to a new file or gain new pending
    // records before the drain completes.
    bool sync_barrier_active = false;

    Partition();
    ~Partition();
  };

  // Context for Env::Schedule seal callback.
  struct BGSealContext {
    BlobFilePartitionManager* mgr;
    Partition* partition;
    DeferredSeal seal;
  };
  // Context for Env::Schedule flush callback.
  struct BGFlushContext {
    BlobFilePartitionManager* mgr;
    Partition* partition;
  };

  // Remove entries from the partition's pending_index for all records in
  // the given deque. Acquires the partition mutex internally.
  void RemoveFromPendingIndex(Partition* partition,
                              const std::deque<PendingRecord>& records);
  // Same as RemoveFromPendingIndex but assumes the partition mutex is
  // already held by the caller.
  void RemoveFromPendingIndexLocked(Partition* partition,
                                    const std::deque<PendingRecord>& records);

  // Register a file_number → partition_idx mapping so GetPendingBlobValue
  // can route lookups to the correct partition. Called when a new blob
  // file is opened.
  void AddFilePartitionMapping(uint64_t file_number, uint32_t partition_idx);
  // Remove the file_number mapping. Called on error paths when a file was
  // never successfully sealed (no data to commit to MANIFEST).
  void RemoveFilePartitionMapping(uint64_t file_number);

  // Reset partition state: clears counters and writer.
  // If remove_mapping is true, also removes the file→partition mapping
  // (used on error paths where the file is unusable). On success paths,
  // the mapping is retained until the file is committed to MANIFEST.
  void ResetPartitionState(Partition* partition, uint64_t file_number,
                           bool remove_mapping = true);

  // Open a new blob file for writing in the given partition. Allocates a
  // file number, creates the file, writes the blob log header, and
  // registers the file→partition mapping.
  Status OpenNewBlobFile(Partition* partition, uint32_t column_family_id,
                         CompressionType compression);
  // Close and seal the blob file in the given partition: flushes pending
  // records, writes the footer, syncs, and records a BlobFileAddition.
  Status CloseBlobFile(Partition* partition);
  // Flush all buffered PendingRecords in the partition to its BlobLogWriter.
  // After writing, removes the corresponding pending_index entries.
  Status FlushPendingRecords(Partition* partition,
                             const WriteOptions& write_options);

  // Prepare a file rollover under the mutex: captures old state into
  // DeferredSeal and opens a new file. Writers can immediately continue
  // on the new file after the mutex is released.
  Status PrepareFileRollover(Partition* partition, uint32_t column_family_id,
                             CompressionType compression,
                             DeferredSeal* deferred);

  // Seal a previously-prepared old file outside the mutex: flushes pending
  // records, writes footer, records BlobFileAddition.
  Status SealDeferredFile(Partition* partition, DeferredSeal* deferred);

  // Drop any cached reader that may have been opened before a footer was
  // appended. After seal, the on-disk file size and footer visibility change.
  void EvictSealedBlobFileReader(uint64_t file_number);

  // Flush deferred-seal records exactly once. Used both by final sealing and
  // the inactive-WAL durability path.
  Status FlushDeferredSealRecords(const WriteOptions& write_options,
                                  Partition* partition, DeferredSeal* deferred);

  // Sync a deferred seal's file body for inactive-WAL durability without
  // sealing the file.
  Status SyncDeferredSealForClosedWal(const WriteOptions& write_options,
                                      Partition* partition,
                                      DeferredSeal* deferred);

  // Drain all currently open files in this manager with a per-partition
  // barrier so no same-partition write can append behind an already-running
  // flush. When `sync_to_disk` is true, also Sync() the active writer and
  // clear sync_required on success. If `had_open_files` is non-null, it is
  // set to true when at least one partition had an open writer.
  Status DrainOpenFilesInternal(const WriteOptions& write_options,
                                bool sync_to_disk, bool* had_open_files);

  // Sync all currently open files in this manager. Flushes pending records
  // first. If `had_open_files` is non-null, it is set to true when at least
  // one partition had an open writer to sync.
  Status SyncOpenFilesInternal(const WriteOptions& write_options,
                               bool* had_open_files);

  // Submit a deferred seal to the background via Env::Schedule.
  void SubmitSeal(Partition* partition, DeferredSeal&& seal);

  // Submit a flush request to the background via Env::Schedule.
  void SubmitFlush(Partition* partition);

  // Wait for all in-flight background operations to complete.
  void DrainBackgroundWork();

  // Record a BG error. First error wins; subsequent errors are dropped.
  void SetBGError(const Status& s);

  // Decrement bg_in_flight_ and signal bg_cv_ if it reaches zero.
  void DecrementBGInFlight();

  // Env::Schedule callback for seal operations.
  static void BGSealWrapper(void* arg);
  // Env::Schedule callback for flush operations.
  static void BGFlushWrapper(void* arg);
  // Env::Schedule callback for periodic flush timer.
  static void BGPeriodicFlushWrapper(void* arg);

  // Flush deferred records to a BlobLogWriter. Returns the number of
  // successfully written records via *records_written and decrements
  // pending_bytes for all records (written or not).
  Status FlushRecordsToDisk(const WriteOptions& write_options,
                            BlobLogWriter* writer, Partition* partition,
                            std::deque<PendingRecord>& records,
                            size_t* records_written);

  // Synchronous write path (when buffer_size_ == 0). Appends the blob
  // record directly to the partition's BlobLogWriter under the mutex.
  Status WriteBlobSync(Partition* partition, const Slice& key,
                       const Slice& value, uint64_t* blob_offset);

  // Deferred write path (when buffer_size_ > 0). Appends a PendingRecord
  // (with pre-copied key/value) to the partition's deque for later BG
  // flush. Applies backpressure if pending_bytes exceeds high_water_mark_.
  Status WriteBlobDeferred(Partition* partition, const Slice& key,
                           const Slice& value, uint64_t* blob_offset,
                           std::string key_copy, std::string value_copy);

  const uint32_t num_partitions_;
  // Partition selection policy (default: round-robin).
  std::shared_ptr<BlobFilePartitionStrategy> strategy_;
  // Allocates globally-unique file numbers via VersionSet::NewFileNumber().
  FileNumberAllocator file_number_allocator_;
  Env* env_;
  FileSystem* fs_;
  SystemClock* clock_;
  Statistics* statistics_;
  FileOptions file_options_;
  std::string db_path_;
  uint64_t blob_file_size_;
  bool use_fsync_;
  uint64_t buffer_size_;
  // Backpressure threshold: when pending_bytes exceeds this, writers stall.
  uint64_t high_water_mark_;
  // Periodic flush interval (microseconds). 0 = disabled.
  uint64_t flush_interval_us_;

  // Default compression for blob records in this CF.
  CompressionType blob_compression_type_;

  std::shared_ptr<IOTracer> io_tracer_;
  // Event listeners notified on blob file creation/deletion.
  std::vector<std::shared_ptr<EventListener>> listeners_;
  FileChecksumGenFactory* file_checksum_gen_factory_;
  FileTypeSet checksum_handoff_file_types_;
  BlobFileCache* blob_file_cache_;
  // Callback to register completed blob files with VersionEdit.
  BlobFileCompletionCallback* blob_callback_;
  // Identifiers embedded in blob file headers for provenance.
  std::string db_id_;
  std::string db_session_id_;
  Logger* info_log_;

  std::vector<std::unique_ptr<Partition>> partitions_;
  // Per-CF cached settings: readers load the pointer (acquire),
  // writers allocate a new copy and store (release). Old copies are
  // retired and freed at destruction.
  std::atomic<const BlobDirectWriteSettings*> cached_settings_{nullptr};
  mutable std::mutex settings_write_mutex_;
  std::vector<const BlobDirectWriteSettings*> retired_settings_;

  // Maps blob file numbers to their owning partition index. Entries are
  // added when a new blob file is opened and removed only when the file
  // is committed to the MANIFEST (by the flush caller via
  // RemoveFilePartitionMappings) or on error (when the file is unusable).
  // This means sealed-but-not-yet-committed files remain in the map,
  // which serves double duty:
  //   1. GetPendingBlobValue routes lookups to the correct partition.
  //   2. GetActiveBlobFileNumbers returns all managed file numbers,
  //      preventing PurgeObsoleteFiles from deleting them.
  // Write-light (file open/close/commit), read-moderate (each
  // GetPendingBlobValue). Protected by file_partition_mutex_.
  std::unordered_map<uint64_t, uint32_t> file_to_partition_;
  mutable port::RWMutex file_partition_mutex_;

  // Background work coordination. Seal and flush operations are submitted
  // to Env::Schedule(BOTTOM). bg_in_flight_ tracks outstanding operations;
  // bg_cv_ is signaled when it reaches zero so DrainBackgroundWork can
  // return. bg_seal_in_progress_ prevents new Env::Schedule calls during
  // SealAllPartitions to avoid races with partition state capture.
  port::Mutex bg_mutex_;
  port::CondVar bg_cv_;
  std::atomic<uint32_t> bg_in_flight_{0};
  bool bg_seal_in_progress_{false};
  // First error from a BG operation; subsequent errors are dropped.
  Status bg_status_;
  // Lock-free check for bg_status_ to avoid mutex on the write hot path.
  std::atomic<bool> bg_has_error_{false};
  // Set during shutdown to stop the periodic flush timer.
  std::atomic<bool> bg_timer_stop_{false};
  // True while the periodic flush timer thread is running.
  std::atomic<bool> bg_timer_running_{false};

  // Tracks whether any blobs have been written since the last
  // SealAllPartitions call. Enables fast-path skip in SealAllPartitions
  // when no blob writes occurred (common when flush fires for non-blob CFs).
  std::atomic<uint64_t> blobs_written_since_seal_{0};

  // Accumulated bytes from failed commits that need to be subtracted
  // from total_blob_bytes during the next seal. This keeps GC accurate
  // by not counting unreferenced blob records as live data.
  // Per-file uncommitted bytes from epoch mismatch retries and write rollbacks.
  // Protected by bg_mutex_.
  std::unordered_map<uint64_t, uint64_t> file_uncommitted_bytes_;

  // Rotation epoch: bumped by RotateAllPartitions at each SwitchMemtable.
  // Writers snapshot with acquire before WriteBlob; the write group leader
  // checks with acquire after PreprocessWrite. Release store in
  // RotateAllPartitions publishes the new file state.
  // Starts at 1 (not 0) so the epoch check in WriteImpl can use
  // blob_write_epoch != 0 as a "blob direct write is active" flag.
  std::atomic<uint64_t> rotation_epoch_{1};

  // DeferredSeals captured by RotateAllPartitions, waiting to be sealed
  // by SealAllPartitions during the flush path. Protected by bg_mutex_.
  // Each RotateAllPartitions call pushes one batch (one entry per partition
  // that had an active writer), tagged with the rotation epoch.
  // SealAllPartitions finds the batch matching the flushing memtable's epoch.
  struct RotationBatch {
    uint64_t epoch;
    std::vector<std::pair<Partition*, DeferredSeal>> seals;
  };
  std::deque<RotationBatch> rotation_deferred_seals_;
  // Serializes SyncWalRelevantFiles() with SealAllPartitions() so
  // deferred-seal state is not moved out from under a concurrent durability
  // walk.
  port::Mutex deferred_seal_sync_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
