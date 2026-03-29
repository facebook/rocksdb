//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_partition_manager.h"

#include <algorithm>

#include "cache/cache_key.h"
#include "cache/typed_cache.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_completion_callback.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_writer.h"
#include "db/blob/blob_source.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

BlobFilePartitionManager::Partition::Partition() : pending_cv(&mutex) {}
BlobFilePartitionManager::Partition::~Partition() = default;

BlobFilePartitionManager::BlobFilePartitionManager(
    uint32_t num_partitions,
    std::shared_ptr<BlobFilePartitionStrategy> strategy,
    FileNumberAllocator file_number_allocator, Env* env, FileSystem* fs,
    SystemClock* clock, Statistics* statistics, const FileOptions& file_options,
    const std::string& db_path, uint64_t blob_file_size, bool use_fsync,
    CompressionType blob_compression_type, uint64_t buffer_size,
    bool use_direct_io, uint64_t flush_interval_ms,
    const std::shared_ptr<IOTracer>& io_tracer,
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    FileChecksumGenFactory* file_checksum_gen_factory,
    const FileTypeSet& checksum_handoff_file_types,
    BlobFileCache* blob_file_cache, BlobFileCompletionCallback* blob_callback,
    const std::string& db_id, const std::string& db_session_id,
    Logger* info_log)
    : num_partitions_(num_partitions),
      strategy_(strategy ? std::move(strategy)
                         : std::make_shared<RoundRobinPartitionStrategy>()),
      file_number_allocator_(std::move(file_number_allocator)),
      env_(env),
      fs_(fs),
      clock_(clock),
      statistics_(statistics),
      file_options_(file_options),
      db_path_(db_path),
      blob_file_size_(blob_file_size),
      use_fsync_(use_fsync),
      buffer_size_(buffer_size),
      high_water_mark_(buffer_size_ > 0 ? buffer_size_ * 3 / 4 : 0),
      flush_interval_us_(flush_interval_ms * 1000),
      blob_compression_type_(blob_compression_type),
      io_tracer_(io_tracer),
      listeners_(listeners),
      file_checksum_gen_factory_(file_checksum_gen_factory),
      checksum_handoff_file_types_(checksum_handoff_file_types),
      blob_file_cache_(blob_file_cache),
      blob_callback_(blob_callback),
      db_id_(db_id),
      db_session_id_(db_session_id),
      info_log_(info_log),
      bg_cv_(&bg_mutex_) {
  assert(num_partitions_ > 0);
  assert(file_number_allocator_);
  assert(fs_);
  assert(env_);

  // Enable O_DIRECT for blob file writes if requested.
  if (use_direct_io) {
    file_options_.use_direct_writes = true;
  }

  partitions_.reserve(num_partitions_);
  for (uint32_t i = 0; i < num_partitions_; ++i) {
    partitions_.emplace_back(std::make_unique<Partition>());
  }

  // Ensure enough BOTTOM-priority threads for write-path seal/flush work.
  // Even in synchronous mode (buffer_size_ == 0), file rollovers submit BG
  // seal tasks. Without BOTTOM threads, callers like SealAllPartitions() can
  // block forever in DrainBackgroundWork() waiting on seals that never run.
  const int extra = (buffer_size_ > 0 && flush_interval_us_ > 0) ? 1 : 0;
  env_->IncBackgroundThreadsIfNeeded(static_cast<int>(num_partitions_) + extra,
                                     Env::Priority::BOTTOM);

  // Schedule periodic flush timer only in deferred mode when configured.
  // Tracked separately from bg_in_flight_ (via bg_timer_running_) so that
  // DrainBackgroundWork during SealAllPartitions doesn't deadlock waiting for
  // the long-lived timer to exit.
  if (buffer_size_ > 0 && flush_interval_us_ > 0) {
    bg_timer_running_.store(true, std::memory_order_release);
    env_->Schedule(&BGPeriodicFlushWrapper, this, Env::Priority::BOTTOM);
  }
}

BlobFilePartitionManager::~BlobFilePartitionManager() {
  // Stop the periodic flush timer (if running) and wait for it to exit.
  bg_timer_stop_.store(true, std::memory_order_release);
  while (bg_timer_running_.load(std::memory_order_acquire)) {
    // Timer thread is sleeping; it will exit within flush_interval_us_.
    clock_->SleepForMicroseconds(1000);  // 1ms poll
  }
  // Wait for all in-flight seal/flush work to complete.
  DrainBackgroundWork();
  // bg_status_ may never be checked if no BG error occurred.
  bg_status_.PermitUncheckedError();
#ifndef NDEBUG
  if (!bg_has_error_.load(std::memory_order_relaxed)) {
    for (const auto& partition : partitions_) {
      assert(!partition->writer &&
             "All partitions must be sealed before destroying "
             "BlobFilePartitionManager");
    }
  }
#endif
  DumpTimingStats();
  // Free the current and all retired settings snapshots.
  delete cached_settings_.load(std::memory_order_relaxed);
  for (auto* s : retired_settings_) {
    delete s;
  }
}

Status BlobFilePartitionManager::OpenNewBlobFile(Partition* partition,
                                                 uint32_t column_family_id,
                                                 CompressionType compression) {
  assert(partition);
  assert(!partition->writer);

  const uint64_t blob_file_number = file_number_allocator_();
  const std::string blob_file_path = BlobFileName(db_path_, blob_file_number);

  // Register the file number in the active set BEFORE creating the file on
  // disk. This prevents a race where PurgeObsoleteFiles collects the active
  // set (via GetActiveBlobFileNumbers) between the file being created on disk
  // and the mapping being registered, which would cause the newly created file
  // to be immediately deleted.
  uint32_t partition_idx = 0;
  for (uint32_t i = 0; i < num_partitions_; ++i) {
    if (partitions_[i].get() == partition) {
      partition_idx = i;
      break;
    }
  }
  AddFilePartitionMapping(blob_file_number, partition_idx);

  std::unique_ptr<FSWritableFile> file;
  Status s = NewWritableFile(fs_, blob_file_path, &file, file_options_);
  if (!s.ok()) {
    RemoveFilePartitionMapping(blob_file_number);
    return s;
  }

  {
    uint64_t fn_copy = blob_file_number;
    TEST_SYNC_POINT_CALLBACK(
        "BlobFilePartitionManager::OpenNewBlobFile:AfterCreate", &fn_copy);
  }

  const bool perform_data_verification =
      checksum_handoff_file_types_.Contains(FileType::kBlobFile);

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, file_options_, clock_, io_tracer_,
      statistics_, Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS, listeners_,
      file_checksum_gen_factory_, perform_data_verification));

  const bool writer_do_flush = (buffer_size_ == 0);

  auto blob_log_writer = std::make_unique<BlobLogWriter>(
      std::move(file_writer), clock_, statistics_, blob_file_number, use_fsync_,
      writer_do_flush);

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range{};
  BlobLogHeader header(column_family_id, compression, has_ttl,
                       expiration_range);

  WriteOptions wo;
  Status ws = blob_log_writer->WriteHeader(wo, header);
  if (!ws.ok()) {
    RemoveFilePartitionMapping(blob_file_number);
    return ws;
  }

  partition->writer = std::move(blob_log_writer);
  partition->file_number = blob_file_number;
  partition->file_size = BlobLogHeader::kSize;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->sync_required = false;
  partition->column_family_id = column_family_id;
  partition->compression = compression;
  partition->next_write_offset = BlobLogHeader::kSize;

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] Opened blob file %" PRIu64 " (%s)",
                 blob_file_number, blob_file_path.c_str());

  if (blob_callback_) {
    blob_callback_->OnBlobFileCreationStarted(
        blob_file_path, /*column_family_name=*/"", /*job_id=*/0,
        BlobFileCreationReason::kDirectWrite);
  }

  return Status::OK();
}

void BlobFilePartitionManager::ResetPartitionState(Partition* partition,
                                                   uint64_t file_number,
                                                   bool remove_mapping) {
  partition->writer.reset();
  partition->file_number = 0;
  partition->file_size = 0;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->sync_required = false;
  partition->next_write_offset = 0;
  if (remove_mapping) {
    ROCKS_LOG_WARN(info_log_,
                   "[BlobDirectWrite] ResetPartitionState: removing mapping "
                   "for file %" PRIu64 " (error path)",
                   file_number);
    RemoveFilePartitionMapping(file_number);
  } else {
    ROCKS_LOG_INFO(info_log_,
                   "[BlobDirectWrite] ResetPartitionState: KEEPING mapping "
                   "for file %" PRIu64 " (success path)",
                   file_number);
  }
}

Status BlobFilePartitionManager::CloseBlobFile(Partition* partition) {
  assert(partition);
  assert(partition->writer);

  const uint64_t file_number_to_close = partition->file_number;

  // Flush pending deferred records before closing.
  // Done inline while holding the mutex to prevent other threads from adding
  // records with pre-calculated offsets for this file during the flush.
  // The mutex is held during I/O, but this only blocks one partition and
  // file close is infrequent (once per blob_file_size bytes).
  if (buffer_size_ > 0 && !partition->pending_records.empty()) {
    std::deque<PendingRecord> records = std::move(partition->pending_records);
    partition->pending_records.clear();
    BlobLogWriter* writer = partition->writer.get();

    size_t records_written = 0;
    WriteOptions wo;
    Status flush_err =
        FlushRecordsToDisk(wo, writer, partition, records, &records_written);

    partition->pending_cv.SignalAll();
    RemoveFromPendingIndexLocked(partition, records);

    if (!flush_err.ok()) {
      ResetPartitionState(partition, file_number_to_close);
      return flush_err;
    }

    IOOptions io_opts;
    Status s = WritableFileWriter::PrepareIOOptions(wo, io_opts);
    if (s.ok()) {
      s = writer->file()->Flush(io_opts);
    }
    if (!s.ok()) {
      ResetPartitionState(partition, file_number_to_close);
      return s;
    }
  }

  BlobLogFooter footer;
  footer.blob_count = partition->blob_count;

  std::string checksum_method;
  std::string checksum_value;
  const uint64_t physical_file_size =
      partition->writer->file()->GetFileSize() + BlobLogFooter::kSize;

  WriteOptions wo;
  Status s = partition->writer->AppendFooter(wo, footer, &checksum_method,
                                             &checksum_value);
  if (!s.ok()) {
    ResetPartitionState(partition, file_number_to_close);
    return s;
  }

  EvictSealedBlobFileReader(file_number_to_close);

  partition->completed_files.emplace_back(
      partition->file_number, partition->blob_count,
      partition->total_blob_bytes, checksum_method, checksum_value,
      physical_file_size);

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] Closed blob file %" PRIu64 ": %" PRIu64
                 " blobs, %" PRIu64 " bytes",
                 partition->file_number, partition->blob_count,
                 partition->total_blob_bytes);

  if (blob_callback_) {
    const std::string file_path =
        BlobFileName(db_path_, partition->file_number);
    Status cb_s = blob_callback_->OnBlobFileCompleted(
        file_path, /*column_family_name=*/"", /*job_id=*/0,
        partition->file_number, BlobFileCreationReason::kDirectWrite, s,
        checksum_value, checksum_method, partition->blob_count,
        partition->total_blob_bytes);
    if (!cb_s.ok()) {
      ResetPartitionState(partition, file_number_to_close);
      return cb_s;
    }
  }

  // On success, keep the file_to_partition_ mapping. The sealed file needs
  // to remain visible to GetActiveBlobFileNumbers (and thus
  // PurgeObsoleteFiles) until it is committed to the MANIFEST. The flush
  // caller will call RemoveFilePartitionMappings after MANIFEST commit.
  ResetPartitionState(partition, file_number_to_close,
                      /*remove_mapping=*/false);

  return Status::OK();
}

Status BlobFilePartitionManager::PrepareFileRollover(
    Partition* partition, uint32_t column_family_id,
    CompressionType compression, DeferredSeal* deferred) {
  assert(partition);
  assert(partition->writer);
  assert(deferred);

  // Capture old file state under the mutex. Records remain visible to
  // GetPendingBlobValue via the per-partition pending_index until
  // RemoveFromPendingIndex is called after the deferred seal completes.
  deferred->writer = std::move(partition->writer);
  deferred->records = std::move(partition->pending_records);
  partition->pending_records.clear();
  deferred->file_number = partition->file_number;
  deferred->blob_count = partition->blob_count;
  deferred->total_blob_bytes = partition->total_blob_bytes;
  deferred->closed_wal_synced = !partition->sync_required;

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] PrepareFileRollover: blob file %" PRIu64
                 " reached size limit (%" PRIu64 " blobs, %" PRIu64
                 " bytes, %zu pending records)",
                 deferred->file_number, deferred->blob_count,
                 deferred->total_blob_bytes, deferred->records.size());

  partition->file_number = 0;
  partition->file_size = 0;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->sync_required = false;
  partition->next_write_offset = 0;

  return OpenNewBlobFile(partition, column_family_id, compression);
}

Status BlobFilePartitionManager::FlushDeferredSealRecords(
    const WriteOptions& write_options, Partition* partition,
    DeferredSeal* deferred) {
  assert(partition);
  assert(deferred);
  assert(deferred->writer);

  if (deferred->records_flushed) {
    return Status::OK();
  }

  size_t records_written = 0;
  Status s = FlushRecordsToDisk(write_options, deferred->writer.get(),
                                partition, deferred->records, &records_written);

  {
    MutexLock lock(&partition->mutex);
    partition->pending_cv.SignalAll();
  }

  if (!s.ok()) {
    return s;
  }

  IOOptions io_opts;
  s = WritableFileWriter::PrepareIOOptions(write_options, io_opts);
  if (s.ok()) {
    s = deferred->writer->file()->Flush(io_opts);
  }
  if (s.ok()) {
    deferred->records_flushed = true;
  }
  return s;
}

Status BlobFilePartitionManager::SyncDeferredSealForClosedWal(
    const WriteOptions& write_options, Partition* partition,
    DeferredSeal* deferred) {
  assert(partition);
  assert(deferred);
  assert(deferred->writer);

  if (deferred->closed_wal_synced) {
    return Status::OK();
  }

  Status s = FlushDeferredSealRecords(write_options, partition, deferred);
  if (!s.ok()) {
    return s;
  }

  s = deferred->writer->Sync(write_options);
  if (s.ok()) {
    deferred->closed_wal_synced = true;
  }
  return s;
}

Status BlobFilePartitionManager::SealDeferredFile(Partition* partition,
                                                  DeferredSeal* deferred) {
  assert(deferred);
  assert(deferred->writer);

  BlobLogWriter* writer = deferred->writer.get();

  WriteOptions wo;
  Status write_err = FlushDeferredSealRecords(wo, partition, deferred);
  if (!write_err.ok()) {
    // Remove ALL records from pending_index — deferred->records will be
    // destroyed when the BGWorkItem goes out of scope, making any
    // remaining PendingBlobValueEntry pointers dangling.
    RemoveFromPendingIndex(partition, deferred->records);
    deferred->writer.reset();
    return write_err;
  }

  // Write footer.
  BlobLogFooter footer;
  footer.blob_count = deferred->blob_count;

  std::string checksum_method;
  std::string checksum_value;
  const uint64_t physical_file_size =
      writer->file()->GetFileSize() + BlobLogFooter::kSize;
  Status s =
      writer->AppendFooter(wo, footer, &checksum_method, &checksum_value);
  if (!s.ok()) {
    RemoveFromPendingIndex(partition, deferred->records);
    deferred->writer.reset();
    return s;
  }

  EvictSealedBlobFileReader(deferred->file_number);

  {
    MutexLock lock(&partition->mutex);
    partition->completed_files.emplace_back(
        deferred->file_number, deferred->blob_count, deferred->total_blob_bytes,
        checksum_method, checksum_value, physical_file_size);
  }

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] Sealed blob file %" PRIu64 ": %" PRIu64
                 " blobs, %" PRIu64 " bytes",
                 deferred->file_number, deferred->blob_count,
                 deferred->total_blob_bytes);

  if (blob_callback_) {
    const std::string file_path = BlobFileName(db_path_, deferred->file_number);
    Status cb_s = blob_callback_->OnBlobFileCompleted(
        file_path, /*column_family_name=*/"", /*job_id=*/0,
        deferred->file_number, BlobFileCreationReason::kDirectWrite, s,
        checksum_value, checksum_method, deferred->blob_count,
        deferred->total_blob_bytes);
    if (!cb_s.ok()) {
      RemoveFromPendingIndex(partition, deferred->records);
      RemoveFilePartitionMapping(deferred->file_number);
      deferred->writer.reset();
      return cb_s;
    }
  }

  RemoveFromPendingIndex(partition, deferred->records);
  // Keep the file_to_partition_ mapping. The sealed file must remain
  // visible to GetActiveBlobFileNumbers until committed to MANIFEST.
  // The flush caller will call RemoveFilePartitionMappings after commit.

  deferred->writer.reset();
  return Status::OK();
}

void BlobFilePartitionManager::EvictSealedBlobFileReader(uint64_t file_number) {
  if (blob_file_cache_ != nullptr) {
    blob_file_cache_->Evict(file_number);
  }
}

void BlobFilePartitionManager::SetBGError(const Status& s) {
  MutexLock lock(&bg_mutex_);
  if (bg_status_.ok()) {
    ROCKS_LOG_ERROR(info_log_, "[BlobDirectWrite] SetBGError: %s",
                    s.ToString().c_str());
    bg_status_ = s;
    bg_has_error_.store(true, std::memory_order_release);
  }
}

void BlobFilePartitionManager::DecrementBGInFlight() {
  if (bg_in_flight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    MutexLock lock(&bg_mutex_);
    bg_cv_.SignalAll();
  }
}

void BlobFilePartitionManager::BGSealWrapper(void* arg) {
  std::unique_ptr<BGSealContext> ctx(static_cast<BGSealContext*>(arg));
  Status s = ctx->mgr->SealDeferredFile(ctx->partition, &ctx->seal);
  if (!s.ok()) {
    ctx->mgr->SetBGError(s);
  }
  ctx->mgr->DecrementBGInFlight();
}

void BlobFilePartitionManager::BGFlushWrapper(void* arg) {
  std::unique_ptr<BGFlushContext> ctx(static_cast<BGFlushContext*>(arg));
  Status s = ctx->mgr->FlushPendingRecords(ctx->partition, WriteOptions());
  // Clear flush_queued AFTER the flush completes so that no concurrent
  // flush is scheduled for the same partition while I/O is in progress.
  ctx->partition->flush_queued.store(false, std::memory_order_release);
  // Signal pending_cv so SubmitSeal wakes up promptly after flush_queued
  // is cleared (SubmitSeal waits for flush_queued==false to avoid racing
  // with the BG flush on the same BlobLogWriter).
  {
    MutexLock lock(&ctx->partition->mutex);
    ctx->partition->pending_cv.SignalAll();
  }
  if (!s.ok()) {
    ctx->mgr->SetBGError(s);
  }
  ctx->mgr->DecrementBGInFlight();
}

void BlobFilePartitionManager::BGPeriodicFlushWrapper(void* arg) {
  auto* mgr = static_cast<BlobFilePartitionManager*>(arg);
  // Loop: sleep for the flush interval, then submit flushes for partitions
  // with pending bytes. Exits when bg_timer_stop_ is set (shutdown).
  // Consumes one BOTTOM thread (mostly sleeping).
  while (!mgr->bg_timer_stop_.load(std::memory_order_acquire)) {
    mgr->clock_->SleepForMicroseconds(
        static_cast<int>(mgr->flush_interval_us_));
    if (mgr->bg_timer_stop_.load(std::memory_order_acquire)) {
      break;
    }
    for (auto& p : mgr->partitions_) {
      if (p->pending_bytes.load(std::memory_order_relaxed) > 0) {
        TEST_SYNC_POINT(
            "BlobFilePartitionManager::BGPeriodicFlush:SubmitFlush");
        mgr->SubmitFlush(p.get());
      }
    }
  }
  mgr->bg_timer_running_.store(false, std::memory_order_release);
}

void BlobFilePartitionManager::SubmitSeal(Partition* partition,
                                          DeferredSeal&& seal) {
  // Wait for any in-flight BG flush to complete before sealing. The BG
  // flush holds a raw pointer to partition->writer (captured under the
  // mutex before I/O) which PrepareFileRollover moved into this
  // DeferredSeal. If we don't wait, SealDeferredFile and
  // FlushPendingRecords would concurrently write to the same
  // BlobLogWriter, causing a data race.
  //
  // This wait is outside the partition mutex, so it does not deadlock
  // with the BG flush's RemoveFromPendingIndex (which acquires the
  // partition mutex). BGFlushWrapper signals pending_cv after clearing
  // flush_queued so we wake up promptly.
  {
    MutexLock lock(&partition->mutex);
    while (partition->flush_queued.load(std::memory_order_acquire)) {
      partition->pending_cv.TimedWait(clock_->NowMicros() + 1000);
    }
  }

  {
    MutexLock lock(&bg_mutex_);
    if (bg_seal_in_progress_) {
      ROCKS_LOG_DEBUG(info_log_,
                     "[BlobDirectWrite] SubmitSeal: sealing blob file %" PRIu64
                     " INLINE (bg_seal_in_progress=true, %" PRIu64 " blobs)",
                     seal.file_number, seal.blob_count);
      Status s = SealDeferredFile(partition, &seal);
      if (!s.ok()) {
        ROCKS_LOG_ERROR(info_log_,
                        "[BlobDirectWrite] SubmitSeal: inline seal FAILED "
                        "for blob file %" PRIu64 ": %s",
                        seal.file_number, s.ToString().c_str());
        SetBGError(s);
      }
      return;
    }
  }
  ROCKS_LOG_DEBUG(info_log_,
                 "[BlobDirectWrite] SubmitSeal: scheduling BG seal for blob "
                 "file %" PRIu64 " (%" PRIu64 " blobs)",
                 seal.file_number, seal.blob_count);
  bg_in_flight_.fetch_add(1, std::memory_order_acq_rel);
  auto* ctx = new BGSealContext{this, partition, std::move(seal)};
  env_->Schedule(&BGSealWrapper, ctx, Env::Priority::BOTTOM);
}

void BlobFilePartitionManager::SubmitFlush(Partition* partition) {
  if (partition->flush_queued.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  {
    MutexLock lock(&partition->mutex);
    if (partition->sync_barrier_active) {
      partition->flush_queued.store(false, std::memory_order_release);
      partition->pending_cv.SignalAll();
      return;
    }
  }
  bool skipped_for_seal = false;
  {
    MutexLock lock(&bg_mutex_);
    if (bg_seal_in_progress_) {
      // SealAllPartitions will handle pending records inline.
      partition->flush_queued.store(false, std::memory_order_release);
      skipped_for_seal = true;
    }
  }
  if (skipped_for_seal) {
    MutexLock lock(&partition->mutex);
    partition->pending_cv.SignalAll();
    return;
  }
  bg_in_flight_.fetch_add(1, std::memory_order_acq_rel);
  auto* ctx = new BGFlushContext{this, partition};
  env_->Schedule(&BGFlushWrapper, ctx, Env::Priority::BOTTOM);
}

void BlobFilePartitionManager::DrainBackgroundWork() {
  MutexLock lock(&bg_mutex_);
  int64_t in_flight = bg_in_flight_.load(std::memory_order_acquire);
  if (in_flight > 0) {
    ROCKS_LOG_DEBUG(info_log_,
                   "[BlobDirectWrite] DrainBackgroundWork: waiting for "
                   "%" PRId64 " in-flight BG tasks",
                   in_flight);
  }
  while (bg_in_flight_.load(std::memory_order_acquire) > 0) {
    bg_cv_.Wait();
  }
}

Status BlobFilePartitionManager::FlushRecordsToDisk(
    const WriteOptions& write_options, BlobLogWriter* writer,
    Partition* partition, std::deque<PendingRecord>& records,
    size_t* records_written) {
  assert(writer);
  assert(records_written);
  *records_written = 0;

  Status s;
  for (auto& record : records) {
    uint64_t key_offset = 0;
    uint64_t actual_blob_offset = 0;
    s = writer->AddRecord(write_options, Slice(record.key), Slice(record.value),
                          &key_offset, &actual_blob_offset);
    if (!s.ok()) {
      break;
    }
    if (actual_blob_offset != record.blob_offset) {
      s = Status::Corruption(
          "BlobDirectWrite: pre-calculated blob offset does not match "
          "actual offset");
      break;
    }

    const uint64_t record_bytes =
        BlobLogRecord::kHeaderSize + record.key.size() + record.value.size();
    partition->pending_bytes.fetch_sub(record_bytes, std::memory_order_relaxed);
    ++(*records_written);
  }

  for (size_t i = *records_written; i < records.size(); ++i) {
    const auto& rec = records[i];
    const uint64_t rec_bytes =
        BlobLogRecord::kHeaderSize + rec.key.size() + rec.value.size();
    partition->pending_bytes.fetch_sub(rec_bytes, std::memory_order_relaxed);
  }

  return s;
}

Status BlobFilePartitionManager::WriteBlobDeferred(
    Partition* partition, const Slice& key, const Slice& value,
    uint64_t* blob_offset, std::string key_copy_, std::string value_copy_) {
  assert(partition);
  assert(buffer_size_ > 0);

  // Pre-calculate the offset where this value will be written.
  *blob_offset =
      partition->next_write_offset + BlobLogRecord::kHeaderSize + key.size();
  const uint64_t record_size =
      BlobLogRecord::kHeaderSize + key.size() + value.size();
  partition->next_write_offset += record_size;

  const uint64_t fn = partition->file_number;

  partition->pending_records.push_back(
      {std::move(key_copy_), std::move(value_copy_), fn, *blob_offset});
  partition->pending_bytes.fetch_add(record_size, std::memory_order_relaxed);
  partition->sync_required = true;

  // Add to per-partition pending index for O(1) read path lookup.
  // Points into the deque element — stable because std::deque::push_back
  // does not invalidate references to existing elements.
  // Partition mutex is already held by caller (WriteBlob).
  partition->pending_index[{fn, *blob_offset}] = {
      &partition->pending_records.back().value, partition->compression};

  return Status::OK();
}

Status BlobFilePartitionManager::WriteBlobSync(Partition* partition,
                                               const Slice& key,
                                               const Slice& value,
                                               uint64_t* blob_offset) {
  assert(partition);

  uint64_t key_offset = 0;
  WriteOptions wo;
  Status s =
      partition->writer->AddRecord(wo, key, value, &key_offset, blob_offset);
  if (!s.ok()) {
    return s;
  }

  partition->sync_required = true;

  return Status::OK();
}

void BlobFilePartitionManager::RemoveFromPendingIndexLocked(
    Partition* partition, const std::deque<PendingRecord>& records) {
  for (const auto& r : records) {
    partition->pending_index.erase({r.file_number, r.blob_offset});
  }
}

void BlobFilePartitionManager::RemoveFromPendingIndex(
    Partition* partition, const std::deque<PendingRecord>& records) {
  MutexLock lock(&partition->mutex);
  RemoveFromPendingIndexLocked(partition, records);
}

void BlobFilePartitionManager::AddFilePartitionMapping(uint64_t file_number,
                                                       uint32_t partition_idx) {
  WriteLock lock(&file_partition_mutex_);
  file_to_partition_[file_number] = partition_idx;
  ROCKS_LOG_DEBUG(info_log_,
                 "[BlobDirectWrite] AddFilePartitionMapping: "
                 "file %" PRIu64
                 " -> partition %u, "
                 "map size now %zu",
                 file_number, partition_idx, file_to_partition_.size());
}

void BlobFilePartitionManager::RemoveFilePartitionMapping(
    uint64_t file_number) {
  ROCKS_LOG_DEBUG(info_log_,
                 "[BlobDirectWrite] RemoveFilePartitionMapping: "
                 "removing file %" PRIu64 " (single)",
                 file_number);
  WriteLock lock(&file_partition_mutex_);
  file_to_partition_.erase(file_number);
}

void BlobFilePartitionManager::RemoveFilePartitionMappings(
    const std::vector<uint64_t>& file_numbers) {
  if (file_numbers.empty()) return;
  std::string nums;
  for (uint64_t fn : file_numbers) {
    if (!nums.empty()) nums += ",";
    nums += std::to_string(fn);
  }
  ROCKS_LOG_DEBUG(info_log_,
                 "[BlobDirectWrite] RemoveFilePartitionMappings: "
                 "removing %zu files: %s",
                 file_numbers.size(), nums.c_str());
  WriteLock lock(&file_partition_mutex_);
  for (uint64_t fn : file_numbers) {
    file_to_partition_.erase(fn);
  }
}

Status BlobFilePartitionManager::GetPendingBlobValue(uint64_t file_number,
                                                     uint64_t offset,
                                                     std::string* value) const {
  uint32_t part_idx;
  {
    ReadLock lock(&file_partition_mutex_);
    auto fit = file_to_partition_.find(file_number);
    if (fit == file_to_partition_.end()) {
      return Status::NotFound();
    }
    part_idx = fit->second;
  }

  Partition* partition = partitions_[part_idx].get();
  std::string raw_value;
  CompressionType compression;
  {
    MutexLock lock(&partition->mutex);
    auto it = partition->pending_index.find({file_number, offset});
    if (it == partition->pending_index.end()) {
      return Status::NotFound();
    }
    // Copy, not reference: the BG flush callback may free the backing
    // PendingRecord (and its std::string) as soon as we release
    // the partition mutex.
    raw_value = *it->second.data;
    compression = it->second.compression;
  }

  if (compression != kNoCompression) {
    auto decomp = GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
        compression);
    if (!decomp) {
      return Status::Corruption(
          "BlobDirectWrite: no decompressor for pending blob value, "
          "compression type " +
          CompressionTypeToString(compression));
    }
    Decompressor::Args args;
    args.compression_type = compression;
    args.compressed_data = Slice(raw_value);
    Status s = decomp->ExtractUncompressedSize(args);
    if (!s.ok()) {
      return s;
    }
    value->resize(args.uncompressed_size);
    s = decomp->DecompressBlock(args, const_cast<char*>(value->data()));
    return s;
  }

  *value = std::move(raw_value);
  return Status::OK();
}

Status BlobFilePartitionManager::WriteBlob(
    const WriteOptions& /*write_options*/, uint32_t column_family_id,
    CompressionType compression, const Slice& key, const Slice& value,
    uint64_t* blob_file_number, uint64_t* blob_offset, uint64_t* blob_size,
    const BlobDirectWriteSettings* caller_settings) {
  assert(blob_file_number);
  assert(blob_offset);
  assert(blob_size);

  // Fail fast if a background I/O error has occurred. Without this check,
  // writers would continue pre-calculating offsets for a corrupt/incomplete
  // blob file, generating BlobIndex entries pointing to invalid offsets.
  if (bg_has_error_.load(std::memory_order_relaxed)) {
    MutexLock lock(&bg_mutex_);
    if (!bg_status_.ok()) {
      return bg_status_;
    }
  }

  const uint32_t partition_idx =
      strategy_->SelectPartition(num_partitions_, column_family_id, key,
                                 value) %
      num_partitions_;

  Partition* partition = partitions_[partition_idx].get();

  // BACKPRESSURE PROTOCOL:
  //
  // Goal: prevent unbounded memory growth from writers outpacing BG I/O.
  //
  //   pending_bytes    Atomic counter per partition; incremented in
  //                    WriteBlobDeferred (record_size), decremented
  //                    in FlushRecordsToDisk (per record, even on error).
  //
  //   buffer_size_     Hard stall threshold. When pending_bytes >=
  //                    buffer_size_, the writer enters a timed-wait loop:
  //                      a. Check for BG errors (fail fast)
  //                      b. SubmitFlush to ensure BG work is scheduled
  //                      c. TimedWait on partition->pending_cv (1ms)
  //                      d. Re-check pending_bytes < buffer_size_ to exit
  //
  //   high_water_mark_ Soft flush trigger (75% of buffer_size_). After
  //                    each WriteBlob, if pending_bytes >= high_water_mark_,
  //                    SubmitFlush is called (non-blocking). This keeps
  //                    the BG thread busy before writers must stall.
  //
  //   pending_cv       Per-partition condvar. Signaled by BG flush
  //                    (FlushPendingRecords) and BG seal (SealDeferredFile)
  //                    after records are written. Wakes stalled writers.
  //
  //   flush_queued     Per-partition atomic flag. Ensures at most one
  //                    flush is scheduled via Env::Schedule at a time.
  //                    Set by SubmitFlush, cleared AFTER FlushPendingRecords
  //                    completes (not before I/O) to prevent concurrent
  //                    flushes writing to the same BlobLogWriter.
  //
  // Flow: Writer -> pending_bytes exceeds threshold -> SubmitFlush ->
  //   Env::Schedule(BGFlushWrapper) -> FlushPendingRecords (I/O) ->
  //   pending_bytes decremented -> pending_cv signaled -> writer wakes
  if (buffer_size_ > 0) {
    while (partition->pending_bytes.load(std::memory_order_relaxed) >=
           buffer_size_) {
      if (bg_has_error_.load(std::memory_order_relaxed)) {
        MutexLock lock(&bg_mutex_);
        if (!bg_status_.ok()) {
          return bg_status_;
        }
      }
      SubmitFlush(partition);
      MutexLock lock(&partition->mutex);
      if (partition->pending_bytes.load(std::memory_order_relaxed) >=
          buffer_size_) {
        RecordTick(statistics_, BLOB_DB_DIRECT_WRITE_STALL_COUNT);
        TEST_SYNC_POINT(
            "BlobFilePartitionManager::WriteBlob:BackpressureStall");
        partition->pending_cv.TimedWait(clock_->NowMicros() + 1000);
      }
    }
  }

  bool need_flush = false;
  DeferredSeal deferred_seal;

  // Compress OUTSIDE the mutex using a per-call compressor matching the CF's
  // compression type. Each CF may have a different compression type, so we
  // must not use a single global compressor.
  GrowableBuffer compressed_buf;
  Slice write_value = value;
  if (compression != kNoCompression) {
    auto compressor = GetBuiltinV2CompressionManager()->GetCompressor(
        CompressionOptions{}, compression);
    if (compressor) {
      auto wa = compressor->ObtainWorkingArea();
      StopWatch stop_watch(clock_, statistics_, BLOB_DB_COMPRESSION_MICROS);
      Status s = LegacyForceBuiltinCompression(*compressor, &wa, value,
                                               &compressed_buf);
      if (!s.ok()) {
        return s;
      }
      write_value = Slice(compressed_buf);
    }
  }

  // Pre-copy key and (compressed) value OUTSIDE the mutex for deferred mode.
  // Only one copy of the final value, not the pre-compression original.
  std::string key_copy;
  std::string value_copy;
  if (buffer_size_ > 0) {
    key_copy.assign(key.data(), key.size());
    value_copy.assign(write_value.data(), write_value.size());
  }

  {
    MutexLock lock(&partition->mutex);
    while (partition->sync_barrier_active) {
      TEST_SYNC_POINT("BlobFilePartitionManager::WriteBlob:WaitOnSyncBarrier");
      partition->pending_cv.Wait();
    }

    if (!partition->writer || partition->column_family_id != column_family_id ||
        partition->compression != compression) {
      if (partition->writer) {
        Status s = CloseBlobFile(partition);
        if (!s.ok()) {
          return s;
        }
      }
      Status s = OpenNewBlobFile(partition, column_family_id, compression);
      if (!s.ok()) {
        return s;
      }
    }

    Status s;
    if (buffer_size_ > 0) {
      s = WriteBlobDeferred(partition, key, write_value, blob_offset,
                            std::move(key_copy), std::move(value_copy));
    } else {
      s = WriteBlobSync(partition, key, write_value, blob_offset);
    }
    if (!s.ok()) {
      return s;
    }

    *blob_file_number = partition->file_number;
    *blob_size = write_value.size();

    partition->blob_count++;
    const uint64_t record_size =
        BlobLogRecord::kHeaderSize + key.size() + write_value.size();
    partition->total_blob_bytes += record_size;
    partition->file_size = partition->total_blob_bytes + BlobLogHeader::kSize;

    if (partition->file_size >= blob_file_size_) {
      s = PrepareFileRollover(partition, column_family_id, compression,
                              &deferred_seal);
      if (!s.ok()) {
        return s;
      }
    }

    if (buffer_size_ > 0 && high_water_mark_ > 0 &&
        partition->pending_bytes.load(std::memory_order_relaxed) >=
            high_water_mark_) {
      need_flush = true;
    }
  }  // mutex released

  RecordTick(statistics_, BLOB_DB_DIRECT_WRITE_COUNT);
  RecordTick(statistics_, BLOB_DB_DIRECT_WRITE_BYTES, write_value.size());
  blobs_written_since_seal_.fetch_add(1, std::memory_order_release);

  // Prepopulate blob cache with uncompressed value (outside mutex).
  {
    BlobDirectWriteSettings local_settings;
    if (!caller_settings) {
      local_settings = GetCachedSettings(column_family_id);
      caller_settings = &local_settings;
    }
    if (caller_settings->blob_cache &&
        caller_settings->prepopulate_blob_cache ==
            PrepopulateBlobCache::kFlushOnly) {
      FullTypedCacheInterface<BlobContents, BlobContentsCreator> blob_cache{
          caller_settings->blob_cache};
      const OffsetableCacheKey base_cache_key(db_id_, db_session_id_,
                                              *blob_file_number);
      const CacheKey cache_key = base_cache_key.WithOffset(*blob_offset);
      const Slice cache_slice = cache_key.AsSlice();
      Status cs = blob_cache.InsertSaved(cache_slice, value, nullptr,
                                         Cache::Priority::BOTTOM,
                                         CacheTier::kVolatileTier);
      if (cs.ok()) {
        RecordTick(statistics_, BLOB_DB_CACHE_ADD);
        RecordTick(statistics_, BLOB_DB_CACHE_BYTES_WRITE, value.size());
      } else {
        RecordTick(statistics_, BLOB_DB_CACHE_ADD_FAILURES);
      }
    }
  }

  // Submit seal to Env::Schedule (non-blocking).
  if (deferred_seal.writer) {
    SubmitSeal(partition, std::move(deferred_seal));
  }

  // Submit flush to Env::Schedule (non-blocking).
  if (need_flush) {
    SubmitFlush(partition);
  }

  return Status::OK();
}

Status BlobFilePartitionManager::FlushPendingRecords(
    Partition* partition, const WriteOptions& write_options) {
  assert(partition);
  TEST_SYNC_POINT("BlobFilePartitionManager::FlushPendingRecords:Begin");

  // Called from BG flush callback (BGFlushWrapper) or inline during
  // SyncOpenFilesInternal/SealAllPartitions. Safe to release the partition
  // mutex during I/O because flush_queued prevents concurrent flushes on the
  // same partition, and the sync barrier / rollover capture prevents the
  // active writer from changing underneath the flush.
  std::deque<PendingRecord> records;
  BlobLogWriter* writer = nullptr;
  {
    MutexLock lock(&partition->mutex);
    if (partition->pending_records.empty()) {
      return Status::OK();
    }
    records = std::move(partition->pending_records);
    partition->pending_records.clear();
    // Records remain visible to GetPendingBlobValue via the per-partition
    // pending_index until RemoveFromPendingIndex is called after flush.
    writer = partition->writer.get();
  }

  if (!writer) {
    RemoveFromPendingIndex(partition, records);
    return Status::OK();
  }

  size_t records_written = 0;
  Status flush_status = FlushRecordsToDisk(write_options, writer, partition,
                                           records, &records_written);

  if (flush_status.ok()) {
    IOOptions io_opts;
    flush_status = WritableFileWriter::PrepareIOOptions(write_options, io_opts);
    if (flush_status.ok()) {
      flush_status = writer->file()->Flush(io_opts);
    }
  }

  if (!records.empty()) {
    RemoveFromPendingIndex(partition, records);
  }
  {
    MutexLock lock(&partition->mutex);
    partition->pending_cv.SignalAll();
  }

  return flush_status;
}

Status BlobFilePartitionManager::RotateAllPartitions() {
  std::vector<std::pair<Partition*, DeferredSeal>> seals;

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    while (partition->sync_barrier_active) {
      partition->pending_cv.Wait();
    }

    if (!partition->writer) {
      continue;
    }

    DeferredSeal seal;
    seal.writer = std::move(partition->writer);
    seal.records = std::move(partition->pending_records);
    partition->pending_records.clear();
    seal.file_number = partition->file_number;
    seal.blob_count = partition->blob_count;
    seal.total_blob_bytes = partition->total_blob_bytes;
    seal.closed_wal_synced = !partition->sync_required;

    // Reset partition state so OpenNewBlobFile succeeds.
    partition->file_number = 0;
    partition->file_size = 0;
    partition->blob_count = 0;
    partition->total_blob_bytes = 0;
    partition->sync_required = false;
    partition->next_write_offset = 0;

    // Open new file immediately so writers can continue after rotation.
    Status s = OpenNewBlobFile(partition.get(), partition->column_family_id,
                               partition->compression);
    if (!s.ok()) {
      // Restore old state on failure.
      partition->writer = std::move(seal.writer);
      partition->pending_records = std::move(seal.records);
      partition->file_number = seal.file_number;
      partition->blob_count = seal.blob_count;
      partition->total_blob_bytes = seal.total_blob_bytes;
      partition->sync_required = !seal.closed_wal_synced;
      return s;
    }

    seals.emplace_back(partition.get(), std::move(seal));
  }

  if (!seals.empty()) {
    MutexLock lock(&bg_mutex_);
    uint64_t current_epoch = rotation_epoch_.load(std::memory_order_relaxed);
    for (const auto& [partition, seal] : seals) {
      (void)partition;
      ROCKS_LOG_DEBUG(info_log_,
                     "[BlobDirectWrite] RotateAllPartitions: captured blob "
                     "file %" PRIu64 " (%" PRIu64 " blobs, %" PRIu64
                     " bytes) into rotation batch epoch=%" PRIu64,
                     seal.file_number, seal.blob_count, seal.total_blob_bytes,
                     current_epoch);
    }
    RotationBatch batch;
    batch.epoch = current_epoch;
    batch.seals = std::move(seals);
    rotation_deferred_seals_.emplace_back(std::move(batch));
    ROCKS_LOG_DEBUG(info_log_,
                   "[BlobDirectWrite] RotateAllPartitions: "
                   "rotation_deferred_seals_ now has %zu batches",
                   rotation_deferred_seals_.size());
  } else {
    ROCKS_LOG_DEBUG(info_log_,
                   "[BlobDirectWrite] RotateAllPartitions: no partitions "
                   "had writers, no seals captured");
  }

  rotation_epoch_.fetch_add(1, std::memory_order_release);

  return Status::OK();
}

Status BlobFilePartitionManager::SealAllPartitions(
    const WriteOptions& write_options, std::vector<BlobFileAddition>* additions,
    bool seal_all, const std::vector<uint64_t>& epochs) {
  assert(additions);
  MutexLock deferred_sync_lock(&deferred_seal_sync_mutex_);
  TEST_SYNC_POINT("BlobFilePartitionManager::SealAllPartitions:BeforeEntryLog");
  size_t file_to_partition_size = 0;
  {
    ReadLock lock(&file_partition_mutex_);
    file_to_partition_size = file_to_partition_.size();
  }
  ROCKS_LOG_DEBUG(info_log_,
                 "[BlobDirectWrite] SealAllPartitions: entry, "
                 "file_to_partition_ size = %zu",
                 file_to_partition_size);

  // Fast path: skip if no blobs have been written since the last seal
  // AND there are no pending rotation seals.
  // Also collect any completed file additions from background seals.
  // Use exchange(0) instead of load()+store(0) to avoid losing increments
  // from writers that race between Phase 1 capture and the reset.
  // Skip fast path when seal_all is true (shutdown) — we must seal
  // everything regardless of blobs_written_since_seal_.
  bool has_pending_rotation = false;
  {
    MutexLock lock(&bg_mutex_);
    has_pending_rotation = !rotation_deferred_seals_.empty();
  }
  if (!seal_all && !has_pending_rotation &&
      blobs_written_since_seal_.exchange(0, std::memory_order_acq_rel) == 0) {
    TakeCompletedBlobFileAdditions(additions);
    ROCKS_LOG_DEBUG(info_log_,
                   "[BlobDirectWrite] SealAllPartitions: FAST PATH "
                   "(no pending rotation, no new blobs), collected %zu "
                   "completed additions",
                   additions->size());
    return Status::OK();
  }

  // Check if there are rotation deferred seals to process. If so, seal
  // those (old memtable's files) instead of the active partition files
  // (which belong to the next memtable). Find the batch matching the
  // flushing memtable's epoch (epoch-tagged matching, not FIFO).
  std::vector<std::pair<Partition*, DeferredSeal>> rotation_seals;
  bool has_rotation = false;
  {
    MutexLock lock(&bg_mutex_);
    if (seal_all) {
      // Shutdown: drain ALL pending rotation batches.
      for (auto& batch : rotation_deferred_seals_) {
        ROCKS_LOG_DEBUG(info_log_,
                       "[BlobDirectWrite] SealAllPartitions: seal_all "
                       "draining rotation batch epoch=%" PRIu64
                       " with %zu seals",
                       batch.epoch, batch.seals.size());
        for (auto& entry : batch.seals) {
          rotation_seals.emplace_back(std::move(entry));
        }
      }
      if (!rotation_deferred_seals_.empty()) {
        rotation_deferred_seals_.clear();
        has_rotation = true;
      }
    } else if (!epochs.empty()) {
      // Find batches matching the requested epochs.
      std::string epoch_str;
      for (uint64_t ep : epochs) {
        if (!epoch_str.empty()) epoch_str += ",";
        epoch_str += std::to_string(ep);
      }
      std::string pending_str;
      for (const auto& b : rotation_deferred_seals_) {
        if (!pending_str.empty()) pending_str += ",";
        pending_str += std::to_string(b.epoch);
      }
      ROCKS_LOG_DEBUG(info_log_,
                     "[BlobDirectWrite] SealAllPartitions: epoch matching, "
                     "requested=[%s], pending=[%s]",
                     epoch_str.c_str(), pending_str.c_str());
      for (uint64_t ep : epochs) {
        if (ep == 0) continue;
        bool found = false;
        for (auto it = rotation_deferred_seals_.begin();
             it != rotation_deferred_seals_.end(); ++it) {
          if (it->epoch == ep) {
            ROCKS_LOG_DEBUG(info_log_,
                           "[BlobDirectWrite] SealAllPartitions: MATCHED "
                           "epoch=%" PRIu64 " with %zu seals",
                           ep, it->seals.size());
            for (auto& entry : it->seals) {
              rotation_seals.emplace_back(std::move(entry));
            }
            rotation_deferred_seals_.erase(it);
            has_rotation = true;
            found = true;
            break;
          }
        }
        if (!found) {
          ROCKS_LOG_DEBUG(info_log_,
                         "[BlobDirectWrite] SealAllPartitions: epoch=%" PRIu64
                         " NOT FOUND in pending rotation batches",
                         ep);
        }
      }
      if (!rotation_deferred_seals_.empty()) {
        std::string remaining;
        for (const auto& b : rotation_deferred_seals_) {
          if (!remaining.empty()) remaining += ",";
          remaining += std::to_string(b.epoch) + "(" +
                       std::to_string(b.seals.size()) + " seals)";
        }
        ROCKS_LOG_DEBUG(info_log_,
                       "[BlobDirectWrite] SealAllPartitions: %zu UNMATCHED "
                       "rotation batches remain: [%s]",
                       rotation_deferred_seals_.size(), remaining.c_str());
      }
    } else if (!rotation_deferred_seals_.empty()) {
      // epoch=0 with pending rotations: fall back to FIFO for backward
      // compatibility (e.g., first flush before any rotation, or callers
      // that don't pass an epoch).
      ROCKS_LOG_DEBUG(info_log_,
                     "[BlobDirectWrite] SealAllPartitions: FIFO fallback "
                     "(epochs empty), popping front batch epoch=%" PRIu64
                     " with %zu seals, %zu batches remain",
                     rotation_deferred_seals_.front().epoch,
                     rotation_deferred_seals_.front().seals.size(),
                     rotation_deferred_seals_.size() - 1);
      auto& batch = rotation_deferred_seals_.front();
      for (auto& entry : batch.seals) {
        rotation_seals.emplace_back(std::move(entry));
      }
      rotation_deferred_seals_.pop_front();
      has_rotation = true;
    }
  }

  if (has_rotation) {
    // Rotation path: seal the captured old-memtable files.
    // Drain any in-flight BG work (normal rollovers that submitted
    // BG seals before the rotation).
    {
      MutexLock lock(&bg_mutex_);
      bg_seal_in_progress_ = true;
    }
    DrainBackgroundWork();

    // Check for background errors.
    {
      MutexLock lock(&bg_mutex_);
      if (!bg_status_.ok()) {
        bg_seal_in_progress_ = false;
        return bg_status_;
      }
    }

    // Collect completed_files from BG rollovers that happened before
    // the rotation. These belong to the old memtable's epoch.
    // NOTE: In the rare case where a normal rollover on a new-epoch file
    // completed between rotation and this point, its addition would also
    // be collected here. This is acceptable because blob_file_size_ is
    // typically much larger than memtable_size/num_partitions, making
    // this scenario extremely unlikely.
    TakeCompletedBlobFileAdditions(additions);

    // Per-file uncommitted bytes subtraction.
    {
      MutexLock lock(&bg_mutex_);
      // First: subtract exact per-file bytes.
      for (auto& [partition, seal] : rotation_seals) {
        (void)partition;
        auto it = file_uncommitted_bytes_.find(seal.file_number);
        if (it != file_uncommitted_bytes_.end()) {
          uint64_t adj = std::min(it->second, seal.total_blob_bytes);
          seal.total_blob_bytes -= adj;
          file_uncommitted_bytes_.erase(it);
        }
      }
      // Then: distribute file_number=0 (wildcard from write rollbacks)
      // proportionally across the sealed files.
      auto wc_it = file_uncommitted_bytes_.find(0);
      if (wc_it != file_uncommitted_bytes_.end() && !rotation_seals.empty()) {
        uint64_t wildcard = wc_it->second;
        uint64_t total_bytes = 0;
        for (const auto& [p, seal] : rotation_seals) {
          (void)p;
          total_bytes += seal.total_blob_bytes;
        }
        if (total_bytes > 0) {
          uint64_t remaining = wildcard;
          for (auto& [p, seal] : rotation_seals) {
            (void)p;
            uint64_t share = (seal.total_blob_bytes * wildcard) / total_bytes;
            share = std::min(share, seal.total_blob_bytes);
            share = std::min(share, remaining);
            seal.total_blob_bytes -= share;
            remaining -= share;
          }
        }
        file_uncommitted_bytes_.erase(wc_it);
      }
    }

    ROCKS_LOG_DEBUG(info_log_,
                   "[BlobDirectWrite] SealAllPartitions: sealing %zu "
                   "rotation files",
                   rotation_seals.size());
    TEST_SYNC_POINT("BlobFilePartitionManager::SealAllPartitions:Phase2");
    Status first_error;
    for (auto& [partition, seal] : rotation_seals) {
      BlobLogWriter* writer = seal.writer.get();

      Status s = FlushDeferredSealRecords(write_options, partition, &seal);

      if (s.ok()) {
        BlobLogFooter footer;
        footer.blob_count = seal.blob_count;

        std::string checksum_method;
        std::string checksum_value;
        const uint64_t physical_file_size =
            writer->file()->GetFileSize() + BlobLogFooter::kSize;
        s = writer->AppendFooter(write_options, footer, &checksum_method,
                                 &checksum_value);
        if (s.ok()) {
          EvictSealedBlobFileReader(seal.file_number);
          additions->emplace_back(seal.file_number, seal.blob_count,
                                  seal.total_blob_bytes, checksum_method,
                                  checksum_value, physical_file_size);
          if (blob_callback_) {
            const std::string file_path =
                BlobFileName(db_path_, seal.file_number);
            blob_callback_
                ->OnBlobFileCompleted(file_path, /*column_family_name=*/"",
                                      /*job_id=*/0, seal.file_number,
                                      BlobFileCreationReason::kDirectWrite, s,
                                      checksum_value, checksum_method,
                                      seal.blob_count, seal.total_blob_bytes)
                .PermitUncheckedError();
          }
        }
      }

      if (!seal.records.empty()) {
        RemoveFromPendingIndex(partition, seal.records);
      }

      if (s.ok()) {
        ROCKS_LOG_DEBUG(info_log_,
                       "[BlobDirectWrite] SealAllPartitions: rotation seal "
                       "OK for blob file %" PRIu64 " (%" PRIu64
                       " blobs, "
                       "%" PRIu64 " bytes)",
                       seal.file_number, seal.blob_count,
                       seal.total_blob_bytes);
      } else {
        ROCKS_LOG_ERROR(
            info_log_,
            "[BlobDirectWrite] SealAllPartitions: rotation seal "
            "FAILED for blob file %" PRIu64 " (%" PRIu64 " blobs): %s",
            seal.file_number, seal.blob_count, s.ToString().c_str());
      }
      seal.writer.reset();

      if (!s.ok() && first_error.ok()) {
        first_error = s;
      }
    }

    ROCKS_LOG_DEBUG(info_log_,
                   "[BlobDirectWrite] SealAllPartitions: rotation path "
                   "produced %zu additions total, first_error=%s",
                   additions->size(), first_error.ToString().c_str());

    {
      MutexLock lock(&bg_mutex_);
      bg_seal_in_progress_ = false;
    }

    if (!seal_all) {
      return first_error;
    }
    // seal_all mode: fall through to also seal active partition files.
    // This handles the shutdown case where rotation happened but the
    // new files also need to be sealed.
    if (!first_error.ok()) {
      return first_error;
    }
  }

  // Non-rotation path: seal all active partition files.
  // This is used for DB shutdown (final memtable) or when no rotation
  // has happened (e.g., manual flush before memtable is full).
  //
  // Step 1: Drain all in-flight BG work and set bg_seal_in_progress_ to
  //   prevent new Env::Schedule calls from SubmitSeal/SubmitFlush. Without
  //   this flag, a writer could submit a seal between drain and Phase 1,
  //   and the BG seal could race with our inline seal of the same partition.
  //
  // Step 2 (Phase 1): Under each partition's mutex, capture the writer and
  //   pending records into DeferredSeals. Collect any completed_files from
  //   BG seals that ran before the drain.
  //
  // Step 3 (Phase 2): Seal all captured files outside any mutex (I/O heavy).
  //
  // Step 4: Clear bg_seal_in_progress_ so writers can submit BG work again.
  //
  // Always drain background work, even when buffer_size_ == 0 (synchronous
  // mode). File rollovers submit BG seal tasks regardless of buffer_size_,
  // and we must wait for them to complete so their BlobFileAdditions land
  // in completed_files before we collect them below.
  {
    MutexLock lock(&bg_mutex_);
    bg_seal_in_progress_ = true;
  }
  DrainBackgroundWork();

  // Check for background errors.
  {
    MutexLock lock(&bg_mutex_);
    if (!bg_status_.ok()) {
      bg_seal_in_progress_ = false;
      return bg_status_;
    }
  }

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] SealAllPartitions: non-rotation path, "
                 "sealing active partition files");

  std::vector<std::pair<Partition*, DeferredSeal>> seals;
  size_t completed_collected __attribute__((unused)) = 0;

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    while (partition->sync_barrier_active) {
      partition->pending_cv.Wait();
    }

    if (partition->writer) {
      DeferredSeal seal;
      seal.writer = std::move(partition->writer);
      seal.records = std::move(partition->pending_records);
      partition->pending_records.clear();
      seal.file_number = partition->file_number;
      seal.blob_count = partition->blob_count;
      seal.total_blob_bytes = partition->total_blob_bytes;

      ROCKS_LOG_INFO(info_log_,
                     "[BlobDirectWrite] SealAllPartitions: non-rotation "
                     "captured blob file %" PRIu64 " (%" PRIu64
                     " blobs, "
                     "%" PRIu64 " bytes, %zu pending records)",
                     seal.file_number, seal.blob_count, seal.total_blob_bytes,
                     seal.records.size());

      partition->file_number = 0;
      partition->file_size = 0;
      partition->blob_count = 0;
      partition->total_blob_bytes = 0;
      partition->next_write_offset = 0;

      seals.emplace_back(partition.get(), std::move(seal));
    }

    for (auto& addition : partition->completed_files) {
      ROCKS_LOG_INFO(
          info_log_,
          "[BlobDirectWrite] SealAllPartitions: non-rotation "
          "collected completed blob file %" PRIu64 " (%" PRIu64 " blobs)",
          addition.GetBlobFileNumber(), addition.GetTotalBlobCount());
      additions->emplace_back(std::move(addition));
      completed_collected++;
    }
    partition->completed_files.clear();
  }

  // Drain uncommitted bytes from failed batches. Distribute the adjustment
  // across seals proportionally to their total_blob_bytes. This keeps GC
  // accurate by not counting unreferenced blob records as live data.
  // Per-file subtraction.
  {
    MutexLock lock(&bg_mutex_);
    for (auto& [partition, seal] : seals) {
      (void)partition;
      auto it = file_uncommitted_bytes_.find(seal.file_number);
      if (it != file_uncommitted_bytes_.end()) {
        uint64_t adj = std::min(it->second, seal.total_blob_bytes);
        seal.total_blob_bytes -= adj;
        file_uncommitted_bytes_.erase(it);
      }
    }
    // Distribute wildcard (file_number=0) proportionally.
    auto wc_it = file_uncommitted_bytes_.find(0);
    if (wc_it != file_uncommitted_bytes_.end() && !seals.empty()) {
      uint64_t wildcard = wc_it->second;
      uint64_t total_bytes = 0;
      for (const auto& [p, seal] : seals) {
        (void)p;
        total_bytes += seal.total_blob_bytes;
      }
      if (total_bytes > 0) {
        uint64_t remaining = wildcard;
        for (auto& [p, seal] : seals) {
          (void)p;
          uint64_t share = (seal.total_blob_bytes * wildcard) / total_bytes;
          share = std::min(share, seal.total_blob_bytes);
          share = std::min(share, remaining);
          seal.total_blob_bytes -= share;
          remaining -= share;
        }
      }
      file_uncommitted_bytes_.erase(wc_it);
    }
  }

  // Phase 2: Seal all captured files outside any mutex.
  // Continue processing remaining partitions even if one fails so we don't
  // leave writers in an abandoned state.
  TEST_SYNC_POINT("BlobFilePartitionManager::SealAllPartitions:Phase2");
  Status first_error;
  for (auto& [partition, seal] : seals) {
    BlobLogWriter* writer = seal.writer.get();

    Status s = FlushDeferredSealRecords(write_options, partition, &seal);

    if (s.ok()) {
      BlobLogFooter footer;
      footer.blob_count = seal.blob_count;

      std::string checksum_method;
      std::string checksum_value;
      const uint64_t physical_file_size =
          writer->file()->GetFileSize() + BlobLogFooter::kSize;
      s = writer->AppendFooter(write_options, footer, &checksum_method,
                               &checksum_value);
      if (s.ok()) {
        EvictSealedBlobFileReader(seal.file_number);
        additions->emplace_back(seal.file_number, seal.blob_count,
                                seal.total_blob_bytes, checksum_method,
                                checksum_value, physical_file_size);
        if (blob_callback_) {
          const std::string file_path =
              BlobFileName(db_path_, seal.file_number);
          blob_callback_
              ->OnBlobFileCompleted(file_path, /*column_family_name=*/"",
                                    /*job_id=*/0, seal.file_number,
                                    BlobFileCreationReason::kDirectWrite, s,
                                    checksum_value, checksum_method,
                                    seal.blob_count, seal.total_blob_bytes)
              .PermitUncheckedError();
        }
      }
    }

    // Remove ALL records from pending_index -- seal.records will be
    // destroyed at the end of this loop iteration, making any remaining
    // PendingBlobValueEntry pointers dangling.
    if (!seal.records.empty()) {
      RemoveFromPendingIndex(partition, seal.records);
    }
    // Keep the file_to_partition_ mapping. The sealed file must remain
    // visible to GetActiveBlobFileNumbers until committed to MANIFEST.
    // The flush caller will call RemoveFilePartitionMappings after commit.
    seal.writer.reset();

    if (!s.ok() && first_error.ok()) {
      first_error = s;
    }
  }

  // Release the seal-in-progress flag so BG work can be submitted again.
  {
    MutexLock lock(&bg_mutex_);
    bg_seal_in_progress_ = false;
  }

  return first_error;
}

void BlobFilePartitionManager::TakeCompletedBlobFileAdditions(
    std::vector<BlobFileAddition>* additions) {
  assert(additions);

  size_t collected = 0;
  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    for (auto& addition : partition->completed_files) {
      ROCKS_LOG_INFO(info_log_,
                     "[BlobDirectWrite] TakeCompletedBlobFileAdditions: "
                     "collecting blob file %" PRIu64 " (%" PRIu64
                     " blobs, %" PRIu64 " bytes) from completed_files",
                     addition.GetBlobFileNumber(), addition.GetTotalBlobCount(),
                     addition.GetTotalBlobBytes());
      additions->emplace_back(std::move(addition));
      collected++;
    }
    partition->completed_files.clear();
  }
  if (collected > 0) {
    ROCKS_LOG_INFO(info_log_,
                   "[BlobDirectWrite] TakeCompletedBlobFileAdditions: "
                   "collected %zu additions",
                   collected);
  }
}

void BlobFilePartitionManager::ReturnUnconsumedAdditions(
    std::vector<BlobFileAddition>&& additions) {
  if (additions.empty()) {
    return;
  }
  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] ReturnUnconsumedAdditions: returning "
                 "%zu additions (mempurge or flush failure)",
                 additions.size());
  for (const auto& a : additions) {
    ROCKS_LOG_INFO(info_log_,
                   "[BlobDirectWrite] ReturnUnconsumedAdditions: blob file "
                   "%" PRIu64 " (%" PRIu64 " blobs, %" PRIu64 " bytes)",
                   a.GetBlobFileNumber(), a.GetTotalBlobCount(),
                   a.GetTotalBlobBytes());
  }
  MutexLock lock(&partitions_[0]->mutex);
  for (auto& a : additions) {
    partitions_[0]->completed_files.emplace_back(std::move(a));
  }
}

Status BlobFilePartitionManager::FlushAllOpenFiles(
    const WriteOptions& write_options) {
  // Deferred mode: drain pending records from user-space buffers to the
  // kernel via a per-partition barriered flush. Writers on the same partition
  // wait behind the barrier, so the caller's BlobIndex cannot become visible
  // ahead of older in-flight flush work on that partition.
  if (buffer_size_ > 0) {
    TEST_SYNC_POINT("BlobFilePartitionManager::FlushAllOpenFiles:Begin");
    return DrainOpenFilesInternal(write_options, /*sync_to_disk=*/false,
                                  /*had_open_files=*/nullptr);
  }
  // In synchronous mode (buffer_size_ == 0), AddRecord is called with
  // do_flush=true, so data reaches the kernel immediately — no extra
  // flush needed.

  return Status::OK();
}

Status BlobFilePartitionManager::DrainOpenFilesInternal(
    const WriteOptions& write_options, bool sync_to_disk,
    bool* had_open_files) {
  if (had_open_files != nullptr) {
    *had_open_files = false;
  }

  for (auto& partition : partitions_) {
    BlobLogWriter* writer = nullptr;
    bool need_flush = false;
    bool sync_required = false;

    {
      MutexLock lock(&partition->mutex);
      while (partition->sync_barrier_active) {
        partition->pending_cv.Wait();
      }
      if (!partition->writer) {
        continue;
      }

      if (had_open_files != nullptr) {
        *had_open_files = true;
      }

      // Take ownership of this partition's active writer state. New writes,
      // rotations, and active-file seals wait behind the barrier while any
      // already-running BG flush drains. This gives Sync() a fixed snapshot of
      // the writer and pending records without starving on newly arriving
      // flushes. FlushAllOpenFiles() uses the same barrier so a new writer
      // cannot append behind an older in-flight flush and return before its
      // own record is disk-readable.
      partition->sync_barrier_active = true;
      if (sync_to_disk) {
        TEST_SYNC_POINT(
            "BlobFilePartitionManager::SyncOpenFilesInternal:BarrierInstalled");
      }
      while (partition->flush_queued.load(std::memory_order_acquire)) {
        partition->pending_cv.Wait();
      }

      writer = partition->writer.get();
      need_flush = buffer_size_ > 0 && !partition->pending_records.empty();
      sync_required = partition->sync_required;
    }

    Status s;
    if (bg_has_error_.load(std::memory_order_relaxed)) {
      MutexLock lock(&bg_mutex_);
      if (!bg_status_.ok()) {
        s = bg_status_;
      }
    }

    if (s.ok() && need_flush) {
      s = FlushPendingRecords(partition.get(), write_options);
    }

    if (s.ok() && sync_to_disk && sync_required) {
      TEST_SYNC_POINT("BlobFilePartitionManager::SyncAllOpenFiles:BeforeSync");
      s = writer->Sync(write_options);
    }

    {
      MutexLock lock(&partition->mutex);
      if (s.ok() && sync_to_disk && sync_required) {
        partition->sync_required = false;
      }
      partition->sync_barrier_active = false;
      partition->pending_cv.SignalAll();
    }

    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status BlobFilePartitionManager::SyncOpenFilesInternal(
    const WriteOptions& write_options, bool* had_open_files) {
  return DrainOpenFilesInternal(write_options, /*sync_to_disk=*/true,
                                had_open_files);
}

Status BlobFilePartitionManager::SyncWalRelevantFiles(
    const WriteOptions& write_options, bool sync_open_files) {
  // Serialize with SealAllPartitions() so deferred seals are not moved out of
  // rotation_deferred_seals_ while we walk and sync them.
  MutexLock deferred_sync_lock(&deferred_seal_sync_mutex_);

  for (;;) {
    const uint64_t start_epoch =
        sync_open_files ? rotation_epoch_.load(std::memory_order_acquire) : 0;

    // Normal rollovers submit BG seals directly and already fsync on footer
    // append. Drain them first so any blob files referenced by closed WALs are
    // either fully sealed or represented in completed_files before we sync the
    // rotation-deferred files below.
    DrainBackgroundWork();

    {
      MutexLock lock(&bg_mutex_);
      if (!bg_status_.ok()) {
        return bg_status_;
      }
    }

    std::vector<std::pair<Partition*, DeferredSeal*>> deferred_seals;
    {
      MutexLock lock(&bg_mutex_);
      for (auto& batch : rotation_deferred_seals_) {
        for (auto& entry : batch.seals) {
          DeferredSeal& seal = entry.second;
          if (seal.writer && !seal.closed_wal_synced) {
            deferred_seals.emplace_back(entry.first, &seal);
          }
        }
      }
    }

    for (auto& [partition, seal] : deferred_seals) {
      Status s = SyncDeferredSealForClosedWal(write_options, partition, seal);
      if (!s.ok()) {
        SetBGError(s);
        return s;
      }
    }

    if (!sync_open_files) {
      return Status::OK();
    }

    bool had_open_files = false;
    Status s = SyncOpenFilesInternal(write_options, &had_open_files);
    if (!s.ok()) {
      SetBGError(s);
      return s;
    }

    const uint64_t end_epoch = rotation_epoch_.load(std::memory_order_acquire);
    if (!had_open_files || start_epoch == end_epoch) {
      return Status::OK();
    }

    ROCKS_LOG_INFO(info_log_,
                   "[BlobDirectWrite] SyncWalRelevantFiles: retrying after "
                   "rotation epoch changed from %" PRIu64 " to %" PRIu64,
                   start_epoch, end_epoch);
  }
}

Status BlobFilePartitionManager::SyncAllOpenFiles(
    const WriteOptions& write_options) {
  return SyncOpenFilesInternal(write_options, /*had_open_files=*/nullptr);
}

void BlobFilePartitionManager::GetActiveBlobFileNumbers(
    std::unordered_set<uint64_t>* file_numbers) const {
  assert(file_numbers);
  // file_to_partition_ tracks all managed files: currently open files,
  // files being sealed (I/O in progress), and sealed files awaiting
  // MANIFEST commit. Mappings are only removed after MANIFEST commit
  // (via RemoveFilePartitionMappings) or on error. This single set
  // provides complete protection against PurgeObsoleteFiles.
  ReadLock lock(&file_partition_mutex_);
  size_t count_before = file_numbers->size();
  for (const auto& [file_number, _] : file_to_partition_) {
    file_numbers->insert(file_number);
  }
  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] GetActiveBlobFileNumbers: "
                 "file_to_partition_ has %zu entries, "
                 "total active set now %zu (was %zu)",
                 file_to_partition_.size(), file_numbers->size(), count_before);
}

void BlobFilePartitionManager::DumpTimingStats() const {}

void BlobFilePartitionManager::SubtractUncommittedBytes(uint64_t bytes,
                                                        uint64_t file_number) {
  // Track uncommitted bytes per-file. Used for:
  // 1. Epoch mismatch retries: the writer wrote to file_number but the
  //    BlobIndex was discarded (epoch changed). The bytes are in the file
  //    but no SST references them. Subtract at seal time so GC accounting
  //    is accurate (garbage can still reach total_blob_bytes).
  // 2. Write failure rollbacks: the write to the WAL/memtable failed after
  //    WriteBlob. The bytes are orphaned in file_number.
  MutexLock lock(&bg_mutex_);
  file_uncommitted_bytes_[file_number] += bytes;
}

Status BlobFilePartitionManager::ResolveBlobDirectWriteIndex(
    const ReadOptions& read_options, const Slice& user_key,
    const BlobIndex& blob_idx, const Version* version,
    BlobFileCache* blob_file_cache, BlobFilePartitionManager* partition_mgr,
    PinnableSlice* blob_value) {
  // Tier 1: Standard version-based blob read (checks blob cache internally).
  // This is the fastest path for data that has been flushed and sealed.
  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr uint64_t* bytes_read = nullptr;
  Status s = version->GetBlob(read_options, user_key, blob_idx, prefetch_buffer,
                              blob_value, bytes_read);
  if (s.ok()) {
    return s;
  }

  // Propagate IO errors directly — do not mask them with in-memory fallbacks.
  // Fault injection and real disk errors must surface to the caller.
  if (s.IsIOError()) {
    return s;
  }

  // Tier 2: Check unflushed pending records (deferred flush mode).
  // The blob may still be in the partition manager's pending buffer.
  if (partition_mgr) {
    std::string pending_value;
    Status pending_s = partition_mgr->GetPendingBlobValue(
        blob_idx.file_number(), blob_idx.offset(), &pending_value);
    if (pending_s.ok()) {
      blob_value->PinSelf(pending_value);
      return Status::OK();
    }
    if (!pending_s.IsNotFound()) {
      return pending_s;
    }
  }

  // Tier 3: Direct read via BlobFileCache for files not yet in version.
  // Allow footer-skip retry since these are write-path files that may be
  // unsealed.
  if (s.IsCorruption() && blob_file_cache) {
    CacheHandleGuard<BlobFileReader> reader;
    s = blob_file_cache->GetBlobFileReader(read_options, blob_idx.file_number(),
                                           &reader,
                                           /*allow_footer_skip_retry=*/true);
    if (s.ok()) {
      std::unique_ptr<BlobContents> blob_contents;
      s = reader.GetValue()->GetBlob(read_options, user_key, blob_idx.offset(),
                                     blob_idx.size(), blob_idx.compression(),
                                     prefetch_buffer, nullptr, &blob_contents,
                                     bytes_read);
      if (s.ok()) {
        blob_value->PinSelf(blob_contents->data());
      } else if (s.IsCorruption()) {
        reader.Reset();
        blob_file_cache->Evict(blob_idx.file_number());
        std::unique_ptr<BlobFileReader> fresh_reader;
        Status open_s = blob_file_cache->OpenBlobFileReaderUncached(
            read_options, blob_idx.file_number(), &fresh_reader);
        if (open_s.ok()) {
          std::unique_ptr<BlobContents> fresh_contents;
          // Always read through our fresh reader -- it has current file_size_.
          s = fresh_reader->GetBlob(read_options, user_key, blob_idx.offset(),
                                    blob_idx.size(), blob_idx.compression(),
                                    prefetch_buffer, nullptr, &fresh_contents,
                                    bytes_read);
          if (s.ok()) {
            blob_value->PinSelf(fresh_contents->data());
          }
          // Best-effort: replenish cache for future reads. Ignore result --
          // this read already succeeded regardless of whether insert wins.
          CacheHandleGuard<BlobFileReader> ignored;
          blob_file_cache
              ->InsertBlobFileReader(blob_idx.file_number(), &fresh_reader,
                                     &ignored)
              .PermitUncheckedError();
        } else {
          s = open_s;
        }
      }
    }
  }

  // Tier 4: Retry pending records. There is a race window where the BG
  // thread has already removed entries from pending_index (tier 1 misses)
  // but the data is not yet readable on disk — e.g., the BG flush has
  // written the records but the file is not yet synced/sealed, or the
  // BlobFileReader cached in tier 3 still has a stale file_size_. This
  // retry closes that gap: if any disk read failed, check pending_index
  // once more because a concurrent writer may have queued a new record
  // for the same file_number (after rotation) or the original record
  // may still be in-flight.
  if (!s.ok() && partition_mgr) {
    std::string pending_value;
    Status pending_s = partition_mgr->GetPendingBlobValue(
        blob_idx.file_number(), blob_idx.offset(), &pending_value);
    if (pending_s.ok()) {
      blob_value->PinSelf(pending_value);
      return Status::OK();
    }
    if (!pending_s.IsNotFound()) {
      return pending_s;
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
