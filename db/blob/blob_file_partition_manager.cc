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
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

BlobFilePartitionManager::Partition::Partition() : pending_cv(&mutex) {}
BlobFilePartitionManager::Partition::~Partition() = default;

BlobFilePartitionManager::BlobFilePartitionManager(
    uint32_t num_partitions,
    std::shared_ptr<BlobFilePartitionStrategy> strategy,
    FileNumberAllocator file_number_allocator, FileSystem* fs,
    SystemClock* clock, Statistics* statistics, const FileOptions& file_options,
    const std::string& db_path, uint64_t blob_file_size, bool use_fsync,
    CompressionType blob_compression_type, uint64_t buffer_size,
    bool use_direct_io, uint64_t flush_interval_ms,
    const std::shared_ptr<IOTracer>& io_tracer,
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    FileChecksumGenFactory* file_checksum_gen_factory,
    const FileTypeSet& checksum_handoff_file_types,
    BlobFileCompletionCallback* blob_callback, const std::string& db_id,
    const std::string& db_session_id, Logger* info_log)
    : num_partitions_(num_partitions),
      strategy_(strategy ? std::move(strategy)
                         : std::make_shared<RoundRobinPartitionStrategy>()),
      file_number_allocator_(std::move(file_number_allocator)),
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
      blob_callback_(blob_callback),
      db_id_(db_id),
      db_session_id_(db_session_id),
      info_log_(info_log),
      bg_cv_(&bg_mutex_),
      bg_drain_cv_(&bg_mutex_),
      bg_seal_done_cv_(&bg_mutex_) {
  assert(num_partitions_ > 0);
  assert(file_number_allocator_);
  assert(fs_);

  // Enable O_DIRECT for blob file writes if requested.
  if (use_direct_io) {
    file_options_.use_direct_writes = true;
  }

  partitions_.reserve(num_partitions_);
  for (uint32_t i = 0; i < num_partitions_; ++i) {
    partitions_.emplace_back(std::make_unique<Partition>());
  }

  // Start background I/O thread pool for deferred seal and flush operations.
  // Use one thread per partition so each partition can flush independently.
  if (buffer_size_ > 0) {
    const uint32_t num_bg_threads = num_partitions_;
    for (uint32_t i = 0; i < num_bg_threads; ++i) {
      bg_threads_.emplace_back(
          &BlobFilePartitionManager::BackgroundIOLoop, this);
    }
  }
}

BlobFilePartitionManager::~BlobFilePartitionManager() {
  // Stop background threads.
  if (!bg_threads_.empty()) {
    {
      MutexLock lock(&bg_mutex_);
      bg_stop_ = true;
      bg_cv_.SignalAll();
    }
    for (auto& t : bg_threads_) {
      t.join();
    }
  }
  DumpTimingStats();
  // Free the current and all retired settings maps.
  delete cached_settings_.load(std::memory_order_relaxed);
  for (auto* m : retired_settings_) {
    delete m;
  }
}

Status BlobFilePartitionManager::OpenNewBlobFile(
    Partition* partition, uint32_t column_family_id,
    CompressionType compression) {
  assert(partition);
  assert(!partition->writer);

  const uint64_t blob_file_number = file_number_allocator_();
  const std::string blob_file_path = BlobFileName(db_path_, blob_file_number);

  std::unique_ptr<FSWritableFile> file;
  const Status s = NewWritableFile(fs_, blob_file_path, &file, file_options_);
  if (!s.ok()) {
    return s;
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
  const Status ws = blob_log_writer->WriteHeader(wo, header);
  if (!ws.ok()) {
    return ws;
  }

  partition->writer = std::move(blob_log_writer);
  partition->file_number = blob_file_number;
  partition->file_size = BlobLogHeader::kSize;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->column_family_id = column_family_id;
  partition->compression = compression;
  partition->next_write_offset = BlobLogHeader::kSize;

  {
    uint32_t partition_idx = 0;
    for (uint32_t i = 0; i < num_partitions_; ++i) {
      if (partitions_[i].get() == partition) {
        partition_idx = i;
        break;
      }
    }
    AddFilePartitionMapping(blob_file_number, partition_idx);
  }

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] Opened blob file %" PRIu64 " (%s)",
                 blob_file_number, blob_file_path.c_str());

  if (blob_callback_) {
    blob_callback_->OnBlobFileCreationStarted(
        blob_file_path, /*column_family_name=*/"", /*job_id=*/0,
        BlobFileCreationReason::kFlush);
  }

  return Status::OK();
}

Status BlobFilePartitionManager::CloseBlobFile(Partition* partition) {
  assert(partition);
  assert(partition->writer);

  // Flush pending deferred records before closing.
  // Done inline while holding the mutex to prevent other threads from adding
  // records with pre-calculated offsets for this file during the flush.
  // The mutex is held during I/O, but this only blocks one partition and
  // file close is infrequent (once per blob_file_size bytes).
  if (buffer_size_ > 0 && !partition->pending_records.empty()) {
    std::deque<PendingRecord> records =
        std::move(partition->pending_records);
    partition->pending_records.clear();
    BlobLogWriter* writer = partition->writer.get();

    Status flush_err;
    size_t records_written = 0;
    for (auto& record : records) {
      uint64_t key_offset = 0;
      uint64_t actual_blob_offset = 0;
      WriteOptions wo;
      flush_err = writer->AddRecord(wo, Slice(record.key),
                                    Slice(record.value), &key_offset,
                                    &actual_blob_offset);
      if (!flush_err.ok()) {
        break;
      }
      if (actual_blob_offset != record.blob_offset) {
        flush_err = Status::Corruption(
            "BlobDirectWrite: pre-calculated blob offset does not match "
            "actual offset");
        break;
      }

      const uint64_t record_bytes = BlobLogRecord::kHeaderSize +
                                    record.key.size() + record.value.size();
      partition->pending_bytes.fetch_sub(record_bytes,
                                         std::memory_order_relaxed);
      ++records_written;
    }

    // Decrement pending_bytes for unwritten records to prevent perpetual
    // backpressure after a partial flush failure.
    for (size_t i = records_written; i < records.size(); ++i) {
      const auto& rec = records[i];
      const uint64_t rec_bytes =
          BlobLogRecord::kHeaderSize + rec.key.size() + rec.value.size();
      partition->pending_bytes.fetch_sub(rec_bytes,
                                         std::memory_order_relaxed);
    }

    // Signal backpressure waiters — mutex is already held by caller.
    partition->pending_cv.SignalAll();

    // Remove successfully written records from per-partition pending index.
    // Mutex is already held by caller. Unwritten records remain visible for
    // reads; cleaned up on destruction.
    for (size_t i = 0; i < records_written; ++i) {
      partition->pending_index.erase(
          {records[i].file_number, records[i].blob_offset});
    }

    if (!flush_err.ok()) {
      partition->writer.reset();
      partition->file_number = 0;
      partition->file_size = 0;
      partition->blob_count = 0;
      partition->total_blob_bytes = 0;
      partition->next_write_offset = 0;
      return flush_err;
    }

    // Flush to OS.
    IOOptions io_opts;
    WriteOptions wo;
    Status s = WritableFileWriter::PrepareIOOptions(wo, io_opts);
    if (s.ok()) {
      s = writer->file()->Flush(io_opts);
    }
    if (!s.ok()) {
      return s;
    }
  }

  // Flush any legacy buffered data before writing footer.
  if (buffer_size_ > 0 && partition->unflushed_bytes > 0) {
    IOOptions io_opts;
    WriteOptions wo;
    Status s = WritableFileWriter::PrepareIOOptions(wo, io_opts);
    if (s.ok()) {
      s = partition->writer->file()->Flush(io_opts);
    }
    if (!s.ok()) {
      return s;
    }
    partition->unflushed_bytes = 0;
  }

  BlobLogFooter footer;
  footer.blob_count = partition->blob_count;

  std::string checksum_method;
  std::string checksum_value;

  WriteOptions wo;
  Status s = partition->writer->AppendFooter(wo, footer, &checksum_method,
                                             &checksum_value);
  if (!s.ok()) {
    return s;
  }

  partition->completed_files.emplace_back(
      partition->file_number, partition->blob_count,
      partition->total_blob_bytes, checksum_method, checksum_value);

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] Closed blob file %" PRIu64 ": %" PRIu64
                 " blobs, %" PRIu64 " bytes",
                 partition->file_number, partition->blob_count,
                 partition->total_blob_bytes);

  if (blob_callback_) {
    const std::string file_path =
        BlobFileName(db_path_, partition->file_number);
    blob_callback_->OnBlobFileCompleted(
        file_path, /*column_family_name=*/"", /*job_id=*/0,
        partition->file_number, BlobFileCreationReason::kFlush, s,
        checksum_value, checksum_method, partition->blob_count,
        partition->total_blob_bytes);
  }

  const uint64_t closed_file_number = partition->file_number;

  partition->writer.reset();
  partition->file_number = 0;
  partition->file_size = 0;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->next_write_offset = 0;

  RemoveFilePartitionMapping(closed_file_number);

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

  // Reset partition state so OpenNewBlobFile succeeds.
  partition->file_number = 0;
  partition->file_size = 0;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->next_write_offset = 0;

  // Open new file immediately so writers can continue.
  return OpenNewBlobFile(partition, column_family_id, compression);
}

Status BlobFilePartitionManager::SealDeferredFile(
    Partition* partition, DeferredSeal* deferred) {
  assert(deferred);
  assert(deferred->writer);

  BlobLogWriter* writer = deferred->writer.get();

  size_t records_written = 0;
  Status write_err;
  for (auto& record : deferred->records) {
    uint64_t key_offset = 0;
    uint64_t actual_blob_offset = 0;
    WriteOptions wo;
    write_err = writer->AddRecord(wo, Slice(record.key), Slice(record.value),
                                  &key_offset, &actual_blob_offset);
    if (!write_err.ok()) {
      break;
    }
    if (actual_blob_offset != record.blob_offset) {
      write_err = Status::Corruption(
          "BlobDirectWrite: pre-calculated blob offset does not match "
          "actual offset");
      break;
    }

    const uint64_t record_bytes =
        BlobLogRecord::kHeaderSize + record.key.size() + record.value.size();
    partition->pending_bytes.fetch_sub(record_bytes,
                                       std::memory_order_relaxed);
    ++records_written;
  }

  // Decrement pending_bytes for unwritten records to prevent perpetual
  // backpressure after a partial write failure.
  for (size_t i = records_written; i < deferred->records.size(); ++i) {
    const auto& rec = deferred->records[i];
    const uint64_t rec_bytes =
        BlobLogRecord::kHeaderSize + rec.key.size() + rec.value.size();
    partition->pending_bytes.fetch_sub(rec_bytes, std::memory_order_relaxed);
  }

  {
    MutexLock lock(&partition->mutex);
    partition->pending_cv.SignalAll();
  }

  if (!write_err.ok()) {
    if (records_written > 0) {
      std::deque<PendingRecord> written(
          deferred->records.begin(),
          deferred->records.begin() +
              static_cast<std::ptrdiff_t>(records_written));
      RemoveFromPendingIndex(partition, written);
    }
    deferred->writer.reset();
    return write_err;
  }

  // Flush to OS.
  {
    IOOptions io_opts;
    WriteOptions wo;
    Status s = WritableFileWriter::PrepareIOOptions(wo, io_opts);
    if (s.ok()) {
      s = writer->file()->Flush(io_opts);
    }
    if (!s.ok()) {
      RemoveFromPendingIndex(partition, deferred->records);
      deferred->writer.reset();
      return s;
    }
  }

  // Write footer.
  BlobLogFooter footer;
  footer.blob_count = deferred->blob_count;

  std::string checksum_method;
  std::string checksum_value;
  WriteOptions wo;
  Status s = writer->AppendFooter(wo, footer, &checksum_method,
                                  &checksum_value);
  if (!s.ok()) {
    RemoveFromPendingIndex(partition, deferred->records);
    deferred->writer.reset();
    return s;
  }

  {
    MutexLock lock(&partition->mutex);
    partition->completed_files.emplace_back(
        deferred->file_number, deferred->blob_count, deferred->total_blob_bytes,
        checksum_method, checksum_value);
  }

  ROCKS_LOG_INFO(info_log_,
                 "[BlobDirectWrite] Sealed blob file %" PRIu64 ": %" PRIu64
                 " blobs, %" PRIu64 " bytes",
                 deferred->file_number, deferred->blob_count,
                 deferred->total_blob_bytes);

  if (blob_callback_) {
    const std::string file_path = BlobFileName(db_path_, deferred->file_number);
    blob_callback_->OnBlobFileCompleted(
        file_path, /*column_family_name=*/"", /*job_id=*/0,
        deferred->file_number, BlobFileCreationReason::kFlush, s,
        checksum_value, checksum_method, deferred->blob_count,
        deferred->total_blob_bytes);
  }

  RemoveFromPendingIndex(partition, deferred->records);
  RemoveFilePartitionMapping(deferred->file_number);

  deferred->writer.reset();
  return Status::OK();
}

void BlobFilePartitionManager::SubmitSeal(Partition* partition,
                                          DeferredSeal&& seal) {
  MutexLock lock(&bg_mutex_);
  BGWorkItem item;
  item.type = BGWorkItem::kSeal;
  item.seal = std::move(seal);
  bool was_empty = partition->bg_queue.empty();
  partition->bg_queue.emplace_back(std::move(item));
  if (was_empty && !partition->bg_in_flight) {
    ready_queue_.push_back(partition);
  }
  bg_cv_.SignalAll();
}

void BlobFilePartitionManager::SubmitFlush(Partition* partition) {
  if (partition->flush_queued.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  MutexLock lock(&bg_mutex_);
  BGWorkItem item;
  item.type = BGWorkItem::kFlush;
  bool was_empty = partition->bg_queue.empty();
  partition->bg_queue.emplace_back(std::move(item));
  if (was_empty && !partition->bg_in_flight) {
    ready_queue_.push_back(partition);
  }
  bg_cv_.SignalAll();
}

void BlobFilePartitionManager::DrainBackgroundWork() {
  MutexLock lock(&bg_mutex_);
  while (true) {
    bool any_work = false;
    for (auto& partition : partitions_) {
      if (partition->bg_in_flight) {
        any_work = true;
        break;
      }
      // When bg_seal_in_progress_, BG threads won't pick up queued work,
      // so only wait for in-flight work. Without this, a writer queueing
      // new flush work during seal would deadlock DrainBackgroundWork.
      if (!bg_seal_in_progress_ && !partition->bg_queue.empty()) {
        any_work = true;
        break;
      }
    }
    if (!any_work) {
      return;
    }
    bg_drain_cv_.Wait();
  }
}

void BlobFilePartitionManager::BackgroundIOLoop() {
  while (true) {
    Partition* partition = nullptr;
    std::deque<BGWorkItem> items;

    {
      MutexLock lock(&bg_mutex_);
      while (true) {
        if (bg_stop_) {
          // During shutdown, ready_queue_ may not contain all partitions
          // with queued work.  Fall back to a linear scan.
          bool any_work = false;
          for (auto& p : partitions_) {
            if (!p->bg_queue.empty() && !p->bg_in_flight) {
              any_work = true;
              ready_queue_.push_back(p.get());
            }
          }
          if (!any_work) {
            return;
          }
        }

        // Dequeue from ready_queue_ instead of scanning all partitions.
        if (!bg_seal_in_progress_) {
          while (!ready_queue_.empty()) {
            Partition* candidate = ready_queue_.front();
            ready_queue_.pop_front();
            if (!candidate->bg_queue.empty() && !candidate->bg_in_flight) {
              partition = candidate;
              break;
            }
          }
        }
        if (partition) {
          break;
        }
        if (bg_seal_in_progress_) {
          bg_seal_done_cv_.Wait();
          continue;
        }
        if (flush_interval_us_ > 0) {
          bg_cv_.TimedWait(clock_->NowMicros() + flush_interval_us_);
        } else {
          bg_cv_.Wait();
        }

        // Periodic flush: scan partitions with pending bytes.
        if (!partition && flush_interval_us_ > 0) {
          for (auto& p : partitions_) {
            if (p->pending_bytes.load(std::memory_order_relaxed) > 0 &&
                !p->bg_in_flight) {
              bool has_flush = false;
              for (const auto& item : p->bg_queue) {
                if (item.type == BGWorkItem::kFlush) {
                  has_flush = true;
                  break;
                }
              }
              if (!has_flush) {
                TEST_SYNC_POINT(
                    "BlobFilePartitionManager::BackgroundIOLoop:"
                    "PeriodicFlush");
                BGWorkItem item;
                item.type = BGWorkItem::kFlush;
                p->bg_queue.emplace_back(std::move(item));
                ready_queue_.push_back(p.get());
              }
            }
          }
        }
      }

      items = std::move(partition->bg_queue);
      partition->bg_queue.clear();
      partition->bg_in_flight = true;
    }

    for (auto& item : items) {
      Status s;
      if (item.type == BGWorkItem::kSeal) {
        s = SealDeferredFile(partition, &item.seal);
      } else if (item.type == BGWorkItem::kFlush) {
        partition->flush_queued.store(false, std::memory_order_release);
        s = FlushPendingRecords(partition);
      }

      if (!s.ok()) {
        MutexLock lock(&bg_mutex_);
        if (bg_status_.ok()) {
          bg_status_ = s;
          bg_has_error_.store(true, std::memory_order_release);
        }
      }
    }

    {
      MutexLock lock(&bg_mutex_);
      partition->bg_in_flight = false;

      // Re-enqueue if more work arrived while we were processing.
      if (!partition->bg_queue.empty()) {
        ready_queue_.push_back(partition);
        bg_cv_.SignalAll();
      }

      // Always signal drain waiters when in-flight work completes.
      // DrainBackgroundWork re-checks its own conditions (which depend on
      // bg_seal_in_progress_) and only returns when appropriate. Without
      // the unconditional signal, a deadlock occurs when bg_seal_in_progress_
      // is set: the BG thread finishes in-flight work, finds queued work,
      // but can't pick it up (bg_seal_in_progress_ blocks dequeue). It
      // skips signaling bg_drain_cv_ because any_work is true (queued work
      // exists). DrainBackgroundWork then never wakes up, even though its
      // wait condition (bg_in_flight) is now false.
      bg_drain_cv_.SignalAll();
    }
  }
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

  // Add to per-partition pending index for O(1) read path lookup.
  // Points into the deque element — stable because std::deque::push_back
  // does not invalidate references to existing elements.
  // Partition mutex is already held by caller (WriteBlob).
  partition->pending_index[{fn, *blob_offset}] = {
      &partition->pending_records.back().value, partition->compression};

  return Status::OK();
}

Status BlobFilePartitionManager::WriteBlobSync(
    Partition* partition, const Slice& key, const Slice& value,
    uint64_t* blob_offset) {
  assert(partition);

  uint64_t key_offset = 0;
  WriteOptions wo;
  Status s =
      partition->writer->AddRecord(wo, key, value, &key_offset, blob_offset);
  if (!s.ok()) {
    return s;
  }

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
  std::lock_guard<std::mutex> lock(file_partition_write_mutex_);
  auto old =
      std::atomic_load_explicit(&file_to_partition_, std::memory_order_acquire);
  auto new_map = old ? std::make_shared<FilePartitionMap>(*old)
                     : std::make_shared<FilePartitionMap>();
  (*new_map)[file_number] = partition_idx;
  std::atomic_store_explicit(&file_to_partition_, std::move(new_map),
                             std::memory_order_release);
}

void BlobFilePartitionManager::RemoveFilePartitionMapping(
    uint64_t file_number) {
  std::lock_guard<std::mutex> lock(file_partition_write_mutex_);
  auto old =
      std::atomic_load_explicit(&file_to_partition_, std::memory_order_acquire);
  if (old && old->count(file_number)) {
    auto new_map = std::make_shared<FilePartitionMap>(*old);
    new_map->erase(file_number);
    std::atomic_store_explicit(&file_to_partition_, std::move(new_map),
                               std::memory_order_release);
  }
}

bool BlobFilePartitionManager::GetPendingBlobValue(
    uint64_t file_number, uint64_t offset, std::string* value) const {
  // Lock-free lookup via RCU snapshot of file_to_partition_.
  uint32_t part_idx;
  {
    auto snapshot = std::atomic_load_explicit(&file_to_partition_,
                                              std::memory_order_acquire);
    if (!snapshot) {
      return false;
    }
    auto fit = snapshot->find(file_number);
    if (fit == snapshot->end()) {
      return false;
    }
    part_idx = fit->second;
  }

  Partition* partition = partitions_[part_idx].get();
  PendingBlobValueEntry entry;
  {
    MutexLock lock(&partition->mutex);
    auto it = partition->pending_index.find({file_number, offset});
    if (it == partition->pending_index.end()) {
      return false;
    }
    entry = it->second;
  }

  if (entry.compression != kNoCompression) {
    auto decomp = GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
        entry.compression);
    if (!decomp) {
      return false;
    }
    Decompressor::Args args;
    args.compression_type = entry.compression;
    args.compressed_data = Slice(*entry.data);
    Status s = decomp->ExtractUncompressedSize(args);
    if (!s.ok()) {
      return false;
    }
    value->resize(args.uncompressed_size);
    s = decomp->DecompressBlock(args, const_cast<char*>(value->data()));
    return s.ok();
  }

  *value = *entry.data;
  return true;
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
      strategy_->SelectPartition(num_partitions_, column_family_id, key, value);
  if (partition_idx >= num_partitions_) {
    return Status::InvalidArgument(
        "BlobFilePartitionStrategy::SelectPartition returned out-of-range "
        "index " +
        std::to_string(partition_idx) +
        " (num_partitions=" + std::to_string(num_partitions_) + ")");
  }

  Partition* partition = partitions_[partition_idx].get();

  // Backpressure: stall if pending bytes exceed buffer_size.
  // Submit flush to background thread and wait, never flush inline.
  // Use a timed wait to avoid permanent hangs from missed signals or
  // consumed flushes, and re-submit flush each iteration.
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
      Status s =
          LegacyForceBuiltinCompression(*compressor, &wa, value, &compressed_buf);
      if (!s.ok()) {
        return s;
      }
      write_value = Slice(compressed_buf);
    }
  }

  // Pre-copy key and (compressed) value OUTSIDE the mutex for deferred mode.
  // Only one copy of the final value, not the pre-compression original.
  std::string key_copy, value_copy;
  if (buffer_size_ > 0) {
    key_copy.assign(key.data(), key.size());
    value_copy.assign(write_value.data(), write_value.size());
  }

  {
    MutexLock lock(&partition->mutex);

    if (!partition->writer ||
        partition->column_family_id != column_family_id ||
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

    RecordTick(statistics_, BLOB_DB_DIRECT_WRITE_COUNT);
    RecordTick(statistics_, BLOB_DB_DIRECT_WRITE_BYTES, value.size());
    blobs_written_since_seal_.fetch_add(1, std::memory_order_relaxed);

    if (buffer_size_ > 0 && high_water_mark_ > 0 &&
        partition->pending_bytes.load(std::memory_order_relaxed) >=
            high_water_mark_) {
      need_flush = true;
    }
  }  // mutex released

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

  // Submit seal to background thread (non-blocking).
  if (deferred_seal.writer) {
    SubmitSeal(partition, std::move(deferred_seal));
  }

  // Submit flush to background thread (non-blocking).
  if (need_flush) {
    SubmitFlush(partition);
  }

  return Status::OK();
}

Status BlobFilePartitionManager::FlushPendingRecords(Partition* partition) {
  assert(partition);

  // Only called from the single background I/O thread. Safe to release the
  // mutex during I/O because:
  // 1. PrepareFileRollover may move partition->writer to a DeferredSeal in the
  //    BG queue, but the DeferredSeal keeps the writer alive until the BG thread
  //    processes it (sequentially, after this flush completes).
  // 2. No other code path calls FlushPendingRecords (backpressure submits to
  //    BG thread and waits).
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

  WriteOptions wo;
  size_t records_written = 0;
  Status flush_status;
  for (auto& record : records) {
    uint64_t key_offset = 0;
    uint64_t actual_blob_offset = 0;

    flush_status = writer->AddRecord(wo, Slice(record.key), Slice(record.value),
                                     &key_offset, &actual_blob_offset);
    if (!flush_status.ok()) {
      break;
    }
    if (actual_blob_offset != record.blob_offset) {
      flush_status = Status::Corruption(
          "BlobDirectWrite: pre-calculated blob offset does not match "
          "actual offset");
      break;
    }

    const uint64_t record_bytes = BlobLogRecord::kHeaderSize +
                                  record.key.size() + record.value.size();

    partition->pending_bytes.fetch_sub(record_bytes,
                                      std::memory_order_relaxed);
    ++records_written;
  }

  if (flush_status.ok()) {
    // Flush to OS (single write() syscall for entire batch).
    IOOptions io_opts;
    flush_status = WritableFileWriter::PrepareIOOptions(wo, io_opts);
    if (flush_status.ok()) {
      flush_status = writer->file()->Flush(io_opts);
    }
  }

  // Decrement pending_bytes for unwritten records to prevent perpetual
  // backpressure after a partial flush failure.
  for (size_t i = records_written; i < records.size(); ++i) {
    const auto& rec = records[i];
    const uint64_t rec_bytes =
        BlobLogRecord::kHeaderSize + rec.key.size() + rec.value.size();
    partition->pending_bytes.fetch_sub(rec_bytes, std::memory_order_relaxed);
  }

  // Only remove records from pending index after both AddRecord and Flush
  // succeed. If Flush fails, records may not be on disk yet — keeping them
  // in pending_index allows Tier 1 reads to still find the data in memory.
  if (flush_status.ok() && records_written > 0) {
    RemoveFromPendingIndex(partition, records);
  } else if (flush_status.ok() && records_written == 0) {
    // No records written, nothing to remove.
  } else if (!flush_status.ok() && records_written > 0) {
    // Flush failed — keep records in pending_index for read visibility.
    // The bg_has_error_ flag will prevent new writes.
  }
  {
    MutexLock lock(&partition->mutex);
    partition->pending_cv.SignalAll();
  }

  if (!flush_status.ok()) {
    return flush_status;
  }

  return Status::OK();
}

Status BlobFilePartitionManager::SealAllPartitions(
    const WriteOptions& /*write_options*/,
    std::vector<BlobFileAddition>* additions) {
  assert(additions);

  // Fast path: skip if no blobs have been written since the last seal.
  // Also collect any completed file additions from background seals.
  // Use exchange(0) instead of load()+store(0) to avoid losing increments
  // from writers that race between Phase 1 capture and the reset.
  if (blobs_written_since_seal_.exchange(0, std::memory_order_relaxed) == 0) {
    TakeCompletedBlobFileAdditions(additions);
    return Status::OK();
  }

  // Drain all pending background work (seals and flushes) before sealing.
  // Set bg_seal_in_progress_ to prevent BG threads from picking up any new
  // work that writers may submit between drain completion and partition
  // mutex acquisition. This prevents the race where a BG thread and
  // SealAllPartitions both write to the same BlobLogWriter.
  if (buffer_size_ > 0) {
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
        bg_seal_done_cv_.SignalAll();
        return bg_status_;
      }
    }
  }

  // Phase 1: Under each partition's mutex, capture state for sealing.
  // No new file is opened (unlike rollover) since we're sealing everything.
  std::vector<std::pair<Partition*, DeferredSeal>> seals;

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);

    if (partition->writer) {
      DeferredSeal seal;
      seal.writer = std::move(partition->writer);
      seal.records = std::move(partition->pending_records);
      partition->pending_records.clear();
      seal.file_number = partition->file_number;
      seal.blob_count = partition->blob_count;
      seal.total_blob_bytes = partition->total_blob_bytes;

      partition->file_number = 0;
      partition->file_size = 0;
      partition->blob_count = 0;
      partition->total_blob_bytes = 0;
      partition->next_write_offset = 0;

      seals.emplace_back(partition.get(), std::move(seal));
    }

    for (auto& addition : partition->completed_files) {
      additions->emplace_back(std::move(addition));
    }
    partition->completed_files.clear();
  }

  // Extract unprocessed DeferredSeals from bg_queues before clearing.
  // These are from PrepareFileRollovers that were queued but not yet
  // processed by the BG thread (which was paused by bg_seal_in_progress_).
  // Discard flush items since Phase 2 handles all pending records inline.
  if (buffer_size_ > 0) {
    MutexLock lock(&bg_mutex_);
    for (auto& p : partitions_) {
      for (auto& item : p->bg_queue) {
        if (item.type == BGWorkItem::kSeal) {
          seals.emplace_back(p.get(), std::move(item.seal));
        }
      }
      p->bg_queue.clear();
      p->flush_queued.store(false, std::memory_order_relaxed);
    }
    ready_queue_.clear();
  }

  // Phase 2: Seal all captured files outside any mutex.
  // Continue processing remaining partitions even if one fails so we don't
  // leave writers in an abandoned state.
  Status first_error;
  for (auto& [partition, seal] : seals) {
    BlobLogWriter* writer = seal.writer.get();

    // Flush pending records, tracking how many succeeded.
    WriteOptions wo;
    Status s;
    size_t records_written = 0;
    for (auto& record : seal.records) {
      uint64_t key_offset = 0;
      uint64_t actual_blob_offset = 0;
      s = writer->AddRecord(wo, Slice(record.key), Slice(record.value),
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

      const uint64_t record_bytes = BlobLogRecord::kHeaderSize +
                                    record.key.size() + record.value.size();
      partition->pending_bytes.fetch_sub(record_bytes,
                                         std::memory_order_relaxed);
      ++records_written;
    }

    // Decrement pending_bytes for unwritten records.
    for (size_t i = records_written; i < seal.records.size(); ++i) {
      const auto& rec = seal.records[i];
      const uint64_t rec_bytes =
          BlobLogRecord::kHeaderSize + rec.key.size() + rec.value.size();
      partition->pending_bytes.fetch_sub(rec_bytes,
                                         std::memory_order_relaxed);
    }

    // Signal backpressure waiters under the mutex.
    {
      MutexLock lock(&partition->mutex);
      partition->pending_cv.SignalAll();
    }

    if (s.ok()) {
      // Flush to OS.
      IOOptions io_opts;
      s = WritableFileWriter::PrepareIOOptions(wo, io_opts);
      if (s.ok()) {
        s = writer->file()->Flush(io_opts);
      }
    }

    if (s.ok()) {
      // Write footer.
      BlobLogFooter footer;
      footer.blob_count = seal.blob_count;

      std::string checksum_method;
      std::string checksum_value;
      s = writer->AppendFooter(wo, footer, &checksum_method, &checksum_value);
      if (s.ok()) {
        additions->emplace_back(seal.file_number, seal.blob_count,
                                seal.total_blob_bytes, checksum_method,
                                checksum_value);
        if (blob_callback_) {
          const std::string file_path =
              BlobFileName(db_path_, seal.file_number);
          blob_callback_->OnBlobFileCompleted(
              file_path, /*column_family_name=*/"", /*job_id=*/0,
              seal.file_number, BlobFileCreationReason::kFlush, s,
              checksum_value, checksum_method, seal.blob_count,
              seal.total_blob_bytes);
        }
      }
    }

    // Only remove successfully written records from per-partition pending
    // index. Unwritten records remain visible to reads; cleaned up on
    // destruction.
    if (records_written > 0) {
      std::deque<PendingRecord> written_records(
          seal.records.begin(),
          seal.records.begin() +
              static_cast<std::ptrdiff_t>(records_written));
      RemoveFromPendingIndex(partition, written_records);
    }
    if (s.ok()) {
      RemoveFilePartitionMapping(seal.file_number);
    }
    seal.writer.reset();

    if (!s.ok() && first_error.ok()) {
      first_error = s;
    }
  }

  // Release the seal-in-progress flag so BG threads can resume.
  if (buffer_size_ > 0) {
    MutexLock lock(&bg_mutex_);
    bg_seal_in_progress_ = false;
    bg_seal_done_cv_.SignalAll();
    bg_cv_.SignalAll();
  }

  return first_error;
}

void BlobFilePartitionManager::TakeCompletedBlobFileAdditions(
    std::vector<BlobFileAddition>* additions) {
  assert(additions);

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    for (auto& addition : partition->completed_files) {
      additions->emplace_back(std::move(addition));
    }
    partition->completed_files.clear();
  }
}

Status BlobFilePartitionManager::FlushAllOpenFiles(
    const WriteOptions& write_options) {
  if (buffer_size_ > 0) {
    for (auto& partition : partitions_) {
      SubmitFlush(partition.get());
    }
    DrainBackgroundWork();

    MutexLock lock(&bg_mutex_);
    if (!bg_status_.ok()) {
      return bg_status_;
    }
  }

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    if (partition->writer && partition->unflushed_bytes > 0) {
      IOOptions io_opts;
      Status s = WritableFileWriter::PrepareIOOptions(write_options, io_opts);
      if (s.ok()) {
        s = partition->writer->file()->Flush(io_opts);
      }
      if (!s.ok()) {
        return s;
      }
      partition->unflushed_bytes = 0;
    }
  }
  return Status::OK();
}

Status BlobFilePartitionManager::SyncAllOpenFiles(
    const WriteOptions& write_options) {
  if (buffer_size_ > 0) {
    for (auto& partition : partitions_) {
      SubmitFlush(partition.get());
    }
    DrainBackgroundWork();

    MutexLock lock(&bg_mutex_);
    if (!bg_status_.ok()) {
      return bg_status_;
    }
  }

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    if (partition->writer) {
      if (partition->unflushed_bytes > 0) {
        IOOptions io_opts;
        Status s = WritableFileWriter::PrepareIOOptions(write_options, io_opts);
        if (s.ok()) {
          s = partition->writer->file()->Flush(io_opts);
        }
        if (!s.ok()) {
          return s;
        }
        partition->unflushed_bytes = 0;
      }
      Status s = partition->writer->Sync(write_options);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return Status::OK();
}

void BlobFilePartitionManager::DumpTimingStats() const {}

Status BlobFilePartitionManager::ResolveBlobDirectWriteIndex(
    const ReadOptions& read_options, const Slice& user_key,
    const BlobIndex& blob_idx, const Version* version,
    BlobFileCache* blob_file_cache, BlobFilePartitionManager* partition_mgr,
    PinnableSlice* blob_value) {
  // Tier 1: Check unflushed pending records (deferred flush mode).
  if (partition_mgr) {
    std::string pending_value;
    if (partition_mgr->GetPendingBlobValue(blob_idx.file_number(),
                                           blob_idx.offset(), &pending_value)) {
      blob_value->PinSelf(std::move(pending_value));
      return Status::OK();
    }
  }

  // Tier 2: Standard version-based blob read.
  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr uint64_t* bytes_read = nullptr;
  Status s = version->GetBlob(read_options, user_key, blob_idx, prefetch_buffer,
                              blob_value, bytes_read);

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
        // Cached reader may have stale file_size_. Evict and retry.
        reader.Reset();
        blob_file_cache->Evict(blob_idx.file_number());
        s = blob_file_cache->GetBlobFileReader(
            read_options, blob_idx.file_number(), &reader,
            /*allow_footer_skip_retry=*/true);
        if (s.ok()) {
          s = reader.GetValue()->GetBlob(
              read_options, user_key, blob_idx.offset(), blob_idx.size(),
              blob_idx.compression(), prefetch_buffer, nullptr, &blob_contents,
              bytes_read);
          if (s.ok()) {
            blob_value->PinSelf(blob_contents->data());
          }
        }
      }
    }
  }

  // Tier 4: Retry pending records. The BG thread may have picked up
  // the records between our tier-1 check and the file read. Retry on
  // any error (not just Corruption) to handle transient IOError, NotFound,
  // etc. when the data still exists in memory.
  if (!s.ok() && partition_mgr) {
    std::string pending_value;
    if (partition_mgr->GetPendingBlobValue(blob_idx.file_number(),
                                           blob_idx.offset(), &pending_value)) {
      blob_value->PinSelf(std::move(pending_value));
      return Status::OK();
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
