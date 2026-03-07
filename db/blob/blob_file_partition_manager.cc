//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_partition_manager.h"

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_writer.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

BlobFilePartitionManager::Partition::Partition() : pending_cv(&mutex) {}
BlobFilePartitionManager::Partition::~Partition() = default;

BlobFilePartitionManager::BlobFilePartitionManager(
    uint32_t num_partitions,
    std::shared_ptr<BlobFilePartitionStrategy> strategy,
    FileNumberAllocator file_number_allocator, FileSystem* fs,
    SystemClock* clock, Statistics* statistics, const FileOptions& file_options,
    const std::string& db_path, uint64_t blob_file_size, bool use_fsync,
    uint64_t buffer_size, bool use_direct_io)
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
      bg_cv_(&bg_mutex_),
      bg_drain_cv_(&bg_mutex_) {
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

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, file_options_, clock_,
      nullptr /* io_tracer */, statistics_,
      Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS));

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
    std::vector<PendingRecord> records =
        std::move(partition->pending_records);
    partition->pending_records.clear();
    BlobLogWriter* writer = partition->writer.get();

    for (auto& record : records) {
      uint64_t key_offset = 0;
      uint64_t actual_blob_offset = 0;
      WriteOptions wo;
      Status s = writer->AddRecord(wo, Slice(record.key), Slice(record.value),
                                   &key_offset, &actual_blob_offset);
      if (!s.ok()) {
        return s;
      }
      assert(actual_blob_offset == record.blob_offset);

      const uint64_t record_bytes = BlobLogRecord::kHeaderSize +
                                    record.key.size() + record.value.size();
      partition->pending_bytes.fetch_sub(record_bytes,
                                         std::memory_order_relaxed);
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
      partition->total_blob_bytes, std::move(checksum_method),
      std::move(checksum_value));

  partition->writer.reset();
  partition->file_number = 0;
  partition->file_size = 0;
  partition->blob_count = 0;
  partition->total_blob_bytes = 0;
  partition->next_write_offset = 0;

  return Status::OK();
}

Status BlobFilePartitionManager::PrepareFileRollover(
    Partition* partition, uint32_t column_family_id,
    CompressionType compression, DeferredSeal* deferred) {
  assert(partition);
  assert(partition->writer);
  assert(deferred);

  // Capture old file state under the mutex.
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

  // Flush pending records to the old writer (outside the mutex).
  BlobLogWriter* writer = deferred->writer.get();

  for (auto& record : deferred->records) {
    uint64_t key_offset = 0;
    uint64_t actual_blob_offset = 0;
    WriteOptions wo;
    Status s = writer->AddRecord(wo, Slice(record.key), Slice(record.value),
                                 &key_offset, &actual_blob_offset);
    if (!s.ok()) {
      return s;
    }
    assert(actual_blob_offset == record.blob_offset);

    const uint64_t record_bytes =
        BlobLogRecord::kHeaderSize + record.key.size() + record.value.size();
    partition->pending_bytes.fetch_sub(record_bytes,
                                       std::memory_order_relaxed);
    partition->pending_cv.SignalAll();
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
    return s;
  }

  // Record completion (re-acquire mutex briefly).
  {
    MutexLock lock(&partition->mutex);
    partition->completed_files.emplace_back(
        deferred->file_number, deferred->blob_count,
        deferred->total_blob_bytes, std::move(checksum_method),
        std::move(checksum_value));
  }

  deferred->writer.reset();
  return Status::OK();
}

void BlobFilePartitionManager::SubmitSeal(Partition* partition,
                                          DeferredSeal&& seal) {
  MutexLock lock(&bg_mutex_);
  BGWorkItem item;
  item.type = BGWorkItem::kSeal;
  item.seal = std::move(seal);
  partition->bg_queue.emplace_back(std::move(item));
  bg_cv_.SignalAll();
}

void BlobFilePartitionManager::SubmitFlush(Partition* partition) {
  MutexLock lock(&bg_mutex_);
  // Avoid duplicate flush requests for the same partition.
  for (const auto& item : partition->bg_queue) {
    if (item.type == BGWorkItem::kFlush) {
      return;
    }
  }
  BGWorkItem item;
  item.type = BGWorkItem::kFlush;
  partition->bg_queue.emplace_back(std::move(item));
  bg_cv_.SignalAll();
}

void BlobFilePartitionManager::DrainBackgroundWork() {
  MutexLock lock(&bg_mutex_);
  while (true) {
    bool any_work = false;
    for (auto& partition : partitions_) {
      if (!partition->bg_queue.empty() || partition->bg_in_flight) {
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
          bool any_work = false;
          for (auto& p : partitions_) {
            if (!p->bg_queue.empty() && !p->bg_in_flight) {
              any_work = true;
              break;
            }
          }
          if (!any_work) {
            return;
          }
        }

        for (auto& p : partitions_) {
          if (!p->bg_queue.empty() && !p->bg_in_flight) {
            partition = p.get();
            break;
          }
        }
        if (partition) {
          break;
        }
        bg_cv_.Wait();
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
        s = FlushPendingRecords(partition);
      }

      if (!s.ok()) {
        MutexLock lock(&bg_mutex_);
        if (bg_status_.ok()) {
          bg_status_ = s;
        }
      }
    }

    // Mark partition as no longer in-flight and check for drain.
    {
      MutexLock lock(&bg_mutex_);
      partition->bg_in_flight = false;

      // If more work arrived for this partition while we were processing,
      // wake up threads to handle it.
      if (!partition->bg_queue.empty()) {
        bg_cv_.SignalAll();
      }

      // Check if all work is done (for drain waiters).
      bool any_work = false;
      for (auto& p : partitions_) {
        if (!p->bg_queue.empty() || p->bg_in_flight) {
          any_work = true;
          break;
        }
      }
      if (!any_work) {
        bg_drain_cv_.SignalAll();
      }
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

  // Move pre-copied strings into pending record (no memcpy under mutex).
  partition->pending_records.push_back(
      {std::move(key_copy_), std::move(value_copy_), partition->file_number, *blob_offset});
  partition->pending_bytes.fetch_add(record_size, std::memory_order_relaxed);

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


bool BlobFilePartitionManager::GetPendingBlobValue(
    uint64_t file_number, uint64_t offset, std::string* value) const {
  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    for (const auto& record : partition->pending_records) {
      if (record.file_number == file_number && record.blob_offset == offset) {
        *value = record.value;
        return true;
      }
    }
  }
  return false;
}

Status BlobFilePartitionManager::WriteBlob(
    const WriteOptions& /*write_options*/, uint32_t column_family_id,
    CompressionType compression, const Slice& key, const Slice& value,
    uint64_t* blob_file_number, uint64_t* blob_offset, uint64_t* blob_size) {
  assert(blob_file_number);
  assert(blob_offset);
  assert(blob_size);

  const uint32_t partition_idx =
      strategy_->SelectPartition(num_partitions_, column_family_id, key, value);
  assert(partition_idx < num_partitions_);

  Partition* partition = partitions_[partition_idx].get();

  // Backpressure: stall if pending bytes exceed buffer_size.
  // Submit flush to background thread and wait, never flush inline.
  if (buffer_size_ > 0) {
    if (partition->pending_bytes.load(std::memory_order_relaxed) >=
        buffer_size_) {
      SubmitFlush(partition);
      MutexLock lock(&partition->mutex);
      while (partition->pending_bytes.load(std::memory_order_relaxed) >=
             buffer_size_) {
        partition->pending_cv.Wait();
      }
    }
  }

  bool need_flush = false;
  DeferredSeal deferred_seal;
  // Pre-copy key and value OUTSIDE the mutex to minimize critical section.
  std::string key_copy, value_copy;
  if (buffer_size_ > 0) {
    key_copy.assign(key.data(), key.size());
    value_copy.assign(value.data(), value.size());
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
      s = WriteBlobDeferred(partition, key, value, blob_offset,
                            std::move(key_copy), std::move(value_copy));
    } else {
      s = WriteBlobSync(partition, key, value, blob_offset);
    }
    if (!s.ok()) {
      return s;
    }

    *blob_file_number = partition->file_number;
    *blob_size = value.size();

    partition->blob_count++;
    const uint64_t record_size =
        BlobLogRecord::kHeaderSize + key.size() + value.size();
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

    if (buffer_size_ > 0 && high_water_mark_ > 0 &&
        partition->pending_bytes.load(std::memory_order_relaxed) >=
            high_water_mark_) {
      need_flush = true;
    }
  }  // mutex released

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
  std::vector<PendingRecord> records;
  BlobLogWriter* writer = nullptr;
  {
    MutexLock lock(&partition->mutex);
    if (partition->pending_records.empty()) {
      return Status::OK();
    }
    records = std::move(partition->pending_records);
    partition->pending_records.clear();
    writer = partition->writer.get();
  }

  if (!writer) {
    return Status::OK();
  }

  for (auto& record : records) {
    uint64_t key_offset = 0;
    uint64_t actual_blob_offset = 0;
    WriteOptions wo;

    Status s = writer->AddRecord(wo, Slice(record.key), Slice(record.value), &key_offset,
                                 &actual_blob_offset);
    if (!s.ok()) {
      partition->pending_cv.SignalAll();
      return s;
    }
    assert(actual_blob_offset == record.blob_offset);

    const uint64_t record_bytes = BlobLogRecord::kHeaderSize +
                                  record.key.size() + record.value.size();

    partition->pending_bytes.fetch_sub(record_bytes,
                                      std::memory_order_relaxed);
    partition->pending_cv.SignalAll();
  }

  // Flush to OS (single write() syscall for entire batch).
  {
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

  return Status::OK();
}

Status BlobFilePartitionManager::SealAllPartitions(
    const WriteOptions& /*write_options*/,
    std::vector<BlobFileAddition>* additions) {
  assert(additions);

  // Drain all pending background work (seals and flushes) before sealing.
  if (buffer_size_ > 0) {
    DrainBackgroundWork();

    // Check for background errors.
    {
      MutexLock lock(&bg_mutex_);
      if (!bg_status_.ok()) {
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

  // Phase 2: Seal all captured files outside any mutex.
  for (auto& [partition, seal] : seals) {
    BlobLogWriter* writer = seal.writer.get();

    // Flush pending records.
    for (auto& record : seal.records) {
      uint64_t key_offset = 0;
      uint64_t actual_blob_offset = 0;
      WriteOptions wo;
      Status s = writer->AddRecord(wo, Slice(record.key), Slice(record.value),
                                   &key_offset, &actual_blob_offset);
      if (!s.ok()) {
        return s;
      }
      assert(actual_blob_offset == record.blob_offset);

      const uint64_t record_bytes = BlobLogRecord::kHeaderSize +
                                    record.key.size() + record.value.size();
      partition->pending_bytes.fetch_sub(record_bytes,
                                         std::memory_order_relaxed);
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
        return s;
      }
    }

    // Write footer.
    BlobLogFooter footer;
    footer.blob_count = seal.blob_count;

    std::string checksum_method;
    std::string checksum_value;
    WriteOptions wo;
    Status s = writer->AppendFooter(wo, footer, &checksum_method,
                                    &checksum_value);
    if (!s.ok()) {
      return s;
    }

    additions->emplace_back(seal.file_number, seal.blob_count,
                            seal.total_blob_bytes, std::move(checksum_method),
                            std::move(checksum_value));
    seal.writer.reset();
  }

  return Status::OK();
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
      Status s = FlushPendingRecords(partition.get());
      if (!s.ok()) {
        return s;
      }
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
      Status s = FlushPendingRecords(partition.get());
      if (!s.ok()) {
        return s;
      }
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

}  // namespace ROCKSDB_NAMESPACE
