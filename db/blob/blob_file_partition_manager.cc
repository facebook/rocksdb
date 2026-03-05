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
    uint64_t buffer_size)
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
      high_water_mark_(buffer_size_ > 0 ? buffer_size_ * 3 / 4 : 0) {
  assert(num_partitions_ > 0);
  assert(file_number_allocator_);
  assert(fs_);

  partitions_.reserve(num_partitions_);
  for (uint32_t i = 0; i < num_partitions_; ++i) {
    partitions_.emplace_back(std::make_unique<Partition>());
  }
}

BlobFilePartitionManager::~BlobFilePartitionManager() { DumpTimingStats(); }

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
  if (buffer_size_ > 0 && !partition->pending_records.empty()) {
    partition->mutex.Unlock();
    Status s = FlushPendingRecords(partition);
    partition->mutex.Lock();
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

Status BlobFilePartitionManager::WriteBlobDeferred(
    Partition* partition, const Slice& key, const Slice& value,
    uint64_t* blob_offset, uint64_t batch_id) {
  assert(partition);
  assert(buffer_size_ > 0);

  const uint64_t t_start = clock_ ? clock_->NowNanos() : 0;

  // Pre-calculate the offset where this value will be written.
  *blob_offset =
      partition->next_write_offset + BlobLogRecord::kHeaderSize + key.size();
  const uint64_t record_size =
      BlobLogRecord::kHeaderSize + key.size() + value.size();
  partition->next_write_offset += record_size;

  const uint64_t t_offset = clock_ ? clock_->NowNanos() : 0;

  // Store zero-copy Slice references into the WriteBatch buffer.
  // current_rep_owner_ holds shared ownership set by AdoptBatchBuffer().
  partition->pending_records.push_back(
      {key, value, {}, partition->file_number, *blob_offset, batch_id});
  partition->pending_bytes.fetch_add(record_size, std::memory_order_relaxed);

  const uint64_t t_end = clock_ ? clock_->NowNanos() : 0;

  if (clock_) {
    deferred_offset_nanos_.fetch_add(t_offset - t_start,
                                     std::memory_order_relaxed);
    deferred_list_nanos_.fetch_add(t_end - t_offset,
                                   std::memory_order_relaxed);
    deferred_count_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status BlobFilePartitionManager::WriteBlobSync(
    Partition* partition, const Slice& key, const Slice& value,
    uint64_t* blob_offset) {
  assert(partition);

  const uint64_t t_start = clock_ ? clock_->NowNanos() : 0;

  uint64_t key_offset = 0;
  WriteOptions wo;
  Status s =
      partition->writer->AddRecord(wo, key, value, &key_offset, blob_offset);
  if (!s.ok()) {
    return s;
  }

  if (clock_) {
    sync_addrecord_nanos_.fetch_add(clock_->NowNanos() - t_start,
                                    std::memory_order_relaxed);
    sync_count_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}


bool BlobFilePartitionManager::GetPendingBlobValue(
    uint64_t file_number, uint64_t offset, std::string* value) const {
  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    for (const auto& record : partition->pending_records) {
      if (record.file_number == file_number && record.blob_offset == offset) {
        value->assign(record.value.data(), record.value.size());
        return true;
      }
    }
  }
  return false;
}

uint64_t BlobFilePartitionManager::AllocateBatchId() {
  return next_batch_id_.fetch_add(1, std::memory_order_relaxed) + 1;
}

void BlobFilePartitionManager::AdoptBatchBuffer(
    uint64_t batch_id, std::shared_ptr<std::string> rep_owner) {
  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    for (auto& record : partition->pending_records) {
      if (record.batch_id == batch_id && !record.rep_owner) {
        record.rep_owner = rep_owner;
      }
    }
  }
}

Status BlobFilePartitionManager::WriteBlob(
    const WriteOptions& /*write_options*/, uint32_t column_family_id,
    CompressionType compression, const Slice& key, const Slice& value,
    uint64_t* blob_file_number, uint64_t* blob_offset, uint64_t* blob_size,
    uint64_t batch_id) {
  assert(blob_file_number);
  assert(blob_offset);
  assert(blob_size);

  const uint32_t partition_idx =
      strategy_->SelectPartition(num_partitions_, column_family_id, key, value);
  assert(partition_idx < num_partitions_);

  Partition* partition = partitions_[partition_idx].get();

  // Backpressure: stall if pending bytes exceed buffer_size.
  if (buffer_size_ > 0) {
    if (partition->pending_bytes.load(std::memory_order_relaxed) >=
        buffer_size_) {
      FlushPendingRecords(partition);
      if (partition->pending_bytes.load(std::memory_order_relaxed) >=
          buffer_size_) {
        MutexLock lock(&partition->mutex);
        while (partition->pending_bytes.load(std::memory_order_relaxed) >=
               buffer_size_) {
          partition->pending_cv.Wait();
        }
      }
    }
  }

  bool need_flush = false;
  const uint64_t t_pre_mutex = clock_ ? clock_->NowNanos() : 0;
  {
    MutexLock lock(&partition->mutex);
    if (clock_) {
      mutex_wait_nanos_.fetch_add(clock_->NowNanos() - t_pre_mutex,
                                  std::memory_order_relaxed);
    }

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
      s = WriteBlobDeferred(partition, key, value, blob_offset, batch_id);
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
      s = CloseBlobFile(partition);
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

  if (need_flush) {
    const uint64_t t_flush_start = clock_ ? clock_->NowNanos() : 0;
    Status s = FlushPendingRecords(partition);
    if (clock_) {
      flush_nanos_.fetch_add(clock_->NowNanos() - t_flush_start,
                             std::memory_order_relaxed);
      flush_count_.fetch_add(1, std::memory_order_relaxed);
    }
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status BlobFilePartitionManager::FlushPendingRecords(Partition* partition) {
  assert(partition);

  std::list<PendingRecord> records;
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

  const uint64_t t_start = clock_ ? clock_->NowNanos() : 0;

  for (auto& record : records) {
    uint64_t key_offset = 0;
    uint64_t actual_blob_offset = 0;
    WriteOptions wo;

    Status s = writer->AddRecord(wo, record.key, record.value, &key_offset,
                                 &actual_blob_offset);
    if (!s.ok()) {
      partition->pending_cv.SignalAll();
      return s;
    }
    assert(actual_blob_offset == record.blob_offset);

    const uint64_t record_bytes = BlobLogRecord::kHeaderSize +
                                  record.key.size() + record.value.size();

    // Release shared ref to WB buffer.
    record.rep_owner.reset();

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

  if (clock_) {
    flush_nanos_.fetch_add(clock_->NowNanos() - t_start,
                           std::memory_order_relaxed);
    flush_count_.fetch_add(1, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status BlobFilePartitionManager::SealAllPartitions(
    const WriteOptions& /*write_options*/,
    std::vector<BlobFileAddition>* additions) {
  assert(additions);

  if (buffer_size_ > 0) {
    for (auto& partition : partitions_) {
      Status s = FlushPendingRecords(partition.get());
      if (!s.ok()) {
        return s;
      }
    }
  }

  struct PendingSeal {
    std::unique_ptr<BlobLogWriter> writer;
    uint64_t file_number;
    uint64_t blob_count;
    uint64_t total_blob_bytes;
  };
  std::vector<PendingSeal> pending;

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);

    if (partition->writer) {
      PendingSeal seal;
      seal.writer = std::move(partition->writer);
      seal.file_number = partition->file_number;
      seal.blob_count = partition->blob_count;
      seal.total_blob_bytes = partition->total_blob_bytes;
      pending.emplace_back(std::move(seal));

      partition->file_number = 0;
      partition->file_size = 0;
      partition->blob_count = 0;
      partition->total_blob_bytes = 0;
      partition->next_write_offset = 0;
    }

    for (auto& addition : partition->completed_files) {
      additions->emplace_back(std::move(addition));
    }
    partition->completed_files.clear();
  }

  for (auto& seal : pending) {
    {
      IOOptions io_opts;
      WriteOptions wo;
      Status s = WritableFileWriter::PrepareIOOptions(wo, io_opts);
      if (s.ok()) {
        s = seal.writer->file()->Flush(io_opts);
      }
      if (!s.ok()) {
        return s;
      }
    }

    BlobLogFooter footer;
    footer.blob_count = seal.blob_count;

    std::string checksum_method;
    std::string checksum_value;

    WriteOptions wo;
    Status s = seal.writer->AppendFooter(wo, footer, &checksum_method,
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

void BlobFilePartitionManager::DumpTimingStats() const {
  uint64_t dc = deferred_count_.load(std::memory_order_relaxed);
  uint64_t sc = sync_count_.load(std::memory_order_relaxed);
  uint64_t total = dc + sc;
  if (dc > 0) {
    fprintf(stderr,
            "=== Blob Direct Write Timing (DEFERRED mode, %lu records) ===\n"
            "  offset_calc:  %7lu ns/op\n"
            "  list_append:  %7lu ns/op  (zero-copy Slice + shared_ptr ref)\n"
            "  mutex_wait:   %7lu ns/op\n"
            "  TOTAL under mutex: %7lu ns/op\n",
            (unsigned long)dc,
            (unsigned long)(deferred_offset_nanos_.load(
                                std::memory_order_relaxed) /
                            dc),
            (unsigned long)(deferred_list_nanos_.load(
                                std::memory_order_relaxed) /
                            dc),
            (unsigned long)(mutex_wait_nanos_.load(std::memory_order_relaxed) /
                            (total > 0 ? total : 1)),
            (unsigned long)((deferred_offset_nanos_.load(
                                 std::memory_order_relaxed) +
                             deferred_list_nanos_.load(
                                 std::memory_order_relaxed)) /
                            dc));
    uint64_t fc = flush_count_.load(std::memory_order_relaxed);
    if (fc > 0) {
      fprintf(
          stderr,
          "  flush:        %7lu ns/flush  (%lu flushes, %lu records/flush)\n",
          (unsigned long)(flush_nanos_.load(std::memory_order_relaxed) / fc),
          (unsigned long)fc, (unsigned long)(dc / fc));
    }
    fprintf(stderr, "\n");
  }
  if (sc > 0) {
    fprintf(stderr,
            "=== Blob Direct Write Timing (SYNC mode, %lu records) ===\n"
            "  AddRecord:    %7lu ns/op\n"
            "  mutex_wait:   %7lu ns/op\n\n",
            (unsigned long)sc,
            (unsigned long)(sync_addrecord_nanos_.load(
                                std::memory_order_relaxed) /
                            sc),
            (unsigned long)(mutex_wait_nanos_.load(std::memory_order_relaxed) /
                            (total > 0 ? total : 1)));
  }
}

}  // namespace ROCKSDB_NAMESPACE
