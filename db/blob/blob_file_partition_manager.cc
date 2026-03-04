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
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

BlobFilePartitionManager::BlobFilePartitionManager(
    uint32_t num_partitions,
    std::shared_ptr<BlobFilePartitionStrategy> strategy,
    FileNumberAllocator file_number_allocator,
    FileSystem* fs,
    SystemClock* clock,
    Statistics* statistics,
    const FileOptions& file_options,
    const std::string& db_path,
    uint64_t blob_file_size,
    bool use_fsync)
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
      use_fsync_(use_fsync) {
  assert(num_partitions_ > 0);
  assert(file_number_allocator_);
  assert(fs_);

  partitions_.reserve(num_partitions_);
  for (uint32_t i = 0; i < num_partitions_; ++i) {
    partitions_.emplace_back(std::make_unique<Partition>());
  }
}

BlobFilePartitionManager::~BlobFilePartitionManager() = default;

Status BlobFilePartitionManager::OpenNewBlobFile(
    Partition* partition, uint32_t column_family_id,
    CompressionType compression) {
  assert(partition);
  assert(!partition->writer);

  const uint64_t blob_file_number = file_number_allocator_();
  const std::string blob_file_path =
      BlobFileName(db_path_, blob_file_number);

  std::unique_ptr<FSWritableFile> file;
  const Status s =
      NewWritableFile(fs_, blob_file_path, &file, file_options_);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, file_options_, clock_,
      nullptr /* io_tracer */, statistics_,
      Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS));

  constexpr bool do_flush = true;

  auto blob_log_writer = std::make_unique<BlobLogWriter>(
      std::move(file_writer), clock_, statistics_, blob_file_number,
      use_fsync_, do_flush);

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

  return Status::OK();
}

Status BlobFilePartitionManager::CloseBlobFile(Partition* partition) {
  assert(partition);
  assert(partition->writer);

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

  return Status::OK();
}

Status BlobFilePartitionManager::WriteBlob(
    const WriteOptions& /*write_options*/, uint32_t column_family_id,
    CompressionType compression, const Slice& key, const Slice& value,
    uint64_t* blob_file_number, uint64_t* blob_offset,
    uint64_t* blob_size) {
  assert(blob_file_number);
  assert(blob_offset);
  assert(blob_size);

  const uint32_t partition_idx =
      strategy_->SelectPartition(num_partitions_, column_family_id, key, value);
  assert(partition_idx < num_partitions_);

  Partition* partition = partitions_[partition_idx].get();
  MutexLock lock(&partition->mutex);

  // Open a new blob file if none is open, or if the CF/compression changed.
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

  // Write the blob record.
  uint64_t key_offset = 0;
  WriteOptions wo;
  Status s = partition->writer->AddRecord(wo, key, value, &key_offset,
                                          blob_offset);
  if (!s.ok()) {
    return s;
  }

  *blob_file_number = partition->file_number;
  *blob_size = value.size();

  partition->blob_count++;
  partition->total_blob_bytes +=
      BlobLogRecord::kHeaderSize + key.size() + value.size();
  partition->file_size = partition->total_blob_bytes + BlobLogHeader::kSize;

  // Rotate to a new file if the current one exceeds the size limit.
  if (partition->file_size >= blob_file_size_) {
    s = CloseBlobFile(partition);
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

  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);

    if (partition->writer) {
      Status s = CloseBlobFile(partition.get());
      if (!s.ok()) {
        return s;
      }
    }

    for (auto& addition : partition->completed_files) {
      additions->emplace_back(std::move(addition));
    }
    partition->completed_files.clear();
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

Status BlobFilePartitionManager::SyncAllOpenFiles(
    const WriteOptions& write_options) {
  for (auto& partition : partitions_) {
    MutexLock lock(&partition->mutex);
    if (partition->writer) {
      Status s = partition->writer->Sync(write_options);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
