//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Writer implementation for partitioned WAL files.

#include "db/partitioned_log_writer.h"

#include "file/writable_file_writer.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::log {

PartitionedLogWriter::PartitionedLogWriter(
    std::unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
    uint32_t partition_id, bool recycle_log_files)
    : dest_(std::move(dest)),
      log_number_(log_number),
      partition_id_(partition_id),
      recycle_log_files_(recycle_log_files) {
  // Pre-compute CRC for record types
  for (uint8_t i = 0; i <= kPartitionedRecordTypeFull; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

PartitionedLogWriter::~PartitionedLogWriter() {
  if (dest_) {
    IOOptions io_opts;
    dest_->Close(io_opts).PermitUncheckedError();
  }
}

IOStatus PartitionedLogWriter::WriteBody(const WriteOptions& opts,
                                         const Slice& body,
                                         uint32_t record_count,
                                         WriteResult* result) {
  IOOptions io_opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(opts, io_opts);
  if (!s.ok()) {
    return s;
  }

  uint64_t record_offset = 0;
  uint32_t body_crc = 0;

  {
    std::lock_guard<std::mutex> lock(mutex_);

    s = EmitPhysicalRecord(io_opts, kPartitionedRecordTypeFull, body.data(),
                           body.size(), &record_offset, &body_crc);
    if (!s.ok()) {
      return s;
    }

    // Flush the buffer to ensure data is written
    s = dest_->Flush(io_opts);
    if (!s.ok()) {
      return s;
    }
  }

  // Fill in the result
  if (result != nullptr) {
    result->wal_number = log_number_;
    result->partition_id = partition_id_;
    result->offset = record_offset;
    result->size = kPartitionedHeaderSize + body.size();
    result->crc = body_crc;
    result->record_count = record_count;
  }

  return IOStatus::OK();
}

IOStatus PartitionedLogWriter::Sync(const WriteOptions& opts) {
  IOOptions io_opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(opts, io_opts);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  return dest_->Sync(io_opts, /*use_fsync=*/false);
}

IOStatus PartitionedLogWriter::Close(const WriteOptions& opts) {
  IOOptions io_opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(opts, io_opts);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (dest_) {
    s = dest_->Close(io_opts);
    dest_.reset();
  }
  return s;
}

uint64_t PartitionedLogWriter::GetFileSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (dest_) {
    return dest_->GetFileSize();
  }
  return 0;
}

IOStatus PartitionedLogWriter::EmitPhysicalRecord(
    const IOOptions& io_opts, RecordType type, const char* data, size_t length,
    uint64_t* record_offset, uint32_t* body_crc) {
  // Record the offset before writing
  *record_offset = dest_->GetFileSize();

  // Build the header
  // Format: CRC (4B) + Length (4B) + Type (1B)
  char header[kPartitionedHeaderSize];

  // Length field (4 bytes)
  EncodeFixed32(header + 4, static_cast<uint32_t>(length));

  // Type field (1 byte)
  header[8] = static_cast<char>(type);

  // Compute CRC of type + payload
  uint32_t crc = type_crc_[type];
  uint32_t payload_crc = crc32c::Value(data, length);
  crc = crc32c::Crc32cCombine(crc, payload_crc, length);

  // Store unmasked CRC for the result
  *body_crc = payload_crc;

  // Mask CRC for storage
  crc = crc32c::Mask(crc);
  EncodeFixed32(header, crc);

  // Write header
  IOStatus s = dest_->Append(io_opts, Slice(header, kPartitionedHeaderSize),
                             /*crc32c_checksum=*/0);
  if (!s.ok()) {
    return s;
  }

  // Write payload
  s = dest_->Append(io_opts, Slice(data, length), payload_crc);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE::log
