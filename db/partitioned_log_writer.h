//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Writer class for partitioned WAL files.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>

#include "file/writable_file_writer.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;

namespace log {

// PartitionedLogWriter writes write batch bodies to partitioned WAL files.
// Unlike the regular log::Writer, this class:
// - Writes only the body portion of WriteBatch (no sequence numbers)
// - Uses a simpler record format without block-based fragmentation
// - Is thread-safe for concurrent writes from multiple threads
//
// Record format for partitioned WAL:
//   +--------+--------+------+--- ... ---+
//   |CRC (4B)| Len(4B)|Type  | Payload   |
//   +--------+--------+(1B)  |           |
//   +--------+--------+------+--- ... ---+
//
// CRC = 32-bit CRC of type + payload (masked for storage)
// Len = 32-bit length of payload
// Type = Record type (kPartitionedRecordTypeFull for now)
//
// The header is 9 bytes total (4 + 4 + 1).
class PartitionedLogWriter {
 public:
  // Record types for partitioned WAL
  enum RecordType : uint8_t {
    kPartitionedRecordTypeNone = 0,
    kPartitionedRecordTypeFull = 1,
  };

  // Header size: CRC (4) + Length (4) + Type (1) = 9 bytes
  static constexpr int kPartitionedHeaderSize = 9;

  // Result of a write operation
  struct WriteResult {
    uint64_t wal_number;    // WAL file number
    uint32_t partition_id;  // Partition ID within the log
    uint64_t offset;        // Offset where the record starts (before header)
    uint64_t size;          // Total size written (header + body)
    uint32_t crc;           // CRC of the body data (unmasked)
    uint32_t record_count;  // Number of records in the batch

    WriteResult()
        : wal_number(0),
          partition_id(0),
          offset(0),
          size(0),
          crc(0),
          record_count(0) {}

    // Encode wal_number and partition_id into a single 64-bit value.
    // Uses the lower 8 bits for partition_id and upper bits for wal_number.
    // This allows up to 256 partitions per log.
    uint64_t GetEncodedWalNumber() const {
      return (wal_number << 8) | (partition_id & 0xFF);
    }

    // Decode wal_number from an encoded value.
    static uint64_t DecodeWalNumber(uint64_t encoded) { return encoded >> 8; }

    // Decode partition_id from an encoded value.
    static uint32_t DecodePartitionId(uint64_t encoded) {
      return static_cast<uint32_t>(encoded & 0xFF);
    }
  };

  // Create a writer that will append data to the provided WritableFileWriter.
  // The writer takes ownership of the file writer.
  // log_number: the WAL file number for this partitioned WAL
  // partition_id: the partition ID within the log
  // recycle_log_files: whether log file recycling is enabled (for future use)
  explicit PartitionedLogWriter(std::unique_ptr<WritableFileWriter>&& dest,
                                uint64_t log_number, uint32_t partition_id,
                                bool recycle_log_files = false);

  // No copying allowed
  PartitionedLogWriter(const PartitionedLogWriter&) = delete;
  void operator=(const PartitionedLogWriter&) = delete;

  ~PartitionedLogWriter();

  // Write a write batch body to the partitioned WAL.
  // The body should be the WriteBatch data without the 12-byte header
  // (i.e., starting after the sequence number and count fields).
  //
  // Parameters:
  //   opts: Write options for the operation
  //   body: The write batch body data to write
  //   record_count: Number of key-value records in this batch
  //   result: Output parameter filled with write result information
  //
  // Returns IOStatus::OK() on success.
  // Thread-safe: multiple threads can call this concurrently.
  IOStatus WriteBody(const WriteOptions& opts, const Slice& body,
                     uint32_t record_count, WriteResult* result);

  // Sync the WAL file to disk.
  IOStatus Sync(const WriteOptions& opts);

  // Close the WAL file.
  IOStatus Close(const WriteOptions& opts);

  // Get the log number of this partitioned WAL file.
  uint64_t GetLogNumber() const { return log_number_; }

  // Get the partition ID of this partitioned WAL file.
  uint32_t GetPartitionId() const { return partition_id_; }

  // Get the current file size (including buffered data).
  uint64_t GetFileSize() const;

  // Get the underlying file writer (for testing).
  WritableFileWriter* file() { return dest_.get(); }
  const WritableFileWriter* file() const { return dest_.get(); }

 private:
  // Write the record header and body to the file.
  // Must be called with mutex_ held.
  IOStatus EmitPhysicalRecord(const IOOptions& io_opts, RecordType type,
                              const char* data, size_t length,
                              uint64_t* record_offset, uint32_t* body_crc);

  std::unique_ptr<WritableFileWriter> dest_;
  uint64_t log_number_;
  uint32_t partition_id_;
  bool recycle_log_files_;

  // Mutex to protect concurrent writes
  mutable std::mutex mutex_;

  // Pre-computed CRC for record types
  uint32_t type_crc_[2];
};

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
