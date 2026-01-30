//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Reader class for partitioned WAL files.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/partitioned_log_writer.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReader;
class SequentialFileReader;

namespace log {

// PartitionedLogReader reads write batch bodies from partitioned WAL files.
// It provides two modes of reading:
// 1. ReadBodyAt: Random access reading at a specific offset (for recovery
//    using CompletionRecord information)
// 2. ReadNextBody: Sequential reading for scanning all records
//
// Record format for partitioned WAL (same as PartitionedLogWriter):
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
class PartitionedLogReader {
 public:
  // Create a reader for the partitioned WAL file.
  // Takes ownership of both file readers.
  // sequential_file: Used for ReadNextBody (sequential scanning)
  // random_file: Used for ReadBodyAt (random access reads)
  // log_number: The WAL file number for this partitioned WAL
  PartitionedLogReader(std::unique_ptr<SequentialFileReader>&& sequential_file,
                       std::unique_ptr<RandomAccessFileReader>&& random_file,
                       uint64_t log_number);

  // No copying allowed
  PartitionedLogReader(const PartitionedLogReader&) = delete;
  void operator=(const PartitionedLogReader&) = delete;

  ~PartitionedLogReader();

  // Read body at specific offset (for recovery using CompletionRecord).
  // Verifies size and CRC match expectations.
  //
  // Parameters:
  //   offset: The file offset where the record starts (at the header)
  //   size: Expected total size of the record (header + body)
  //   expected_crc: Expected CRC of the body data (unmasked)
  //   body: Output parameter filled with the body data
  //
  // Returns:
  //   Status::OK() on success
  //   Status::Corruption() if CRC mismatch or size mismatch
  //   Other errors on I/O failures
  Status ReadBodyAt(uint64_t offset, uint64_t size, uint32_t expected_crc,
                    std::string* body);

  // Sequential reading for scanning all records.
  //
  // Parameters:
  //   body: Output slice pointing to the body data (valid until next call)
  //   scratch: Scratch buffer to hold the body data
  //   offset: Output parameter set to the file offset of the record
  //   crc: Output parameter set to the CRC of the body (unmasked)
  //
  // Returns:
  //   true if a record was successfully read
  //   false if EOF or error (check GetLastStatus() for details)
  bool ReadNextBody(Slice* body, std::string* scratch, uint64_t* offset,
                    uint32_t* crc);

  // Get the log number of this partitioned WAL file.
  uint64_t GetLogNumber() const { return log_number_; }

  // Get the status of the last ReadNextBody call.
  // Returns OK if no error occurred, or the error status.
  Status GetLastStatus() const { return last_status_; }

  // Check if we've reached end of file during sequential reading.
  bool IsEOF() const { return eof_; }

  // Get the current sequential read position.
  uint64_t GetSequentialPosition() const { return sequential_pos_; }

 private:
  // Read and verify a record header at the current sequential position.
  // Returns true if header was read successfully.
  bool ReadHeader(char* header, uint32_t* length, uint8_t* type,
                  uint32_t* stored_crc);

  std::unique_ptr<SequentialFileReader> sequential_file_;
  std::unique_ptr<RandomAccessFileReader> random_file_;
  uint64_t log_number_;

  // Sequential reading state
  uint64_t sequential_pos_;
  bool eof_;
  Status last_status_;

  // Header buffer for sequential reading
  char header_buf_[PartitionedLogWriter::kPartitionedHeaderSize];
};

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
