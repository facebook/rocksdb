//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Record format definitions for partitioned WAL files.

#pragma once

#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {
namespace log {

// CompletionRecord is written to the main WAL to indicate that a write batch
// body has been successfully written to a partitioned WAL file. This allows
// the main WAL to be a lightweight index of completed writes while the actual
// write batch data resides in partitioned WAL files.
//
// Total serialized size: 52 bytes (fixed-size for efficient parsing)
//
// Layout:
//   partition_wal_number  (8 bytes) - Partitioned WAL file number
//   partition_offset      (8 bytes) - Offset in partitioned WAL
//   body_size             (8 bytes) - Size of write batch body
//   body_crc              (4 bytes) - CRC of write batch body
//   record_count          (4 bytes) - Number of key-value pairs
//   first_sequence        (8 bytes) - First sequence number
//   last_sequence         (8 bytes) - Last sequence number
//   column_family_id      (4 bytes) - Column family ID
struct CompletionRecord {
  uint64_t partition_wal_number;  // Partitioned WAL file number
  uint64_t partition_offset;      // Offset in partitioned WAL
  uint64_t body_size;             // Size of write batch body
  uint32_t body_crc;              // CRC of write batch body
  uint32_t record_count;          // Number of key-value pairs
  SequenceNumber first_sequence;  // First sequence number
  SequenceNumber last_sequence;   // Last sequence number
  uint32_t column_family_id;      // Column family ID

  // Size of the serialized CompletionRecord in bytes
  static constexpr size_t kEncodedSize = 8 + 8 + 8 + 4 + 4 + 8 + 8 + 4;

  CompletionRecord()
      : partition_wal_number(0),
        partition_offset(0),
        body_size(0),
        body_crc(0),
        record_count(0),
        first_sequence(0),
        last_sequence(0),
        column_family_id(0) {}

  CompletionRecord(uint64_t wal_number, uint64_t offset, uint64_t size,
                   uint32_t crc, uint32_t count, SequenceNumber first_seq,
                   SequenceNumber last_seq, uint32_t cf_id)
      : partition_wal_number(wal_number),
        partition_offset(offset),
        body_size(size),
        body_crc(crc),
        record_count(count),
        first_sequence(first_seq),
        last_sequence(last_seq),
        column_family_id(cf_id) {}

  // Encode the CompletionRecord to a string for writing to WAL.
  // The encoding uses fixed-size fields for efficient parsing.
  void EncodeTo(std::string* dst) const {
    // Reserve space for the entire record
    size_t original_size = dst->size();
    dst->reserve(original_size + kEncodedSize);

    PutFixed64(dst, partition_wal_number);
    PutFixed64(dst, partition_offset);
    PutFixed64(dst, body_size);
    PutFixed32(dst, body_crc);
    PutFixed32(dst, record_count);
    PutFixed64(dst, first_sequence);
    PutFixed64(dst, last_sequence);
    PutFixed32(dst, column_family_id);
  }

  // Decode a CompletionRecord from a Slice.
  // Returns Status::OK() on success, or Status::Corruption() on failure.
  // The input slice is advanced past the decoded data on success.
  Status DecodeFrom(Slice* input) {
    if (input->size() < kEncodedSize) {
      return Status::Corruption("CompletionRecord",
                                "Input too short for CompletionRecord");
    }

    if (!GetFixed64(input, &partition_wal_number)) {
      return Status::Corruption("CompletionRecord",
                                "Error decoding partition_wal_number");
    }
    if (!GetFixed64(input, &partition_offset)) {
      return Status::Corruption("CompletionRecord",
                                "Error decoding partition_offset");
    }
    if (!GetFixed64(input, &body_size)) {
      return Status::Corruption("CompletionRecord", "Error decoding body_size");
    }
    if (!GetFixed32(input, &body_crc)) {
      return Status::Corruption("CompletionRecord", "Error decoding body_crc");
    }
    if (!GetFixed32(input, &record_count)) {
      return Status::Corruption("CompletionRecord",
                                "Error decoding record_count");
    }
    if (!GetFixed64(input, &first_sequence)) {
      return Status::Corruption("CompletionRecord",
                                "Error decoding first_sequence");
    }
    if (!GetFixed64(input, &last_sequence)) {
      return Status::Corruption("CompletionRecord",
                                "Error decoding last_sequence");
    }
    if (!GetFixed32(input, &column_family_id)) {
      return Status::Corruption("CompletionRecord",
                                "Error decoding column_family_id");
    }

    return Status::OK();
  }

  // Compute the CRC32C of the given data and compare it with body_crc.
  // Returns true if the CRC matches, false otherwise.
  bool ValidateBodyCRC(const Slice& body_data) const {
    uint32_t actual_crc = crc32c::Value(body_data.data(), body_data.size());
    return actual_crc == body_crc;
  }

  // Compute the CRC32C of the given data and store it in body_crc.
  void ComputeBodyCRC(const Slice& body_data) {
    body_crc = crc32c::Value(body_data.data(), body_data.size());
  }

  // Returns a debug string representation of this record.
  std::string DebugString() const {
    std::string result;
    result.append("CompletionRecord{");
    result.append("partition_wal_number=");
    result.append(std::to_string(partition_wal_number));
    result.append(", partition_offset=");
    result.append(std::to_string(partition_offset));
    result.append(", body_size=");
    result.append(std::to_string(body_size));
    result.append(", body_crc=");
    result.append(std::to_string(body_crc));
    result.append(", record_count=");
    result.append(std::to_string(record_count));
    result.append(", first_sequence=");
    result.append(std::to_string(first_sequence));
    result.append(", last_sequence=");
    result.append(std::to_string(last_sequence));
    result.append(", column_family_id=");
    result.append(std::to_string(column_family_id));
    result.append("}");
    return result;
  }

  // Equality operator for testing
  bool operator==(const CompletionRecord& other) const {
    return partition_wal_number == other.partition_wal_number &&
           partition_offset == other.partition_offset &&
           body_size == other.body_size && body_crc == other.body_crc &&
           record_count == other.record_count &&
           first_sequence == other.first_sequence &&
           last_sequence == other.last_sequence &&
           column_family_id == other.column_family_id;
  }

  bool operator!=(const CompletionRecord& other) const {
    return !(*this == other);
  }
};

static_assert(CompletionRecord::kEncodedSize == 52,
              "CompletionRecord encoded size must be 52 bytes");

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
