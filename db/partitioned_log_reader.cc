//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Reader implementation for partitioned WAL files.

#include "db/partitioned_log_reader.h"

#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::log {

PartitionedLogReader::PartitionedLogReader(
    std::unique_ptr<SequentialFileReader>&& sequential_file,
    std::unique_ptr<RandomAccessFileReader>&& random_file, uint64_t log_number)
    : sequential_file_(std::move(sequential_file)),
      random_file_(std::move(random_file)),
      log_number_(log_number),
      sequential_pos_(0),
      eof_(false),
      last_status_(Status::OK()) {}

PartitionedLogReader::~PartitionedLogReader() = default;

Status PartitionedLogReader::ReadBodyAt(uint64_t offset, uint64_t size,
                                        uint32_t expected_crc,
                                        std::string* body) {
  assert(random_file_);
  assert(body);

  // Verify size is at least header size
  if (size <
      static_cast<uint64_t>(PartitionedLogWriter::kPartitionedHeaderSize)) {
    return Status::Corruption("Record size too small for header");
  }

  const uint64_t body_size =
      size - PartitionedLogWriter::kPartitionedHeaderSize;

  // Read the entire record (header + body)
  std::string record_buf;
  record_buf.resize(static_cast<size_t>(size));

  Slice result;
  IOStatus ios =
      random_file_->Read(IOOptions(), offset, static_cast<size_t>(size),
                         &result, record_buf.data(), nullptr);
  if (!ios.ok()) {
    return ios;
  }

  if (result.size() != size) {
    return Status::Corruption("Short read: expected " + std::to_string(size) +
                              " bytes, got " + std::to_string(result.size()));
  }

  // Parse header
  const char* header = result.data();
  uint32_t stored_crc = DecodeFixed32(header);
  uint32_t length = DecodeFixed32(header + 4);
  uint8_t type = static_cast<uint8_t>(header[8]);

  // Verify length matches expected body size
  if (length != body_size) {
    return Status::Corruption("Length mismatch: header says " +
                              std::to_string(length) + ", expected " +
                              std::to_string(body_size));
  }

  // Verify record type
  if (type != PartitionedLogWriter::kPartitionedRecordTypeFull) {
    return Status::Corruption("Invalid record type: " + std::to_string(type));
  }

  // Extract body
  const char* body_data =
      result.data() + PartitionedLogWriter::kPartitionedHeaderSize;

  // Compute and verify CRC
  // The stored CRC is masked(crc(type + body))
  char t = static_cast<char>(type);
  uint32_t type_crc = crc32c::Value(&t, 1);
  uint32_t body_crc = crc32c::Value(body_data, static_cast<size_t>(body_size));
  uint32_t expected_full_crc =
      crc32c::Crc32cCombine(type_crc, body_crc, static_cast<size_t>(body_size));
  expected_full_crc = crc32c::Mask(expected_full_crc);

  if (stored_crc != expected_full_crc) {
    return Status::Corruption("CRC mismatch in stored record");
  }

  // Verify body CRC matches expected_crc from caller
  if (body_crc != expected_crc) {
    return Status::Corruption("Body CRC mismatch: expected " +
                              std::to_string(expected_crc) + ", got " +
                              std::to_string(body_crc));
  }

  *body = std::string(body_data, static_cast<size_t>(body_size));
  return Status::OK();
}

bool PartitionedLogReader::ReadHeader(char* header, uint32_t* length,
                                      uint8_t* type, uint32_t* stored_crc) {
  assert(sequential_file_);

  Slice result;
  IOStatus ios = sequential_file_->file()->Read(
      PartitionedLogWriter::kPartitionedHeaderSize, IOOptions(), &result,
      header, nullptr);

  if (!ios.ok()) {
    last_status_ = ios;
    return false;
  }

  if (result.size() == 0) {
    // EOF
    eof_ = true;
    last_status_ = Status::OK();
    return false;
  }

  if (result.size() <
      static_cast<size_t>(PartitionedLogWriter::kPartitionedHeaderSize)) {
    // Partial header - truncated file
    last_status_ = Status::Corruption("Truncated record header");
    return false;
  }

  *stored_crc = DecodeFixed32(header);
  *length = DecodeFixed32(header + 4);
  *type = static_cast<uint8_t>(header[8]);

  return true;
}

bool PartitionedLogReader::ReadNextBody(Slice* body, std::string* scratch,
                                        uint64_t* offset, uint32_t* crc) {
  assert(sequential_file_);
  assert(body);
  assert(scratch);

  if (eof_) {
    return false;
  }

  // Record the offset before reading
  *offset = sequential_pos_;

  // Read header
  uint32_t length;
  uint8_t type;
  uint32_t stored_crc;
  if (!ReadHeader(header_buf_, &length, &type, &stored_crc)) {
    return false;
  }

  sequential_pos_ += PartitionedLogWriter::kPartitionedHeaderSize;

  // Verify record type
  if (type != PartitionedLogWriter::kPartitionedRecordTypeFull) {
    last_status_ =
        Status::Corruption("Invalid record type: " + std::to_string(type));
    return false;
  }

  // Read body
  scratch->resize(length);
  Slice result;
  IOStatus ios = sequential_file_->file()->Read(length, IOOptions(), &result,
                                                scratch->data(), nullptr);

  if (!ios.ok()) {
    last_status_ = ios;
    return false;
  }

  if (result.size() != length) {
    last_status_ = Status::Corruption("Short read for body: expected " +
                                      std::to_string(length) + ", got " +
                                      std::to_string(result.size()));
    return false;
  }

  sequential_pos_ += length;

  // Compute CRC and verify
  char t = static_cast<char>(type);
  uint32_t type_crc = crc32c::Value(&t, 1);
  uint32_t body_crc = crc32c::Value(result.data(), result.size());
  uint32_t expected_crc =
      crc32c::Crc32cCombine(type_crc, body_crc, result.size());
  expected_crc = crc32c::Mask(expected_crc);

  if (stored_crc != expected_crc) {
    last_status_ = Status::Corruption("CRC mismatch in record at offset " +
                                      std::to_string(*offset));
    return false;
  }

  *body = result;
  *crc = body_crc;
  last_status_ = Status::OK();
  return true;
}

}  // namespace ROCKSDB_NAMESPACE::log
