//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/trace_reader_writer.h"

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReader;
class WritableFileWriter;

// Each trace record on disk is framed as:
//   [4-byte CRC32c of data][4-byte data length][data ...]
// This allows the reader to detect crash-truncated records and return
// Status::Incomplete() instead of Status::Corruption().
static constexpr unsigned int kTraceCRCSize = 4;
static constexpr unsigned int kTraceRecordLengthSize = 4;
static constexpr unsigned int kTraceFrameHeaderSize =
    kTraceCRCSize + kTraceRecordLengthSize;

// FileTraceReader allows reading RocksDB traces from a file.
class FileTraceReader : public TraceReader {
 public:
  explicit FileTraceReader(std::unique_ptr<RandomAccessFileReader>&& reader);
  ~FileTraceReader();

  Status Read(std::string* data) override;
  Status Close() override;
  Status Reset() override;
  void EnableCRCFraming() override { crc_framing_ = true; }

 private:
  Status ReadLegacy(std::string* data);
  Status ReadCRC(std::string* data);
  Status ReadData(unsigned int bytes_to_read, std::string* data);
  void DetectCRCFromHeader(const std::string& header_data);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  Slice result_;
  size_t offset_;
  char* const buffer_;
  bool crc_framing_ = false;
  bool first_read_done_ = false;

  static const unsigned int kBufferSize;
};

// FileTraceWriter allows writing RocksDB traces to a file.
class FileTraceWriter : public TraceWriter {
 public:
  explicit FileTraceWriter(std::unique_ptr<WritableFileWriter>&& file_writer);
  ~FileTraceWriter();

  Status Write(const Slice& data) override;
  Status Close() override;
  uint64_t GetFileSize() override;
  void EnableCRCFraming() override { crc_framing_ = true; }

 private:
  std::unique_ptr<WritableFileWriter> file_writer_;
  bool crc_framing_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
