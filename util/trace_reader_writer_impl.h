// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include "rocksdb/trace_reader_writer.h"
#include "util/file_reader_writer.h"
#include "util/tracer_replayer.h"

namespace rocksdb {

class TraceWriterImpl : public TraceWriter {
 public:
  TraceWriterImpl(unique_ptr<WritableFileWriter>&& dest);
  ~TraceWriterImpl() {}

  virtual Status AddRecord(const std::string& key,
                           const std::string& value) override;
  virtual void Close() override;

 private:
  std::unique_ptr<WritableFileWriter> writer_;
};

class TraceReaderImpl : public TraceReader {
 public:
  static const unsigned int kBufferSize;
  static const unsigned int kHeaderSize;
  static const unsigned int kLastRecordSize;

  TraceReaderImpl(unique_ptr<RandomAccessFileReader>&& dest,
                  uint64_t file_size);
  ~TraceReaderImpl() {}
  virtual Status ReadRecord(std::string* key, std::string* value) override;

  virtual Status ReadLastRecord(std::string* key, std::string* value) override;

  virtual void Close() override;

 private:
  unique_ptr<RandomAccessFileReader> reader_;
  uint64_t file_size_;
  size_t offset_;
  Slice result_;
  char* const header_buffer_;
  char* const buffer_;
};

}  // namespace rocksdb
