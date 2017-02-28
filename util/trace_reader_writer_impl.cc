// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "util/trace_reader_writer_impl.h"
#include "util/coding.h"
#include "util/tracer_replayer.h"

namespace rocksdb {

/*
 * TraceWriterImpl
 */
TraceWriterImpl::TraceWriterImpl(unique_ptr<WritableFileWriter>&& dest)
    : writer_(std::move(dest)) {}

Status TraceWriterImpl::AddRecord(const std::string& key,
                                  const std::string& value) {
  Status s = writer_->Append(Slice(key));
  std::string payload_length;
  PutFixed32(&payload_length, value.size());
  if (s.ok()) {
    s = writer_->Append(Slice(payload_length));
  }
  if (s.ok()) {
    writer_->Append(Slice(value));
  }
  return s;
}

void TraceWriterImpl::Close() {
  return writer_.reset();
}

/*
 * TraceReaderImpl
 */

const unsigned int TraceReaderImpl::kBufferSize = 32 * 1024; // 32KB
const unsigned int TraceReaderImpl::kHeaderSize = Tracer::kKeySize + 4;
const unsigned int TraceReaderImpl::kLastRecordSize =
    TraceReaderImpl::kHeaderSize + 8;

TraceReaderImpl::TraceReaderImpl(unique_ptr<RandomAccessFileReader>&& dest,
                                 uint64_t file_size)
    : reader_(std::move(dest)),
      file_size_(file_size),
      offset_(0),
      header_buffer_(new char[kHeaderSize]),
      buffer_(new char[kBufferSize]) {
  assert(file_size_ >= kLastRecordSize);
}

Status TraceReaderImpl::ReadRecord(std::string* key, std::string* value) {
  Status s = reader_->Read(offset_, kHeaderSize, &result_, header_buffer_);
  if (!s.ok()) {
    return s;
  }
  offset_ += result_.size();
  if (result_.size() < kHeaderSize) {
    if (result_.size() < Tracer::kKeySize) {
      return Status::Corruption("The trace file is corrupted");
    }
  }
  key->clear();
  value->clear();
  key->append(header_buffer_, Tracer::kKeySize);
  uint32_t len = DecodeFixed32(header_buffer_ + Tracer::kKeySize);
  while (len > 0) {
    s = reader_->Read(offset_, std::min(len, kBufferSize), &result_, buffer_);
    if (!s.ok()) {
      break;
    }
    offset_ += result_.size();
    value->append(result_.data(), result_.size());
    len -= result_.size();
  }
  return s;
}

Status TraceReaderImpl::ReadLastRecord(std::string* key, std::string* value) {
  size_t last_record_offset = file_size_ - kLastRecordSize;
  char buffer[kLastRecordSize];
  Slice record;
  Status s =
      reader_->Read(last_record_offset, kLastRecordSize, &record, buffer);
  if (s.ok()) {
    if (record.size() < kLastRecordSize) {
      return Status::Corruption("File is too short to be an Trace file");
    }
    key->clear();
    value->clear();
    key->append(buffer, Tracer::kKeySize);
    assert(DecodeFixed32(buffer + Tracer::kKeySize) == 8);
    value->append(buffer + kLastRecordSize - 8, 8);
  }
  return s;
}

void TraceReaderImpl::Close() {
  return reader_.reset();
}


} // namespace rocksdb
