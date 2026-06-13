//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trace/file_trace_reader_writer.h"

#include "env/composite_env_wrapper.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "trace_replay/trace_replay.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

const unsigned int FileTraceReader::kBufferSize = 1024;  // 1KB

FileTraceReader::FileTraceReader(
    std::unique_ptr<RandomAccessFileReader>&& reader)
    : file_reader_(std::move(reader)),
      offset_(0),
      buffer_(new char[kBufferSize]) {}

FileTraceReader::~FileTraceReader() {
  Close().PermitUncheckedError();
  delete[] buffer_;
}

Status FileTraceReader::Close() {
  file_reader_.reset();
  return Status::OK();
}

Status FileTraceReader::Reset() {
  if (file_reader_ == nullptr) {
    return Status::IOError("TraceReader is closed.");
  }
  offset_ = 0;
  crc_framing_ = false;
  first_read_done_ = false;
  return Status::OK();
}

Status FileTraceReader::Read(std::string* data) {
  assert(file_reader_ != nullptr);
  if (!first_read_done_) {
    first_read_done_ = true;
    Status s = ReadLegacy(data);
    if (s.ok()) {
      DetectCRCFromHeader(*data);
    }
    return s;
  }
  return crc_framing_ ? ReadCRC(data) : ReadLegacy(data);
}

Status FileTraceReader::ReadData(unsigned int bytes_to_read,
                                 std::string* data) {
  unsigned int to_read =
      bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
  while (to_read > 0) {
    IOStatus io_s = file_reader_->Read(IOOptions(), offset_, to_read, &result_,
                                       buffer_, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    if (result_.size() < to_read) {
      return Status::Incomplete("Trace record data truncated");
    }
    data->append(result_.data(), result_.size());

    offset_ += to_read;
    bytes_to_read -= to_read;
    to_read = bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
  }
  return Status::OK();
}

Status FileTraceReader::ReadLegacy(std::string* data) {
  IOStatus io_s = file_reader_->Read(IOOptions(), offset_, kTraceMetadataSize,
                                     &result_, buffer_, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }
  if (result_.size() == 0) {
    return Status::Incomplete();
  }
  if (result_.size() < kTraceMetadataSize) {
    return Status::Incomplete("Trace metadata truncated");
  }
  *data = result_.ToString();
  offset_ += kTraceMetadataSize;

  uint32_t payload_len =
      DecodeFixed32(&buffer_[kTraceTimestampSize + kTraceTypeSize]);
  return ReadData(payload_len, data);
}

Status FileTraceReader::ReadCRC(std::string* data) {
  IOStatus io_s = file_reader_->Read(
      IOOptions(), offset_, kTraceFrameHeaderSize, &result_, buffer_, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }
  if (result_.size() == 0) {
    return Status::Incomplete();
  }
  if (result_.size() < kTraceFrameHeaderSize) {
    return Status::Incomplete("Trace frame header truncated");
  }

  uint32_t expected_crc = DecodeFixed32(result_.data());
  uint32_t data_len = DecodeFixed32(result_.data() + kTraceCRCSize);
  offset_ += kTraceFrameHeaderSize;

  data->clear();
  data->reserve(data_len);
  Status s = ReadData(data_len, data);
  if (!s.ok()) {
    return s;
  }

  uint32_t actual_crc = crc32c::Value(data->data(), data->size());
  if (actual_crc != expected_crc) {
    return Status::Incomplete("Trace record CRC mismatch");
  }
  return Status::OK();
}

void FileTraceReader::DetectCRCFromHeader(const std::string& header_data) {
  if (header_data.size() <= kTraceMetadataSize) {
    return;
  }
  auto pos = header_data.find("Trace Version: ");
  if (pos == std::string::npos) {
    return;
  }
  std::string ver = header_data.substr(pos + 15);
  auto tab = ver.find('\t');
  if (tab != std::string::npos) {
    ver.resize(tab);
  }
  int version = 0;
  for (char c : ver) {
    if (c == '.') {
      continue;
    }
    if (isdigit(c)) {
      version = version * 10 + (c - '0');
    }
  }
  if (version >= kTraceFileCRCFramingVersion) {
    crc_framing_ = true;
  }
}

FileTraceWriter::FileTraceWriter(
    std::unique_ptr<WritableFileWriter>&& file_writer)
    : file_writer_(std::move(file_writer)) {}

FileTraceWriter::~FileTraceWriter() { Close().PermitUncheckedError(); }

Status FileTraceWriter::Close() {
  file_writer_.reset();
  return Status::OK();
}

Status FileTraceWriter::Write(const Slice& data) {
  if (!crc_framing_) {
    return file_writer_->Append(IOOptions(), data);
  }
  char frame_header[kTraceFrameHeaderSize];
  uint32_t crc = crc32c::Value(data.data(), data.size());
  EncodeFixed32(frame_header, crc);
  EncodeFixed32(frame_header + kTraceCRCSize,
                static_cast<uint32_t>(data.size()));
  Status s = file_writer_->Append(IOOptions(),
                                  Slice(frame_header, sizeof(frame_header)));
  if (s.ok()) {
    s = file_writer_->Append(IOOptions(), data);
  }
  return s;
}

uint64_t FileTraceWriter::GetFileSize() { return file_writer_->GetFileSize(); }

Status NewFileTraceReader(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceReader>* trace_reader) {
  std::unique_ptr<RandomAccessFileReader> file_reader;
  Status s = RandomAccessFileReader::Create(
      env->GetFileSystem(), trace_filename, FileOptions(env_options),
      &file_reader, nullptr);
  if (!s.ok()) {
    return s;
  }
  trace_reader->reset(new FileTraceReader(std::move(file_reader)));
  return s;
}

Status NewFileTraceWriter(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceWriter>* trace_writer) {
  std::unique_ptr<WritableFileWriter> file_writer;
  Status s = WritableFileWriter::Create(env->GetFileSystem(), trace_filename,
                                        FileOptions(env_options), &file_writer,
                                        nullptr);
  if (!s.ok()) {
    return s;
  }
  trace_writer->reset(new FileTraceWriter(std::move(file_writer)));
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
