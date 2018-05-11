//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/trace_replay.h"

#include <chrono>
#include <sstream>
#include <thread>
#include "db/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"

namespace rocksdb {
TraceWriter::~TraceWriter() { file_writer_.reset(); }

Status TraceWriter::WriteHeader() {
  std::ostringstream s;
  s << kTraceMagic << "\t"
    << "Trace Version: 0.1\t"
    << "RocksDB Version: " << kMajorVersion << "." << kMinorVersion << "\t"
    << "Format: Timestamp OpType Payload\n";
  std::string header(s.str());

  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceBegin;
  trace.payload = header;

  return WriteRecord(trace);
}

Status TraceWriter::WriteFooter() { return Status::OK(); }

Status TraceWriter::WriteRecord(Trace& trace) {
  Status s;
  // Metadata: 9 bytes: 8 bytes timestamp, 1 byte op type
  std::string metadata(kMetadataSize, '\0');
  EncodeFixed64(&metadata[0], trace.ts);
  metadata[kMetadataSize - 1] = trace.type;
  s = file_writer_->Append(Slice(metadata));
  if (!s.ok()) {
    return s;
  }

  // Payload
  std::string payload_length;
  PutFixed32(&payload_length, static_cast<uint32_t>(trace.payload.size()));
  s = file_writer_->Append(payload_length);
  if (!s.ok()) {
    return s;
  }
  s = file_writer_->Append(Slice(trace.payload));
  return s;
}

const unsigned int TraceReader::kBufferSize = 1024;  // 1KB

TraceReader::TraceReader(std::unique_ptr<RandomAccessFileReader>&& reader)
    : file_reader_(std::move(reader)),
      offset_(0),
      buffer_(new char[kBufferSize]) {}

TraceReader::~TraceReader() { file_reader_.reset(); }

Status TraceReader::ReadHeader(Trace& header) {
  Status s;
  s = ReadRecord(header);
  if (header.type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header.payload.substr(0, 16) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

Status TraceReader::ReadFooter(Trace& /*footer*/) { return Status::OK(); }

Status TraceReader::ReadRecord(Trace& trace) {
  // Read Timestamp
  Status s;
  assert(file_reader_ != nullptr);
  s = file_reader_->Read(offset_, 8, &result_, buffer_);
  if (!s.ok()) {
    return s;
  }
  if (result_.size() == 0) {
    // No more data to read
    // Todo: Come up with a better way to indicate end of data. May be this
    // could be avoided once footer is introduced.
    return Status::Incomplete();
  }
  if (result_.size() < 8) {
    return Status::Corruption("Corrupted trace file.");
  }
  offset_ += 8;
  assert(buffer_ != nullptr);
  uint64_t ts = DecodeFixed64(buffer_);
  trace.ts = ts;

  // Read TraceType
  s = file_reader_->Read(offset_, 1, &result_, buffer_);
  if (!s.ok()) {
    return s;
  }
  offset_ += 1;
  if (result_.size() < 1) {
    return Status::Corruption("Corrupted trace file.");
  }
  TraceType type = static_cast<TraceType>(buffer_[0]);
  trace.type = type;

  // Read Payload length
  s = file_reader_->Read(offset_, 4, &result_, buffer_);
  if (!s.ok()) {
    return s;
  }
  offset_ += 4;
  if (result_.size() < 4) {
    return Status::Corruption("Corrupted trace file.");
  }
  uint32_t payload_len = DecodeFixed32(buffer_);

  // Read Payload
  unsigned int bytes_to_read = payload_len;
  unsigned int to_read =
      bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
  while (to_read > 0) {
    s = file_reader_->Read(offset_, to_read, &result_, buffer_);
    if (!s.ok()) {
      return s;
    }
    if (result_.size() < to_read) {
      return Status::Corruption("Corrupted trace file.");
    }
    trace.payload.append(result_.data(), result_.size());

    offset_ += to_read;
    bytes_to_read -= to_read;
    to_read = bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
  }

  return s;
}

Tracer::Tracer(Env* env, std::unique_ptr<TraceWriter>&& trace_writer)
    : env_(env), trace_writer_(std::move(trace_writer)) {
  trace_writer_->WriteHeader();
}

Tracer::~Tracer() { trace_writer_.reset(); }

Status Tracer::TraceWrite(WriteBatch* write_batch) {
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceWrite;
  trace.payload = write_batch->Data();
  return trace_writer_->WriteRecord(trace);
}

Status Tracer::TraceGet(const Slice& key) {
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceGet;
  trace.payload = key.ToString();
  return trace_writer_->WriteRecord(trace);
}

Status Tracer::Close() {
  Status s = trace_writer_->WriteFooter();
  trace_writer_.reset();
  return s;
}

Replayer::Replayer(DBImpl* db, unique_ptr<TraceReader>&& reader)
    : db_(db), trace_reader_(std::move(reader)) {
  Replay();
}

Replayer::~Replayer() { trace_reader_.reset(); }

Status Replayer::Replay() {
  Status s;
  Trace header;
  s = trace_reader_->ReadHeader(header);
  if (!s.ok()) {
    return s;
  }

  Trace footer;
  s = trace_reader_->ReadFooter(footer);
  if (!s.ok()) {
    return s;
  }

  std::chrono::system_clock::time_point replay_epoch =
      std::chrono::system_clock::now();
  WriteOptions woptions;
  ReadOptions roptions;
  Trace trace;
  uint64_t ops = 0;
  while (s.ok()) {
    trace.reset();
    s = trace_reader_->ReadRecord(trace);
    if (!s.ok()) {
      break;
    }

    std::this_thread::sleep_until(
        replay_epoch + std::chrono::microseconds(trace.ts - header.ts));
    if (trace.type == kTraceWrite) {
      WriteBatch batch(trace.payload);
      db_->Write(woptions, &batch);
      ops++;
    } else if (trace.type == kTraceGet) {
      std::string value;
      db_->Get(roptions, trace.payload, &value);
      ops++;
    }
  }
  // fprintf(stderr, "Ops Written: %ld\n", ops);

  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
    return Status::OK();
  }
  return s;
}

}  // namespace rocksdb
