//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/trace_replay.h"

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>
#include "db/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"

namespace rocksdb {

namespace {
void EncodeCFAndKey(std::string* dst, uint32_t cf_id, const Slice& key) {
  PutFixed32(dst, cf_id);
  PutLengthPrefixedSlice(dst, key);
}

void DecodeCFAndKey(std::string& buffer, uint32_t* cf_id, Slice* key) {
  Slice buf(buffer);
  GetFixed32(&buf, cf_id);
  GetLengthPrefixedSlice(&buf, key);
}
}  // namespace

FileTraceWriter::~FileTraceWriter() { Close(); }

Status FileTraceWriter::Close() {
  file_writer_.reset();
  return Status::OK();
}

Status FileTraceWriter::Write(const Slice& data) {
  return file_writer_->Append(data);
}

const unsigned int FileTraceReader::kBufferSize = 1024;  // 1KB

FileTraceReader::FileTraceReader(
    std::unique_ptr<RandomAccessFileReader>&& reader)
    : file_reader_(std::move(reader)),
      offset_(0),
      buffer_(new char[kBufferSize]) {}

FileTraceReader::~FileTraceReader() { Close(); }

Status FileTraceReader::Close() {
  file_reader_.reset();
  return Status::OK();
}

Status FileTraceReader::Read(std::string* data) {
  assert(file_reader_ != nullptr);
  Status s = file_reader_->Read(offset_, 13, &result_, buffer_);
  if (!s.ok()) {
    return s;
  }
  if (result_.size() == 0) {
    // No more data to read
    // Todo: Come up with a better way to indicate end of data. May be this
    // could be avoided once footer is introduced.
    return Status::Incomplete();
  }
  if (result_.size() < 13) {
    return Status::Corruption("Corrupted trace file.");
  }
  *data = result_.ToString();
  offset_ += 13;

  uint32_t payload_len = DecodeFixed32(&buffer_[9]);

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
    data->append(result_.data(), result_.size());

    offset_ += to_read;
    bytes_to_read -= to_read;
    to_read = bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
  }

  return s;
}

Tracer::Tracer(Env* env, std::unique_ptr<TraceWriter>&& trace_writer)
    : env_(env), trace_writer_(std::move(trace_writer)) {
  WriteHeader();
}

Tracer::~Tracer() { trace_writer_.reset(); }

Status Tracer::Write(WriteBatch* write_batch) {
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceWrite;
  trace.payload = write_batch->Data();
  return WriteTrace(trace);
}

Status Tracer::Get(ColumnFamilyHandle* column_family, const Slice& key) {
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceGet;
  EncodeCFAndKey(&trace.payload, column_family->GetID(), key);
  return WriteTrace(trace);
}

Status Tracer::WriteHeader() {
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
  return WriteTrace(trace);
}

Status Tracer::WriteFooter() {
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = kTraceEnd;
  trace.payload = "";
  return WriteTrace(trace);
}

Status Tracer::WriteTrace(Trace& trace) {
  std::string encoded_trace;
  PutFixed64(&encoded_trace, trace.ts);
  encoded_trace.push_back(trace.type);
  PutFixed32(&encoded_trace, static_cast<uint32_t>(trace.payload.size()));
  encoded_trace.append(trace.payload);
  return trace_writer_->Write(Slice(encoded_trace));
}

Status Tracer::Close() { return WriteFooter(); }

Replayer::Replayer(DBImpl* db, std::vector<ColumnFamilyHandle*>& handles,
                   unique_ptr<TraceReader>&& reader)
    : db_(db), trace_reader_(std::move(reader)) {
  for (ColumnFamilyHandle* cfh : handles) {
    cf_map_[cfh->GetID()] = cfh;
  }
  Replay();
}

Replayer::~Replayer() { trace_reader_.reset(); }

Status Replayer::Replay() {
  Status s;
  Trace header;
  s = ReadHeader(header);
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
    s = ReadTrace(trace);
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
      uint32_t cf_id = 0;
      Slice key;
      DecodeCFAndKey(trace.payload, &cf_id, &key);
      if (cf_map_.find(cf_id) == cf_map_.end()) {
        return Status::Corruption("Invalid Column Family ID");
      }

      std::string value;
      db_->Get(roptions, cf_map_[cf_id], key, &value);
      ops++;
    }
  }

  Trace footer;
  s = ReadFooter(footer);
  if (!s.ok()) {
    return s;
  }

  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
    return Status::OK();
  }
  return s;
}

Status Replayer::ReadHeader(Trace& header) {
  Status s = ReadTrace(header);
  if (!s.ok()) {
    return s;
  }
  if (header.type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header.payload.substr(0, 16) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

Status Replayer::ReadFooter(Trace& footer) {
  Status s = ReadTrace(footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.type != kTraceEnd) {
    return Status::Corruption("Corrupted trace file. Incorrect footer.");
  }

  // TODO: Add more validations later
  return s;
}

Status Replayer::ReadTrace(Trace& trace) {
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  Slice enc_slice = Slice(encoded_trace);
  GetFixed64(&enc_slice, &trace.ts);
  trace.type = static_cast<TraceType>(enc_slice[0]);
  enc_slice.remove_prefix(5);
  trace.payload = enc_slice.ToString();
  return s;
}

}  // namespace rocksdb
