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

const std::string kTraceMagic = "feedcafedeadbeef";
const unsigned int kTraceTimestampSize = 8;
const unsigned int kTraceTypeSize = 1;
const unsigned int kTracePayloadLengthSize = 4;
const unsigned int kTraceMetadataSize =
    kTraceTimestampSize + kTraceTypeSize + kTracePayloadLengthSize;
const unsigned int FileTraceReader::kBufferSize = 1024;  // 1KB

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
  Status s = file_reader_->Read(offset_, kTraceMetadataSize, &result_, buffer_);
  if (!s.ok()) {
    return s;
  }
  if (result_.size() == 0) {
    // No more data to read
    // Todo: Come up with a better way to indicate end of data. May be this
    // could be avoided once footer is introduced.
    return Status::Incomplete();
  }
  if (result_.size() < kTraceMetadataSize) {
    return Status::Corruption("Corrupted trace file.");
  }
  *data = result_.ToString();
  offset_ += kTraceMetadataSize;

  uint32_t payload_len =
      DecodeFixed32(&buffer_[kTraceTimestampSize + kTraceTypeSize]);

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

Status Tracer::WriteTrace(const Trace& trace) {
  std::string encoded_trace;
  PutFixed64(&encoded_trace, trace.ts);
  encoded_trace.push_back(trace.type);
  PutFixed32(&encoded_trace, static_cast<uint32_t>(trace.payload.size()));
  encoded_trace.append(trace.payload);
  return trace_writer_->Write(Slice(encoded_trace));
}

Status Tracer::Close() { return WriteFooter(); }

Replayer::Replayer(DBImpl* db, const std::vector<ColumnFamilyHandle*>& handles,
                   unique_ptr<TraceReader>&& reader)
    : db_(db), trace_reader_(std::move(reader)) {
  for (ColumnFamilyHandle* cfh : handles) {
    cf_map_[cfh->GetID()] = cfh;
  }
}

Replayer::~Replayer() { trace_reader_.reset(); }

Status Replayer::Replay() {
  Status s;
  Trace header;
  s = ReadHeader(&header);
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
    s = ReadTrace(&trace);
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
      if (cf_id > 0 && cf_map_.find(cf_id) == cf_map_.end()) {
        return Status::Corruption("Invalid Column Family ID.");
      }

      std::string value;
      if (cf_id == 0) {
        db_->Get(roptions, key, &value);
      } else {
        db_->Get(roptions, cf_map_[cf_id], key, &value);
      }
      ops++;
    } else if (trace.type == kTraceEnd) {
      // Do nothing for now.
      // TODO: Add some validations later.
      break;
    }
  }

  if (s.IsIncomplete()) {
    // Reaching eof returns Incomplete status at the moment.
    // Could happen when killing a process with calling EndTrace() API.
    // TODO: Add better error handling.
    return Status::OK();
  }
  return s;
}

Status Replayer::ReadHeader(Trace* header) {
  assert(header != nullptr);
  Status s = ReadTrace(header);
  if (!s.ok()) {
    return s;
  }
  if (header->type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header->payload.substr(0, kTraceMagic.length()) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

Status Replayer::ReadFooter(Trace* footer) {
  assert(footer != nullptr);
  Status s = ReadTrace(footer);
  if (!s.ok()) {
    return s;
  }
  if (footer->type != kTraceEnd) {
    return Status::Corruption("Corrupted trace file. Incorrect footer.");
  }

  // TODO: Add more validations later
  return s;
}

Status Replayer::ReadTrace(Trace* trace) {
  assert(trace != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  Slice enc_slice = Slice(encoded_trace);
  GetFixed64(&enc_slice, &trace->ts);
  trace->type = static_cast<TraceType>(enc_slice[0]);
  enc_slice.remove_prefix(kTraceTypeSize + kTracePayloadLengthSize);
  trace->payload = enc_slice.ToString();
  return s;
}

Status NewFileTraceWriter(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceWriter>* trace_writer) {
  unique_ptr<WritableFile> trace_file;
  Status s = env->NewWritableFile(trace_filename, &trace_file, env_options);
  if (!s.ok()) {
    return s;
  }

  unique_ptr<WritableFileWriter> file_writer;
  file_writer.reset(new WritableFileWriter(std::move(trace_file), env_options));
  trace_writer->reset(new FileTraceWriter(std::move(file_writer)));
  return s;
}

Status NewFileTraceReader(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceReader>* trace_reader) {
  unique_ptr<RandomAccessFile> trace_file;
  Status s = env->NewRandomAccessFile(trace_filename, &trace_file, env_options);
  if (!s.ok()) {
    return s;
  }

  unique_ptr<RandomAccessFileReader> file_reader;
  file_reader.reset(
      new RandomAccessFileReader(std::move(trace_file), trace_filename));
  trace_reader->reset(new FileTraceReader(std::move(file_reader)));
  return s;
}

}  // namespace rocksdb
