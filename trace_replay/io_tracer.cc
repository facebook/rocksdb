//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/io_tracer.h"

#include <cinttypes>
#include <cstdio>
#include <cstdlib>

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

IOTraceWriter::IOTraceWriter(Env* env, const TraceOptions& trace_options,
                             std::unique_ptr<TraceWriter>&& trace_writer)
    : env_(env),
      trace_options_(trace_options),
      trace_writer_(std::move(trace_writer)) {}

Status IOTraceWriter::WriteIOOp(const IOTraceRecord& record) {
  uint64_t trace_file_size = trace_writer_->GetFileSize();
  if (trace_file_size > trace_options_.max_trace_file_size) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = record.access_timestamp;
  trace.type = record.trace_type;
  Slice file_operation(record.file_operation);
  PutLengthPrefixedSlice(&trace.payload, file_operation);
  Slice io_status(record.io_status);
  PutLengthPrefixedSlice(&trace.payload, io_status);
  Slice file_name(record.file_name);
  PutLengthPrefixedSlice(&trace.payload, file_name);
  // TODO: add below options based on file_operation
  trace.payload.push_back(record.len);
  PutFixed64(&trace.payload, record.offset);
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return trace_writer_->Write(encoded_trace);
}

Status IOTraceWriter::WriteHeader() {
  Trace trace;
  trace.ts = env_->NowMicros();
  trace.type = TraceType::kTraceBegin;
  PutLengthPrefixedSlice(&trace.payload, kTraceMagic);
  PutFixed32(&trace.payload, kMajorVersion);
  PutFixed32(&trace.payload, kMinorVersion);
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return trace_writer_->Write(encoded_trace);
}

IOTraceReader::IOTraceReader(std::unique_ptr<TraceReader>&& reader)
    : trace_reader_(std::move(reader)) {}

Status IOTraceReader::ReadHeader(IOTraceHeader* header) {
  assert(header != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  Trace trace;
  s = TracerHelper::DecodeTrace(encoded_trace, &trace);
  if (!s.ok()) {
    return s;
  }
  header->start_time = trace.ts;
  Slice enc_slice = Slice(trace.payload);
  Slice magic_number;
  if (!GetLengthPrefixedSlice(&enc_slice, &magic_number)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read the magic number.");
  }
  if (magic_number.ToString() != kTraceMagic) {
    return Status::Corruption(
        "Corrupted header in the trace file: Magic number does not match.");
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_major_version)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read rocksdb major "
        "version number.");
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_minor_version)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read rocksdb minor "
        "version number.");
  }
  // We should have retrieved all information in the header.
  if (!enc_slice.empty()) {
    return Status::Corruption(
        "Corrupted header in the trace file: The length of header is too "
        "long.");
  }
  return Status::OK();
}

Status IOTraceReader::ReadIOOp(IOTraceRecord* record) {
  assert(record);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  Trace trace;
  s = TracerHelper::DecodeTrace(encoded_trace, &trace);
  if (!s.ok()) {
    return s;
  }
  record->access_timestamp = trace.ts;
  record->trace_type = trace.type;
  Slice enc_slice = Slice(trace.payload);

  Slice file_operation;
  if (!GetLengthPrefixedSlice(&enc_slice, &file_operation)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read file operation.");
  }
  record->file_operation = file_operation.ToString();
  Slice io_status;
  if (!GetLengthPrefixedSlice(&enc_slice, &io_status)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read IO status.");
  }
  record->io_status = io_status.ToString();
  Slice file_name;
  if (!GetLengthPrefixedSlice(&enc_slice, &file_name)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read file name.");
  }
  record->file_name = file_name.ToString();
  // TODO: Read below options based on file_operation.
  record->len = static_cast<size_t>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);
  if (enc_slice.empty()) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read is_cache_hit.");
  }
  if (!GetFixed64(&enc_slice, &record->offset)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read offset.");
  }
  return Status::OK();
}

IOTracer::IOTracer() : tracing_enabled(false) { writer_.store(nullptr); }

IOTracer::~IOTracer() { EndIOTrace(); }

Status IOTracer::StartIOTrace(Env* env, const TraceOptions& trace_options,
                              std::unique_ptr<TraceWriter>&& trace_writer) {
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (writer_.load()) {
    return Status::Busy();
  }
  trace_options_ = trace_options;
  writer_.store(new IOTraceWriter(env, trace_options, std::move(trace_writer)));
  tracing_enabled = true;
  return writer_.load()->WriteHeader();
}

void IOTracer::EndIOTrace() {
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (!writer_.load()) {
    return;
  }
  delete writer_.load();
  writer_.store(nullptr);
  tracing_enabled = false;
}

Status IOTracer::WriteIOOp(const IOTraceRecord& record) {
  if (!writer_.load()) {
    return Status::OK();
  }
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (!writer_.load()) {
    return Status::OK();
  }
  return writer_.load()->WriteIOOp(record);
}
}  // namespace ROCKSDB_NAMESPACE
