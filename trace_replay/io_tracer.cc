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
#include "rocksdb/system_clock.h"
#include "rocksdb/trace_reader_writer.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
IOTraceWriter::IOTraceWriter(SystemClock* clock,
                             const TraceOptions& trace_options,
                             std::unique_ptr<TraceWriter>&& trace_writer)
    : clock_(clock),
      trace_options_(trace_options),
      trace_writer_(std::move(trace_writer)) {}

Status IOTraceWriter::WriteIOOp(const IOTraceRecord& record,
                                IODebugContext* dbg) {
  uint64_t trace_file_size = trace_writer_->GetFileSize();
  if (trace_file_size > trace_options_.max_trace_file_size) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = record.access_timestamp;
  trace.type = record.trace_type;
  PutFixed64(&trace.payload, record.io_op_data);
  Slice file_operation(record.file_operation);
  PutLengthPrefixedSlice(&trace.payload, file_operation);
  PutFixed64(&trace.payload, record.latency);
  Slice io_status(record.io_status);
  PutLengthPrefixedSlice(&trace.payload, io_status);
  Slice file_name(record.file_name);
  PutLengthPrefixedSlice(&trace.payload, file_name);

  // Each bit in io_op_data stores which corresponding info from IOTraceOp will
  // be added in the trace. Foreg, if bit at position 1 is set then
  // IOTraceOp::kIOLen (length) will be logged in the record (Since
  // IOTraceOp::kIOLen = 1 in the enum). So find all the set positions in
  // io_op_data one by one and, update corresponsing info in the trace record,
  // unset that bit to find other set bits until io_op_data = 0.
  /* Write remaining options based on io_op_data set by file operation */
  int64_t io_op_data = static_cast<int64_t>(record.io_op_data);
  while (io_op_data) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(io_op_data & -io_op_data));
    switch (set_pos) {
      case IOTraceOp::kIOFileSize:
        PutFixed64(&trace.payload, record.file_size);
        break;
      case IOTraceOp::kIOLen:
        PutFixed64(&trace.payload, record.len);
        break;
      case IOTraceOp::kIOOffset:
        PutFixed64(&trace.payload, record.offset);
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    io_op_data &= (io_op_data - 1);
  }

  int64_t trace_data = 0;
  if (dbg) {
    trace_data = static_cast<int64_t>(dbg->trace_data);
  }
  PutFixed64(&trace.payload, trace_data);
  while (trace_data) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(trace_data & -trace_data));
    switch (set_pos) {
      case IODebugContext::TraceData::kRequestID: {
        Slice request_id(dbg->request_id);
        PutLengthPrefixedSlice(&trace.payload, request_id);
      } break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    trace_data &= (trace_data - 1);
  }

  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return trace_writer_->Write(encoded_trace);
}

Status IOTraceWriter::WriteHeader() {
  Trace trace;
  trace.ts = clock_->NowMicros();
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

  if (!GetFixed64(&enc_slice, &record->io_op_data)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read trace data.");
  }
  Slice file_operation;
  if (!GetLengthPrefixedSlice(&enc_slice, &file_operation)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read file operation.");
  }
  record->file_operation = file_operation.ToString();
  if (!GetFixed64(&enc_slice, &record->latency)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read latency.");
  }
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

  // Each bit in io_op_data stores which corresponding info from IOTraceOp will
  // be added in the trace. Foreg, if bit at position 1 is set then
  // IOTraceOp::kIOLen (length) will be logged in the record (Since
  // IOTraceOp::kIOLen = 1 in the enum). So find all the set positions in
  // io_op_data one by one and, update corresponsing info in the trace record,
  // unset that bit to find other set bits until io_op_data = 0.
  /* Read remaining options based on io_op_data set by file operation */
  // Assuming 63 bits will be used at max.
  int64_t io_op_data = static_cast<int64_t>(record->io_op_data);
  while (io_op_data) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(io_op_data & -io_op_data));
    switch (set_pos) {
      case IOTraceOp::kIOFileSize:
        if (!GetFixed64(&enc_slice, &record->file_size)) {
          return Status::Incomplete(
              "Incomplete access record: Failed to read file size.");
        }
        break;
      case IOTraceOp::kIOLen:
        if (!GetFixed64(&enc_slice, &record->len)) {
          return Status::Incomplete(
              "Incomplete access record: Failed to read length.");
        }
        break;
      case IOTraceOp::kIOOffset:
        if (!GetFixed64(&enc_slice, &record->offset)) {
          return Status::Incomplete(
              "Incomplete access record: Failed to read offset.");
        }
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    io_op_data &= (io_op_data - 1);
  }

  if (!GetFixed64(&enc_slice, &record->trace_data)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read trace op.");
  }
  int64_t trace_data = static_cast<int64_t>(record->trace_data);
  while (trace_data) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(trace_data & -trace_data));
    switch (set_pos) {
      case IODebugContext::TraceData::kRequestID: {
        Slice request_id;
        if (!GetLengthPrefixedSlice(&enc_slice, &request_id)) {
          return Status::Incomplete(
              "Incomplete access record: Failed to request id.");
        }
        record->request_id = request_id.ToString();
      } break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    trace_data &= (trace_data - 1);
  }

  return Status::OK();
}

IOTracer::IOTracer() : tracing_enabled(false) { writer_.store(nullptr); }

IOTracer::~IOTracer() { EndIOTrace(); }

Status IOTracer::StartIOTrace(SystemClock* clock,
                              const TraceOptions& trace_options,
                              std::unique_ptr<TraceWriter>&& trace_writer) {
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (writer_.load()) {
    return Status::Busy();
  }
  trace_options_ = trace_options;
  writer_.store(
      new IOTraceWriter(clock, trace_options, std::move(trace_writer)));
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

void IOTracer::WriteIOOp(const IOTraceRecord& record, IODebugContext* dbg) {
  if (!writer_.load()) {
    return;
  }
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  if (!writer_.load()) {
    return;
  }
  writer_.load()->WriteIOOp(record, dbg).PermitUncheckedError();
}
}  // namespace ROCKSDB_NAMESPACE
