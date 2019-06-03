//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/block_cache_tracer.h"

#include "db/db_impl/db_impl.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

namespace {
const unsigned int kCharSize = 1;
bool ShouldTraceReferencedKey(const TraceRecord& record) {
  return (record.block_type == TraceType::kBlockTraceDataBlock) &&
         (record.caller == BlockCacheLookupCaller::kUserGet ||
          record.caller == BlockCacheLookupCaller::kUserMGet);
}
}  // namespace

BlockCacheTraceWriter::BlockCacheTraceWriter(
    Env* env, const TraceOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer)
    : env_(env),
      trace_options_(trace_options),
      trace_writer_(std::move(trace_writer)) {
  WriteHeader();
}

BlockCacheTraceWriter::~BlockCacheTraceWriter() { trace_writer_.reset(); }

Status BlockCacheTraceWriter::WriteBlockAccess(const TraceRecord& record) {
  uint64_t trace_file_size = trace_writer_->GetFileSize();
  if (trace_file_size > trace_options_.max_trace_file_size) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = record.access_timestamp;
  trace.type = record.block_type;
  PutLengthPrefixedSlice(&trace.payload, record.block_key);
  PutFixed64(&trace.payload, record.block_size);
  PutFixed32(&trace.payload, record.cf_id);
  PutLengthPrefixedSlice(&trace.payload, record.cf_name);
  PutFixed32(&trace.payload, record.level);
  PutFixed32(&trace.payload, record.sst_fd_number);
  trace.payload.push_back(record.caller);
  trace.payload.push_back(record.is_cache_hit);
  if (ShouldTraceReferencedKey(record)) {
    PutLengthPrefixedSlice(&trace.payload, record.referenced_key);
    PutFixed64(&trace.payload, record.num_keys_in_block);
    trace.payload.push_back(record.is_referenced_key_exist_in_block);
  }
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return trace_writer_->Write(encoded_trace);
}

Status BlockCacheTraceWriter::WriteHeader() {
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

BlockCacheTraceReader::BlockCacheTraceReader(
    std::unique_ptr<TraceReader>&& reader)
    : trace_reader_(std::move(reader)) {}

BlockCacheTraceReader::~BlockCacheTraceReader() { trace_reader_.reset(); }

Status BlockCacheTraceReader::ReadHeader(TraceHeader* header) {
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
  Status corrupt_status =
      Status::Corruption("Corrupted header in the trace file.");
  Slice magnic_number;
  if (!GetLengthPrefixedSlice(&enc_slice, &magnic_number)) {
    return corrupt_status;
  }
  if (magnic_number.ToString() != kTraceMagic) {
    return corrupt_status;
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_major_version)) {
    return corrupt_status;
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_minor_version)) {
    return corrupt_status;
  }
  // We should have retrieved all information in the header.
  if (!enc_slice.empty()) {
    return corrupt_status;
  }
  return Status::OK();
}

Status BlockCacheTraceReader::ReadAccess(TraceRecord* record) {
  assert(record);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  Status incomplete_record = Status::Incomplete("Incomplete access record");
  Trace trace;
  s = TracerHelper::DecodeTrace(encoded_trace, &trace);
  if (!s.ok()) {
    return s;
  }
  record->access_timestamp = trace.ts;
  record->block_type = trace.type;
  Slice enc_slice = Slice(trace.payload);
  Slice block_key;
  if (!GetLengthPrefixedSlice(&enc_slice, &block_key)) {
    return incomplete_record;
  }
  record->block_key = block_key.ToString();
  if (!GetFixed64(&enc_slice, &record->block_size)) {
    return incomplete_record;
  }
  if (!GetFixed32(&enc_slice, &record->cf_id)) {
    return incomplete_record;
  }
  Slice cf_name;
  if (!GetLengthPrefixedSlice(&enc_slice, &cf_name)) {
    return incomplete_record;
  }
  record->cf_name = cf_name.ToString();
  if (!GetFixed32(&enc_slice, &record->level)) {
    return incomplete_record;
  }
  if (!GetFixed32(&enc_slice, &record->sst_fd_number)) {
    return incomplete_record;
  }
  if (enc_slice.empty()) {
    return incomplete_record;
  }
  record->caller = static_cast<BlockCacheLookupCaller>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);
  if (enc_slice.empty()) {
    return incomplete_record;
  }
  record->is_cache_hit = static_cast<Boolean>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);

  if (ShouldTraceReferencedKey(*record)) {
    Slice referenced_key;
    if (!GetLengthPrefixedSlice(&enc_slice, &referenced_key)) {
      return incomplete_record;
    }
    record->referenced_key = referenced_key.ToString();
    if (!GetFixed64(&enc_slice, &record->num_keys_in_block)) {
      return incomplete_record;
    }
    if (enc_slice.empty()) {
      return incomplete_record;
    }
    record->is_referenced_key_exist_in_block =
        static_cast<Boolean>(enc_slice[0]);
  }
  return Status::OK();
}

}  // namespace rocksdb
