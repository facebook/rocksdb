//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/block_cache_tracer.h"

#include "db/db_impl/db_impl.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace rocksdb {

namespace {
const unsigned int kCharSize = 1;
}  // namespace

bool ShouldTraceReferencedKey(const BlockCacheTraceRecord& record) {
  return (record.block_type == TraceType::kBlockTraceDataBlock) &&
         (record.caller == BlockCacheLookupCaller::kUserGet ||
          record.caller == BlockCacheLookupCaller::kUserMGet);
}

BlockCacheTraceWriter::BlockCacheTraceWriter(
    Env* env, const TraceOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer)
    : env_(env),
      trace_options_(trace_options),
      trace_writer_(std::move(trace_writer)) {}

bool BlockCacheTraceWriter::ShouldTrace(
    const BlockCacheTraceRecord& record) const {
  if (trace_options_.sampling_frequency == 0 ||
      trace_options_.sampling_frequency == 1) {
    return true;
  }
  // We use spatial downsampling so that we have a complete access history for a
  // block.
  const uint64_t hash = GetSliceNPHash64(Slice(record.block_key));
  return hash % trace_options_.sampling_frequency == 0;
}

Status BlockCacheTraceWriter::WriteBlockAccess(
    const BlockCacheTraceRecord& record) {
  uint64_t trace_file_size = trace_writer_->GetFileSize();
  if (trace_file_size > trace_options_.max_trace_file_size ||
      !ShouldTrace(record)) {
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
  trace.payload.push_back(record.no_insert);
  if (ShouldTraceReferencedKey(record)) {
    PutLengthPrefixedSlice(&trace.payload, record.referenced_key);
    PutFixed64(&trace.payload, record.num_keys_in_block);
    trace.payload.push_back(record.is_referenced_key_exist_in_block);
  }
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
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
  InstrumentedMutexLock lock_guard(&trace_writer_mutex_);
  return trace_writer_->Write(encoded_trace);
}

BlockCacheTraceReader::BlockCacheTraceReader(
    std::unique_ptr<TraceReader>&& reader)
    : trace_reader_(std::move(reader)) {}

Status BlockCacheTraceReader::ReadHeader(BlockCacheTraceHeader* header) {
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
  Slice magnic_number;
  if (!GetLengthPrefixedSlice(&enc_slice, &magnic_number)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read the magic number.");
  }
  if (magnic_number.ToString() != kTraceMagic) {
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

Status BlockCacheTraceReader::ReadAccess(BlockCacheTraceRecord* record) {
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
  record->block_type = trace.type;
  Slice enc_slice = Slice(trace.payload);
  Slice block_key;
  if (!GetLengthPrefixedSlice(&enc_slice, &block_key)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read block key.");
  }
  record->block_key = block_key.ToString();
  if (!GetFixed64(&enc_slice, &record->block_size)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read block size.");
  }
  if (!GetFixed32(&enc_slice, &record->cf_id)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read column family ID.");
  }
  Slice cf_name;
  if (!GetLengthPrefixedSlice(&enc_slice, &cf_name)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read column family name.");
  }
  record->cf_name = cf_name.ToString();
  if (!GetFixed32(&enc_slice, &record->level)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read level.");
  }
  if (!GetFixed32(&enc_slice, &record->sst_fd_number)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read SST file number.");
  }
  if (enc_slice.empty()) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read caller.");
  }
  record->caller = static_cast<BlockCacheLookupCaller>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);
  if (enc_slice.empty()) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read is_cache_hit.");
  }
  record->is_cache_hit = static_cast<Boolean>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);
  if (enc_slice.empty()) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read no_insert.");
  }
  record->no_insert = static_cast<Boolean>(enc_slice[0]);
  enc_slice.remove_prefix(kCharSize);

  if (ShouldTraceReferencedKey(*record)) {
    Slice referenced_key;
    if (!GetLengthPrefixedSlice(&enc_slice, &referenced_key)) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read the referenced key.");
    }
    record->referenced_key = referenced_key.ToString();
    if (!GetFixed64(&enc_slice, &record->num_keys_in_block)) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read the number of keys in the "
          "block.");
    }
    if (enc_slice.empty()) {
      return Status::Incomplete(
          "Incomplete access record: Failed to read "
          "is_referenced_key_exist_in_block.");
    }
    record->is_referenced_key_exist_in_block =
        static_cast<Boolean>(enc_slice[0]);
  }
  return Status::OK();
}

}  // namespace rocksdb
