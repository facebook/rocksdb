//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/trace_replay.h"

#include <chrono>
#include <sstream>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const std::string kTraceMagic = "feedcafedeadbeef";

namespace {
void DecodeCFAndKey(std::string& buffer, uint32_t* cf_id, Slice* key) {
  Slice buf(buffer);
  GetFixed32(&buf, cf_id);
  GetLengthPrefixedSlice(&buf, key);
}
}  // namespace

Status TracerHelper::ParseVersionStr(std::string& v_string, int* v_num) {
  if (v_string.find_first_of('.') == std::string::npos ||
      v_string.find_first_of('.') != v_string.find_last_of('.')) {
    return Status::Corruption(
        "Corrupted trace file. Incorrect version format.");
  }
  int tmp_num = 0;
  for (int i = 0; i < static_cast<int>(v_string.size()); i++) {
    if (v_string[i] == '.') {
      continue;
    } else if (isdigit(v_string[i])) {
      tmp_num = tmp_num * 10 + (v_string[i] - '0');
    } else {
      return Status::Corruption(
          "Corrupted trace file. Incorrect version format");
    }
  }
  *v_num = tmp_num;
  return Status::OK();
}

Status TracerHelper::ParseTraceHeader(const Trace& header, int* trace_version,
                                      int* db_version) {
  std::vector<std::string> s_vec;
  int begin = 0, end;
  for (int i = 0; i < 3; i++) {
    assert(header.payload.find("\t", begin) != std::string::npos);
    end = static_cast<int>(header.payload.find("\t", begin));
    s_vec.push_back(header.payload.substr(begin, end - begin));
    begin = end + 1;
  }

  std::string t_v_str, db_v_str;
  assert(s_vec.size() == 3);
  assert(s_vec[1].find("Trace Version: ") != std::string::npos);
  t_v_str = s_vec[1].substr(15);
  assert(s_vec[2].find("RocksDB Version: ") != std::string::npos);
  db_v_str = s_vec[2].substr(17);

  Status s;
  s = ParseVersionStr(t_v_str, trace_version);
  if (s != Status::OK()) {
    return s;
  }
  s = ParseVersionStr(db_v_str, db_version);
  return s;
}

void TracerHelper::EncodeTrace(const Trace& trace, std::string* encoded_trace) {
  assert(encoded_trace);
  PutFixed64(encoded_trace, trace.ts);
  encoded_trace->push_back(trace.type);
  PutFixed32(encoded_trace, static_cast<uint32_t>(trace.payload.size()));
  encoded_trace->append(trace.payload);
}

Status TracerHelper::DecodeTrace(const std::string& encoded_trace,
                                 Trace* trace) {
  assert(trace != nullptr);
  Slice enc_slice = Slice(encoded_trace);
  if (!GetFixed64(&enc_slice, &trace->ts)) {
    return Status::Incomplete("Decode trace string failed");
  }
  if (enc_slice.size() < kTraceTypeSize + kTracePayloadLengthSize) {
    return Status::Incomplete("Decode trace string failed");
  }
  trace->type = static_cast<TraceType>(enc_slice[0]);
  enc_slice.remove_prefix(kTraceTypeSize + kTracePayloadLengthSize);
  trace->payload = enc_slice.ToString();
  return Status::OK();
}

Status TracerHelper::DecodeHeader(const std::string& encoded_trace,
                                  Trace* header) {
  Status s = TracerHelper::DecodeTrace(encoded_trace, header);

  if (header->type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header->payload.substr(0, kTraceMagic.length()) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

bool TracerHelper::SetPayloadMap(uint64_t& payload_map,
                                 const TracePayloadType payload_type) {
  uint64_t old_state = payload_map;
  uint64_t tmp = 1;
  payload_map |= (tmp << payload_type);
  return old_state != payload_map;
}

Status TracerHelper::DecodeTraceRecord(Trace* trace, int trace_file_version,
                                       std::unique_ptr<TraceRecord>* record) {
  assert(trace != nullptr);

  if (record != nullptr) {
    record->reset(nullptr);
  }

  switch (trace->type) {
    // Write
    case kTraceWrite: {
      PinnableSlice rep;
      if (trace_file_version < 2) {
        rep.PinSelf(trace->payload);
      } else {
        Slice buf(trace->payload);
        GetFixed64(&buf, &trace->payload_map);
        int64_t payload_map = static_cast<int64_t>(trace->payload_map);
        Slice write_batch_data;
        while (payload_map) {
          // Find the rightmost set bit.
          uint32_t set_pos =
              static_cast<uint32_t>(log2(payload_map & -payload_map));
          switch (set_pos) {
            case TracePayloadType::kWriteBatchData: {
              GetLengthPrefixedSlice(&buf, &write_batch_data);
              break;
            }
            default: {
              assert(false);
            }
          }
          // unset the rightmost bit.
          payload_map &= (payload_map - 1);
        }
        rep.PinSelf(write_batch_data);
      }

      if (record != nullptr) {
        record->reset(new WriteQueryTraceRecord(std::move(rep), trace->ts));
      }

      return Status::OK();
    }
    // Get
    case kTraceGet: {
      uint32_t cf_id = 0;
      Slice get_key;

      if (trace_file_version < 2) {
        DecodeCFAndKey(trace->payload, &cf_id, &get_key);
      } else {
        Slice buf(trace->payload);
        GetFixed64(&buf, &trace->payload_map);
        int64_t payload_map = static_cast<int64_t>(trace->payload_map);
        while (payload_map) {
          // Find the rightmost set bit.
          uint32_t set_pos =
              static_cast<uint32_t>(log2(payload_map & -payload_map));
          switch (set_pos) {
            case TracePayloadType::kGetCFID: {
              GetFixed32(&buf, &cf_id);
              break;
            }
            case TracePayloadType::kGetKey: {
              GetLengthPrefixedSlice(&buf, &get_key);
              break;
            }
            default: {
              assert(false);
            }
          }
          // unset the rightmost bit.
          payload_map &= (payload_map - 1);
        }
      }

      if (record != nullptr) {
        PinnableSlice ps;
        ps.PinSelf(get_key);
        record->reset(new GetQueryTraceRecord(cf_id, std::move(ps), trace->ts));
      }

      return Status::OK();
    }
    // Iterator Seek and SeekForPrev
    case kTraceIteratorSeek:
    case kTraceIteratorSeekForPrev: {
      uint32_t cf_id = 0;
      Slice iter_key;
      Slice lower_bound;
      Slice upper_bound;

      if (trace_file_version < 2) {
        DecodeCFAndKey(trace->payload, &cf_id, &iter_key);
      } else {
        Slice buf(trace->payload);
        GetFixed64(&buf, &trace->payload_map);
        int64_t payload_map = static_cast<int64_t>(trace->payload_map);
        while (payload_map) {
          // Find the rightmost set bit.
          uint32_t set_pos =
              static_cast<uint32_t>(log2(payload_map & -payload_map));
          switch (set_pos) {
            case TracePayloadType::kIterCFID: {
              GetFixed32(&buf, &cf_id);
              break;
            }
            case TracePayloadType::kIterKey: {
              GetLengthPrefixedSlice(&buf, &iter_key);
              break;
            }
            case TracePayloadType::kIterLowerBound: {
              GetLengthPrefixedSlice(&buf, &lower_bound);
              break;
            }
            case TracePayloadType::kIterUpperBound: {
              GetLengthPrefixedSlice(&buf, &upper_bound);
              break;
            }
            default: {
              assert(false);
            }
          }
          // unset the rightmost bit.
          payload_map &= (payload_map - 1);
        }
      }

      if (record != nullptr) {
        PinnableSlice ps_key;
        ps_key.PinSelf(iter_key);
        PinnableSlice ps_lower;
        ps_lower.PinSelf(lower_bound);
        PinnableSlice ps_upper;
        ps_upper.PinSelf(upper_bound);
        record->reset(new IteratorSeekQueryTraceRecord(
            static_cast<IteratorSeekQueryTraceRecord::SeekType>(trace->type),
            cf_id, std::move(ps_key), std::move(ps_lower), std::move(ps_upper),
            trace->ts));
      }

      return Status::OK();
    }
    // MultiGet
    case kTraceMultiGet: {
      if (trace_file_version < 2) {
        return Status::Corruption("MultiGet is not supported.");
      }

      uint32_t multiget_size = 0;
      std::vector<uint32_t> cf_ids;
      std::vector<PinnableSlice> multiget_keys;

      Slice cfids_payload;
      Slice keys_payload;
      Slice buf(trace->payload);
      GetFixed64(&buf, &trace->payload_map);
      int64_t payload_map = static_cast<int64_t>(trace->payload_map);
      while (payload_map) {
        // Find the rightmost set bit.
        uint32_t set_pos =
            static_cast<uint32_t>(log2(payload_map & -payload_map));
        switch (set_pos) {
          case TracePayloadType::kMultiGetSize: {
            GetFixed32(&buf, &multiget_size);
            break;
          }
          case TracePayloadType::kMultiGetCFIDs: {
            GetLengthPrefixedSlice(&buf, &cfids_payload);
            break;
          }
          case TracePayloadType::kMultiGetKeys: {
            GetLengthPrefixedSlice(&buf, &keys_payload);
            break;
          }
          default: {
            assert(false);
          }
        }
        // unset the rightmost bit.
        payload_map &= (payload_map - 1);
      }
      if (multiget_size == 0) {
        return Status::InvalidArgument("Empty MultiGet cf_ids or keys.");
      }

      // Decode the cfids_payload and keys_payload
      cf_ids.reserve(multiget_size);
      multiget_keys.reserve(multiget_size);
      for (uint32_t i = 0; i < multiget_size; i++) {
        uint32_t tmp_cfid = 0;
        Slice tmp_key;
        GetFixed32(&cfids_payload, &tmp_cfid);
        GetLengthPrefixedSlice(&keys_payload, &tmp_key);
        cf_ids.push_back(tmp_cfid);
        Slice s(tmp_key);
        PinnableSlice ps;
        ps.PinSelf(s);
        multiget_keys.push_back(std::move(ps));
      }

      if (record != nullptr) {
        record->reset(new MultiGetQueryTraceRecord(
            std::move(cf_ids), std::move(multiget_keys), trace->ts));
      }

      return Status::OK();
    }
    default:
      return Status::NotSupported("Unsupported trace type.");
  }
}

Tracer::Tracer(SystemClock* clock, const TraceOptions& trace_options,
               std::unique_ptr<TraceWriter>&& trace_writer)
    : clock_(clock),
      trace_options_(trace_options),
      trace_writer_(std::move(trace_writer)),
      trace_request_count_(0) {
  // TODO: What if this fails?
  WriteHeader().PermitUncheckedError();
}

Tracer::~Tracer() { trace_writer_.reset(); }

Status Tracer::Write(WriteBatch* write_batch) {
  TraceType trace_type = kTraceWrite;
  if (ShouldSkipTrace(trace_type)) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = trace_type;
  TracerHelper::SetPayloadMap(trace.payload_map,
                              TracePayloadType::kWriteBatchData);
  PutFixed64(&trace.payload, trace.payload_map);
  PutLengthPrefixedSlice(&trace.payload, Slice(write_batch->Data()));
  return WriteTrace(trace);
}

Status Tracer::Get(ColumnFamilyHandle* column_family, const Slice& key) {
  TraceType trace_type = kTraceGet;
  if (ShouldSkipTrace(trace_type)) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = trace_type;
  // Set the payloadmap of the struct member that will be encoded in the
  // payload.
  TracerHelper::SetPayloadMap(trace.payload_map, TracePayloadType::kGetCFID);
  TracerHelper::SetPayloadMap(trace.payload_map, TracePayloadType::kGetKey);
  // Encode the Get struct members into payload. Make sure add them in order.
  PutFixed64(&trace.payload, trace.payload_map);
  PutFixed32(&trace.payload, column_family->GetID());
  PutLengthPrefixedSlice(&trace.payload, key);
  return WriteTrace(trace);
}

Status Tracer::IteratorSeek(const uint32_t& cf_id, const Slice& key,
                            const Slice& lower_bound, const Slice upper_bound) {
  TraceType trace_type = kTraceIteratorSeek;
  if (ShouldSkipTrace(trace_type)) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = trace_type;
  // Set the payloadmap of the struct member that will be encoded in the
  // payload.
  TracerHelper::SetPayloadMap(trace.payload_map, TracePayloadType::kIterCFID);
  TracerHelper::SetPayloadMap(trace.payload_map, TracePayloadType::kIterKey);
  if (lower_bound.size() > 0) {
    TracerHelper::SetPayloadMap(trace.payload_map,
                                TracePayloadType::kIterLowerBound);
  }
  if (upper_bound.size() > 0) {
    TracerHelper::SetPayloadMap(trace.payload_map,
                                TracePayloadType::kIterUpperBound);
  }
  // Encode the Iterator struct members into payload. Make sure add them in
  // order.
  PutFixed64(&trace.payload, trace.payload_map);
  PutFixed32(&trace.payload, cf_id);
  PutLengthPrefixedSlice(&trace.payload, key);
  if (lower_bound.size() > 0) {
    PutLengthPrefixedSlice(&trace.payload, lower_bound);
  }
  if (upper_bound.size() > 0) {
    PutLengthPrefixedSlice(&trace.payload, upper_bound);
  }
  return WriteTrace(trace);
}

Status Tracer::IteratorSeekForPrev(const uint32_t& cf_id, const Slice& key,
                                   const Slice& lower_bound,
                                   const Slice upper_bound) {
  TraceType trace_type = kTraceIteratorSeekForPrev;
  if (ShouldSkipTrace(trace_type)) {
    return Status::OK();
  }
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = trace_type;
  // Set the payloadmap of the struct member that will be encoded in the
  // payload.
  TracerHelper::SetPayloadMap(trace.payload_map, TracePayloadType::kIterCFID);
  TracerHelper::SetPayloadMap(trace.payload_map, TracePayloadType::kIterKey);
  if (lower_bound.size() > 0) {
    TracerHelper::SetPayloadMap(trace.payload_map,
                                TracePayloadType::kIterLowerBound);
  }
  if (upper_bound.size() > 0) {
    TracerHelper::SetPayloadMap(trace.payload_map,
                                TracePayloadType::kIterUpperBound);
  }
  // Encode the Iterator struct members into payload. Make sure add them in
  // order.
  PutFixed64(&trace.payload, trace.payload_map);
  PutFixed32(&trace.payload, cf_id);
  PutLengthPrefixedSlice(&trace.payload, key);
  if (lower_bound.size() > 0) {
    PutLengthPrefixedSlice(&trace.payload, lower_bound);
  }
  if (upper_bound.size() > 0) {
    PutLengthPrefixedSlice(&trace.payload, upper_bound);
  }
  return WriteTrace(trace);
}

Status Tracer::MultiGet(const size_t num_keys,
                        ColumnFamilyHandle** column_families,
                        const Slice* keys) {
  if (num_keys == 0) {
    return Status::OK();
  }
  std::vector<ColumnFamilyHandle*> v_column_families;
  std::vector<Slice> v_keys;
  v_column_families.resize(num_keys);
  v_keys.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    v_column_families[i] = column_families[i];
    v_keys[i] = keys[i];
  }
  return MultiGet(v_column_families, v_keys);
}

Status Tracer::MultiGet(const size_t num_keys,
                        ColumnFamilyHandle* column_family, const Slice* keys) {
  if (num_keys == 0) {
    return Status::OK();
  }
  std::vector<ColumnFamilyHandle*> column_families;
  std::vector<Slice> v_keys;
  column_families.resize(num_keys);
  v_keys.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    column_families[i] = column_family;
    v_keys[i] = keys[i];
  }
  return MultiGet(column_families, v_keys);
}

Status Tracer::MultiGet(const std::vector<ColumnFamilyHandle*>& column_families,
                        const std::vector<Slice>& keys) {
  if (column_families.size() != keys.size()) {
    return Status::Corruption("the CFs size and keys size does not match!");
  }
  TraceType trace_type = kTraceMultiGet;
  if (ShouldSkipTrace(trace_type)) {
    return Status::OK();
  }
  uint32_t multiget_size = static_cast<uint32_t>(keys.size());
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = trace_type;
  // Set the payloadmap of the struct member that will be encoded in the
  // payload.
  TracerHelper::SetPayloadMap(trace.payload_map,
                              TracePayloadType::kMultiGetSize);
  TracerHelper::SetPayloadMap(trace.payload_map,
                              TracePayloadType::kMultiGetCFIDs);
  TracerHelper::SetPayloadMap(trace.payload_map,
                              TracePayloadType::kMultiGetKeys);
  // Encode the CFIDs inorder
  std::string cfids_payload;
  std::string keys_payload;
  for (uint32_t i = 0; i < multiget_size; i++) {
    assert(i < column_families.size());
    assert(i < keys.size());
    PutFixed32(&cfids_payload, column_families[i]->GetID());
    PutLengthPrefixedSlice(&keys_payload, keys[i]);
  }
  // Encode the Get struct members into payload. Make sure add them in order.
  PutFixed64(&trace.payload, trace.payload_map);
  PutFixed32(&trace.payload, multiget_size);
  PutLengthPrefixedSlice(&trace.payload, cfids_payload);
  PutLengthPrefixedSlice(&trace.payload, keys_payload);
  return WriteTrace(trace);
}

bool Tracer::ShouldSkipTrace(const TraceType& trace_type) {
  if (IsTraceFileOverMax()) {
    return true;
  }

  TraceFilterType filter_mask = kTraceFilterNone;
  switch (trace_type) {
    case kTraceNone:
    case kTraceBegin:
    case kTraceEnd:
      filter_mask = kTraceFilterNone;
      break;
    case kTraceWrite:
      filter_mask = kTraceFilterWrite;
      break;
    case kTraceGet:
      filter_mask = kTraceFilterGet;
      break;
    case kTraceIteratorSeek:
      filter_mask = kTraceFilterIteratorSeek;
      break;
    case kTraceIteratorSeekForPrev:
      filter_mask = kTraceFilterIteratorSeekForPrev;
      break;
    case kBlockTraceIndexBlock:
    case kBlockTraceFilterBlock:
    case kBlockTraceDataBlock:
    case kBlockTraceUncompressionDictBlock:
    case kBlockTraceRangeDeletionBlock:
    case kIOTracer:
      filter_mask = kTraceFilterNone;
      break;
    case kTraceMultiGet:
      filter_mask = kTraceFilterMultiGet;
      break;
    case kTraceMax:
      assert(false);
      filter_mask = kTraceFilterNone;
      break;
  }
  if (filter_mask != kTraceFilterNone && trace_options_.filter & filter_mask) {
    return true;
  }

  ++trace_request_count_;
  if (trace_request_count_ < trace_options_.sampling_frequency) {
    return true;
  }
  trace_request_count_ = 0;
  return false;
}

bool Tracer::IsTraceFileOverMax() {
  uint64_t trace_file_size = trace_writer_->GetFileSize();
  return (trace_file_size > trace_options_.max_trace_file_size);
}

Status Tracer::WriteHeader() {
  std::ostringstream s;
  s << kTraceMagic << "\t"
    << "Trace Version: " << kTraceFileMajorVersion << "."
    << kTraceFileMinorVersion << "\t"
    << "RocksDB Version: " << kMajorVersion << "." << kMinorVersion << "\t"
    << "Format: Timestamp OpType Payload\n";
  std::string header(s.str());

  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = kTraceBegin;
  trace.payload = header;
  return WriteTrace(trace);
}

Status Tracer::WriteFooter() {
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = kTraceEnd;
  TracerHelper::SetPayloadMap(trace.payload_map,
                              TracePayloadType::kEmptyPayload);
  trace.payload = "";
  return WriteTrace(trace);
}

Status Tracer::WriteTrace(const Trace& trace) {
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return trace_writer_->Write(Slice(encoded_trace));
}

Status Tracer::Close() { return WriteFooter(); }

}  // namespace ROCKSDB_NAMESPACE
