//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/trace_replay.h"

#include <chrono>
#include <cmath>
#include <sstream>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "rocksdb/env.h"
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

bool TracerHelper::SetPayloadMap(uint64_t& payload_map,
                                 const TracePayloadType payload_type) {
  uint64_t old_state = payload_map;
  uint64_t tmp = 1;
  payload_map |= (tmp << payload_type);
  return old_state != payload_map;
}

void TracerHelper::DecodeWritePayload(Trace* trace,
                                      WritePayload* write_payload) {
  assert(write_payload != nullptr);
  Slice buf(trace->payload);
  GetFixed64(&buf, &trace->payload_map);
  int64_t payload_map = static_cast<int64_t>(trace->payload_map);
  while (payload_map) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(payload_map & -payload_map));
    switch (set_pos) {
      case TracePayloadType::kWriteBatchData:
        GetLengthPrefixedSlice(&buf, &(write_payload->write_batch_data));
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    payload_map &= (payload_map - 1);
  }
}

void TracerHelper::DecodeGetPayload(Trace* trace, GetPayload* get_payload) {
  assert(get_payload != nullptr);
  Slice buf(trace->payload);
  GetFixed64(&buf, &trace->payload_map);
  int64_t payload_map = static_cast<int64_t>(trace->payload_map);
  while (payload_map) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(payload_map & -payload_map));
    switch (set_pos) {
      case TracePayloadType::kGetCFID:
        GetFixed32(&buf, &(get_payload->cf_id));
        break;
      case TracePayloadType::kGetKey:
        GetLengthPrefixedSlice(&buf, &(get_payload->get_key));
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    payload_map &= (payload_map - 1);
  }
}

void TracerHelper::DecodeIterPayload(Trace* trace, IterPayload* iter_payload) {
  assert(iter_payload != nullptr);
  Slice buf(trace->payload);
  GetFixed64(&buf, &trace->payload_map);
  int64_t payload_map = static_cast<int64_t>(trace->payload_map);
  while (payload_map) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(payload_map & -payload_map));
    switch (set_pos) {
      case TracePayloadType::kIterCFID:
        GetFixed32(&buf, &(iter_payload->cf_id));
        break;
      case TracePayloadType::kIterKey:
        GetLengthPrefixedSlice(&buf, &(iter_payload->iter_key));
        break;
      case TracePayloadType::kIterLowerBound:
        GetLengthPrefixedSlice(&buf, &(iter_payload->lower_bound));
        break;
      case TracePayloadType::kIterUpperBound:
        GetLengthPrefixedSlice(&buf, &(iter_payload->upper_bound));
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    payload_map &= (payload_map - 1);
  }
}

void TracerHelper::DecodeMultiGetPayload(Trace* trace,
                                         MultiGetPayload* multiget_payload) {
  assert(multiget_payload != nullptr);
  Slice cfids_payload;
  Slice keys_payload;
  Slice buf(trace->payload);
  GetFixed64(&buf, &trace->payload_map);
  int64_t payload_map = static_cast<int64_t>(trace->payload_map);
  while (payload_map) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(payload_map & -payload_map));
    switch (set_pos) {
      case TracePayloadType::kMultiGetSize:
        GetFixed32(&buf, &(multiget_payload->multiget_size));
        break;
      case TracePayloadType::kMultiGetCFIDs:
        GetLengthPrefixedSlice(&buf, &cfids_payload);
        break;
      case TracePayloadType::kMultiGetKeys:
        GetLengthPrefixedSlice(&buf, &keys_payload);
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    payload_map &= (payload_map - 1);
  }

  // Decode the cfids_payload and keys_payload
  multiget_payload->cf_ids.reserve(multiget_payload->multiget_size);
  multiget_payload->multiget_keys.reserve(multiget_payload->multiget_size);
  for (uint32_t i = 0; i < multiget_payload->multiget_size; i++) {
    uint32_t tmp_cfid;
    Slice tmp_key;
    GetFixed32(&cfids_payload, &tmp_cfid);
    GetLengthPrefixedSlice(&keys_payload, &tmp_key);
    multiget_payload->cf_ids.push_back(tmp_cfid);
    multiget_payload->multiget_keys.push_back(tmp_key.ToString());
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
  if ((trace_options_.filter & kTraceFilterGet && trace_type == kTraceGet) ||
      (trace_options_.filter & kTraceFilterWrite &&
       trace_type == kTraceWrite)) {
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

ReplayerImpl::ReplayerImpl(DBImpl* db,
                           const std::vector<ColumnFamilyHandle*>& handles,
                           std::unique_ptr<TraceReader>&& reader)
    : Replayer(),
      trace_reader_(std::move(reader)),
      prepared_(false),
      trace_end_(false),
      header_ts_(0) {
  assert(db != nullptr);
  db_ = db;
  env_ = Env::Default();
  for (ColumnFamilyHandle* cfh : handles) {
    cf_map_[cfh->GetID()] = cfh;
  }
}

ReplayerImpl::~ReplayerImpl() { trace_reader_.reset(); }

Status ReplayerImpl::Prepare() {
  Trace header;
  int db_version;
  Status s = ReadHeader(&header);
  if (!s.ok()) {
    return s;
  }
  s = TracerHelper::ParseTraceHeader(header, &trace_file_version_, &db_version);
  if (!s.ok()) {
    return s;
  }
  header_ts_ = header.ts;
  prepared_ = true;
  trace_end_ = false;
  return Status::OK();
}

Status ReplayerImpl::NextTraceRecord(std::unique_ptr<TraceRecord>* record) {
  if (!prepared_) {
    return Status::Incomplete("Not prepared!");
  }
  if (trace_end_) {
    return Status::Incomplete("Trace end.");
  }

  Trace trace;
  Status s = ReadTrace(&trace);  // ReadTrace is atomic
  // Reached the trace end.
  if (s.ok() && trace.type == kTraceEnd) {
    trace_end_ = true;
    return Status::Incomplete("Trace end.");
  }
  if (!s.ok() || record == nullptr) {
    return s;
  }

  return ToTraceRecord(&trace, &cf_map_, trace_file_version_, record);
}

Status ReplayerImpl::Execute(std::unique_ptr<TraceRecord>&& record) {
  return ExecuteTrace(db_, std::move(record));
}

Status ReplayerImpl::Replay(const ReplayOptions& options) {
  if (options.fast_forward <= 0.0) {
    return Status::InvalidArgument("Wrong fast forward speed!");
  }

  if (!prepared_) {
    return Status::Incomplete("Not prepared!");
  }
  if (trace_end_) {
    return Status::Incomplete("Trace end.");
  }

  Status s = Status::OK();
  ThreadPoolImpl* thread_pool = nullptr;
  // num_threads == 0 or num_threads == 1 uses single thread.
  if (options.num_threads > 1) {
    thread_pool = new ThreadPoolImpl();
    thread_pool->SetHostEnv(env_);
    thread_pool->SetBackgroundThreads(static_cast<int>(options.num_threads));
  }

  std::chrono::system_clock::time_point replay_epoch =
      std::chrono::system_clock::now();

  while (s.ok() || s.IsNotSupported()) {
    Trace trace;
    s = ReadTrace(&trace);
    // If already at trace end, ReadTrace should return Status::Incomplete().
    if (!s.ok()) {
      break;
    }

    TraceType trace_type = trace.type;

    // No need to sleep before breaking the loop if at the trace end.
    if (trace_type == kTraceEnd) {
      trace_end_ = true;
      s = Status::Incomplete("Trace end.");
      break;
    }

    std::this_thread::sleep_until(
        replay_epoch +
        std::chrono::microseconds(static_cast<uint64_t>(
            llround(1.0 * (trace.ts - header_ts_) / options.fast_forward))));

    if (thread_pool) {
      std::unique_ptr<ReplayerWorkerArg> ra(new ReplayerWorkerArg);
      ra->db = db_;
      ra->trace_entry = std::move(trace);
      ra->cf_map = &cf_map_;
      ra->trace_file_version = trace_file_version_;

      switch (trace_type) {
        case kTraceGet:
        case kTraceWrite:
        case kTraceIteratorSeek:
        case kTraceIteratorSeekForPrev:
        case kTraceMultiGet:
          thread_pool->Schedule(&ReplayerImpl::BackgroundWork, ra.release(),
                                nullptr, nullptr);
          break;
        default:
          s = Status::NotSupported("Unsupported trace type.");
          break;
      }
    } else {
      std::unique_ptr<TraceRecord> record;
      s = ToTraceRecord(&trace, &cf_map_, trace_file_version_, &record);
      if (s.ok()) {
        s = ExecuteTrace(db_, std::move(record));
      }
    }
  }

  if (thread_pool) {
    thread_pool->JoinAllThreads();
    delete thread_pool;
  }

  if (s.IsIncomplete() && trace_end_) {
    // Reaching eof returns Incomplete status at the moment.
    // Could happen when killing a process without calling EndTrace() API.
    // TODO: Add better error handling.
    return Status::OK();
  }
  return s;
}

uint64_t ReplayerImpl::GetHeaderTimestamp() const { return header_ts_; }

Status ReplayerImpl::ReadHeader(Trace* header) {
  assert(header != nullptr);
  Status s = trace_reader_->Reset();
  if (!s.ok()) {
    return s;
  }
  std::string encoded_trace;
  // Read the trace head
  s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  s = TracerHelper::DecodeTrace(encoded_trace, header);

  if (header->type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header->payload.substr(0, kTraceMagic.length()) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

Status ReplayerImpl::ReadFooter(Trace* footer) {
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

Status ReplayerImpl::ReadTrace(Trace* trace) {
  assert(trace != nullptr);
  std::string encoded_trace;
  // We don't know if TraceReader is implemented thread-safe, so we protect the
  // reading trace part with a mutex. The decoding part does not need to be
  // protected since it's local.
  {
    std::lock_guard<std::mutex> guard(mutex_);
    Status s = trace_reader_->Read(&encoded_trace);
    if (!s.ok()) {
      return s;
    }
  }
  return TracerHelper::DecodeTrace(encoded_trace, trace);
}

Status ReplayerImpl::ToTraceRecord(
    Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
    int trace_file_version, std::unique_ptr<TraceRecord>* record) {
  switch (trace->type) {
    case kTraceWrite:
      return ToWriteTraceRecord(trace, cf_map, trace_file_version, record);
    case kTraceGet:
      return ToGetTraceRecord(trace, cf_map, trace_file_version, record);
    case kTraceIteratorSeek:
    case kTraceIteratorSeekForPrev:
      return ToIterSeekTraceRecord(trace, cf_map, trace_file_version, record);
    case kTraceMultiGet:
      return ToMultiGetTraceRecord(trace, cf_map, trace_file_version, record);
    case kTraceEnd:
      return Status::Incomplete("Trace end.");
    default:
      return Status::NotSupported("Unsupported trace type.");
  }
}

Status ReplayerImpl::ExecuteTrace(DB* db,
                                  std::unique_ptr<TraceRecord>&& record) {
  switch (record->GetType()) {
    case kTraceWrite:
      return ExecuteWriteTrace(db, std::move(record));
    case kTraceGet:
      return ExecuteGetTrace(db, std::move(record));
    case kTraceIteratorSeek:
    case kTraceIteratorSeekForPrev:
      return ExecuteIterSeekTrace(db, std::move(record));
    case kTraceMultiGet:
      return ExecuteMultiGetTrace(db, std::move(record));
    case kTraceEnd:
      return Status::Incomplete("Trace end.");
    default:
      return Status::NotSupported("Unsupported trace type.");
  }
}

void ReplayerImpl::BackgroundWork(void* arg) {
  std::unique_ptr<ReplayerWorkerArg> ra(
      reinterpret_cast<ReplayerWorkerArg*>(arg));
  assert(ra != nullptr);

  std::unique_ptr<TraceRecord> record;
  Status s = ToTraceRecord(&(ra->trace_entry), ra->cf_map,
                           ra->trace_file_version, &record);
  if (s.ok()) {
    ExecuteTrace(ra->db, std::move(record)).PermitUncheckedError();
  }
}

Status ReplayerImpl::ToWriteTraceRecord(
    Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* /*cf_map*/,
    int trace_file_version, std::unique_ptr<TraceRecord>* record) {
  assert(trace != nullptr);
  assert(trace->type == kTraceWrite);
  if (record == nullptr) {
    return Status::OK();
  }

  // To do: is there any validation to convert a payload string to a WriteBatch?
  if (record != nullptr) {
    WriteQueryTraceRecord* r = new WriteQueryTraceRecord(trace->ts);

    if (trace_file_version < 2) {
      r->batch = WriteBatch(trace->payload);
    } else {
      WritePayload w_payload;
      TracerHelper::DecodeWritePayload(trace, &w_payload);
      r->batch = WriteBatch(w_payload.write_batch_data.ToString());
    }
    record->reset(r);
  }
  return Status::OK();
}

Status ReplayerImpl::ToGetTraceRecord(
    Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
    int trace_file_version, std::unique_ptr<TraceRecord>* record) {
  assert(trace != nullptr);
  assert(trace->type == kTraceGet);
  GetPayload get_payload;
  get_payload.cf_id = 0;
  if (trace_file_version < 2) {
    DecodeCFAndKey(trace->payload, &get_payload.cf_id, &get_payload.get_key);
  } else {
    TracerHelper::DecodeGetPayload(trace, &get_payload);
  }
  if (get_payload.cf_id > 0 &&
      cf_map->find(get_payload.cf_id) == cf_map->end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }

  if (record != nullptr) {
    GetQueryTraceRecord* r = new GetQueryTraceRecord(trace->ts);
    r->handle = (*cf_map)[get_payload.cf_id];
    r->key = get_payload.get_key;
    record->reset(r);
  }
  return Status::OK();
}

Status ReplayerImpl::ToIterSeekTraceRecord(
    Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
    int trace_file_version, std::unique_ptr<TraceRecord>* record) {
  assert(trace != nullptr);
  assert(trace->type == kTraceIteratorSeek ||
         trace->type == kTraceIteratorSeekForPrev);
  IterPayload iter_payload;
  iter_payload.cf_id = 0;

  if (trace_file_version < 2) {
    DecodeCFAndKey(trace->payload, &iter_payload.cf_id, &iter_payload.iter_key);
  } else {
    TracerHelper::DecodeIterPayload(trace, &iter_payload);
  }
  if (iter_payload.cf_id > 0 &&
      cf_map->find(iter_payload.cf_id) == cf_map->end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }

  if (record != nullptr) {
    IteratorSeekQueryTraceRecord* r =
        new IteratorSeekQueryTraceRecord(trace->ts);
    r->seekType =
        static_cast<IteratorSeekQueryTraceRecord::SeekType>(trace->type);
    r->handle = (*cf_map)[iter_payload.cf_id];
    r->key = iter_payload.iter_key;
    record->reset(r);
  }
  return Status::OK();
}

Status ReplayerImpl::ToMultiGetTraceRecord(
    Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
    int trace_file_version, std::unique_ptr<TraceRecord>* record) {
  assert(trace != nullptr);
  assert(trace->type == kTraceMultiGet);
  MultiGetPayload multiget_payload;
  if (trace_file_version < 2) {
    return Status::Corruption("MultiGet is not supported.");
  }

  TracerHelper::DecodeMultiGetPayload(trace, &multiget_payload);

  if (static_cast<uint32_t>(multiget_payload.cf_ids.size()) !=
          multiget_payload.multiget_size ||
      static_cast<uint32_t>(multiget_payload.multiget_keys.size()) !=
          multiget_payload.multiget_size) {
    return Status::Corruption("MultiGet size mismatch.");
  }

  std::vector<ColumnFamilyHandle*> handles;
  handles.reserve(multiget_payload.multiget_size);
  for (uint32_t cf_id : multiget_payload.cf_ids) {
    if (cf_id > 0 && cf_map->find(cf_id) == cf_map->end()) {
      return Status::Corruption("Invalid Column Family ID.");
    }
    handles.push_back((*cf_map)[cf_id]);
  }

  if (record != nullptr) {
    MultiGetQueryTraceRecord* r = new MultiGetQueryTraceRecord(trace->ts);
    r->handles = std::move(handles);
    r->keys.assign(multiget_payload.multiget_keys.begin(),
                   multiget_payload.multiget_keys.end());
    record->reset(r);
  }
  return Status::OK();
}

Status ReplayerImpl::ExecuteWriteTrace(DB* db,
                                       std::unique_ptr<TraceRecord>&& record) {
  std::unique_ptr<WriteQueryTraceRecord> r(
      reinterpret_cast<WriteQueryTraceRecord*>(record.release()));
  assert(r != nullptr);

  return db->Write(WriteOptions(), &(r->batch));
}

Status ReplayerImpl::ExecuteGetTrace(DB* db,
                                     std::unique_ptr<TraceRecord>&& record) {
  std::unique_ptr<GetQueryTraceRecord> r(
      reinterpret_cast<GetQueryTraceRecord*>(record.release()));
  assert(r != nullptr);
  assert(r->handle != nullptr);

  std::string value;
  Status s = db->Get(ReadOptions(), r->handle, r->key, &value);

  // Treat not found as ok and return other errors.
  return s.IsNotFound() ? Status::OK() : s;
}

Status ReplayerImpl::ExecuteIterSeekTrace(
    DB* db, std::unique_ptr<TraceRecord>&& record) {
  std::unique_ptr<IteratorSeekQueryTraceRecord> r(
      reinterpret_cast<IteratorSeekQueryTraceRecord*>(record.release()));
  assert(r != nullptr);
  assert(r->handle != nullptr);

  Iterator* single_iter = db->NewIterator(ReadOptions(), r->handle);

  switch (r->seekType) {
    case IteratorSeekQueryTraceRecord::kSeekForPrev: {
      single_iter->SeekForPrev(r->key);
      break;
    }
    default: {
      single_iter->Seek(r->key);
      break;
    }
  }
  Status s = single_iter->status();
  delete single_iter;
  return s;
}

Status ReplayerImpl::ExecuteMultiGetTrace(
    DB* db, std::unique_ptr<TraceRecord>&& record) {
  std::unique_ptr<MultiGetQueryTraceRecord> r(
      reinterpret_cast<MultiGetQueryTraceRecord*>(record.release()));
  assert(r != nullptr);
  assert(!r->handles.empty());
  assert(!r->keys.empty());

  std::vector<std::string> values;
  std::vector<Status> ss =
      db->MultiGet(ReadOptions(), r->handles, r->keys, &values);

  // Treat not found as ok, return other errors.
  for (Status s : ss) {
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
