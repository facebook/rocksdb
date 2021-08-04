//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/replayer.h"
#include "util/threadpool_imp.h"

namespace ROCKSDB_NAMESPACE {

// This file contains Tracer and Replayer classes that enable capturing and
// replaying RocksDB traces.

class ColumnFamilyHandle;
class ColumnFamilyData;
class DB;
class DBImpl;
class Env;
class Slice;
class SystemClock;
class TraceReader;
class TraceWriter;
class WriteBatch;

struct ReadOptions;
struct TraceOptions;
struct WriteOptions;

extern const std::string kTraceMagic;
const unsigned int kTraceTimestampSize = 8;
const unsigned int kTraceTypeSize = 1;
const unsigned int kTracePayloadLengthSize = 4;
const unsigned int kTraceMetadataSize =
    kTraceTimestampSize + kTraceTypeSize + kTracePayloadLengthSize;

static const int kTraceFileMajorVersion = 0;
static const int kTraceFileMinorVersion = 2;

// TODO: This should also be made part of public interface to help users build
// custom TracerReaders and TraceWriters.
//
// The data structure that defines a single trace.
struct Trace {
  uint64_t ts;  // timestamp
  TraceType type;
  // Each bit in payload_map stores which corresponding struct member added in
  // the payload. Each TraceType has its corresponding payload struct. For
  // example, if bit at position 0 is set in write payload, then the write batch
  // will be addedd.
  uint64_t payload_map = 0;
  // Each trace type has its own payload_struct, which will be serilized in the
  // payload.
  std::string payload;

  void reset() {
    ts = 0;
    type = kTraceMax;
    payload_map = 0;
    payload.clear();
  }
};

enum TracePayloadType : char {
  // Each member of all query payload structs should have a corresponding flag
  // here. Make sure to add them sequentially in the order of it is added.
  kEmptyPayload = 0,
  kWriteBatchData = 1,
  kGetCFID = 2,
  kGetKey = 3,
  kIterCFID = 4,
  kIterKey = 5,
  kIterLowerBound = 6,
  kIterUpperBound = 7,
  kMultiGetSize = 8,
  kMultiGetCFIDs = 9,
  kMultiGetKeys = 10,
};

struct WritePayload {
  Slice write_batch_data;
};

struct GetPayload {
  uint32_t cf_id = 0;
  Slice get_key;
};

struct IterPayload {
  uint32_t cf_id = 0;
  Slice iter_key;
  Slice lower_bound;
  Slice upper_bound;
};

struct MultiGetPayload {
  uint32_t multiget_size;
  std::vector<uint32_t> cf_ids;
  std::vector<std::string> multiget_keys;
};

class TracerHelper {
 public:
  // Parse the string with major and minor version only
  static Status ParseVersionStr(std::string& v_string, int* v_num);

  // Parse the trace file version and db version in trace header
  static Status ParseTraceHeader(const Trace& header, int* trace_version,
                                 int* db_version);

  // Encode a version 0.1 trace object into the given string.
  static void EncodeTrace(const Trace& trace, std::string* encoded_trace);

  // Decode a string into the given trace object.
  static Status DecodeTrace(const std::string& encoded_trace, Trace* trace);

  // Set the payload map based on the payload type
  static bool SetPayloadMap(uint64_t& payload_map,
                            const TracePayloadType payload_type);

  // Decode the write payload and store in WrteiPayload
  static void DecodeWritePayload(Trace* trace, WritePayload* write_payload);

  // Decode the get payload and store in WrteiPayload
  static void DecodeGetPayload(Trace* trace, GetPayload* get_payload);

  // Decode the iter payload and store in WrteiPayload
  static void DecodeIterPayload(Trace* trace, IterPayload* iter_payload);

  // Decode the multiget payload and store in MultiGetPayload
  static void DecodeMultiGetPayload(Trace* trace,
                                    MultiGetPayload* multiget_payload);
};

// Tracer captures all RocksDB operations using a user-provided TraceWriter.
// Every RocksDB operation is written as a single trace. Each trace will have a
// timestamp and type, followed by the trace payload.
class Tracer {
 public:
  Tracer(SystemClock* clock, const TraceOptions& trace_options,
         std::unique_ptr<TraceWriter>&& trace_writer);
  ~Tracer();

  // Trace all write operations -- Put, Merge, Delete, SingleDelete, Write
  Status Write(WriteBatch* write_batch);

  // Trace Get operations.
  Status Get(ColumnFamilyHandle* cfname, const Slice& key);

  // Trace Iterators.
  Status IteratorSeek(const uint32_t& cf_id, const Slice& key,
                      const Slice& lower_bound, const Slice upper_bound);
  Status IteratorSeekForPrev(const uint32_t& cf_id, const Slice& key,
                             const Slice& lower_bound, const Slice upper_bound);

  // Trace MultiGet

  Status MultiGet(const size_t num_keys, ColumnFamilyHandle** column_families,
                  const Slice* keys);

  Status MultiGet(const size_t num_keys, ColumnFamilyHandle* column_family,
                  const Slice* keys);

  Status MultiGet(const std::vector<ColumnFamilyHandle*>& column_family,
                  const std::vector<Slice>& keys);

  // Returns true if the trace is over the configured max trace file limit.
  // False otherwise.
  bool IsTraceFileOverMax();

  // Writes a trace footer at the end of the tracing
  Status Close();

 private:
  // Write a trace header at the beginning, typically on initiating a trace,
  // with some metadata like a magic number, trace version, RocksDB version, and
  // trace format.
  Status WriteHeader();

  // Write a trace footer, typically on ending a trace, with some metadata.
  Status WriteFooter();

  // Write a single trace using the provided TraceWriter to the underlying
  // system, say, a filesystem or a streaming service.
  Status WriteTrace(const Trace& trace);

  // Helps in filtering and sampling of traces.
  // Returns true if a trace should be skipped, false otherwise.
  bool ShouldSkipTrace(const TraceType& type);

  SystemClock* clock_;
  TraceOptions trace_options_;
  std::unique_ptr<TraceWriter> trace_writer_;
  uint64_t trace_request_count_;
};

#ifndef ROCKSDB_LITE

class ReplayerImpl : public Replayer {
 public:
  ReplayerImpl(DBImpl* db, const std::vector<ColumnFamilyHandle*>& handles,
               std::unique_ptr<TraceReader>&& reader);
  ~ReplayerImpl() override;

  using Replayer::Prepare;
  Status Prepare() override;

  using Replayer::NextTraceRecord;
  Status NextTraceRecord(std::unique_ptr<TraceRecord>* record) override;

  using Replayer::Execute;
  Status Execute(std::unique_ptr<TraceRecord>&& record) override;

  using Replayer::Replay;
  Status Replay(const ReplayOptions& options) override;

  using Replayer::GetHeaderTimestamp;
  uint64_t GetHeaderTimestamp() const override;

 private:
  Status ReadHeader(Trace* header);
  Status ReadFooter(Trace* footer);
  Status ReadTrace(Trace* trace);

  // General function to convert a Trace to TraceRecord.
  static Status ToTraceRecord(
      Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
      int trace_file_version, std::unique_ptr<TraceRecord>* record);

  // General function to execute a TraceRecord.
  static Status ExecuteTrace(DB* db, std::unique_ptr<TraceRecord>&& record);

  // General function to execute a Trace in a thread pool.
  static void BackgroundWork(void* arg);

  // Functions to convert to the corresponding TraceRecord.
  // TraceRecord for DB::Write().
  static Status ToWriteTraceRecord(
      Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
      int trace_file_version, std::unique_ptr<TraceRecord>* record);
  // TraceRecord for DB::Get().
  static Status ToGetTraceRecord(
      Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
      int trace_file_version, std::unique_ptr<TraceRecord>* record);
  // TraceRecord for Iterator::Seek().
  static Status ToIterSeekTraceRecord(
      Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
      int trace_file_version, std::unique_ptr<TraceRecord>* record);
  // TraceRecord for DB::MultiGet().
  static Status ToMultiGetTraceRecord(
      Trace* trace, std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map,
      int trace_file_version, std::unique_ptr<TraceRecord>* record);

  // Functions to execute a trace record depending on its type.
  // Execute a WriteQueryTraceRecord (Put, Delete, SingleDelete and DeleteRange
  // as a WriteBatch).
  static Status ExecuteWriteTrace(DB* db,
                                  std::unique_ptr<TraceRecord>&& record);
  // Execute a GetQueryTraceRecord.
  static Status ExecuteGetTrace(DB* db, std::unique_ptr<TraceRecord>&& record);
  // Execute an IteratorSeekQueryTraceRecord.
  static Status ExecuteIterSeekTrace(DB* db,
                                     std::unique_ptr<TraceRecord>&& record);
  // Execute a MultiGetQueryTraceRecord.
  static Status ExecuteMultiGetTrace(DB* db,
                                     std::unique_ptr<TraceRecord>&& record);

  DBImpl* db_;
  Env* env_;
  std::unique_ptr<TraceReader> trace_reader_;
  std::unordered_map<uint32_t, ColumnFamilyHandle*> cf_map_;
  // When reading the trace header, the trace file version can be parsed.
  // Replayer will use different decode method to get the trace content based
  // on different trace file version.
  int trace_file_version_;
  std::mutex mutex_;
  bool prepared_;
  uint64_t header_ts_;
};

// The passin arg of MultiThreadRepkay for each trace record.
struct ReplayerWorkerArg {
  DB* db;
  Trace trace_entry;
  std::unordered_map<uint32_t, ColumnFamilyHandle*>* cf_map;
  int trace_file_version;
};

#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
