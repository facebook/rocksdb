//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/trace_reader_writer.h"

namespace rocksdb {

// This file contains Tracer and Replayer classes that enable capturing and
// replaying RocksDB traces.

class ColumnFamilyHandle;
class ColumnFamilyData;
class DB;
class DBImpl;
class Slice;
class WriteBatch;

extern const std::string kTraceMagic;
const unsigned int kTraceTimestampSize = 8;
const unsigned int kTraceTypeSize = 1;
const unsigned int kTracePayloadLengthSize = 4;
const unsigned int kTraceMetadataSize =
    kTraceTimestampSize + kTraceTypeSize + kTracePayloadLengthSize;

// Supported Trace types.
enum TraceType : char {
  kTraceBegin = 1,
  kTraceEnd = 2,
  kTraceWrite = 3,
  kTraceGet = 4,
  kTraceIteratorSeek = 5,
  kTraceIteratorSeekForPrev = 6,
  // Block cache related types.
  kBlockTraceIndexBlock = 7,
  kBlockTraceFilterBlock = 8,
  kBlockTraceDataBlock = 9,
  kBlockTraceUncompressionDictBlock = 10,
  kBlockTraceRangeDeletionBlock = 11,
  // All trace types should be added before kTraceMax
  kTraceMax,
};

// TODO: This should also be made part of public interface to help users build
// custom TracerReaders and TraceWriters.
//
// The data structure that defines a single trace.
struct Trace {
  uint64_t ts;  // timestamp
  TraceType type;
  std::string payload;

  void reset() {
    ts = 0;
    type = kTraceMax;
    payload.clear();
  }
};

class TracerHelper {
 public:
  // Encode a trace object into the given string.
  static void EncodeTrace(const Trace& trace, std::string* encoded_trace);

  // Decode a string into the given trace object.
  static Status DecodeTrace(const std::string& encoded_trace, Trace* trace);
};

// Tracer captures all RocksDB operations using a user-provided TraceWriter.
// Every RocksDB operation is written as a single trace. Each trace will have a
// timestamp and type, followed by the trace payload.
class Tracer {
 public:
  Tracer(Env* env, const TraceOptions& trace_options,
         std::unique_ptr<TraceWriter>&& trace_writer);
  ~Tracer();

  // Trace all write operations -- Put, Merge, Delete, SingleDelete, Write
  Status Write(WriteBatch* write_batch);

  // Trace Get operations.
  Status Get(ColumnFamilyHandle* cfname, const Slice& key);

  // Trace Iterators.
  Status IteratorSeek(const uint32_t& cf_id, const Slice& key);
  Status IteratorSeekForPrev(const uint32_t& cf_id, const Slice& key);

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

  Env* env_;
  TraceOptions trace_options_;
  std::unique_ptr<TraceWriter> trace_writer_;
  uint64_t trace_request_count_;
};

// Replayer helps to replay the captured RocksDB operations, using a user
// provided TraceReader.
// The Replayer is instantiated via db_bench today, on using "replay" benchmark.
class Replayer {
 public:
  Replayer(DB* db, const std::vector<ColumnFamilyHandle*>& handles,
           std::unique_ptr<TraceReader>&& reader);
  ~Replayer();

  // Replay all the traces from the provided trace stream, taking the delay
  // between the traces into consideration.
  Status Replay();

  // Enables fast forwarding a replay by reducing the delay between the ingested
  // traces.
  // fast_forward : Rate of replay speedup.
  //   If 1, replay the operations at the same rate as in the trace stream.
  //   If > 1, speed up the replay by this amount.
  Status SetFastForward(uint32_t fast_forward);

 private:
  Status ReadHeader(Trace* header);
  Status ReadFooter(Trace* footer);
  Status ReadTrace(Trace* trace);

  DBImpl* db_;
  std::unique_ptr<TraceReader> trace_reader_;
  std::unordered_map<uint32_t, ColumnFamilyHandle*> cf_map_;
  uint32_t fast_forward_;
};

}  // namespace rocksdb
