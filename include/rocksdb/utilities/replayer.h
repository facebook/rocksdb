//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <memory>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_record.h"

namespace ROCKSDB_NAMESPACE {

struct ReplayOptions {
  // Number of threads used for replaying. If 0 or 1, replay using
  // single thread.
  uint32_t num_threads;

  // Enables fast forwarding a replay by increasing/reducing the delay between
  // the ingested traces.
  //   If > 0.0 and < 1.0, slow down the replay by this amount.
  //   If 1.0, replay the operations at the same rate as in the trace stream.
  //   If > 1, speed up the replay by this amount.
  double fast_forward;

  ReplayOptions() : num_threads(1), fast_forward(1.0) {}
  ReplayOptions(uint32_t num_of_threads, double fast_forward_ratio)
      : num_threads(num_of_threads), fast_forward(fast_forward_ratio) {}
};

// Replayer helps to replay the captured RocksDB query level operations.
// The Replayer can either be created from DB::NewReplayer method, or be
// instantiated via db_bench today, on using "replay" benchmark.
class Replayer {
 public:
  virtual ~Replayer() {}

  // Make some preparation before replaying the trace. This will also reset the
  // replayer in order to restart replaying.
  virtual Status Prepare() = 0;

  // Return the timestamp when the trace recording was started.
  virtual uint64_t GetHeaderTimestamp() const = 0;

  // Atomically read one trace into a TraceRecord (excluding the header and
  // footer traces).
  // Return Status::OK() on success;
  // Status::Incomplete() if Prepare() was not called or no more available
  // trace;
  // Status::NotSupported() if the read trace type is not supported.
  virtual Status Next(std::unique_ptr<TraceRecord>* record) = 0;

  // Execute one TraceRecord.
  // Return Status::OK() if the execution was successful. Get/MultiGet traces
  // will still return Status::OK() even if they got Status::NotFound()
  // from DB::Get() or DB::MultiGet();
  // Status::Incomplete() if Prepare() was not called or no more available
  // trace;
  // Status::NotSupported() if the operation is not supported;
  // Otherwise, return the corresponding error status.
  virtual Status Execute(const std::unique_ptr<TraceRecord>& record) = 0;
  virtual Status Execute(std::unique_ptr<TraceRecord>&& record) = 0;

  // Replay all the traces from the provided trace stream, taking the delay
  // between the traces into consideration.
  virtual Status Replay(const ReplayOptions& options) = 0;
  virtual Status Replay() { return Replay(ReplayOptions()); }
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
