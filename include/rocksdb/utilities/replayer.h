//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <memory>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct ReplayOptions {
  uint32_t num_threads;
  double fast_forward;

  ReplayOptions() : num_threads(1), fast_forward(1.0) {}
  ReplayOptions(uint32_t num_of_threads, double fast_forward_ratio)
      : num_threads(num_of_threads), fast_forward(fast_forward_ratio) {}
};

// Replayer helps to replay the captured RocksDB operations.
// The Replayer can either be created from DB::NewReplayer method, or be
// instantiated via db_bench today, on using "replay" benchmark.
class Replayer {
 public:
  virtual ~Replayer() {}

  // Make some preparation before replaying the trace.
  virtual Status Prepare() { return Status::NotSupported(); }

  // Read one trace and execute it. This function is thread-safe. Trace
  // timestamps are ignored.
  // Return Status::OK() if the execution was successful (Get() and MultiGet()
  // will still return Status::OK() even if they returned Status::NotFound());
  // Status::Incomplete() if no more trace available;
  // Status::NotSupported() if the operation is not supported;
  // Otherwise, return the corresponding error.
  virtual Status Step() { return Status::NotSupported(); }

  // Replay all the traces from the provided trace stream, taking the delay
  // between the traces into consideration.
  virtual Status Replay(ReplayOptions /*options*/ = ReplayOptions()) {
    return Status::NotSupported();
  }
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
