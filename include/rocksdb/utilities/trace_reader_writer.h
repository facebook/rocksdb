//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env.h"

namespace rocksdb {

class TraceWriter {
 public:
  TraceWriter() {}
  virtual ~TraceWriter() {}

  virtual Status Write(const Slice& data) = 0;
  virtual Status Close() = 0;
};

class TraceReader {
 public:
  TraceReader() {}
  virtual ~TraceReader() {}

  virtual Status Read(std::string* data) = 0;
  virtual Status Close() = 0;
};

// Factory methods to read and write traces to a file.
Status NewFileTraceWriter(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceWriter>* trace_writer);
Status NewFileTraceReader(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceReader>* trace_reader);
}  // namespace rocksdb
