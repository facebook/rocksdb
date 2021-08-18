//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_record.h"

namespace ROCKSDB_NAMESPACE {

class MultiValuesTraceExecutionResult;
class SingleValueTraceExecutionResult;
class StatusOnlyTraceExecutionResult;

// Base class for the results of all types of trace records.
class TraceRecordResult {
 public:
  explicit TraceRecordResult(TraceType trace_type);

  virtual ~TraceRecordResult() = default;

  // Trace type of the corresponding TraceRecord.
  virtual TraceType GetTraceType() const;

  class Handler {
   public:
    virtual ~Handler() = default;

    // Handle StatusOnlyTraceExecutionResult
    virtual Status Handle(const StatusOnlyTraceExecutionResult& result) = 0;

    // Handle SingleValueTraceExecutionResult
    virtual Status Handle(const SingleValueTraceExecutionResult& result) = 0;

    // Handle MultiValuesTraceExecutionResult
    virtual Status Handle(const MultiValuesTraceExecutionResult& result) = 0;
  };

  virtual Status Accept(Handler* handler) = 0;

 private:
  TraceType trace_type_;
};

// Base class for the execution results types of trace records.
class TraceExecutionResult : public TraceRecordResult {
 public:
  TraceExecutionResult(uint64_t start_timestamp, uint64_t end_timestamp,
                       TraceType trace_type);

  // Execution start/end timestamps and request latency in microseconds.
  virtual uint64_t GetStartTimestamp() const;
  virtual uint64_t GetEndTimestamp() const;
  inline uint64_t GetLatency() const {
    return GetEndTimestamp() - GetStartTimestamp();
  }

 private:
  uint64_t ts_start_;
  uint64_t ts_end_;
};

// Operation that only returns a Status.
// Example operations: DB::Write(), Iterator::Seek() and
// Iterator::SeekForPrev().
class StatusOnlyTraceExecutionResult : public TraceExecutionResult {
 public:
  StatusOnlyTraceExecutionResult(Status status, uint64_t start_timestamp,
                                 uint64_t end_timestamp, TraceType trace_type);

  virtual ~StatusOnlyTraceExecutionResult() override = default;

  // Return value of DB::Write(), etc.
  virtual const Status& GetStatus() const;

  virtual Status Accept(Handler* handler) override;

 private:
  Status status_;
};

// Operation that returns a Status and a value.
// Example operation: DB::Get()
class SingleValueTraceExecutionResult : public TraceExecutionResult {
 public:
  SingleValueTraceExecutionResult(Status status, const std::string& value,
                                  uint64_t start_timestamp,
                                  uint64_t end_timestamp, TraceType trace_type);

  SingleValueTraceExecutionResult(Status status, std::string&& value,
                                  uint64_t start_timestamp,
                                  uint64_t end_timestamp, TraceType trace_type);

  virtual ~SingleValueTraceExecutionResult() override;

  // Return status of DB::Get(), etc.
  virtual const Status& GetStatus() const;

  // Value for the searched key.
  virtual const std::string& GetValue() const;

  virtual Status Accept(Handler* handler) override;

 private:
  Status status_;
  std::string value_;
};

// Operation that returns a vector of values and status.
// Example operation: DB::MultiGet()
class MultiValuesTraceExecutionResult : public TraceExecutionResult {
 public:
  MultiValuesTraceExecutionResult(std::vector<Status> multi_status,
                                  std::vector<std::string> values,
                                  uint64_t start_timestamp,
                                  uint64_t end_timestamp, TraceType trace_type);

  virtual ~MultiValuesTraceExecutionResult() override;

  // Return status of DB::MultiGet(), etc.
  virtual const std::vector<Status>& GetMultiStatus() const;

  // Values for the searched keys.
  virtual const std::vector<std::string>& GetValues() const;

  virtual Status Accept(Handler* handler) override;

 private:
  std::vector<Status> multi_status_;
  std::vector<std::string> values_;
};

}  // namespace ROCKSDB_NAMESPACE
