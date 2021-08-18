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
// Theses classes can be used to report the execution result of
// TraceRecord::Handler::Handle() or TraceRecord::Accept().
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

  /*
   * Example handler to just print the trace record execution results.
   *
   * class ResultPrintHandler : public TraceRecordResult::Handler {
   *  public:
   *   ResultPrintHandler();
   *   ~ResultPrintHandler() override {}
   *
   *   Status Handle(const StatusOnlyTraceExecutionResult& result) override {
   *     std::cout << "Status: " << result.GetStatus().ToString() << std::endl;
   *   }
   *
   *   Status Handle(const SingleValueTraceExecutionResult& result) override {
   *     std::cout << "Status: " << result.GetStatus().ToString()
   *               << ", value: " << result.GetValue() << std::endl;
   *   }
   *
   *   Status Handle(const MultiValuesTraceExecutionResult& result) override {
   *     size_t size = result.GetMultiStatus().size();
   *     for (size_t i = 0; i < size; i++) {
   *       std::cout << "Status: " << result.GetMultiStatus()[i].ToString()
   *                 << ", value: " << result.GetValues()[i] << std::endl;
   *     }
   *   }
   * };
   * */

  // Accept the handler.
  virtual Status Accept(Handler* handler) = 0;

 private:
  TraceType trace_type_;
};

// Base class for the results from the trace record execution handler (created
// by TraceRecord::NewExecutionHandler()).
//
// The actual execution status or returned values may be hidden from
// TraceRecord::Handler::Handle and TraceRecord::Accept. For example, a
// GetQueryTraceRecord's execution calls DB::Get() internally. DB::Get() may
// return Status::NotFound() but TraceRecord::Handler::Handle() or
// TraceRecord::Accept() will still return Status::OK(). The actual status from
// DB::Get() and the returned value string may be saved in a
// SingleValueTraceExecutionResult.
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

// Result for operations that only return a single Status.
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

// Result for operations that return a Status and a value.
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

// Result for operations that return multiple Status(es) and values.
// Example operation: DB::MultiGet()
class MultiValuesTraceExecutionResult : public TraceExecutionResult {
 public:
  MultiValuesTraceExecutionResult(std::vector<Status> multi_status,
                                  std::vector<std::string> values,
                                  uint64_t start_timestamp,
                                  uint64_t end_timestamp, TraceType trace_type);

  virtual ~MultiValuesTraceExecutionResult() override;

  // Returned Status(es) of DB::MultiGet(), etc.
  virtual const std::vector<Status>& GetMultiStatus() const;

  // Returned values for the searched keys.
  virtual const std::vector<std::string>& GetValues() const;

  virtual Status Accept(Handler* handler) override;

 private:
  std::vector<Status> multi_status_;
  std::vector<std::string> values_;
};

}  // namespace ROCKSDB_NAMESPACE
