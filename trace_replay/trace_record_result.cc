//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/trace_record_result.h"

namespace ROCKSDB_NAMESPACE {

// TraceRecordResult
TraceRecordResult::TraceRecordResult(TraceType trace_type)
    : trace_type_(trace_type) {}

TraceRecordResult::~TraceRecordResult() {}

TraceType TraceRecordResult::GetTraceType() const { return trace_type_; }

// TraceExecutionResult
TraceExecutionResult::TraceExecutionResult(uint64_t latency,
                                           TraceType trace_type)
    : TraceRecordResult(trace_type), latency_(latency) {}

TraceExecutionResult::~TraceExecutionResult() {}

uint64_t TraceExecutionResult::GetLatency() const { return latency_; }

// StatusOnlyTraceExecutionResult
StatusOnlyTraceExecutionResult::StatusOnlyTraceExecutionResult(
    const Status& status, uint64_t latency, TraceType trace_type)
    : TraceExecutionResult(latency, trace_type), status_(status) {}

StatusOnlyTraceExecutionResult::~StatusOnlyTraceExecutionResult() {}

Status StatusOnlyTraceExecutionResult::GetStatus() const { return status_; }

Status StatusOnlyTraceExecutionResult::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// SingleValueTraceExecutionResult
SingleValueTraceExecutionResult::SingleValueTraceExecutionResult(
    const Status& status, const std::string& value, uint64_t latency,
    TraceType trace_type)
    : TraceExecutionResult(latency, trace_type),
      status_(status),
      value_(value) {}

SingleValueTraceExecutionResult::SingleValueTraceExecutionResult(
    const Status& status, std::string&& value, uint64_t latency,
    TraceType trace_type)
    : TraceExecutionResult(latency, trace_type),
      status_(status),
      value_(std::move(value)) {}

SingleValueTraceExecutionResult::~SingleValueTraceExecutionResult() {}

Status SingleValueTraceExecutionResult::GetStatus() const { return status_; }

std::string SingleValueTraceExecutionResult::GetValue() const { return value_; }

Status SingleValueTraceExecutionResult::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// MultiValuesTraceExecutionResult
MultiValuesTraceExecutionResult::MultiValuesTraceExecutionResult(
    const std::vector<Status>& multi_status,
    const std::vector<std::string>& values, uint64_t latency,
    TraceType trace_type)
    : TraceExecutionResult(latency, trace_type),
      multi_status_(multi_status),
      values_(values) {}

MultiValuesTraceExecutionResult::MultiValuesTraceExecutionResult(
    std::vector<Status>&& multi_status, std::vector<std::string>&& values,
    uint64_t latency, TraceType trace_type)
    : TraceExecutionResult(latency, trace_type),
      multi_status_(std::move(multi_status)),
      values_(std::move(values)) {}

MultiValuesTraceExecutionResult::~MultiValuesTraceExecutionResult() {}

std::vector<Status> MultiValuesTraceExecutionResult::GetMultiStatus() const {
  return multi_status_;
}

std::vector<std::string> MultiValuesTraceExecutionResult::GetValues() const {
  return values_;
}

Status MultiValuesTraceExecutionResult::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

}  // namespace ROCKSDB_NAMESPACE
