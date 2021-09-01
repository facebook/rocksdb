//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/trace_record_result.h"

namespace ROCKSDB_NAMESPACE {

// TraceRecordResult
TraceRecordResult::TraceRecordResult(TraceType trace_type)
    : trace_type_(trace_type) {}

TraceType TraceRecordResult::GetTraceType() const { return trace_type_; }

// TraceExecutionResult
TraceExecutionResult::TraceExecutionResult(uint64_t start_timestamp,
                                           uint64_t end_timestamp,
                                           TraceType trace_type)
    : TraceRecordResult(trace_type),
      ts_start_(start_timestamp),
      ts_end_(end_timestamp) {
  assert(ts_start_ <= ts_end_);
}

uint64_t TraceExecutionResult::GetStartTimestamp() const { return ts_start_; }

uint64_t TraceExecutionResult::GetEndTimestamp() const { return ts_end_; }

// StatusOnlyTraceExecutionResult
StatusOnlyTraceExecutionResult::StatusOnlyTraceExecutionResult(
    Status status, uint64_t start_timestamp, uint64_t end_timestamp,
    TraceType trace_type)
    : TraceExecutionResult(start_timestamp, end_timestamp, trace_type),
      status_(std::move(status)) {}

const Status& StatusOnlyTraceExecutionResult::GetStatus() const {
  return status_;
}

Status StatusOnlyTraceExecutionResult::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// SingleValueTraceExecutionResult
SingleValueTraceExecutionResult::SingleValueTraceExecutionResult(
    Status status, const std::string& value, uint64_t start_timestamp,
    uint64_t end_timestamp, TraceType trace_type)
    : TraceExecutionResult(start_timestamp, end_timestamp, trace_type),
      status_(std::move(status)),
      value_(value) {}

SingleValueTraceExecutionResult::SingleValueTraceExecutionResult(
    Status status, std::string&& value, uint64_t start_timestamp,
    uint64_t end_timestamp, TraceType trace_type)
    : TraceExecutionResult(start_timestamp, end_timestamp, trace_type),
      status_(std::move(status)),
      value_(std::move(value)) {}

SingleValueTraceExecutionResult::~SingleValueTraceExecutionResult() {
  value_.clear();
}

const Status& SingleValueTraceExecutionResult::GetStatus() const {
  return status_;
}

const std::string& SingleValueTraceExecutionResult::GetValue() const {
  return value_;
}

Status SingleValueTraceExecutionResult::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// MultiValuesTraceExecutionResult
MultiValuesTraceExecutionResult::MultiValuesTraceExecutionResult(
    std::vector<Status> multi_status, std::vector<std::string> values,
    uint64_t start_timestamp, uint64_t end_timestamp, TraceType trace_type)
    : TraceExecutionResult(start_timestamp, end_timestamp, trace_type),
      multi_status_(std::move(multi_status)),
      values_(std::move(values)) {}

MultiValuesTraceExecutionResult::~MultiValuesTraceExecutionResult() {
  multi_status_.clear();
  values_.clear();
}

const std::vector<Status>& MultiValuesTraceExecutionResult::GetMultiStatus()
    const {
  return multi_status_;
}

const std::vector<std::string>& MultiValuesTraceExecutionResult::GetValues()
    const {
  return values_;
}

Status MultiValuesTraceExecutionResult::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// IteratorTraceExecutionResult
IteratorTraceExecutionResult::IteratorTraceExecutionResult(
    bool valid, Status status, PinnableSlice&& key, PinnableSlice&& value,
    uint64_t start_timestamp, uint64_t end_timestamp, TraceType trace_type)
    : TraceExecutionResult(start_timestamp, end_timestamp, trace_type),
      valid_(valid),
      status_(std::move(status)),
      key_(std::move(key)),
      value_(std::move(value)) {}

IteratorTraceExecutionResult::IteratorTraceExecutionResult(
    bool valid, Status status, const std::string& key, const std::string& value,
    uint64_t start_timestamp, uint64_t end_timestamp, TraceType trace_type)
    : TraceExecutionResult(start_timestamp, end_timestamp, trace_type),
      valid_(valid),
      status_(std::move(status)) {
  key_.PinSelf(key);
  value_.PinSelf(value);
}

IteratorTraceExecutionResult::~IteratorTraceExecutionResult() {
  key_.clear();
  value_.clear();
}

bool IteratorTraceExecutionResult::GetValid() const { return valid_; }

const Status& IteratorTraceExecutionResult::GetStatus() const {
  return status_;
}

Slice IteratorTraceExecutionResult::GetKey() const { return Slice(key_); }

Slice IteratorTraceExecutionResult::GetValue() const { return Slice(value_); }

Status IteratorTraceExecutionResult::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

}  // namespace ROCKSDB_NAMESPACE
