//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/trace_record_handler.h"

#include "rocksdb/iterator.h"
#include "rocksdb/trace_record_result.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

// TraceExecutionHandler
TraceExecutionHandler::TraceExecutionHandler(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles)
    : TraceRecord::Handler(),
      db_(db),
      write_opts_(WriteOptions()),
      read_opts_(ReadOptions()) {
  assert(db != nullptr);
  assert(!handles.empty());
  cf_map_.reserve(handles.size());
  for (ColumnFamilyHandle* handle : handles) {
    assert(handle != nullptr);
    cf_map_.insert({handle->GetID(), handle});
  }
  clock_ = db_->GetEnv()->GetSystemClock().get();
}

TraceExecutionHandler::~TraceExecutionHandler() { cf_map_.clear(); }

Status TraceExecutionHandler::Handle(
    const WriteQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  uint64_t start = clock_->NowMicros();

  WriteBatch batch(record.GetWriteBatchRep().ToString());
  Status s = db_->Write(write_opts_, &batch);

  uint64_t end = clock_->NowMicros();

  if (s.ok() && result != nullptr) {
    result->reset(new StatusOnlyTraceExecutionResult(s, start, end,
                                                     record.GetTraceType()));
  }

  return s;
}

Status TraceExecutionHandler::Handle(
    const GetQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  auto it = cf_map_.find(record.GetColumnFamilyID());
  if (it == cf_map_.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }

  uint64_t start = clock_->NowMicros();

  std::string value;
  Status s = db_->Get(read_opts_, it->second, record.GetKey(), &value);

  uint64_t end = clock_->NowMicros();

  // Treat not found as ok, return other errors.
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (result != nullptr) {
    // Report the actual opetation status in TraceExecutionResult
    result->reset(new SingleValueTraceExecutionResult(
        std::move(s), std::move(value), start, end, record.GetTraceType()));
  }
  return Status::OK();
}

Status TraceExecutionHandler::Handle(
    const IteratorSeekQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  auto it = cf_map_.find(record.GetColumnFamilyID());
  if (it == cf_map_.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }

  ReadOptions r_opts = read_opts_;
  Slice lower = record.GetLowerBound();
  if (!lower.empty()) {
    r_opts.iterate_lower_bound = &lower;
  }
  Slice upper = record.GetUpperBound();
  if (!upper.empty()) {
    r_opts.iterate_upper_bound = &upper;
  }
  Iterator* single_iter = db_->NewIterator(r_opts, it->second);

  uint64_t start = clock_->NowMicros();

  switch (record.GetSeekType()) {
    case IteratorSeekQueryTraceRecord::kSeekForPrev: {
      single_iter->SeekForPrev(record.GetKey());
      break;
    }
    default: {
      single_iter->Seek(record.GetKey());
      break;
    }
  }

  uint64_t end = clock_->NowMicros();

  Status s = single_iter->status();
  if (s.ok() && result != nullptr) {
    if (single_iter->Valid()) {
      PinnableSlice ps_key;
      ps_key.PinSelf(single_iter->key());
      PinnableSlice ps_value;
      ps_value.PinSelf(single_iter->value());
      result->reset(new IteratorTraceExecutionResult(
          true, s, std::move(ps_key), std::move(ps_value), start, end,
          record.GetTraceType()));
    } else {
      result->reset(new IteratorTraceExecutionResult(
          false, s, "", "", start, end, record.GetTraceType()));
    }
  }
  delete single_iter;

  return s;
}

Status TraceExecutionHandler::Handle(
    const MultiGetQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  std::vector<ColumnFamilyHandle*> handles;
  handles.reserve(record.GetColumnFamilyIDs().size());
  for (uint32_t cf_id : record.GetColumnFamilyIDs()) {
    auto it = cf_map_.find(cf_id);
    if (it == cf_map_.end()) {
      return Status::Corruption("Invalid Column Family ID.");
    }
    handles.push_back(it->second);
  }

  std::vector<Slice> keys = record.GetKeys();

  if (handles.empty() || keys.empty()) {
    return Status::InvalidArgument("Empty MultiGet cf_ids or keys.");
  }
  if (handles.size() != keys.size()) {
    return Status::InvalidArgument("MultiGet cf_ids and keys size mismatch.");
  }

  uint64_t start = clock_->NowMicros();

  std::vector<std::string> values;
  std::vector<Status> ss = db_->MultiGet(read_opts_, handles, keys, &values);

  uint64_t end = clock_->NowMicros();

  // Treat not found as ok, return other errors.
  for (const Status& s : ss) {
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }

  if (result != nullptr) {
    // Report the actual opetation status in TraceExecutionResult
    result->reset(new MultiValuesTraceExecutionResult(
        std::move(ss), std::move(values), start, end, record.GetTraceType()));
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
