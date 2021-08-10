//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/trace_record.h"

#include <utility>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// TraceRecord
TraceRecord::TraceRecord(uint64_t ts) : timestamp_(ts) {}

TraceRecord::~TraceRecord() {}

uint64_t TraceRecord::timestamp() const { return timestamp_; }

// TraceRecord::ExecutionHandler
TraceRecord::ExecutionHandler::ExecutionHandler(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles)
    : TraceRecord::Handler(), db_(db) {
  assert(db != nullptr);
  assert(!handles.empty());
  cf_map_.reserve(handles.size());
  for (ColumnFamilyHandle* handle : handles) {
    assert(handle != nullptr);
    cf_map_.insert({handle->GetID(), handle});
  }
}

TraceRecord::ExecutionHandler::ExecutionHandler(
    DB* db, const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map)
    : TraceRecord::Handler(), db_(db), cf_map_(cf_map) {}

TraceRecord::ExecutionHandler::~ExecutionHandler() {}

Status TraceRecord::ExecutionHandler::Handle(
    const WriteQueryTraceRecord& record) {
  return db_->Write(WriteOptions(), record.writeBatch());
}

Status TraceRecord::ExecutionHandler::Handle(
    const GetQueryTraceRecord& record) {
  auto it = cf_map_.find(record.columnFamilyID());
  if (it == cf_map_.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }
  assert(it->second != nullptr);

  std::string value;
  Status s = db_->Get(ReadOptions(), it->second, record.key(), &value);

  // Treat not found as ok and return other errors.
  return s.IsNotFound() ? Status::OK() : s;
}

Status TraceRecord::ExecutionHandler::Handle(
    const IteratorSeekQueryTraceRecord& record) {
  auto it = cf_map_.find(record.columnFamilyID());
  if (it == cf_map_.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }
  assert(it->second != nullptr);

  Iterator* single_iter = db_->NewIterator(ReadOptions(), it->second);

  switch (record.seekType()) {
    case IteratorSeekQueryTraceRecord::kSeekForPrev: {
      single_iter->SeekForPrev(record.key());
      break;
    }
    default: {
      single_iter->Seek(record.key());
      break;
    }
  }
  Status s = single_iter->status();
  delete single_iter;
  return s;
}

Status TraceRecord::ExecutionHandler::Handle(
    const MultiGetQueryTraceRecord& record) {
  std::vector<ColumnFamilyHandle*> handles;
  handles.reserve(record.columnFamilyIDs().size());
  for (uint32_t cf_id : record.columnFamilyIDs()) {
    auto it = cf_map_.find(cf_id);
    if (it == cf_map_.end()) {
      return Status::Corruption("Invalid Column Family ID.");
    }
    assert(it->second != nullptr);
    handles.push_back(it->second);
  }

  std::vector<Slice> keys = record.keys();

  if (handles.empty() || keys.empty()) {
    return Status::InvalidArgument("Empty MultiGet cf_ids or keys.");
  }
  if (handles.size() != keys.size()) {
    return Status::InvalidArgument("MultiGet cf_ids and keys size mismatch.");
  }

  std::vector<std::string> values;
  std::vector<Status> ss = db_->MultiGet(ReadOptions(), handles, keys, &values);

  // Treat not found as ok, return other errors.
  for (Status s : ss) {
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
  }
  return Status::OK();
}

// QueryTraceRecord
QueryTraceRecord::QueryTraceRecord(uint64_t ts) : TraceRecord(ts) {}

QueryTraceRecord::~QueryTraceRecord() {}

// WriteQueryTraceRecord
WriteQueryTraceRecord::WriteQueryTraceRecord(const WriteBatch& batch,
                                             uint64_t ts)
    : QueryTraceRecord(ts), batch_(new WriteBatch(batch)) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(WriteBatch&& batch, uint64_t ts)
    : QueryTraceRecord(ts), batch_(new WriteBatch(std::move(batch))) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(const std::string& rep,
                                             uint64_t ts)
    : QueryTraceRecord(ts), batch_(new WriteBatch(rep)) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(std::string&& rep, uint64_t ts)
    : QueryTraceRecord(ts), batch_(new WriteBatch(std::move(rep))) {}

WriteQueryTraceRecord::~WriteQueryTraceRecord() { delete batch_; }

WriteBatch* WriteQueryTraceRecord::writeBatch() const { return batch_; }

Status WriteQueryTraceRecord::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// GetQueryTraceRecord
GetQueryTraceRecord::GetQueryTraceRecord(uint32_t column_family_id,
                                         PinnableSlice&& get_key, uint64_t ts)
    : QueryTraceRecord(ts),
      cf_id_(column_family_id),
      key_(std::move(get_key)) {}

GetQueryTraceRecord::GetQueryTraceRecord(uint32_t column_family_id,
                                         const std::string& get_key,
                                         uint64_t ts)
    : QueryTraceRecord(ts), cf_id_(column_family_id) {
  key_.PinSelf(get_key);
}

GetQueryTraceRecord::~GetQueryTraceRecord() {}

uint32_t GetQueryTraceRecord::columnFamilyID() const { return cf_id_; }

Slice GetQueryTraceRecord::key() const { return Slice(key_); }

Status GetQueryTraceRecord::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// IteratorQueryTraceRecord
IteratorQueryTraceRecord::IteratorQueryTraceRecord(uint64_t ts)
    : QueryTraceRecord(ts) {}

IteratorQueryTraceRecord::~IteratorQueryTraceRecord() {}

// IteratorSeekQueryTraceRecord
IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType type, uint32_t column_family_id, PinnableSlice&& iter_key,
    uint64_t ts)
    : IteratorQueryTraceRecord(ts),
      type_(type),
      cf_id_(column_family_id),
      key_(std::move(iter_key)) {}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType type, uint32_t column_family_id, const std::string& iter_key,
    uint64_t ts)
    : IteratorQueryTraceRecord(ts), type_(type), cf_id_(column_family_id) {
  key_.PinSelf(iter_key);
}

IteratorSeekQueryTraceRecord::~IteratorSeekQueryTraceRecord() {}

TraceType IteratorSeekQueryTraceRecord::type() const {
  return static_cast<TraceType>(type_);
}

IteratorSeekQueryTraceRecord::SeekType IteratorSeekQueryTraceRecord::seekType()
    const {
  return type_;
}

uint32_t IteratorSeekQueryTraceRecord::columnFamilyID() const { return cf_id_; }

Slice IteratorSeekQueryTraceRecord::key() const { return Slice(key_); }

Status IteratorSeekQueryTraceRecord::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

// MultiGetQueryTraceRecord
MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t> column_family_ids,
    std::vector<PinnableSlice>&& multiget_keys, uint64_t ts)
    : QueryTraceRecord(ts),
      cf_ids_(column_family_ids),
      keys_(std::move(multiget_keys)) {}

MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t> column_family_ids,
    const std::vector<std::string>& multiget_keys, uint64_t ts)
    : QueryTraceRecord(ts), cf_ids_(column_family_ids) {
  keys_.reserve(multiget_keys.size());
  for (const std::string& key : multiget_keys) {
    PinnableSlice ps;
    ps.PinSelf(key);
    keys_.push_back(std::move(ps));
  }
}

MultiGetQueryTraceRecord::~MultiGetQueryTraceRecord() {}

std::vector<uint32_t> MultiGetQueryTraceRecord::columnFamilyIDs() const {
  return cf_ids_;
}

std::vector<Slice> MultiGetQueryTraceRecord::keys() const {
  return std::vector<Slice>(keys_.begin(), keys_.end());
}

Status MultiGetQueryTraceRecord::Accept(Handler* handler) {
  assert(handler != nullptr);
  return handler->Handle(*this);
}

}  // namespace ROCKSDB_NAMESPACE
