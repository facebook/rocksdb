//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/trace_record.h"

#include <utility>

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "trace_replay/trace_replay.h"

namespace ROCKSDB_NAMESPACE {

// TraceRecord
TraceRecord::TraceRecord(uint64_t timestamp) : timestamp_(timestamp) {}

TraceRecord::~TraceRecord() {}

uint64_t TraceRecord::timestamp() const { return timestamp_; }

// QueryTraceRecord
QueryTraceRecord::QueryTraceRecord(uint64_t timestamp)
    : TraceRecord(timestamp) {}

QueryTraceRecord::~QueryTraceRecord() {}

// WriteQueryTraceRecord
WriteQueryTraceRecord::WriteQueryTraceRecord(const WriteBatch& batch,
                                             uint64_t timestamp)
    : QueryTraceRecord(timestamp), batch_(batch) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(WriteBatch&& batch,
                                             uint64_t timestamp)
    : QueryTraceRecord(timestamp), batch_(std::move(batch)) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(const std::string& rep,
                                             uint64_t timestamp)
    : QueryTraceRecord(timestamp), batch_(rep) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(std::string&& rep,
                                             uint64_t timestamp)
    : QueryTraceRecord(timestamp), batch_(std::move(rep)) {}

WriteQueryTraceRecord::~WriteQueryTraceRecord() {}

Status WriteQueryTraceRecord::Execute(
    DB* db, const std::vector<ColumnFamilyHandle*>& /*handles*/) {
  return TracerHelper::ExecuteWriteRecord(this, db);
}

Status WriteQueryTraceRecord::Execute(
    DB* db,
    const std::unordered_map<uint32_t, ColumnFamilyHandle*>& /*cf_map*/) {
  return TracerHelper::ExecuteWriteRecord(this, db);
}

const WriteBatch* WriteQueryTraceRecord::writeBatch() const { return &batch_; }

// GetQueryTraceRecord
GetQueryTraceRecord::GetQueryTraceRecord(uint32_t column_family_id,
                                         PinnableSlice&& key,
                                         uint64_t timestamp)
    : QueryTraceRecord(timestamp),
      cf_id_(column_family_id),
      key_(std::move(key)) {}

GetQueryTraceRecord::GetQueryTraceRecord(uint32_t column_family_id,
                                         const std::string& key,
                                         uint64_t timestamp)
    : QueryTraceRecord(timestamp), cf_id_(column_family_id) {
  key_.PinSelf(key);
}

GetQueryTraceRecord::~GetQueryTraceRecord() {}

Status GetQueryTraceRecord::Execute(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles) {
  for (ColumnFamilyHandle* handle : handles) {
    if (handle->GetID() == cf_id_) {
      return TracerHelper::ExecuteGetRecord(this, db, handle);
    }
  }
  return Status::Corruption("Invalid Column Family ID.");
}

Status GetQueryTraceRecord::Execute(
    DB* db, const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) {
  auto it = cf_map.find(cf_id_);
  if (it == cf_map.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }
  return TracerHelper::ExecuteGetRecord(this, db, it->second);
}

uint32_t GetQueryTraceRecord::columnFamilyID() const { return cf_id_; }

Slice GetQueryTraceRecord::key() const { return Slice(key_); }

// IteratorQueryTraceRecord
IteratorQueryTraceRecord::IteratorQueryTraceRecord(uint64_t timestamp)
    : QueryTraceRecord(timestamp) {}

IteratorQueryTraceRecord::~IteratorQueryTraceRecord() {}

// IteratorSeekQueryTraceRecord
IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType type, uint32_t column_family_id, PinnableSlice&& key,
    uint64_t timestamp)
    : IteratorQueryTraceRecord(timestamp),
      type_(type),
      cf_id_(column_family_id),
      key_(std::move(key)) {}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType type, uint32_t column_family_id, const std::string& key,
    uint64_t timestamp)
    : IteratorQueryTraceRecord(timestamp),
      type_(type),
      cf_id_(column_family_id) {
  key_.PinSelf(key);
}

IteratorSeekQueryTraceRecord::~IteratorSeekQueryTraceRecord() {}

TraceType IteratorSeekQueryTraceRecord::type() const {
  return static_cast<TraceType>(type_);
}

Status IteratorSeekQueryTraceRecord::Execute(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles) {
  for (ColumnFamilyHandle* handle : handles) {
    if (handle->GetID() == cf_id_) {
      return TracerHelper::ExecuteIterSeekRecord(this, db, handle);
    }
  }
  return Status::Corruption("Invalid Column Family ID.");
}

Status IteratorSeekQueryTraceRecord::Execute(
    DB* db, const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) {
  auto it = cf_map.find(cf_id_);
  if (it == cf_map.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }
  return TracerHelper::ExecuteIterSeekRecord(this, db, it->second);
}

IteratorSeekQueryTraceRecord::SeekType IteratorSeekQueryTraceRecord::seekType()
    const {
  return type_;
}

uint32_t IteratorSeekQueryTraceRecord::columnFamilyID() const { return cf_id_; }

Slice IteratorSeekQueryTraceRecord::key() const { return Slice(key_); }

// MultiGetQueryTraceRecord
MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t> column_family_ids, std::vector<PinnableSlice>&& keys,
    uint64_t timestamp)
    : QueryTraceRecord(timestamp),
      cf_ids_(column_family_ids),
      keys_(std::move(keys)) {}

MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t> column_family_ids,
    const std::vector<std::string>& keys, uint64_t timestamp)
    : QueryTraceRecord(timestamp), cf_ids_(column_family_ids) {
  keys_.reserve(keys.size());
  for (const std::string& key : keys) {
    PinnableSlice ps;
    ps.PinSelf(key);
    keys_.push_back(std::move(ps));
  }
}

MultiGetQueryTraceRecord::~MultiGetQueryTraceRecord() {}

Status MultiGetQueryTraceRecord::Execute(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles) {
  std::unordered_map<uint32_t, ColumnFamilyHandle*> cf_map;
  for (ColumnFamilyHandle* handle : handles) {
    cf_map.insert({handle->GetID(), handle});
  }
  return Execute(db, cf_map);
}

Status MultiGetQueryTraceRecord::Execute(
    DB* db, const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) {
  std::vector<ColumnFamilyHandle*> handles;
  handles.reserve(cf_ids_.size());
  for (uint32_t cf_id : cf_ids_) {
    auto it = cf_map.find(cf_id);
    if (it == cf_map.end()) {
      return Status::Corruption("Invalid Column Family ID.");
    }
    handles.push_back(it->second);
  }
  return TracerHelper::ExecuteMultiGetRecord(this, db, handles);
}

std::vector<uint32_t> MultiGetQueryTraceRecord::columnFamilyIDs() const {
  return cf_ids_;
}

std::vector<Slice> MultiGetQueryTraceRecord::keys() const {
  return std::vector<Slice>(keys_.begin(), keys_.end());
}

}  // namespace ROCKSDB_NAMESPACE
