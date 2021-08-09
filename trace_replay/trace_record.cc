//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/trace_record.h"

#include <utility>

#include "rocksdb/db.h"
#include "trace_replay/trace_replay.h"

namespace ROCKSDB_NAMESPACE {

// TraceRecord
TraceRecord::TraceRecord() : timestamp(0) {}

TraceRecord::TraceRecord(uint64_t ts) : timestamp(ts) {}

TraceRecord::~TraceRecord() {}

// QueryTraceRecord
QueryTraceRecord::QueryTraceRecord() : TraceRecord() {}

QueryTraceRecord::QueryTraceRecord(uint64_t ts) : TraceRecord(ts) {}

QueryTraceRecord::~QueryTraceRecord() {}

// WriteQueryTraceRecord
WriteQueryTraceRecord::WriteQueryTraceRecord() : QueryTraceRecord() {}

WriteQueryTraceRecord::WriteQueryTraceRecord(const std::string& batch_rep,
                                             uint64_t ts)
    : QueryTraceRecord(ts), rep(batch_rep) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(std::string&& batch_rep,
                                             uint64_t ts)
    : QueryTraceRecord(ts), rep(std::move(batch_rep)) {}

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

// GetQueryTraceRecord
GetQueryTraceRecord::GetQueryTraceRecord() : QueryTraceRecord(), cf_id(0) {}

GetQueryTraceRecord::GetQueryTraceRecord(uint32_t get_cf_id,
                                         PinnableSlice&& get_key, uint64_t ts)
    : QueryTraceRecord(ts), cf_id(get_cf_id), key(std::move(get_key)) {}

GetQueryTraceRecord::GetQueryTraceRecord(uint32_t get_cf_id,
                                         const std::string& get_key,
                                         uint64_t ts)
    : QueryTraceRecord(ts), cf_id(get_cf_id) {
  Slice s(get_key);
  key.PinSelf(s);
}

GetQueryTraceRecord::GetQueryTraceRecord(uint32_t get_cf_id,
                                         std::string&& get_key, uint64_t ts)
    : QueryTraceRecord(ts), cf_id(get_cf_id), key(&get_key) {
  key.PinSelf();
}

GetQueryTraceRecord::~GetQueryTraceRecord() {}

Status GetQueryTraceRecord::Execute(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles) {
  for (ColumnFamilyHandle* handle : handles) {
    if (handle->GetID() == cf_id) {
      return TracerHelper::ExecuteGetRecord(this, db, handle);
    }
  }
  return Status::Corruption("Invalid Column Family ID.");
}

Status GetQueryTraceRecord::Execute(
    DB* db, const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) {
  auto it = cf_map.find(cf_id);
  if (it == cf_map.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }
  return TracerHelper::ExecuteGetRecord(this, db, it->second);
}

// IteratorQueryTraceRecord
IteratorQueryTraceRecord::IteratorQueryTraceRecord() : QueryTraceRecord() {}

IteratorQueryTraceRecord::IteratorQueryTraceRecord(uint64_t ts)
    : QueryTraceRecord(ts) {}

IteratorQueryTraceRecord::~IteratorQueryTraceRecord() {}

// IteratorSeekQueryTraceRecord
IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord()
    : IteratorQueryTraceRecord(), seekType(kSeek), cf_id(0) {}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType type, uint32_t iter_cf_id, PinnableSlice&& iter_key, uint64_t ts)
    : IteratorQueryTraceRecord(ts),
      seekType(type),
      cf_id(iter_cf_id),
      key(std::move(iter_key)) {}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType type, uint32_t iter_cf_id, const std::string& iter_key,
    uint64_t ts)
    : IteratorQueryTraceRecord(ts), seekType(type), cf_id(iter_cf_id) {
  Slice s(iter_key);
  key.PinSelf(s);
}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType type, uint32_t iter_cf_id, std::string&& iter_key, uint64_t ts)
    : IteratorQueryTraceRecord(ts),
      seekType(type),
      cf_id(iter_cf_id),
      key(&iter_key) {
  key.PinSelf();
}

IteratorSeekQueryTraceRecord::~IteratorSeekQueryTraceRecord() {}

TraceType IteratorSeekQueryTraceRecord::GetType() const {
  return static_cast<TraceType>(seekType);
}

Status IteratorSeekQueryTraceRecord::Execute(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles) {
  for (ColumnFamilyHandle* handle : handles) {
    if (handle->GetID() == cf_id) {
      return TracerHelper::ExecuteIterSeekRecord(this, db, handle);
    }
  }
  return Status::Corruption("Invalid Column Family ID.");
}

Status IteratorSeekQueryTraceRecord::Execute(
    DB* db, const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) {
  auto it = cf_map.find(cf_id);
  if (it == cf_map.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }
  return TracerHelper::ExecuteIterSeekRecord(this, db, it->second);
}

// MultiGetQueryTraceRecord
MultiGetQueryTraceRecord::MultiGetQueryTraceRecord() : QueryTraceRecord() {}

MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    const std::vector<uint32_t>& multiget_cf_ids,
    std::vector<PinnableSlice>&& multiget_keys, uint64_t ts)
    : QueryTraceRecord(ts),
      cf_ids(multiget_cf_ids),
      keys(std::move(multiget_keys)) {}

MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t>&& multiget_cf_ids,
    std::vector<PinnableSlice>&& multiget_keys, uint64_t ts)
    : QueryTraceRecord(ts),
      cf_ids(std::move(multiget_cf_ids)),
      keys(std::move(multiget_keys)) {}

MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    const std::vector<uint32_t>& multiget_cf_ids,
    const std::vector<std::string>& multiget_keys, uint64_t ts)
    : QueryTraceRecord(ts), cf_ids(multiget_cf_ids) {
  keys.reserve(multiget_keys.size());
  for (const std::string& key : multiget_keys) {
    Slice s(key);
    PinnableSlice ps;
    ps.PinSelf(s);
    keys.push_back(std::move(ps));
  }
}

MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t>&& multiget_cf_ids,
    std::vector<std::string>&& multiget_keys, uint64_t ts)
    : QueryTraceRecord(ts), cf_ids(std::move(multiget_cf_ids)) {
  keys.reserve(multiget_keys.size());
  for (std::string& key : multiget_keys) {
    Slice s(key);
    PinnableSlice ps;
    ps.PinSelf(s);
    keys.push_back(std::move(ps));
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
  handles.reserve(cf_ids.size());
  for (uint32_t cf_id : cf_ids) {
    auto it = cf_map.find(cf_id);
    if (it == cf_map.end()) {
      return Status::Corruption("Invalid Column Family ID.");
    }
    handles.push_back(it->second);
  }
  return TracerHelper::ExecuteMultiGetRecord(this, db, handles);
}

}  // namespace ROCKSDB_NAMESPACE
