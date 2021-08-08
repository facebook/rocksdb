//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

// To do: ROCKSDB_LITE ?

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;

// Supported trace record types.
enum TraceType : char {
  kTraceNone = 0,
  kTraceBegin = 1,
  kTraceEnd = 2,
  // Query level tracing related trace types.
  kTraceWrite = 3,
  kTraceGet = 4,
  kTraceIteratorSeek = 5,
  kTraceIteratorSeekForPrev = 6,
  // Block cache tracing related trace types.
  kBlockTraceIndexBlock = 7,
  kBlockTraceFilterBlock = 8,
  kBlockTraceDataBlock = 9,
  kBlockTraceUncompressionDictBlock = 10,
  kBlockTraceRangeDeletionBlock = 11,
  // IO tracing related trace type.
  kIOTracer = 12,
  // Query level tracing related trace type.
  kTraceMultiGet = 13,
  // All trace types should be added before kTraceMax
  kTraceMax,
};

// Base class for all types of trace records.
class TraceRecord {
 public:
  TraceRecord();
  explicit TraceRecord(uint64_t ts);
  virtual ~TraceRecord();

  virtual TraceType GetType() const = 0;

  // Execute the trace record on the specified DB and handles.
  virtual Status Execute(DB* /*db*/,
                         const std::vector<ColumnFamilyHandle*>& /*handles*/) {
    return Status::NotSupported();
  }
  virtual Status Execute(
      DB* /*db*/,
      const std::unordered_map<uint32_t, ColumnFamilyHandle*>& /*cf_map*/) {
    return Status::NotSupported();
  }

  // Timestamp (in microseconds) of this trace.
  uint64_t timestamp;
};

// Base class for all query types of trace records.
class QueryTraceRecord : public TraceRecord {
 public:
  QueryTraceRecord();
  explicit QueryTraceRecord(uint64_t ts);
  virtual ~QueryTraceRecord() override;
};

// Trace record for DB::Write() operation.
class WriteQueryTraceRecord : public QueryTraceRecord {
 public:
  WriteQueryTraceRecord();
  WriteQueryTraceRecord(const std::string& rep, uint64_t ts);
  WriteQueryTraceRecord(std::string&& rep, uint64_t ts);
  virtual ~WriteQueryTraceRecord() override;

  TraceType GetType() const override { return kTraceWrite; };

  virtual Status Execute(
      DB* db, const std::vector<ColumnFamilyHandle*>& handles) override;
  virtual Status Execute(
      DB* db,
      const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) override;

  // Serialized string object to construct a WriteBatch.
  std::string rep;
};

// Trace record for DB::Get() operation
class GetQueryTraceRecord : public QueryTraceRecord {
 public:
  GetQueryTraceRecord();
  GetQueryTraceRecord(uint32_t cf_id, const std::string& key, uint64_t ts);
  GetQueryTraceRecord(uint32_t cf_id, std::string&& key, uint64_t ts);
  virtual ~GetQueryTraceRecord() override;

  TraceType GetType() const override { return kTraceGet; };

  virtual Status Execute(
      DB* db, const std::vector<ColumnFamilyHandle*>& handles) override;
  virtual Status Execute(
      DB* db,
      const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) override;

  // Column family ID.
  uint32_t cf_id;

  // Key to get.
  std::string key;
};

// Base class for all Iterator related operations.
class IteratorQueryTraceRecord : public QueryTraceRecord {
 public:
  IteratorQueryTraceRecord();
  explicit IteratorQueryTraceRecord(uint64_t ts);
  virtual ~IteratorQueryTraceRecord() override;
};

// Trace record for Iterator::Seek() and Iterator::SeekForPrev() operation.
class IteratorSeekQueryTraceRecord : public IteratorQueryTraceRecord {
 public:
  // Currently we only Seek() and SeekForPrev().
  enum SeekType {
    kSeek = kTraceIteratorSeek,
    kSeekForPrev = kTraceIteratorSeekForPrev
  };

  IteratorSeekQueryTraceRecord();
  IteratorSeekQueryTraceRecord(SeekType type, uint32_t cf_id,
                               const std::string& key, uint64_t ts);
  IteratorSeekQueryTraceRecord(SeekType type, uint32_t cf_id, std::string&& key,
                               uint64_t ts);

  virtual ~IteratorSeekQueryTraceRecord() override;

  TraceType GetType() const override;

  virtual Status Execute(
      DB* db, const std::vector<ColumnFamilyHandle*>& handles) override;
  virtual Status Execute(
      DB* db,
      const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) override;

  SeekType seekType;

  // Column family ID.
  uint32_t cf_id;

  // Key to seek to.
  std::string key;
};

// Trace record for DB::MultiGet() operation.
class MultiGetQueryTraceRecord : public QueryTraceRecord {
 public:
  MultiGetQueryTraceRecord();
  MultiGetQueryTraceRecord(const std::vector<uint32_t>& cf_ids,
                           const std::vector<std::string>& keys, uint64_t ts);
  MultiGetQueryTraceRecord(std::vector<uint32_t>&& cf_ids,
                           std::vector<std::string>&& keys, uint64_t ts);
  virtual ~MultiGetQueryTraceRecord() override;

  TraceType GetType() const override { return kTraceMultiGet; };

  virtual Status Execute(
      DB* db, const std::vector<ColumnFamilyHandle*>& handles) override;
  virtual Status Execute(
      DB* db,
      const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map) override;

  // Column familiy IDs.
  std::vector<uint32_t> cf_ids;

  // Keys to get.
  std::vector<std::string> keys;
};

}  // namespace ROCKSDB_NAMESPACE
