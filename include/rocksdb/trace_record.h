//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

// To do: ROCKSDB_LITE ?

#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

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
  TraceRecord() : timestamp(0) {}
  explicit TraceRecord(uint64_t ts) : timestamp(ts) {}
  virtual ~TraceRecord() {}

  virtual TraceType GetType() const = 0;

  // Timestamp (in microseconds) of this trace.
  uint64_t timestamp;
};

// Base class for all query types of trace records.
class QueryTraceRecord : public TraceRecord {
 public:
  explicit QueryTraceRecord() : TraceRecord() {}
  explicit QueryTraceRecord(uint64_t ts) : TraceRecord(ts) {}
  virtual ~QueryTraceRecord() override {}
};

// Trace record for DB::Write() operation.
class WriteQueryTraceRecord : public QueryTraceRecord {
 public:
  WriteQueryTraceRecord() : QueryTraceRecord() {}
  WriteQueryTraceRecord(const std::string& batch_rep, uint64_t ts)
      : QueryTraceRecord(ts), rep(batch_rep) {}
  WriteQueryTraceRecord(std::string&& batch_rep, uint64_t ts)
      : QueryTraceRecord(ts), rep(std::move(batch_rep)) {}
  virtual ~WriteQueryTraceRecord() override {}

  TraceType GetType() const override { return kTraceWrite; };

  // Serialized string object to construct a WriteBatch.
  std::string rep;
};

// Trace record for DB::Get() operation
class GetQueryTraceRecord : public QueryTraceRecord {
 public:
  GetQueryTraceRecord() : QueryTraceRecord(), cf_id(0) {}
  GetQueryTraceRecord(uint32_t get_cf_id, const std::string& get_key,
                      uint64_t ts)
      : QueryTraceRecord(ts), cf_id(get_cf_id), key(get_key) {}
  GetQueryTraceRecord(uint32_t get_cf_id, std::string&& get_key, uint64_t ts)
      : QueryTraceRecord(ts), cf_id(get_cf_id), key(std::move(get_key)) {}
  virtual ~GetQueryTraceRecord() override {}

  TraceType GetType() const override { return kTraceGet; };

  // Column family ID.
  uint32_t cf_id;

  // Key to get.
  std::string key;
};

// Base class for all Iterator related operations.
class IteratorQueryTraceRecord : public QueryTraceRecord {
 public:
  IteratorQueryTraceRecord() : QueryTraceRecord() {}
  explicit IteratorQueryTraceRecord(uint64_t ts) : QueryTraceRecord(ts) {}
  virtual ~IteratorQueryTraceRecord() override {}
};

// Trace record for Iterator::Seek() and Iterator::SeekForPrev() operation.
class IteratorSeekQueryTraceRecord : public IteratorQueryTraceRecord {
 public:
  // Currently we only Seek() and SeekForPrev().
  enum SeekType {
    kSeek = kTraceIteratorSeek,
    kSeekForPrev = kTraceIteratorSeekForPrev
  };

  IteratorSeekQueryTraceRecord()
      : IteratorQueryTraceRecord(), seekType(kSeek), cf_id(0) {}
  IteratorSeekQueryTraceRecord(SeekType type, uint32_t iter_cf_id,
                               const std::string& iter_key, uint64_t ts)
      : IteratorQueryTraceRecord(ts),
        seekType(type),
        cf_id(iter_cf_id),
        key(iter_key) {}
  IteratorSeekQueryTraceRecord(SeekType type, uint32_t iter_cf_id,
                               std::string&& iter_key, uint64_t ts)
      : IteratorQueryTraceRecord(ts),
        seekType(type),
        cf_id(iter_cf_id),
        key(std::move(iter_key)) {}

  virtual ~IteratorSeekQueryTraceRecord() override {}

  TraceType GetType() const override {
    return static_cast<TraceType>(seekType);
  }

  SeekType seekType;

  // Column family ID.
  uint32_t cf_id;

  // Key to seek to.
  std::string key;
};

// Trace record for DB::MultiGet() operation.
class MultiGetQueryTraceRecord : public QueryTraceRecord {
 public:
  MultiGetQueryTraceRecord() : QueryTraceRecord() {}
  MultiGetQueryTraceRecord(const std::vector<uint32_t>& multiget_cf_ids,
                           const std::vector<std::string>& multiget_keys,
                           uint64_t ts)
      : QueryTraceRecord(ts), cf_ids(multiget_cf_ids), keys(multiget_keys) {}
  MultiGetQueryTraceRecord(std::vector<uint32_t>&& multiget_cf_ids,
                           std::vector<std::string>&& multiget_keys,
                           uint64_t ts)
      : QueryTraceRecord(ts),
        cf_ids(std::move(multiget_cf_ids)),
        keys(std::move(multiget_keys)) {}
  virtual ~MultiGetQueryTraceRecord() override {}

  TraceType GetType() const override { return kTraceMultiGet; };

  // Column familiy IDs.
  std::vector<uint32_t> cf_ids;

  // Keys to get.
  std::vector<std::string> keys;
};

}  // namespace ROCKSDB_NAMESPACE
