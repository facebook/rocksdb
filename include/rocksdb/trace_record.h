//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

// To do: ROCKSDB_LITE ?

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;
class Status;

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

class WriteQueryTraceRecord;
class GetQueryTraceRecord;
class IteratorSeekQueryTraceRecord;
class MultiGetQueryTraceRecord;

// Base class for all types of trace records.
class TraceRecord {
 public:
  TraceRecord();
  explicit TraceRecord(uint64_t timestamp);
  virtual ~TraceRecord();

  virtual TraceType type() const = 0;

  virtual uint64_t timestamp() const;

  class Handler {
   public:
    virtual ~Handler() {}

    virtual Status Handle(const WriteQueryTraceRecord& record) = 0;
    virtual Status Handle(const GetQueryTraceRecord& record) = 0;
    virtual Status Handle(const IteratorSeekQueryTraceRecord& record) = 0;
    virtual Status Handle(const MultiGetQueryTraceRecord& record) = 0;
  };

  class ExecutionHandler : public Handler {
   public:
    ExecutionHandler(DB* db, const std::vector<ColumnFamilyHandle*>& handles);

    ExecutionHandler(
        DB* db,
        const std::unordered_map<uint32_t, ColumnFamilyHandle*>& cf_map);

    virtual ~ExecutionHandler() override;

    virtual Status Handle(const WriteQueryTraceRecord& record) override;
    virtual Status Handle(const GetQueryTraceRecord& record) override;
    virtual Status Handle(const IteratorSeekQueryTraceRecord& record) override;
    virtual Status Handle(const MultiGetQueryTraceRecord& record) override;

   private:
    DB* db_;
    std::unordered_map<uint32_t, ColumnFamilyHandle*> cf_map_;
  };

  virtual Status Accept(Handler* handler) = 0;

 private:
  // Timestamp (in microseconds) of this trace.
  uint64_t timestamp_;
};

// Base class for all query types of trace records.
class QueryTraceRecord : public TraceRecord {
 public:
  explicit QueryTraceRecord(uint64_t timestamp);

  virtual ~QueryTraceRecord() override;
};

// Trace record for DB::Write() operation.
class WriteQueryTraceRecord : public QueryTraceRecord {
 public:
  WriteQueryTraceRecord(const WriteBatch& batch, uint64_t timestamp);

  WriteQueryTraceRecord(WriteBatch&& batch, uint64_t timestamp);

  WriteQueryTraceRecord(const std::string& rep, uint64_t timestamp);

  WriteQueryTraceRecord(std::string&& rep, uint64_t timestamp);

  WriteQueryTraceRecord(std::shared_ptr<WriteBatch> batch, uint64_t timestamp);

  virtual ~WriteQueryTraceRecord() override;

  TraceType type() const override { return kTraceWrite; };

  virtual std::shared_ptr<WriteBatch> writeBatch() const;

  virtual Status Accept(Handler* handler) override;

 private:
  std::shared_ptr<WriteBatch> batch_;
};

// Trace record for DB::Get() operation
class GetQueryTraceRecord : public QueryTraceRecord {
 public:
  GetQueryTraceRecord(uint32_t column_family_id, PinnableSlice&& key,
                      uint64_t timestamp);

  GetQueryTraceRecord(uint32_t column_family_id, const std::string& key,
                      uint64_t timestamp);

  virtual ~GetQueryTraceRecord() override;

  TraceType type() const override { return kTraceGet; };

  virtual uint32_t columnFamilyID() const;

  virtual Slice key() const;

  virtual Status Accept(Handler* handler) override;

 private:
  // Column family ID.
  uint32_t cf_id_;
  // Key to get.
  PinnableSlice key_;
};

// Base class for all Iterator related operations.
class IteratorQueryTraceRecord : public QueryTraceRecord {
 public:
  explicit IteratorQueryTraceRecord(uint64_t timestamp);

  virtual ~IteratorQueryTraceRecord() override;
};

// Trace record for Iterator::Seek() and Iterator::SeekForPrev() operation.
class IteratorSeekQueryTraceRecord : public IteratorQueryTraceRecord {
 public:
  // Currently we only support Seek() and SeekForPrev().
  enum SeekType {
    kSeek = kTraceIteratorSeek,
    kSeekForPrev = kTraceIteratorSeekForPrev
  };

  IteratorSeekQueryTraceRecord(SeekType seekType, uint32_t column_family_id,
                               PinnableSlice&& key, uint64_t timestamp);

  IteratorSeekQueryTraceRecord(SeekType seekType, uint32_t column_family_id,
                               const std::string& key, uint64_t timestamp);

  virtual ~IteratorSeekQueryTraceRecord() override;

  TraceType type() const override;

  virtual SeekType seekType() const;

  virtual uint32_t columnFamilyID() const;

  virtual Slice key() const;

  virtual Status Accept(Handler* handler) override;

 private:
  SeekType type_;
  // Column family ID.
  uint32_t cf_id_;
  // Key to seek to.
  PinnableSlice key_;
};

// Trace record for DB::MultiGet() operation.
class MultiGetQueryTraceRecord : public QueryTraceRecord {
 public:
  MultiGetQueryTraceRecord(std::vector<uint32_t> column_family_ids,
                           std::vector<PinnableSlice>&& keys,
                           uint64_t timestamp);

  MultiGetQueryTraceRecord(std::vector<uint32_t> column_family_ids,
                           const std::vector<std::string>& keys,
                           uint64_t timestamp);

  virtual ~MultiGetQueryTraceRecord() override;

  TraceType type() const override { return kTraceMultiGet; };

  virtual std::vector<uint32_t> columnFamilyIDs() const;

  virtual std::vector<Slice> keys() const;

  virtual Status Accept(Handler* handler) override;

 private:
  // Column familiy IDs.
  std::vector<uint32_t> cf_ids_;
  // Keys to get.
  std::vector<PinnableSlice> keys_;
};

}  // namespace ROCKSDB_NAMESPACE
