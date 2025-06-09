//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

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
  // TODO: split out kinds of filter blocks?
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

class GetQueryTraceRecord;
class IteratorSeekQueryTraceRecord;
class MultiGetQueryTraceRecord;
class TraceRecordResult;
class WriteQueryTraceRecord;

// Base class for all types of trace records.
class TraceRecord {
 public:
  explicit TraceRecord(uint64_t timestamp);

  virtual ~TraceRecord() = default;

  // Type of the trace record.
  virtual TraceType GetTraceType() const = 0;

  // Timestamp (in microseconds) of this trace.
  virtual uint64_t GetTimestamp() const;

  class Handler {
   public:
    virtual ~Handler() = default;

    virtual Status Handle(const WriteQueryTraceRecord& record,
                          std::unique_ptr<TraceRecordResult>* result) = 0;

    virtual Status Handle(const GetQueryTraceRecord& record,
                          std::unique_ptr<TraceRecordResult>* result) = 0;

    virtual Status Handle(const IteratorSeekQueryTraceRecord& record,
                          std::unique_ptr<TraceRecordResult>* result) = 0;

    virtual Status Handle(const MultiGetQueryTraceRecord& record,
                          std::unique_ptr<TraceRecordResult>* result) = 0;
  };

  // Accept the handler and report the corresponding result in `result`.
  virtual Status Accept(Handler* handler,
                        std::unique_ptr<TraceRecordResult>* result) = 0;

  // Create a handler for the exeution of TraceRecord.
  static Handler* NewExecutionHandler(
      DB* db, const std::vector<ColumnFamilyHandle*>& handles);

 private:
  uint64_t timestamp_;
};

// Base class for all query types of trace records.
class QueryTraceRecord : public TraceRecord {
 public:
  explicit QueryTraceRecord(uint64_t timestamp);
};

// Trace record for DB::Write() operation.
class WriteQueryTraceRecord : public QueryTraceRecord {
 public:
  WriteQueryTraceRecord(PinnableSlice&& write_batch_rep, uint64_t timestamp);

  WriteQueryTraceRecord(const std::string& write_batch_rep, uint64_t timestamp);

  ~WriteQueryTraceRecord() override;

  TraceType GetTraceType() const override { return kTraceWrite; }

  // rep string for the WriteBatch.
  virtual Slice GetWriteBatchRep() const;

  Status Accept(Handler* handler,
                std::unique_ptr<TraceRecordResult>* result) override;

 private:
  PinnableSlice rep_;
};

// Trace record for DB::Get() operation
class GetQueryTraceRecord : public QueryTraceRecord {
 public:
  GetQueryTraceRecord(uint32_t column_family_id, PinnableSlice&& key,
                      uint64_t timestamp);

  GetQueryTraceRecord(uint32_t column_family_id, const std::string& key,
                      uint64_t timestamp);

  ~GetQueryTraceRecord() override;

  TraceType GetTraceType() const override { return kTraceGet; }

  // Column family ID.
  virtual uint32_t GetColumnFamilyID() const;

  // Key to get.
  virtual Slice GetKey() const;

  Status Accept(Handler* handler,
                std::unique_ptr<TraceRecordResult>* result) override;

 private:
  uint32_t cf_id_;
  PinnableSlice key_;
};

// Base class for all Iterator related operations.
class IteratorQueryTraceRecord : public QueryTraceRecord {
 public:
  explicit IteratorQueryTraceRecord(uint64_t timestamp);

  IteratorQueryTraceRecord(PinnableSlice&& lower_bound,
                           PinnableSlice&& upper_bound, uint64_t timestamp);

  IteratorQueryTraceRecord(const std::string& lower_bound,
                           const std::string& upper_bound, uint64_t timestamp);

  ~IteratorQueryTraceRecord() override;

  // Get the iterator's lower/upper bound. They may be used in ReadOptions to
  // create an Iterator instance.
  virtual Slice GetLowerBound() const;
  virtual Slice GetUpperBound() const;

 private:
  PinnableSlice lower_;
  PinnableSlice upper_;
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

  IteratorSeekQueryTraceRecord(SeekType seekType, uint32_t column_family_id,
                               PinnableSlice&& key, PinnableSlice&& lower_bound,
                               PinnableSlice&& upper_bound, uint64_t timestamp);

  IteratorSeekQueryTraceRecord(SeekType seekType, uint32_t column_family_id,
                               const std::string& key,
                               const std::string& lower_bound,
                               const std::string& upper_bound,
                               uint64_t timestamp);

  ~IteratorSeekQueryTraceRecord() override;

  // Trace type matches the seek type.
  TraceType GetTraceType() const override;

  // Type of seek, Seek or SeekForPrev.
  virtual SeekType GetSeekType() const;

  // Column family ID.
  virtual uint32_t GetColumnFamilyID() const;

  // Key to seek to.
  virtual Slice GetKey() const;

  Status Accept(Handler* handler,
                std::unique_ptr<TraceRecordResult>* result) override;

 private:
  SeekType type_;
  uint32_t cf_id_;
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

  ~MultiGetQueryTraceRecord() override;

  TraceType GetTraceType() const override { return kTraceMultiGet; }

  // Column familiy IDs.
  virtual std::vector<uint32_t> GetColumnFamilyIDs() const;

  // Keys to get.
  virtual std::vector<Slice> GetKeys() const;

  Status Accept(Handler* handler,
                std::unique_ptr<TraceRecordResult>* result) override;

 private:
  std::vector<uint32_t> cf_ids_;
  std::vector<PinnableSlice> keys_;
};

}  // namespace ROCKSDB_NAMESPACE
