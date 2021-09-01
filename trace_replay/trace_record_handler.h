//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/trace_record.h"

namespace ROCKSDB_NAMESPACE {

// Handler to execute TraceRecord.
class TraceExecutionHandler : public TraceRecord::Handler {
 public:
  TraceExecutionHandler(DB* db,
                        const std::vector<ColumnFamilyHandle*>& handles);
  virtual ~TraceExecutionHandler() override;

  virtual Status Handle(const WriteQueryTraceRecord& record,
                        std::unique_ptr<TraceRecordResult>* result) override;
  virtual Status Handle(const GetQueryTraceRecord& record,
                        std::unique_ptr<TraceRecordResult>* result) override;
  virtual Status Handle(const IteratorSeekQueryTraceRecord& record,
                        std::unique_ptr<TraceRecordResult>* result) override;
  virtual Status Handle(const MultiGetQueryTraceRecord& record,
                        std::unique_ptr<TraceRecordResult>* result) override;

 private:
  DB* db_;
  std::unordered_map<uint32_t, ColumnFamilyHandle*> cf_map_;
  WriteOptions write_opts_;
  ReadOptions read_opts_;
  SystemClock* clock_;
};

// To do: Handler for trace_analyzer.

}  // namespace ROCKSDB_NAMESPACE
