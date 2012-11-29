// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/db_impl_hotcold.h"
#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <algorithm>
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/build_version.h"

// Tweakable constants
namespace {
const std::string kMetricsDBSubdir = "metrics";
} // Tweakable constants

namespace leveldb {

DBImplHotCold::DBImplHotCold(const Options& options,
    const std::string& dbname, Status& s)
    : dataDB_(NULL),
      metricsDB_(NULL) {
  Log(options.info_log, "Opening the db with hot-cold separation");

  // Setup internal database.
  Log(options.info_log, "Opening internal database.");
  s = DB::Open(options, dbname, &dataDB_);
  if (!s.ok()) {
    return;
  }

  // Setup metrics database.
  Options metricsOpts;
  metricsOpts.create_if_missing = true;

  Log(options.info_log, "Opening database to contain metrics.");
  s = DB::Open(metricsOpts, dbname + "/" + kMetricsDBSubdir, &metricsDB_);
  if (!s.ok()) {
    delete dataDB_;
    dataDB_ = NULL;
    return;
  }

  assert(dataDB_ != NULL);
  assert(metricsDB_ != NULL);
}

DBImplHotCold::~DBImplHotCold() {
  delete dataDB_;
  delete metricsDB_;
}

Status DBImplHotCold::Put(const WriteOptions& options, const Slice& key,
                          const Slice& value) {
  // TODO: implement taking metrics.
  return dataDB_->Put(options, key, value);
}

Status DBImplHotCold::Delete(const WriteOptions& options, const Slice& key) {
  // TODO: implement taking metrics.
  return dataDB_->Delete(options, key);
}

Status DBImplHotCold::Write(const WriteOptions& options, WriteBatch* updates) {
  // TODO: implement taking metrics.
  return dataDB_->Write(options, updates);
}

Status DBImplHotCold::Get(const ReadOptions& options,
                          const Slice& key,
                          std::string* value) {
  // TODO: implement taking metrics.
  return dataDB_->Get(options, key, value);
}

Iterator* DBImplHotCold::NewIterator(const ReadOptions& options) {
  // TODO: implement taking metrics.
  return dataDB_->NewIterator(options);
}

const Snapshot* DBImplHotCold::GetSnapshot() {
  return dataDB_->GetSnapshot();
}

void DBImplHotCold::ReleaseSnapshot(const Snapshot* snapshot) {
  dataDB_->ReleaseSnapshot(snapshot);
}

bool DBImplHotCold::GetProperty(const Slice& property, std::string* value) {
  return dataDB_->GetProperty(property, value);
}

void DBImplHotCold::GetApproximateSizes(const Range* range, int n,
                                        uint64_t* sizes) {
  dataDB_->GetApproximateSizes(range, n, sizes);
}

void DBImplHotCold::CompactRange(const Slice* begin, const Slice* end) {
  // TODO: implement
}

int DBImplHotCold::NumberLevels() {
  return dataDB_->NumberLevels();
}

int DBImplHotCold::MaxMemCompactionLevel() {
  return dataDB_->MaxMemCompactionLevel();
}

int DBImplHotCold::Level0StopWriteTrigger() {
  return dataDB_->Level0StopWriteTrigger();
}

Status DBImplHotCold::Flush(const FlushOptions& options) {
  // TODO: consider what to do with metricsDB_
  return dataDB_->Flush(options);
}

Status DBImplHotCold::DisableFileDeletions() {
  // TODO: consider what to do with metricsDB_
  return dataDB_->DisableFileDeletions();
}

Status DBImplHotCold::EnableFileDeletions() {
  // TODO: consider what to do with metricsDB_
  return dataDB_->EnableFileDeletions();
}

Status DBImplHotCold::GetLiveFiles(std::vector<std::string>& ret,
                                   uint64_t* manifest_file_size) {
  // TODO: consider what to do with metricsDB_
  return dataDB_->GetLiveFiles(ret, manifest_file_size);
}

Status DB::OpenWithHotCold(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  Status s = Status::OK();
  DBImplHotCold* impl = new DBImplHotCold(options, dbname, s);
  if (!s.ok()) {
    delete impl;
  } else {
    *dbptr = impl;
  }

  return s;
}

} // namespace leveldb
