//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "db/version_edit.h"
#include "rocksdb/table_properties.h"
#include "util/autovector.h"

#include <atomic>

namespace rocksdb {

class Arena;
class ColumnFamilyData;
class DBImpl;
class EventLogger;
class InstrumentedMutex;
class LogBuffer;
class LogsWithPrepTracker;
class MemTable;
class SnapshotChecker;
class Statistics;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

struct ImmutableDBOptions;
struct JobContext;
struct MutableCFOptions;

enum CompressionType : unsigned char;


class FlushJob {
 public:
  // TODO(icanadi) make effort to reduce number of parameters here
  // IMPORTANT: mutable_cf_options needs to be alive while FlushJob is alive
  FlushJob(const std::string& dbname, ColumnFamilyData* cfd,
           const ImmutableDBOptions& db_options,
           const MutableCFOptions& mutable_cf_options,
           const EnvOptions env_options, VersionSet* versions,
           InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
           std::vector<SequenceNumber> existing_snapshots,
           SequenceNumber earliest_write_conflict_snapshot,
           SnapshotChecker* snapshot_checker, JobContext* job_context,
           LogBuffer* log_buffer, Directory* db_directory,
           Directory* output_file_directory, CompressionType output_compression,
           Statistics* stats, EventLogger* event_logger, bool measure_io_stats);

  ~FlushJob();

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be called.
  void PickMemTable();
  Status Run(LogsWithPrepTracker* prep_tracker = nullptr,
             FileMetaData* file_meta = nullptr);
  void Cancel();
  TableProperties GetTableProperties() const { return table_properties_; }

 private:
  void ReportStartedFlush();
  void ReportFlushInputSize(const autovector<MemTable*>& mems);
  void RecordFlushIOStats();
  Status WriteLevel0Table();
  const std::string& dbname_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const MutableCFOptions& mutable_cf_options_;
  const EnvOptions env_options_;
  VersionSet* versions_;
  InstrumentedMutex* db_mutex_;
  std::atomic<bool>* shutting_down_;
  std::vector<SequenceNumber> existing_snapshots_;
  SequenceNumber earliest_write_conflict_snapshot_;
  SnapshotChecker* snapshot_checker_;
  JobContext* job_context_;
  LogBuffer* log_buffer_;
  Directory* db_directory_;
  Directory* output_file_directory_;
  CompressionType output_compression_;
  Statistics* stats_;
  EventLogger* event_logger_;
  TableProperties table_properties_;
  bool measure_io_stats_;

  // Variables below are set by PickMemTable():
  FileMetaData meta_;
  autovector<MemTable*> mems_;
  VersionEdit* edit_;
  Version* base_;
  bool pick_memtable_called;
};

}  // namespace rocksdb
