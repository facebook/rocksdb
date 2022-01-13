//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <list>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_completion_callback.h"
#include "db/column_family.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable_list.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "logging/event_logger.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/transaction_log.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class MemTable;
class SnapshotChecker;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class Arena;

class FlushJob {
 public:
  // TODO(icanadi) make effort to reduce number of parameters here
  // IMPORTANT: mutable_cf_options needs to be alive while FlushJob is alive
  FlushJob(const std::string& dbname, ColumnFamilyData* cfd,
           const ImmutableDBOptions& db_options,
           const MutableCFOptions& mutable_cf_options, uint64_t max_memtable_id,
           const FileOptions& file_options, VersionSet* versions,
           InstrumentedMutex* db_mutex, std::atomic<bool>* shutting_down,
           std::vector<SequenceNumber> existing_snapshots,
           SequenceNumber earliest_write_conflict_snapshot,
           SnapshotChecker* snapshot_checker, JobContext* job_context,
           LogBuffer* log_buffer, FSDirectory* db_directory,
           FSDirectory* output_file_directory,
           CompressionType output_compression, Statistics* stats,
           EventLogger* event_logger, bool measure_io_stats,
           const bool sync_output_directory, const bool write_manifest,
           Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
           const std::string& db_id = "", const std::string& db_session_id = "",
           std::string full_history_ts_low = "",
           BlobFileCompletionCallback* blob_callback = nullptr);

  ~FlushJob();

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be called.
  void PickMemTable();
  Status Run(LogsWithPrepTracker* prep_tracker = nullptr,
             FileMetaData* file_meta = nullptr,
             bool* switched_to_mempurge = nullptr);
  void Cancel();
  const autovector<MemTable*>& GetMemTables() const { return mems_; }

#ifndef ROCKSDB_LITE
  std::list<std::unique_ptr<FlushJobInfo>>* GetCommittedFlushJobsInfo() {
    return &committed_flush_jobs_info_;
  }
#endif  // !ROCKSDB_LITE

  // Return the IO status
  IOStatus io_status() const { return io_status_; }

 private:
  void ReportStartedFlush();
  void ReportFlushInputSize(const autovector<MemTable*>& mems);
  void RecordFlushIOStats();
  Status WriteLevel0Table();

  // Memtable Garbage Collection algorithm: a MemPurge takes the list
  // of immutable memtables and filters out (or "purge") the outdated bytes
  // out of it. The output (the filtered bytes, or "useful payload") is
  // then transfered into a new memtable. If this memtable is filled, then
  // the mempurge is aborted and rerouted to a regular flush process. Else,
  // depending on the heuristics, placed onto the immutable memtable list.
  // The addition to the imm list will not trigger a flush operation. The
  // flush of the imm list will instead be triggered once the mutable memtable
  // is added to the imm list.
  // This process is typically intended for workloads with heavy overwrites
  // when we want to avoid SSD writes (and reads) as much as possible.
  // "MemPurge" is an experimental feature still at a very early stage
  // of development. At the moment it is only compatible with the Get, Put,
  // Delete operations as well as Iterators and CompactionFilters.
  // For this early version, "MemPurge" is called by setting the
  // options.experimental_mempurge_threshold value as >0.0. When this is
  // the case, ALL automatic flush operations (kWRiteBufferManagerFull) will
  // first go through the MemPurge process. Therefore, we strongly
  // recommend all users not to set this flag as true given that the MemPurge
  // process has not matured yet.
  Status MemPurge();
  bool MemPurgeDecider();
#ifndef ROCKSDB_LITE
  std::unique_ptr<FlushJobInfo> GetFlushJobInfo() const;
#endif  // !ROCKSDB_LITE

  const std::string& dbname_;
  const std::string db_id_;
  const std::string db_session_id_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const MutableCFOptions& mutable_cf_options_;
  // A variable storing the largest memtable id to flush in this
  // flush job. RocksDB uses this variable to select the memtables to flush in
  // this job. All memtables in this column family with an ID smaller than or
  // equal to max_memtable_id_ will be selected for flush.
  uint64_t max_memtable_id_;
  const FileOptions file_options_;
  VersionSet* versions_;
  InstrumentedMutex* db_mutex_;
  std::atomic<bool>* shutting_down_;
  std::vector<SequenceNumber> existing_snapshots_;
  SequenceNumber earliest_write_conflict_snapshot_;
  SnapshotChecker* snapshot_checker_;
  JobContext* job_context_;
  LogBuffer* log_buffer_;
  FSDirectory* db_directory_;
  FSDirectory* output_file_directory_;
  CompressionType output_compression_;
  Statistics* stats_;
  EventLogger* event_logger_;
  TableProperties table_properties_;
  bool measure_io_stats_;
  // True if this flush job should call fsync on the output directory. False
  // otherwise.
  // Usually sync_output_directory_ is true. A flush job needs to call sync on
  // the output directory before committing to the MANIFEST.
  // However, an individual flush job does not have to call sync on the output
  // directory if it is part of an atomic flush. After all flush jobs in the
  // atomic flush succeed, call sync once on each distinct output directory.
  const bool sync_output_directory_;
  // True if this flush job should write to MANIFEST after successfully
  // flushing memtables. False otherwise.
  // Usually write_manifest_ is true. A flush job commits to the MANIFEST after
  // flushing the memtables.
  // However, an individual flush job cannot rashly write to the MANIFEST
  // immediately after it finishes the flush if it is part of an atomic flush.
  // In this case, only after all flush jobs succeed in flush can RocksDB
  // commit to the MANIFEST.
  const bool write_manifest_;
  // The current flush job can commit flush result of a concurrent flush job.
  // We collect FlushJobInfo of all jobs committed by current job and fire
  // OnFlushCompleted for them.
  std::list<std::unique_ptr<FlushJobInfo>> committed_flush_jobs_info_;

  // Variables below are set by PickMemTable():
  FileMetaData meta_;
  autovector<MemTable*> mems_;
  VersionEdit* edit_;
  Version* base_;
  bool pick_memtable_called;
  Env::Priority thread_pri_;
  IOStatus io_status_;

  const std::shared_ptr<IOTracer> io_tracer_;
  SystemClock* clock_;

  const std::string full_history_ts_low_;
  BlobFileCompletionCallback* blob_callback_;
};

}  // namespace ROCKSDB_NAMESPACE
