//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <set>
#include <utility>
#include <vector>
#include <string>
#include <functional>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "db/memtable_list.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/transaction_log.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"
#include "util/scoped_arena_iterator.h"
#include "db/internal_stats.h"
#include "db/write_controller.h"
#include "db/flush_scheduler.h"
#include "db/write_thread.h"
#include "db/job_context.h"

namespace rocksdb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class Arena;

class CompactionJob {
 public:
  // TODO(icanadi) make effort to reduce number of parameters here
  // IMPORTANT: mutable_cf_options needs to be alive while CompactionJob is
  // alive
  CompactionJob(int job_id, Compaction* compaction, const DBOptions& db_options,
                const MutableCFOptions& mutable_cf_options,
                const EnvOptions& env_options, VersionSet* versions,
                std::atomic<bool>* shutting_down, LogBuffer* log_buffer,
                Directory* db_directory, Directory* output_directory,
                Statistics* stats, SnapshotList* snapshot_list,
                bool is_snapshot_supported, std::shared_ptr<Cache> table_cache,
                std::function<uint64_t()> yield_callback);

  ~CompactionJob();

  // no copy/move
  CompactionJob(CompactionJob&& job) = delete;
  CompactionJob(const CompactionJob& job) = delete;
  CompactionJob& operator=(const CompactionJob& job) = delete;

  // REQUIRED: mutex held
  void Prepare();
  // REQUIRED mutex not held
  Status Run();
  // REQUIRED: mutex held
  // status is the return of Run()
  void Install(Status* status, InstrumentedMutex* db_mutex);

 private:
  void AllocateCompactionOutputFileNumbers();
  // Call compaction filter if is_compaction_v2 is not true. Then iterate
  // through input and compact the kv-pairs
  Status ProcessKeyValueCompaction(int64_t* imm_micros, Iterator* input,
                                   bool is_compaction_v2);
  // Call compaction_filter_v2->Filter() on kv-pairs in compact
  void CallCompactionFilterV2(CompactionFilterV2* compaction_filter_v2,
                              uint64_t* time);
  Status FinishCompactionOutputFile(Iterator* input);
  Status InstallCompactionResults(InstrumentedMutex* db_mutex);
  SequenceNumber findEarliestVisibleSnapshot(
      SequenceNumber in, const std::vector<SequenceNumber>& snapshots,
      SequenceNumber* prev_snapshot);
  void RecordCompactionIOStats();
  Status OpenCompactionOutputFile();
  void CleanupCompaction(const Status& status);

  int job_id_;

  // CompactionJob state
  struct CompactionState;
  CompactionState* compact_;

  bool bottommost_level_;
  SequenceNumber earliest_snapshot_;
  SequenceNumber visible_at_tip_;
  SequenceNumber latest_snapshot_;

  InternalStats::CompactionStats compaction_stats_;

  // DBImpl state
  const DBOptions& db_options_;
  const MutableCFOptions& mutable_cf_options_;
  const EnvOptions& env_options_;
  Env* env_;
  VersionSet* versions_;
  std::atomic<bool>* shutting_down_;
  LogBuffer* log_buffer_;
  Directory* db_directory_;
  Directory* output_directory_;
  Statistics* stats_;
  SnapshotList* snapshots_;
  bool is_snapshot_supported_;
  std::shared_ptr<Cache> table_cache_;

  // yield callback
  std::function<uint64_t()> yield_callback_;
};

}  // namespace rocksdb
