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
#include <functional>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/memtable_list.h"
#include "db/range_del_aggregator.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "options/db_options.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/transaction_log.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/event_logger.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace rocksdb {

class Arena;
class ErrorHandler;
class MemTable;
class SnapshotChecker;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class MapBuilder {
 public:
  MapBuilder(int job_id, const ImmutableDBOptions& db_options,
             const EnvOptions env_options, VersionSet* versions,
             LogBuffer* log_buffer,
             Statistics* stats, InstrumentedMutex* db_mutex,
             ErrorHandler* db_error_handler,
             std::vector<SequenceNumber> existing_snapshots,
             std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
             const std::string& dbname);

  // no copy/move
  MapBuilder(MapBuilder&& job) = delete;
  MapBuilder(const MapBuilder& job) = delete;
  MapBuilder& operator=(const MapBuilder& job) = delete;

  Status Build(const std::vector<CompactionInputFiles>& inputs,
               const std::vector<RangePtr>& deleted_range,
               const std::vector<const FileMetaData*>& added_files,
               uint32_t output_path_id, VersionStorageInfo* vstorage,
               ColumnFamilyData* cfd, VersionEdit* edit,
               FileMetaData* file_meta, TableProperties* porp);

 private:
  int job_id_;

  // DBImpl state
  const std::string& dbname_;
  const ImmutableDBOptions& db_options_;
  const EnvOptions env_options_;

  Env* env_;
  // env_option optimized for compaction table reads
  EnvOptions env_optiosn_for_read_;
  VersionSet* versions_;
  LogBuffer* log_buffer_;
  Statistics* stats_;
  InstrumentedMutex* db_mutex_;
  ErrorHandler* db_error_handler_;
  // If there were two snapshots with seq numbers s1 and
  // s2 and s1 < s2, and if we find two instances of a key k1 then lies
  // entirely within s1 and s2, then the earlier version of k1 can be safely
  // deleted because that version is not visible in any snapshot.
  std::vector<SequenceNumber> existing_snapshots_;

  std::shared_ptr<Cache> table_cache_;

  EventLogger* event_logger_;
};

}  // namespace rocksdb
