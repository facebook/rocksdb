//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/flush_job.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/version_set.h"
#include "port/port.h"
#include "port/likely.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/block_based_table_factory.h"
#include "table/merger.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/event_logger.h"
#include "util/file_util.h"
#include "util/logging.h"
#include "util/log_buffer.h"
#include "util/mutexlock.h"
#include "util/perf_context_imp.h"
#include "util/iostats_context_imp.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "util/thread_status_util.h"

namespace rocksdb {

FlushJob::FlushJob(const std::string& dbname, ColumnFamilyData* cfd,
                   const DBOptions& db_options,
                   const MutableCFOptions& mutable_cf_options,
                   const EnvOptions& env_options, VersionSet* versions,
                   InstrumentedMutex* db_mutex,
                   std::atomic<bool>* shutting_down,
                   SequenceNumber newest_snapshot, JobContext* job_context,
                   LogBuffer* log_buffer, Directory* db_directory,
                   Directory* output_file_directory,
                   CompressionType output_compression, Statistics* stats,
                   EventLogger* event_logger)
    : dbname_(dbname),
      cfd_(cfd),
      db_options_(db_options),
      mutable_cf_options_(mutable_cf_options),
      env_options_(env_options),
      versions_(versions),
      db_mutex_(db_mutex),
      shutting_down_(shutting_down),
      newest_snapshot_(newest_snapshot),
      job_context_(job_context),
      log_buffer_(log_buffer),
      db_directory_(db_directory),
      output_file_directory_(output_file_directory),
      output_compression_(output_compression),
      stats_(stats),
      event_logger_(event_logger) {
  // Update the thread status to indicate flush.
  ThreadStatusUtil::SetColumnFamily(cfd_);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
  TEST_SYNC_POINT("FlushJob::FlushJob()");
}

FlushJob::~FlushJob() {
  TEST_SYNC_POINT("FlushJob::~FlushJob()");
  ThreadStatusUtil::ResetThreadStatus();
}

Status FlushJob::Run(uint64_t* file_number) {
  AutoThreadOperationStageUpdater stage_run(
      ThreadStatus::STAGE_FLUSH_RUN);
  // Save the contents of the earliest memtable as a new Table
  uint64_t fn;
  autovector<MemTable*> mems;
  cfd_->imm()->PickMemtablesToFlush(&mems);
  if (mems.empty()) {
    LogToBuffer(log_buffer_, "[%s] Nothing in memtable to flush",
                cfd_->GetName().c_str());
    return Status::OK();
  }


  // entries mems are (implicitly) sorted in ascending order by their created
  // time. We will use the first memtable's `edit` to keep the meta info for
  // this flush.
  MemTable* m = mems[0];
  VersionEdit* edit = m->GetEdits();
  edit->SetPrevLogNumber(0);
  // SetLogNumber(log_num) indicates logs with number smaller than log_num
  // will no longer be picked up for recovery.
  edit->SetLogNumber(mems.back()->GetNextLogNumber());
  edit->SetColumnFamily(cfd_->GetID());

  // This will release and re-acquire the mutex.
  Status s = WriteLevel0Table(mems, edit, &fn);

  if (s.ok() &&
      (shutting_down_->load(std::memory_order_acquire) || cfd_->IsDropped())) {
    s = Status::ShutdownInProgress(
        "Database shutdown or Column family drop during flush");
  }

  if (!s.ok()) {
    cfd_->imm()->RollbackMemtableFlush(mems, fn);
  } else {
    // Replace immutable memtable with the generated Table
    s = cfd_->imm()->InstallMemtableFlushResults(
        cfd_, mutable_cf_options_, mems, versions_, db_mutex_, fn,
        &job_context_->memtables_to_free, db_directory_, log_buffer_);
  }

  if (s.ok() && file_number != nullptr) {
    *file_number = fn;
  }

  return s;
}

Status FlushJob::WriteLevel0Table(const autovector<MemTable*>& mems,
                                  VersionEdit* edit, uint64_t* filenumber) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_FLUSH_WRITE_L0);
  db_mutex_->AssertHeld();
  const uint64_t start_micros = db_options_.env->NowMicros();
  FileMetaData meta;
  // path 0 for level 0 file.
  meta.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  *filenumber = meta.fd.GetNumber();

  const SequenceNumber earliest_seqno_in_memtable =
      mems[0]->GetFirstSequenceNumber();
  Version* base = cfd_->current();
  base->Ref();  // it is likely that we do not need this reference
  Status s;
  {
    db_mutex_->Unlock();
    if (log_buffer_) {
      log_buffer_->FlushBufferToLog();
    }
    std::vector<Iterator*> memtables;
    ReadOptions ro;
    ro.total_order_seek = true;
    Arena arena;
    for (MemTable* m : mems) {
      Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
          "[%s] [JOB %d] Flushing memtable with next log file: %" PRIu64 "\n",
          cfd_->GetName().c_str(), job_context_->job_id, m->GetNextLogNumber());
      memtables.push_back(m->NewIterator(ro, &arena));
    }
    {
      ScopedArenaIterator iter(
          NewMergingIterator(&cfd_->internal_comparator(), &memtables[0],
                             static_cast<int>(memtables.size()), &arena));
      Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
          "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": started",
          cfd_->GetName().c_str(), job_context_->job_id, meta.fd.GetNumber());

      TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:output_compression",
                               &output_compression_);
      s = BuildTable(dbname_, db_options_.env, *cfd_->ioptions(), env_options_,
                     cfd_->table_cache(), iter.get(), &meta,
                     cfd_->internal_comparator(),
                     cfd_->int_tbl_prop_collector_factories(), newest_snapshot_,
                     earliest_seqno_in_memtable, output_compression_,
                     cfd_->ioptions()->compression_opts, Env::IO_HIGH);
      LogFlush(db_options_.info_log);
    }
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "[%s] [JOB %d] Level-0 flush table #%" PRIu64 ": %" PRIu64 " bytes %s",
        cfd_->GetName().c_str(), job_context_->job_id, meta.fd.GetNumber(),
        meta.fd.GetFileSize(), s.ToString().c_str());
    event_logger_->Log() << "event"
                         << "table_file_creation"
                         << "file_number" << meta.fd.GetNumber() << "file_size"
                         << meta.fd.GetFileSize();
    if (!db_options_.disableDataSync && output_file_directory_ != nullptr) {
      output_file_directory_->Fsync();
    }
    db_mutex_->Lock();
  }
  base->Unref();

  // re-acquire the most current version
  base = cfd_->current();

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.fd.GetFileSize() > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    // if we have more than 1 background thread, then we cannot
    // insert files directly into higher levels because some other
    // threads could be concurrently producing compacted files for
    // that key range.
    if (base != nullptr && db_options_.max_background_compactions <= 1 &&
        db_options_.max_background_flushes == 0 &&
        cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
      level = base->storage_info()->PickLevelForMemTableOutput(
          mutable_cf_options_, min_user_key, max_user_key);
      // If level does not match path id, reset level back to 0
      uint32_t fdpath = LevelCompactionPicker::GetPathId(
          *cfd_->ioptions(), mutable_cf_options_, level);
      if (fdpath != 0) {
        level = 0;
      }
    }
    edit->AddFile(level, meta.fd.GetNumber(), meta.fd.GetPathId(),
                  meta.fd.GetFileSize(), meta.smallest, meta.largest,
                  meta.smallest_seqno, meta.largest_seqno);
  }

  InternalStats::CompactionStats stats(1);
  stats.micros = db_options_.env->NowMicros() - start_micros;
  stats.bytes_written = meta.fd.GetFileSize();
  cfd_->internal_stats()->AddCompactionStats(level, stats);
  cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_FLUSHED,
                                     meta.fd.GetFileSize());
  RecordTick(stats_, COMPACT_WRITE_BYTES, meta.fd.GetFileSize());
  return s;
}

}  // namespace rocksdb
