//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>

#include "db/builder.h"
#include "db/event_helpers.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "util/sst_file_manager_impl.h"
#include "util/sync_point.h"

namespace rocksdb {
Status DBImpl::SyncClosedLogs(JobContext* job_context) {
  TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Start");
  mutex_.AssertHeld();
  autovector<log::Writer*, 1> logs_to_sync;
  uint64_t current_log_number = logfile_number_;
  while (logs_.front().number < current_log_number &&
         logs_.front().getting_synced) {
    log_sync_cv_.Wait();
  }
  for (auto it = logs_.begin();
       it != logs_.end() && it->number < current_log_number; ++it) {
    auto& log = *it;
    assert(!log.getting_synced);
    log.getting_synced = true;
    logs_to_sync.push_back(log.writer);
  }

  Status s;
  if (!logs_to_sync.empty()) {
    mutex_.Unlock();

    for (log::Writer* log : logs_to_sync) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] Syncing log #%" PRIu64, job_context->job_id,
                     log->get_log_number());
      s = log->file()->Sync(immutable_db_options_.use_fsync);
    }
    if (s.ok()) {
      s = directories_.GetWalDir()->Fsync();
    }

    mutex_.Lock();

    // "number <= current_log_number - 1" is equivalent to
    // "number < current_log_number".
    MarkLogsSynced(current_log_number - 1, true, s);
    if (!s.ok()) {
      Status new_bg_error = s;
      // may temporarily unlock and lock the mutex.
      EventHelpers::NotifyOnBackgroundError(immutable_db_options_.listeners,
                                            BackgroundErrorReason::kFlush,
                                            &new_bg_error, &mutex_);
      if (!new_bg_error.ok()) {
        bg_error_ = new_bg_error;
      }
      TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Failed");
      return s;
    }
  }
  return s;
}

Status DBImpl::FlushMemTableToOutputFile(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    bool* made_progress, JobContext* job_context, LogBuffer* log_buffer) {
  mutex_.AssertHeld();
  assert(cfd->imm()->NumNotFlushed() != 0);
  assert(cfd->imm()->IsFlushPending());

  SequenceNumber earliest_write_conflict_snapshot;
  std::vector<SequenceNumber> snapshot_seqs =
      snapshots_.GetAll(&earliest_write_conflict_snapshot);

  auto snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && snapshot_checker == nullptr) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }
  FlushJob flush_job(
      dbname_, cfd, immutable_db_options_, mutable_cf_options,
      env_options_for_compaction_, versions_.get(), &mutex_, &shutting_down_,
      snapshot_seqs, earliest_write_conflict_snapshot, snapshot_checker,
      job_context, log_buffer, directories_.GetDbDir(),
      directories_.GetDataDir(0U),
      GetCompressionFlush(*cfd->ioptions(), mutable_cf_options), stats_,
      &event_logger_, mutable_cf_options.report_bg_io_stats);

  FileMetaData file_meta;

  flush_job.PickMemTable();

#ifndef ROCKSDB_LITE
  // may temporarily unlock and lock the mutex.
  NotifyOnFlushBegin(cfd, &file_meta, mutable_cf_options, job_context->job_id,
                     flush_job.GetTableProperties());
#endif  // ROCKSDB_LITE

  Status s;
  if (logfile_number_ > 0 &&
      versions_->GetColumnFamilySet()->NumberOfColumnFamilies() > 0) {
    // If there are more than one column families, we need to make sure that
    // all the log files except the most recent one are synced. Otherwise if
    // the host crashes after flushing and before WAL is persistent, the
    // flushed SST may contain data from write batches whose updates to
    // other column families are missing.
    // SyncClosedLogs() may unlock and re-lock the db_mutex.
    s = SyncClosedLogs(job_context);
  }

  // Within flush_job.Run, rocksdb may call event listener to notify
  // file creation and deletion.
  //
  // Note that flush_job.Run will unlock and lock the db_mutex,
  // and EventListener callback will be called when the db_mutex
  // is unlocked by the current thread.
  if (s.ok()) {
    s = flush_job.Run(&file_meta);
  } else {
    flush_job.Cancel();
  }

  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, &job_context->superversion_context,
                                       mutable_cf_options);
    if (made_progress) {
      *made_progress = 1;
    }
    VersionStorageInfo::LevelSummaryStorage tmp;
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                     cfd->GetName().c_str(),
                     cfd->current()->storage_info()->LevelSummary(&tmp));
  }

  if (!s.ok() && !s.IsShutdownInProgress() &&
      immutable_db_options_.paranoid_checks && bg_error_.ok()) {
    Status new_bg_error = s;
    // may temporarily unlock and lock the mutex.
    EventHelpers::NotifyOnBackgroundError(immutable_db_options_.listeners,
                                          BackgroundErrorReason::kFlush,
                                          &new_bg_error, &mutex_);
    if (!new_bg_error.ok()) {
      // if a bad error happened (not ShutdownInProgress), paranoid_checks is
      // true, and the error isn't handled by callback, mark DB read-only
      bg_error_ = new_bg_error;
    }
  }
  if (s.ok()) {
#ifndef ROCKSDB_LITE
    // may temporarily unlock and lock the mutex.
    NotifyOnFlushCompleted(cfd, &file_meta, mutable_cf_options,
                           job_context->job_id, flush_job.GetTableProperties());
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm) {
      // Notify sst_file_manager that a new file was added
      std::string file_path = MakeTableFileName(
          immutable_db_options_.db_paths[0].path, file_meta.fd.GetNumber());
      sfm->OnAddFile(file_path);
      if (sfm->IsMaxAllowedSpaceReached() && bg_error_.ok()) {
        Status new_bg_error = Status::IOError("Max allowed space was reached");
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
            &new_bg_error);
        // may temporarily unlock and lock the mutex.
        EventHelpers::NotifyOnBackgroundError(immutable_db_options_.listeners,
                                              BackgroundErrorReason::kFlush,
                                              &new_bg_error, &mutex_);
        if (!new_bg_error.ok()) {
          bg_error_ = new_bg_error;
        }
      }
    }
#endif  // ROCKSDB_LITE
  }
  return s;
}

void DBImpl::NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                                const MutableCFOptions& mutable_cf_options,
                                int job_id, TableProperties prop) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    FlushJobInfo info;
    info.cf_name = cfd->GetName();
    // TODO(yhchiang): make db_paths dynamic in case flush does not
    //                 go to L0 in the future.
    info.file_path = MakeTableFileName(immutable_db_options_.db_paths[0].path,
                                       file_meta->fd.GetNumber());
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.triggered_writes_slowdown = triggered_writes_slowdown;
    info.triggered_writes_stop = triggered_writes_stop;
    info.smallest_seqno = file_meta->smallest_seqno;
    info.largest_seqno = file_meta->largest_seqno;
    info.table_properties = prop;
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnFlushBegin(this, info);
    }
  }
  mutex_.Lock();
// no need to signal bg_cv_ as it will be signaled at the end of the
// flush process.
#endif  // ROCKSDB_LITE
}

void DBImpl::NotifyOnFlushCompleted(ColumnFamilyData* cfd,
                                    FileMetaData* file_meta,
                                    const MutableCFOptions& mutable_cf_options,
                                    int job_id, TableProperties prop) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    FlushJobInfo info;
    info.cf_name = cfd->GetName();
    // TODO(yhchiang): make db_paths dynamic in case flush does not
    //                 go to L0 in the future.
    info.file_path = MakeTableFileName(immutable_db_options_.db_paths[0].path,
                                       file_meta->fd.GetNumber());
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.triggered_writes_slowdown = triggered_writes_slowdown;
    info.triggered_writes_stop = triggered_writes_stop;
    info.smallest_seqno = file_meta->smallest_seqno;
    info.largest_seqno = file_meta->largest_seqno;
    info.table_properties = prop;
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnFlushCompleted(this, info);
    }
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
#endif  // ROCKSDB_LITE
}

Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice* begin, const Slice* end) {
  if (options.target_path_id >= immutable_db_options_.db_paths.size()) {
    return Status::InvalidArgument("Invalid target path ID");
  }

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  bool exclusive = options.exclusive_manual_compaction;

  Status s = FlushMemTable(cfd, FlushOptions());
  if (!s.ok()) {
    LogFlush(immutable_db_options_.info_log);
    return s;
  }

  int max_level_with_files = 0;
  {
    InstrumentedMutexLock l(&mutex_);
    Version* base = cfd->current();
    for (int level = 1; level < base->storage_info()->num_non_empty_levels();
         level++) {
      if (base->storage_info()->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }

  int final_output_level = 0;
  if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal &&
      cfd->NumberLevels() > 1) {
    // Always compact all files together.
    final_output_level = cfd->NumberLevels() - 1;
    // if bottom most level is reserved
    if (immutable_db_options_.allow_ingest_behind) {
      final_output_level--;
    }
    s = RunManualCompaction(cfd, ColumnFamilyData::kCompactAllLevels,
                            final_output_level, options.target_path_id,
                            begin, end, exclusive);
  } else {
    for (int level = 0; level <= max_level_with_files; level++) {
      int output_level;
      // in case the compaction is universal or if we're compacting the
      // bottom-most level, the output level will be the same as input one.
      // level 0 can never be the bottommost level (i.e. if all files are in
      // level 0, we will compact to level 1)
      if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
          cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
        output_level = level;
      } else if (level == max_level_with_files && level > 0) {
        if (options.bottommost_level_compaction ==
            BottommostLevelCompaction::kSkip) {
          // Skip bottommost level compaction
          continue;
        } else if (options.bottommost_level_compaction ==
                       BottommostLevelCompaction::kIfHaveCompactionFilter &&
                   cfd->ioptions()->compaction_filter == nullptr &&
                   cfd->ioptions()->compaction_filter_factory == nullptr) {
          // Skip bottommost level compaction since we don't have a compaction
          // filter
          continue;
        }
        output_level = level;
      } else {
        output_level = level + 1;
        if (cfd->ioptions()->compaction_style == kCompactionStyleLevel &&
            cfd->ioptions()->level_compaction_dynamic_level_bytes &&
            level == 0) {
          output_level = ColumnFamilyData::kCompactToBaseLevel;
        }
      }
      s = RunManualCompaction(cfd, level, output_level, options.target_path_id,
                              begin, end, exclusive);
      if (!s.ok()) {
        break;
      }
      if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
        final_output_level = cfd->NumberLevels() - 1;
      } else if (output_level > final_output_level) {
        final_output_level = output_level;
      }
      TEST_SYNC_POINT("DBImpl::RunManualCompaction()::1");
      TEST_SYNC_POINT("DBImpl::RunManualCompaction()::2");
    }
  }
  if (!s.ok()) {
    LogFlush(immutable_db_options_.info_log);
    return s;
  }

  if (options.change_level) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[RefitLevel] waiting for background threads to stop");
    s = PauseBackgroundWork();
    if (s.ok()) {
      s = ReFitLevel(cfd, final_output_level, options.target_level);
    }
    ContinueBackgroundWork();
  }
  LogFlush(immutable_db_options_.info_log);

  {
    InstrumentedMutexLock l(&mutex_);
    // an automatic compaction that has been scheduled might have been
    // preempted by the manual compactions. Need to schedule it back.
    MaybeScheduleFlushOrCompaction();
  }

  return s;
}

Status DBImpl::CompactFiles(
    const CompactionOptions& compact_options,
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& input_file_names,
    const int output_level, const int output_path_id) {
#ifdef ROCKSDB_LITE
    // not supported in lite version
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  if (column_family == nullptr) {
    return Status::InvalidArgument("ColumnFamilyHandle must be non-null.");
  }

  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  assert(cfd);

  Status s;
  JobContext job_context(0, true);
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  // Perform CompactFiles
  SuperVersion* sv = cfd->GetReferencedSuperVersion(&mutex_);
  {
    InstrumentedMutexLock l(&mutex_);

    // This call will unlock/lock the mutex to wait for current running
    // IngestExternalFile() calls to finish.
    WaitForIngestFile();

    s = CompactFilesImpl(compact_options, cfd, sv->current,
                         input_file_names, output_level,
                         output_path_id, &job_context, &log_buffer);
  }
  if (sv->Unref()) {
    mutex_.Lock();
    sv->Cleanup();
    mutex_.Unlock();
    delete sv;
  }

  // Find and delete obsolete files
  {
    InstrumentedMutexLock l(&mutex_);
    // If !s.ok(), this means that Compaction failed. In that case, we want
    // to delete all obsolete files we might have created and we force
    // FindObsoleteFiles(). This is because job_context does not
    // catch all created files if compaction failed.
    FindObsoleteFiles(&job_context, !s.ok());
  }  // release the mutex

  // delete unnecessary files if any, this is done outside the mutex
  if (job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
    // Have to flush the info logs before bg_compaction_scheduled_--
    // because if bg_flush_scheduled_ becomes 0 and the lock is
    // released, the deconstructor of DB can kick in and destroy all the
    // states of DB so info_log might not be available after that point.
    // It also applies to access other states that DB owns.
    log_buffer.FlushBufferToLog();
    if (job_context.HaveSomethingToDelete()) {
      // no mutex is locked here.  No need to Unlock() and Lock() here.
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
  }

  return s;
#endif  // ROCKSDB_LITE
}

#ifndef ROCKSDB_LITE
Status DBImpl::CompactFilesImpl(
    const CompactionOptions& compact_options, ColumnFamilyData* cfd,
    Version* version, const std::vector<std::string>& input_file_names,
    const int output_level, int output_path_id, JobContext* job_context,
    LogBuffer* log_buffer) {
  mutex_.AssertHeld();

  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }

  std::unordered_set<uint64_t> input_set;
  for (auto file_name : input_file_names) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  ColumnFamilyMetaData cf_meta;
  // TODO(yhchiang): can directly use version here if none of the
  // following functions call is pluggable to external developers.
  version->GetColumnFamilyMetaData(&cf_meta);

  if (output_path_id < 0) {
    if (immutable_db_options_.db_paths.size() == 1U) {
      output_path_id = 0;
    } else {
      return Status::NotSupported(
          "Automatic output path selection is not "
          "yet supported in CompactFiles()");
    }
  }

  Status s = cfd->compaction_picker()->SanitizeCompactionInputFiles(
      &input_set, cf_meta, output_level);
  if (!s.ok()) {
    return s;
  }

  std::vector<CompactionInputFiles> input_files;
  s = cfd->compaction_picker()->GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, version->storage_info(), compact_options);
  if (!s.ok()) {
    return s;
  }

  for (auto inputs : input_files) {
    if (cfd->compaction_picker()->AreFilesInCompaction(inputs.files)) {
      return Status::Aborted(
          "Some of the necessary compaction input "
          "files are already being compacted");
    }
  }

  // At this point, CompactFiles will be run.
  bg_compaction_scheduled_++;

  unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  c.reset(cfd->compaction_picker()->CompactFiles(
      compact_options, input_files, output_level, version->storage_info(),
      *cfd->GetLatestMutableCFOptions(), output_path_id));
  // we already sanitized the set of input files and checked for conflicts
  // without releasing the lock, so we're guaranteed a compaction can be formed.
  assert(c != nullptr);

  c->SetInputVersion(version);
  // deletion compaction currently not allowed in CompactFiles.
  assert(!c->deletion_compaction());

  SequenceNumber earliest_write_conflict_snapshot;
  std::vector<SequenceNumber> snapshot_seqs =
      snapshots_.GetAll(&earliest_write_conflict_snapshot);

  auto pending_outputs_inserted_elem =
      CaptureCurrentFileNumberInPendingOutputs();

  auto snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && snapshot_checker == nullptr) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }
  assert(is_snapshot_supported_ || snapshots_.empty());
  CompactionJob compaction_job(
      job_context->job_id, c.get(), immutable_db_options_,
      env_options_for_compaction_, versions_.get(), &shutting_down_,
      preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
      directories_.GetDataDir(c->output_path_id()), stats_, &mutex_, &bg_error_,
      snapshot_seqs, earliest_write_conflict_snapshot, snapshot_checker,
      table_cache_, &event_logger_,
      c->mutable_cf_options()->paranoid_file_checks,
      c->mutable_cf_options()->report_bg_io_stats, dbname_,
      nullptr);  // Here we pass a nullptr for CompactionJobStats because
                 // CompactFiles does not trigger OnCompactionCompleted(),
                 // which is the only place where CompactionJobStats is
                 // returned.  The idea of not triggering OnCompationCompleted()
                 // is that CompactFiles runs in the caller thread, so the user
                 // should always know when it completes.  As a result, it makes
                 // less sense to notify the users something they should already
                 // know.
                 //
                 // In the future, if we would like to add CompactionJobStats
                 // support for CompactFiles, we should have CompactFiles API
                 // pass a pointer of CompactionJobStats as the out-value
                 // instead of using EventListener.

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here.
  version->storage_info()->ComputeCompactionScore(*cfd->ioptions(),
                                                  *c->mutable_cf_options());

  compaction_job.Prepare();

  mutex_.Unlock();
  TEST_SYNC_POINT("CompactFilesImpl:0");
  TEST_SYNC_POINT("CompactFilesImpl:1");
  compaction_job.Run();
  TEST_SYNC_POINT("CompactFilesImpl:2");
  TEST_SYNC_POINT("CompactFilesImpl:3");
  mutex_.Lock();

  Status status = compaction_job.Install(*c->mutable_cf_options());
  if (status.ok()) {
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), &job_context->superversion_context,
       *c->mutable_cf_options());
  }
  c->ReleaseCompactionFiles(s);

  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  if (status.ok()) {
    // Done
  } else if (status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Compaction error: %s",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id, status.ToString().c_str());
    if (immutable_db_options_.paranoid_checks && bg_error_.ok()) {
      Status new_bg_error = status;
      // may temporarily unlock and lock the mutex.
      EventHelpers::NotifyOnBackgroundError(immutable_db_options_.listeners,
                                            BackgroundErrorReason::kCompaction,
                                            &new_bg_error, &mutex_);
      if (!new_bg_error.ok()) {
        bg_error_ = new_bg_error;
      }
    }
  }

  c.reset();

  bg_compaction_scheduled_--;
  if (bg_compaction_scheduled_ == 0) {
    bg_cv_.SignalAll();
  }

  return status;
}
#endif  // ROCKSDB_LITE

Status DBImpl::PauseBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  bg_compaction_paused_++;
  while (bg_bottom_compaction_scheduled_ > 0 || bg_compaction_scheduled_ > 0 ||
         bg_flush_scheduled_ > 0) {
    bg_cv_.Wait();
  }
  bg_work_paused_++;
  return Status::OK();
}

Status DBImpl::ContinueBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  if (bg_work_paused_ == 0) {
    return Status::InvalidArgument();
  }
  assert(bg_work_paused_ > 0);
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  bg_work_paused_--;
  // It's sufficient to check just bg_work_paused_ here since
  // bg_work_paused_ is always no greater than bg_compaction_paused_
  if (bg_work_paused_ == 0) {
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

void DBImpl::NotifyOnCompactionCompleted(
    ColumnFamilyData* cfd, Compaction *c, const Status &st,
    const CompactionJobStats& compaction_job_stats,
    const int job_id) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  Version* current = cfd->current();
  current->Ref();
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionCompleted::UnlockMutex");
  {
    CompactionJobInfo info;
    info.cf_name = cfd->GetName();
    info.status = st;
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.base_input_level = c->start_level();
    info.output_level = c->output_level();
    info.stats = compaction_job_stats;
    info.table_properties = c->GetOutputTableProperties();
    info.compaction_reason = c->compaction_reason();
    info.compression = c->output_compression();
    for (size_t i = 0; i < c->num_input_levels(); ++i) {
      for (const auto fmd : *c->inputs(i)) {
        auto fn = TableFileName(immutable_db_options_.db_paths,
                                fmd->fd.GetNumber(), fmd->fd.GetPathId());
        info.input_files.push_back(fn);
        if (info.table_properties.count(fn) == 0) {
          std::shared_ptr<const TableProperties> tp;
          auto s = current->GetTableProperties(&tp, fmd, &fn);
          if (s.ok()) {
            info.table_properties[fn] = tp;
          }
        }
      }
    }
    for (const auto newf : c->edit()->GetNewFiles()) {
      info.output_files.push_back(TableFileName(immutable_db_options_.db_paths,
                                                newf.second.fd.GetNumber(),
                                                newf.second.fd.GetPathId()));
    }
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnCompactionCompleted(this, info);
    }
  }
  mutex_.Lock();
  current->Unref();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
#endif  // ROCKSDB_LITE
}

// REQUIREMENT: block all background work by calling PauseBackgroundWork()
// before calling this function
Status DBImpl::ReFitLevel(ColumnFamilyData* cfd, int level, int target_level) {
  assert(level < cfd->NumberLevels());
  if (target_level >= cfd->NumberLevels()) {
    return Status::InvalidArgument("Target level exceeds number of levels");
  }

  SuperVersionContext sv_context(/* create_superversion */ true);

  Status status;

  InstrumentedMutexLock guard_lock(&mutex_);

  // only allow one thread refitting
  if (refitting_level_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[ReFitLevel] another thread is refitting");
    return Status::NotSupported("another thread is refitting");
  }
  refitting_level_ = true;

  const MutableCFOptions mutable_cf_options = *cfd->GetLatestMutableCFOptions();
  // move to a smaller level
  int to_level = target_level;
  if (target_level < 0) {
    to_level = FindMinimumEmptyLevelFitting(cfd, mutable_cf_options, level);
  }

  auto* vstorage = cfd->current()->storage_info();
  if (to_level > level) {
    if (level == 0) {
      return Status::NotSupported(
          "Cannot change from level 0 to other levels.");
    }
    // Check levels are empty for a trivial move
    for (int l = level + 1; l <= to_level; l++) {
      if (vstorage->NumLevelFiles(l) > 0) {
        return Status::NotSupported(
            "Levels between source and target are not empty for a move.");
      }
    }
  }
  if (to_level != level) {
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Before refitting:\n%s", cfd->GetName().c_str(),
                    cfd->current()->DebugString().data());

    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    for (const auto& f : vstorage->LevelFiles(level)) {
      edit.DeleteFile(level, f->fd.GetNumber());
      edit.AddFile(to_level, f->fd.GetNumber(), f->fd.GetPathId(),
                   f->fd.GetFileSize(), f->smallest, f->largest,
                   f->smallest_seqno, f->largest_seqno,
                   f->marked_for_compaction);
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Apply version edit:\n%s", cfd->GetName().c_str(),
                    edit.DebugString().data());

    status = versions_->LogAndApply(cfd, mutable_cf_options, &edit, &mutex_,
                                    directories_.GetDbDir());
    InstallSuperVersionAndScheduleWork(cfd, &sv_context, mutable_cf_options);

    ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] LogAndApply: %s\n",
                    cfd->GetName().c_str(), status.ToString().data());

    if (status.ok()) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] After refitting:\n%s", cfd->GetName().c_str(),
                      cfd->current()->DebugString().data());
    }
  }

  sv_context.Clean();
  refitting_level_ = false;

  return status;
}

int DBImpl::NumberLevels(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return cfh->cfd()->NumberLevels();
}

int DBImpl::MaxMemCompactionLevel(ColumnFamilyHandle* column_family) {
  return 0;
}

int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  InstrumentedMutexLock l(&mutex_);
  return cfh->cfd()->GetSuperVersion()->
      mutable_cf_options.level0_stop_writes_trigger;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return FlushMemTable(cfh->cfd(), flush_options);
}

Status DBImpl::RunManualCompaction(ColumnFamilyData* cfd, int input_level,
                                   int output_level, uint32_t output_path_id,
                                   const Slice* begin, const Slice* end,
                                   bool exclusive, bool disallow_trivial_move) {
  assert(input_level == ColumnFamilyData::kCompactAllLevels ||
         input_level >= 0);

  InternalKey begin_storage, end_storage;
  CompactionArg* ca;

  bool scheduled = false;
  bool manual_conflict = false;
  ManualCompactionState manual;
  manual.cfd = cfd;
  manual.input_level = input_level;
  manual.output_level = output_level;
  manual.output_path_id = output_path_id;
  manual.done = false;
  manual.in_progress = false;
  manual.incomplete = false;
  manual.exclusive = exclusive;
  manual.disallow_trivial_move = disallow_trivial_move;
  // For universal compaction, we enforce every manual compaction to compact
  // all files.
  if (begin == nullptr ||
      cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
    manual.begin = nullptr;
  } else {
    begin_storage.SetMinPossibleForUserKey(*begin);
    manual.begin = &begin_storage;
  }
  if (end == nullptr ||
      cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
    manual.end = nullptr;
  } else {
    end_storage.SetMaxPossibleForUserKey(*end);
    manual.end = &end_storage;
  }

  TEST_SYNC_POINT("DBImpl::RunManualCompaction:0");
  TEST_SYNC_POINT("DBImpl::RunManualCompaction:1");
  InstrumentedMutexLock l(&mutex_);

  // When a manual compaction arrives, temporarily disable scheduling of
  // non-manual compactions and wait until the number of scheduled compaction
  // jobs drops to zero. This is needed to ensure that this manual compaction
  // can compact any range of keys/files.
  //
  // HasPendingManualCompaction() is true when at least one thread is inside
  // RunManualCompaction(), i.e. during that time no other compaction will
  // get scheduled (see MaybeScheduleFlushOrCompaction).
  //
  // Note that the following loop doesn't stop more that one thread calling
  // RunManualCompaction() from getting to the second while loop below.
  // However, only one of them will actually schedule compaction, while
  // others will wait on a condition variable until it completes.

  AddManualCompaction(&manual);
  TEST_SYNC_POINT_CALLBACK("DBImpl::RunManualCompaction:NotScheduled", &mutex_);
  if (exclusive) {
    while (bg_bottom_compaction_scheduled_ > 0 ||
           bg_compaction_scheduled_ > 0) {
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:WaitScheduled");
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "[%s] Manual compaction waiting for all other scheduled background "
          "compactions to finish",
          cfd->GetName().c_str());
      bg_cv_.Wait();
    }
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Manual compaction starting", cfd->GetName().c_str());

  // We don't check bg_error_ here, because if we get the error in compaction,
  // the compaction will set manual.status to bg_error_ and set manual.done to
  // true.
  while (!manual.done) {
    assert(HasPendingManualCompaction());
    manual_conflict = false;
    Compaction* compaction = nullptr;
    if (ShouldntRunManualCompaction(&manual) || (manual.in_progress == true) ||
        scheduled ||
        (((manual.manual_end = &manual.tmp_storage1) != nullptr) &&
             ((compaction = manual.cfd->CompactRange(
                  *manual.cfd->GetLatestMutableCFOptions(), manual.input_level,
                  manual.output_level, manual.output_path_id, manual.begin,
                  manual.end, &manual.manual_end, &manual_conflict)) == nullptr &&
         manual_conflict))) {
      // exclusive manual compactions should not see a conflict during
      // CompactRange
      assert(!exclusive || !manual_conflict);
      // Running either this or some other manual compaction
      bg_cv_.Wait();
      if (scheduled && manual.incomplete == true) {
        assert(!manual.in_progress);
        scheduled = false;
        manual.incomplete = false;
      }
    } else if (!scheduled) {
      if (compaction == nullptr) {
        manual.done = true;
        bg_cv_.SignalAll();
        continue;
      }
      ca = new CompactionArg;
      ca->db = this;
      ca->prepicked_compaction = new PrepickedCompaction;
      ca->prepicked_compaction->manual_compaction_state = &manual;
      ca->prepicked_compaction->compaction = compaction;
      manual.incomplete = false;
      bg_compaction_scheduled_++;
      env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW, this,
                     &DBImpl::UnscheduleCallback);
      scheduled = true;
    }
  }

  assert(!manual.in_progress);
  assert(HasPendingManualCompaction());
  RemoveManualCompaction(&manual);
  bg_cv_.SignalAll();
  return manual.status;
}

Status DBImpl::FlushMemTable(ColumnFamilyData* cfd,
                             const FlushOptions& flush_options,
                             bool writes_stopped) {
  Status s;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    if (cfd->imm()->NumNotFlushed() == 0 && cfd->mem()->IsEmpty() &&
        cached_recoverable_state_empty_.load()) {
      // Nothing to flush
      return Status::OK();
    }

    WriteThread::Writer w;
    if (!writes_stopped) {
      write_thread_.EnterUnbatched(&w, &mutex_);
    }

    // SwitchMemtable() will release and reacquire mutex during execution
    s = SwitchMemtable(cfd, &context);

    if (!writes_stopped) {
      write_thread_.ExitUnbatched(&w);
    }

    cfd->imm()->FlushRequested();

    // schedule flush
    SchedulePendingFlush(cfd);
    MaybeScheduleFlushOrCompaction();
  }

  if (s.ok() && flush_options.wait) {
    // Wait until the compaction completes
    s = WaitForFlushMemTable(cfd);
  }
  return s;
}

Status DBImpl::WaitForFlushMemTable(ColumnFamilyData* cfd) {
  Status s;
  // Wait until the compaction completes
  InstrumentedMutexLock l(&mutex_);
  while (cfd->imm()->NumNotFlushed() > 0 && bg_error_.ok()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress();
    }
    if (cfd->IsDropped()) {
      // FlushJob cannot flush a dropped CF, if we did not break here
      // we will loop forever since cfd->imm()->NumNotFlushed() will never
      // drop to zero
      return Status::InvalidArgument("Cannot flush a dropped CF");
    }
    bg_cv_.Wait();
  }
  if (!bg_error_.ok()) {
    s = bg_error_;
  }
  return s;
}

Status DBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  Status s;
  for (auto cf_ptr : column_family_handles) {
    Status status =
        this->SetOptions(cf_ptr, {{"disable_auto_compactions", "false"}});
    if (!status.ok()) {
      s = status;
    }
  }

  return s;
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();
  if (!opened_successfully_) {
    // Compaction may introduce data race to DB open
    return;
  }
  if (bg_work_paused_ > 0) {
    // we paused the background work
    return;
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
    return;
  }
  auto bg_job_limits = GetBGJobLimits();
  bool is_flush_pool_empty =
    env_->GetBackgroundThreads(Env::Priority::HIGH) == 0;
  while (!is_flush_pool_empty && unscheduled_flushes_ > 0 &&
         bg_flush_scheduled_ < bg_job_limits.max_flushes) {
    unscheduled_flushes_--;
    bg_flush_scheduled_++;
    env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::HIGH, this);
  }

  // special case -- if high-pri (flush) thread pool is empty, then schedule
  // flushes in low-pri (compaction) thread pool.
  if (is_flush_pool_empty) {
    while (unscheduled_flushes_ > 0 &&
           bg_flush_scheduled_ + bg_compaction_scheduled_ <
               bg_job_limits.max_flushes) {
      unscheduled_flushes_--;
      bg_flush_scheduled_++;
      env_->Schedule(&DBImpl::BGWorkFlush, this, Env::Priority::LOW, this);
    }
  }

  if (bg_compaction_paused_ > 0) {
    // we paused the background compaction
    return;
  }

  if (HasExclusiveManualCompaction()) {
    // only manual compactions are allowed to run. don't schedule automatic
    // compactions
    return;
  }

  while (bg_compaction_scheduled_ < bg_job_limits.max_compactions &&
         unscheduled_compactions_ > 0) {
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->prepicked_compaction = nullptr;
    bg_compaction_scheduled_++;
    unscheduled_compactions_--;
    env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW, this,
                   &DBImpl::UnscheduleCallback);
  }
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits() const {
  mutex_.AssertHeld();
  return GetBGJobLimits(immutable_db_options_.max_background_flushes,
                        mutable_db_options_.max_background_compactions,
                        mutable_db_options_.max_background_jobs,
                        write_controller_.NeedSpeedupCompaction());
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits(int max_background_flushes,
                                           int max_background_compactions,
                                           int max_background_jobs,
                                           bool parallelize_compactions) {
  BGJobLimits res;
  if (max_background_flushes == -1 && max_background_compactions == -1) {
    // for our first stab implementing max_background_jobs, simply allocate a
    // quarter of the threads to flushes.
    res.max_flushes = std::max(1, max_background_jobs / 4);
    res.max_compactions = std::max(1, max_background_jobs - res.max_flushes);
  } else {
    // compatibility code in case users haven't migrated to max_background_jobs,
    // which automatically computes flush/compaction limits
    res.max_flushes = std::max(1, max_background_flushes);
    res.max_compactions = std::max(1, max_background_compactions);
  }
  if (!parallelize_compactions) {
    // throttle background compactions until we deem necessary
    res.max_compactions = 1;
  }
  return res;
}

void DBImpl::AddToCompactionQueue(ColumnFamilyData* cfd) {
  assert(!cfd->pending_compaction());
  cfd->Ref();
  compaction_queue_.push_back(cfd);
  cfd->set_pending_compaction(true);
}

ColumnFamilyData* DBImpl::PopFirstFromCompactionQueue() {
  assert(!compaction_queue_.empty());
  auto cfd = *compaction_queue_.begin();
  compaction_queue_.pop_front();
  assert(cfd->pending_compaction());
  cfd->set_pending_compaction(false);
  return cfd;
}

void DBImpl::AddToFlushQueue(ColumnFamilyData* cfd) {
  assert(!cfd->pending_flush());
  cfd->Ref();
  flush_queue_.push_back(cfd);
  cfd->set_pending_flush(true);
}

ColumnFamilyData* DBImpl::PopFirstFromFlushQueue() {
  assert(!flush_queue_.empty());
  auto cfd = *flush_queue_.begin();
  flush_queue_.pop_front();
  assert(cfd->pending_flush());
  cfd->set_pending_flush(false);
  return cfd;
}

void DBImpl::SchedulePendingFlush(ColumnFamilyData* cfd) {
  if (!cfd->pending_flush() && cfd->imm()->IsFlushPending()) {
    AddToFlushQueue(cfd);
    ++unscheduled_flushes_;
  }
}

void DBImpl::SchedulePendingCompaction(ColumnFamilyData* cfd) {
  if (!cfd->pending_compaction() && cfd->NeedsCompaction()) {
    AddToCompactionQueue(cfd);
    ++unscheduled_compactions_;
  }
}

void DBImpl::SchedulePendingPurge(std::string fname, FileType type,
                                  uint64_t number, uint32_t path_id,
                                  int job_id) {
  mutex_.AssertHeld();
  PurgeFileInfo file_info(fname, type, number, path_id, job_id);
  purge_queue_.push_back(std::move(file_info));
}

void DBImpl::BGWorkFlush(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush");
  reinterpret_cast<DBImpl*>(db)->BackgroundCallFlush();
  TEST_SYNC_POINT("DBImpl::BGWorkFlush:done");
}

void DBImpl::BGWorkCompaction(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkCompaction");
  auto prepicked_compaction =
      static_cast<PrepickedCompaction*>(ca.prepicked_compaction);
  reinterpret_cast<DBImpl*>(ca.db)->BackgroundCallCompaction(
      prepicked_compaction, Env::Priority::LOW);
  delete prepicked_compaction;
}

void DBImpl::BGWorkBottomCompaction(void* arg) {
  CompactionArg ca = *(static_cast<CompactionArg*>(arg));
  delete static_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::BOTTOM);
  TEST_SYNC_POINT("DBImpl::BGWorkBottomCompaction");
  auto* prepicked_compaction = ca.prepicked_compaction;
  assert(prepicked_compaction && prepicked_compaction->compaction &&
         !prepicked_compaction->manual_compaction_state);
  ca.db->BackgroundCallCompaction(prepicked_compaction, Env::Priority::BOTTOM);
  delete prepicked_compaction;
}

void DBImpl::BGWorkPurge(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:start");
  reinterpret_cast<DBImpl*>(db)->BackgroundCallPurge();
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:end");
}

void DBImpl::UnscheduleCallback(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  if (ca.prepicked_compaction != nullptr) {
    if (ca.prepicked_compaction->compaction != nullptr) {
      delete ca.prepicked_compaction->compaction;
    }
    delete ca.prepicked_compaction;
  }
  TEST_SYNC_POINT("DBImpl::UnscheduleCallback");
}

Status DBImpl::BackgroundFlush(bool* made_progress, JobContext* job_context,
                               LogBuffer* log_buffer) {
  mutex_.AssertHeld();

  Status status = bg_error_;
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::ShutdownInProgress();
  }

  if (!status.ok()) {
    return status;
  }

  ColumnFamilyData* cfd = nullptr;
  while (!flush_queue_.empty()) {
    // This cfd is already referenced
    auto first_cfd = PopFirstFromFlushQueue();

    if (first_cfd->IsDropped() || !first_cfd->imm()->IsFlushPending()) {
      // can't flush this CF, try next one
      if (first_cfd->Unref()) {
        delete first_cfd;
      }
      continue;
    }

    // found a flush!
    cfd = first_cfd;
    break;
  }

  if (cfd != nullptr) {
    const MutableCFOptions mutable_cf_options =
        *cfd->GetLatestMutableCFOptions();
    auto bg_job_limits = GetBGJobLimits();
    ROCKS_LOG_BUFFER(
        log_buffer,
        "Calling FlushMemTableToOutputFile with column "
        "family [%s], flush slots available %d, compaction slots available %d, "
        "flush slots scheduled %d, compaction slots scheduled %d",
        cfd->GetName().c_str(), bg_job_limits.max_flushes,
        bg_job_limits.max_compactions, bg_flush_scheduled_,
        bg_compaction_scheduled_);
    status = FlushMemTableToOutputFile(cfd, mutable_cf_options, made_progress,
                                       job_context, log_buffer);
    if (cfd->Unref()) {
      delete cfd;
    }
  }
  return status;
}

void DBImpl::BackgroundCallFlush() {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);

  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:start");

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);
    assert(bg_flush_scheduled_);
    num_running_flushes_++;

    auto pending_outputs_inserted_elem =
        CaptureCurrentFileNumberInPendingOutputs();

    Status s = BackgroundFlush(&made_progress, &job_context, &log_buffer);
    if (!s.ok() && !s.IsShutdownInProgress()) {
      // Wait a little bit before retrying background flush in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed flushes for the duration of
      // the problem.
      uint64_t error_cnt =
        default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background flush error: %s"
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      log_buffer.FlushBufferToLog();
      LogFlush(immutable_db_options_.info_log);
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If flush failed, we want to delete all temporary files that we might have
    // created. Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress());
    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_flush_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
      }
      job_context.Clean();
      mutex_.Lock();
    }

    assert(num_running_flushes_ > 0);
    num_running_flushes_--;
    bg_flush_scheduled_--;
    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();
    bg_cv_.SignalAll();
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

void DBImpl::BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                      Env::Priority bg_thread_pri) {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  TEST_SYNC_POINT("BackgroundCallCompaction:0");
  MaybeDumpStats();
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);

    // This call will unlock/lock the mutex to wait for current running
    // IngestExternalFile() calls to finish.
    WaitForIngestFile();

    num_running_compactions_++;

    auto pending_outputs_inserted_elem =
        CaptureCurrentFileNumberInPendingOutputs();

    assert((bg_thread_pri == Env::Priority::BOTTOM &&
            bg_bottom_compaction_scheduled_) ||
           (bg_thread_pri == Env::Priority::LOW && bg_compaction_scheduled_));
    Status s = BackgroundCompaction(&made_progress, &job_context, &log_buffer,
                                    prepicked_compaction);
    TEST_SYNC_POINT("BackgroundCallCompaction:1");
    if (!s.ok() && !s.IsShutdownInProgress()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      log_buffer.FlushBufferToLog();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background compaction error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      LogFlush(immutable_db_options_.info_log);
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If compaction failed, we want to delete all temporary files that we might
    // have created (they might not be all recorded in job_context in case of a
    // failure). Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress());

    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_compaction_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
      }
      job_context.Clean();
      mutex_.Lock();
    }

    assert(num_running_compactions_ > 0);
    num_running_compactions_--;
    if (bg_thread_pri == Env::Priority::LOW) {
      bg_compaction_scheduled_--;
    } else {
      assert(bg_thread_pri == Env::Priority::BOTTOM);
      bg_bottom_compaction_scheduled_--;
    }

    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();
    if (made_progress ||
        (bg_compaction_scheduled_ == 0 &&
         bg_bottom_compaction_scheduled_ == 0) ||
        HasPendingManualCompaction()) {
      // signal if
      // * made_progress -- need to wakeup DelayWrite
      // * bg_{bottom,}_compaction_scheduled_ == 0 -- need to wakeup ~DBImpl
      // * HasPendingManualCompaction -- need to wakeup RunManualCompaction
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

Status DBImpl::BackgroundCompaction(bool* made_progress,
                                    JobContext* job_context,
                                    LogBuffer* log_buffer,
                                    PrepickedCompaction* prepicked_compaction) {
  ManualCompactionState* manual_compaction =
      prepicked_compaction == nullptr
          ? nullptr
          : prepicked_compaction->manual_compaction_state;
  *made_progress = false;
  mutex_.AssertHeld();
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Start");

  bool is_manual = (manual_compaction != nullptr);
  unique_ptr<Compaction> c;
  if (prepicked_compaction != nullptr &&
      prepicked_compaction->compaction != nullptr) {
    c.reset(prepicked_compaction->compaction);
  }
  bool is_prepicked = is_manual || c;

  // (manual_compaction->in_progress == false);
  bool trivial_move_disallowed =
      is_manual && manual_compaction->disallow_trivial_move;

  CompactionJobStats compaction_job_stats;
  Status status = bg_error_;
  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::ShutdownInProgress();
  }

  if (!status.ok()) {
    if (is_manual) {
      manual_compaction->status = status;
      manual_compaction->done = true;
      manual_compaction->in_progress = false;
      manual_compaction = nullptr;
    }
    return status;
  }

  if (is_manual) {
    // another thread cannot pick up the same work
    manual_compaction->in_progress = true;
  }

  // InternalKey manual_end_storage;
  // InternalKey* manual_end = &manual_end_storage;
  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    assert(m->in_progress);
    if (!c) {
      m->done = true;
      m->manual_end = nullptr;
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Manual compaction from level-%d from %s .. "
                       "%s; nothing to do\n",
                       m->cfd->GetName().c_str(), m->input_level,
                       (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                       (m->end ? m->end->DebugString().c_str() : "(end)"));
    } else {
      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Manual compaction from level-%d to level-%d from %s .. "
          "%s; will stop at %s\n",
          m->cfd->GetName().c_str(), m->input_level, c->output_level(),
          (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
          (m->end ? m->end->DebugString().c_str() : "(end)"),
          ((m->done || m->manual_end == nullptr)
               ? "(end)"
               : m->manual_end->DebugString().c_str()));
    }
  } else if (!is_prepicked && !compaction_queue_.empty()) {
    if (HaveManualCompaction(compaction_queue_.front())) {
      // Can't compact right now, but try again later
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction()::Conflict");

      // Stay in the compaction queue.
      unscheduled_compactions_++;

      return Status::OK();
    }

    // cfd is referenced here
    auto cfd = PopFirstFromCompactionQueue();
    // We unreference here because the following code will take a Ref() on
    // this cfd if it is going to use it (Compaction class holds a
    // reference).
    // This will all happen under a mutex so we don't have to be afraid of
    // somebody else deleting it.
    if (cfd->Unref()) {
      delete cfd;
      // This was the last reference of the column family, so no need to
      // compact.
      return Status::OK();
    }

    // Pick up latest mutable CF Options and use it throughout the
    // compaction job
    // Compaction makes a copy of the latest MutableCFOptions. It should be used
    // throughout the compaction procedure to make sure consistency. It will
    // eventually be installed into SuperVersion
    auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
    if (!mutable_cf_options->disable_auto_compactions && !cfd->IsDropped()) {
      // NOTE: try to avoid unnecessary copy of MutableCFOptions if
      // compaction is not necessary. Need to make sure mutex is held
      // until we make a copy in the following code
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():BeforePickCompaction");
      c.reset(cfd->PickCompaction(*mutable_cf_options, log_buffer));
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():AfterPickCompaction");
      if (c != nullptr) {
        // update statistics
        MeasureTime(stats_, NUM_FILES_IN_SINGLE_COMPACTION,
                    c->inputs(0)->size());
        // There are three things that can change compaction score:
        // 1) When flush or compaction finish. This case is covered by
        // InstallSuperVersionAndScheduleWork
        // 2) When MutableCFOptions changes. This case is also covered by
        // InstallSuperVersionAndScheduleWork, because this is when the new
        // options take effect.
        // 3) When we Pick a new compaction, we "remove" those files being
        // compacted from the calculation, which then influences compaction
        // score. Here we check if we need the new compaction even without the
        // files that are currently being compacted. If we need another
        // compaction, we might be able to execute it in parallel, so we add it
        // to the queue and schedule a new thread.
        if (cfd->NeedsCompaction()) {
          // Yes, we need more compactions!
          AddToCompactionQueue(cfd);
          ++unscheduled_compactions_;
          MaybeScheduleFlushOrCompaction();
        }
      }
    }
  }

  if (!c) {
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "Compaction nothing to do");
  } else if (c->deletion_compaction()) {
    // TODO(icanadi) Do we want to honor snapshots here? i.e. not delete old
    // file if there is alive snapshot pointing to it
    assert(c->num_input_files(1) == 0);
    assert(c->level() == 0);
    assert(c->column_family_data()->ioptions()->compaction_style ==
           kCompactionStyleFIFO);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    for (const auto& f : *c->inputs(0)) {
      c->edit()->DeleteFile(c->level(), f->fd.GetNumber());
    }
    status = versions_->LogAndApply(c->column_family_data(),
                                    *c->mutable_cf_options(), c->edit(),
                                    &mutex_, directories_.GetDbDir());
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), &job_context->superversion_context,
        *c->mutable_cf_options());
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Deleted %d files\n",
                     c->column_family_data()->GetName().c_str(),
                     c->num_input_files(0));
    *made_progress = true;
  } else if (!trivial_move_disallowed && c->IsTrivialMove()) {
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:TrivialMove");
    // Instrument for event update
    // TODO(yhchiang): add op details for showing trivial-move.
    ThreadStatusUtil::SetColumnFamily(
        c->column_family_data(), c->column_family_data()->ioptions()->env,
        immutable_db_options_.enable_thread_tracking);
    ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    // Move files to next level
    int32_t moved_files = 0;
    int64_t moved_bytes = 0;
    for (unsigned int l = 0; l < c->num_input_levels(); l++) {
      if (c->level(l) == c->output_level()) {
        continue;
      }
      for (size_t i = 0; i < c->num_input_files(l); i++) {
        FileMetaData* f = c->input(l, i);
        c->edit()->DeleteFile(c->level(l), f->fd.GetNumber());
        c->edit()->AddFile(c->output_level(), f->fd.GetNumber(),
                           f->fd.GetPathId(), f->fd.GetFileSize(), f->smallest,
                           f->largest, f->smallest_seqno, f->largest_seqno,
                           f->marked_for_compaction);

        ROCKS_LOG_BUFFER(log_buffer, "[%s] Moving #%" PRIu64
                                     " to level-%d %" PRIu64 " bytes\n",
                         c->column_family_data()->GetName().c_str(),
                         f->fd.GetNumber(), c->output_level(),
                         f->fd.GetFileSize());
        ++moved_files;
        moved_bytes += f->fd.GetFileSize();
      }
    }

    status = versions_->LogAndApply(c->column_family_data(),
                                    *c->mutable_cf_options(), c->edit(),
                                    &mutex_, directories_.GetDbDir());
    // Use latest MutableCFOptions
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), &job_context->superversion_context,
        *c->mutable_cf_options());

    VersionStorageInfo::LevelSummaryStorage tmp;
    c->column_family_data()->internal_stats()->IncBytesMoved(c->output_level(),
                                                             moved_bytes);
    {
      event_logger_.LogToBuffer(log_buffer)
          << "job" << job_context->job_id << "event"
          << "trivial_move"
          << "destination_level" << c->output_level() << "files" << moved_files
          << "total_files_size" << moved_bytes;
    }
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Moved #%d files to level-%d %" PRIu64 " bytes %s: %s\n",
        c->column_family_data()->GetName().c_str(), moved_files,
        c->output_level(), moved_bytes, status.ToString().c_str(),
        c->column_family_data()->current()->storage_info()->LevelSummary(&tmp));
    *made_progress = true;

    // Clear Instrument
    ThreadStatusUtil::ResetThreadStatus();
  } else if (c->column_family_data()->ioptions()->compaction_style ==
                 kCompactionStyleUniversal &&
             !is_prepicked && c->output_level() > 0 &&
             c->output_level() ==
                 c->column_family_data()
                     ->current()
                     ->storage_info()
                     ->MaxOutputLevel(
                         immutable_db_options_.allow_ingest_behind) &&
             env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
    // Forward universal compactions involving last level to the bottom pool
    // if it exists, such that long-running compactions can't block short-
    // lived ones, like L0->L0s.
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:ForwardToBottomPriPool");
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->prepicked_compaction = new PrepickedCompaction;
    ca->prepicked_compaction->compaction = c.release();
    ca->prepicked_compaction->manual_compaction_state = nullptr;
    ++bg_bottom_compaction_scheduled_;
    env_->Schedule(&DBImpl::BGWorkBottomCompaction, ca, Env::Priority::BOTTOM,
                   this, &DBImpl::UnscheduleCallback);
  } else {
    int output_level  __attribute__((unused));
    output_level = c->output_level();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:NonTrivial",
                             &output_level);
    SequenceNumber earliest_write_conflict_snapshot;
    std::vector<SequenceNumber> snapshot_seqs =
        snapshots_.GetAll(&earliest_write_conflict_snapshot);

    auto snapshot_checker = snapshot_checker_.get();
    if (use_custom_gc_ && snapshot_checker == nullptr) {
      snapshot_checker = DisableGCSnapshotChecker::Instance();
    }
    assert(is_snapshot_supported_ || snapshots_.empty());
    CompactionJob compaction_job(
        job_context->job_id, c.get(), immutable_db_options_,
        env_options_for_compaction_, versions_.get(), &shutting_down_,
        preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
        directories_.GetDataDir(c->output_path_id()), stats_, &mutex_,
        &bg_error_, snapshot_seqs, earliest_write_conflict_snapshot,
        snapshot_checker, table_cache_, &event_logger_,
        c->mutable_cf_options()->paranoid_file_checks,
        c->mutable_cf_options()->report_bg_io_stats, dbname_,
        &compaction_job_stats);
    compaction_job.Prepare();

    mutex_.Unlock();
    compaction_job.Run();
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:NonTrivial:AfterRun");
    mutex_.Lock();

    status = compaction_job.Install(*c->mutable_cf_options());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          c->column_family_data(), &job_context->superversion_context,
          *c->mutable_cf_options());
    }
    *made_progress = true;
  }
  if (c != nullptr) {
    c->ReleaseCompactionFiles(status);
    *made_progress = true;
    NotifyOnCompactionCompleted(
        c->column_family_data(), c.get(), status,
        compaction_job_stats, job_context->job_id);
  }
  // this will unref its input_version and column_family_data
  c.reset();

  if (status.ok()) {
    // Done
  } else if (status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Compaction error: %s",
                   status.ToString().c_str());
    if (immutable_db_options_.paranoid_checks && bg_error_.ok()) {
      Status new_bg_error = status;
      // may temporarily unlock and lock the mutex.
      EventHelpers::NotifyOnBackgroundError(immutable_db_options_.listeners,
                                            BackgroundErrorReason::kCompaction,
                                            &new_bg_error, &mutex_);
      if (!new_bg_error.ok()) {
        bg_error_ = new_bg_error;
      }
    }
  }

  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    if (!status.ok()) {
      m->status = status;
      m->done = true;
    }
    // For universal compaction:
    //   Because universal compaction always happens at level 0, so one
    //   compaction will pick up all overlapped files. No files will be
    //   filtered out due to size limit and left for a successive compaction.
    //   So we can safely conclude the current compaction.
    //
    //   Also note that, if we don't stop here, then the current compaction
    //   writes a new file back to level 0, which will be used in successive
    //   compaction. Hence the manual compaction will never finish.
    //
    // Stop the compaction if manual_end points to nullptr -- this means
    // that we compacted the whole range. manual_end should always point
    // to nullptr in case of universal compaction
    if (m->manual_end == nullptr) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      // Universal and FIFO compactions should always compact the whole range
      assert(m->cfd->ioptions()->compaction_style !=
                 kCompactionStyleUniversal ||
             m->cfd->ioptions()->num_levels > 1);
      assert(m->cfd->ioptions()->compaction_style != kCompactionStyleFIFO);
      m->tmp_storage = *m->manual_end;
      m->begin = &m->tmp_storage;
      m->incomplete = true;
    }
    m->in_progress = false; // not being processed anymore
  }
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Finish");
  return status;
}

bool DBImpl::HasPendingManualCompaction() {
  return (!manual_compaction_dequeue_.empty());
}

void DBImpl::AddManualCompaction(DBImpl::ManualCompactionState* m) {
  manual_compaction_dequeue_.push_back(m);
}

void DBImpl::RemoveManualCompaction(DBImpl::ManualCompactionState* m) {
  // Remove from queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      it = manual_compaction_dequeue_.erase(it);
      return;
    }
    it++;
  }
  assert(false);
  return;
}

bool DBImpl::ShouldntRunManualCompaction(ManualCompactionState* m) {
  if (num_running_ingest_file_ > 0) {
    // We need to wait for other IngestExternalFile() calls to finish
    // before running a manual compaction.
    return true;
  }
  if (m->exclusive) {
    return (bg_bottom_compaction_scheduled_ > 0 ||
            bg_compaction_scheduled_ > 0);
  }
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  bool seen = false;
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      it++;
      seen = true;
      continue;
    } else if (MCOverlap(m, (*it)) && (!seen && !(*it)->in_progress)) {
      // Consider the other manual compaction *it, conflicts if:
      // overlaps with m
      // and (*it) is ahead in the queue and is not yet in progress
      return true;
    }
    it++;
  }
  return false;
}

bool DBImpl::HaveManualCompaction(ColumnFamilyData* cfd) {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    if ((cfd == (*it)->cfd) && (!((*it)->in_progress || (*it)->done))) {
      // Allow automatic compaction if manual compaction is
      // in progress
      return true;
    }
    it++;
  }
  return false;
}

bool DBImpl::HasExclusiveManualCompaction() {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    it++;
  }
  return false;
}

bool DBImpl::MCOverlap(ManualCompactionState* m, ManualCompactionState* m1) {
  if ((m->exclusive) || (m1->exclusive)) {
    return true;
  }
  if (m->cfd != m1->cfd) {
    return false;
  }
  return true;
}

// SuperVersionContext gets created and destructed outside of the lock --
// we
// use this convinently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// However, if InstallSuperVersionAndScheduleWork() gets called twice with the
// same sv_context, we can't reuse the SuperVersion() that got
// malloced because
// first call already used it. In that rare case, we take a hit and create a
// new SuperVersion() inside of the mutex. We do similar thing
// for superversion_to_free

void DBImpl::InstallSuperVersionAndScheduleWork(
    ColumnFamilyData* cfd, SuperVersionContext* sv_context,
    const MutableCFOptions& mutable_cf_options) {
  mutex_.AssertHeld();

  // Update max_total_in_memory_state_
  size_t old_memtable_size = 0;
  auto* old_sv = cfd->GetSuperVersion();
  if (old_sv) {
    old_memtable_size = old_sv->mutable_cf_options.write_buffer_size *
                        old_sv->mutable_cf_options.max_write_buffer_number;
  }

  if (sv_context->new_superversion == nullptr) {
    sv_context->NewSuperVersion();
  }
  cfd->InstallSuperVersion(sv_context, &mutex_,
                           mutable_cf_options);

  // Whenever we install new SuperVersion, we might need to issue new flushes or
  // compactions.
  SchedulePendingFlush(cfd);
  SchedulePendingCompaction(cfd);
  MaybeScheduleFlushOrCompaction();

  // Update max_total_in_memory_state_
  max_total_in_memory_state_ =
      max_total_in_memory_state_ - old_memtable_size +
      mutable_cf_options.write_buffer_size *
      mutable_cf_options.max_write_buffer_number;
}

void DBImpl::SetSnapshotChecker(SnapshotChecker* snapshot_checker) {
  InstrumentedMutexLock l(&mutex_);
  // snapshot_checker_ should only set once. If we need to set it multiple
  // times, we need to make sure the old one is not deleted while it is still
  // using by a compaction job.
  assert(!snapshot_checker_);
  snapshot_checker_.reset(snapshot_checker);
}
}  // namespace rocksdb
