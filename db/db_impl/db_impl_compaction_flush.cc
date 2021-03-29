//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cinttypes>

#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "file/sst_file_manager_impl.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/concurrent_task_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {

bool DBImpl::EnoughRoomForCompaction(
    ColumnFamilyData* cfd, const std::vector<CompactionInputFiles>& inputs,
    bool* sfm_reserved_compact_space, LogBuffer* log_buffer) {
  // Check if we have enough room to do the compaction
  bool enough_room = true;
#ifndef ROCKSDB_LITE
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm) {
    // Pass the current bg_error_ to SFM so it can decide what checks to
    // perform. If this DB instance hasn't seen any error yet, the SFM can be
    // optimistic and not do disk space checks
    Status bg_error = error_handler_.GetBGError();
    enough_room = sfm->EnoughRoomForCompaction(cfd, inputs, bg_error);
    bg_error.PermitUncheckedError();  // bg_error is just a copy of the Status
                                      // from the error_handler_
    if (enough_room) {
      *sfm_reserved_compact_space = true;
    }
  }
#else
  (void)cfd;
  (void)inputs;
  (void)sfm_reserved_compact_space;
#endif  // ROCKSDB_LITE
  if (!enough_room) {
    // Just in case tests want to change the value of enough_room
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction():CancelledCompaction", &enough_room);
    ROCKS_LOG_BUFFER(log_buffer,
                     "Cancelled compaction because not enough room");
    RecordTick(stats_, COMPACTION_CANCELLED, 1);
  }
  return enough_room;
}

bool DBImpl::RequestCompactionToken(ColumnFamilyData* cfd, bool force,
                                    std::unique_ptr<TaskLimiterToken>* token,
                                    LogBuffer* log_buffer) {
  assert(*token == nullptr);
  auto limiter = static_cast<ConcurrentTaskLimiterImpl*>(
      cfd->ioptions()->compaction_thread_limiter.get());
  if (limiter == nullptr) {
    return true;
  }
  *token = limiter->GetToken(force);
  if (*token != nullptr) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "Thread limiter [%s] increase [%s] compaction task, "
                     "force: %s, tasks after: %d",
                     limiter->GetName().c_str(), cfd->GetName().c_str(),
                     force ? "true" : "false", limiter->GetOutstandingTask());
    return true;
  }
  return false;
}

IOStatus DBImpl::SyncClosedLogs(JobContext* job_context) {
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

  IOStatus io_s;
  if (!logs_to_sync.empty()) {
    mutex_.Unlock();

    for (log::Writer* log : logs_to_sync) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] Syncing log #%" PRIu64, job_context->job_id,
                     log->get_log_number());
      io_s = log->file()->Sync(immutable_db_options_.use_fsync);
      if (!io_s.ok()) {
        break;
      }

      if (immutable_db_options_.recycle_log_file_num > 0) {
        io_s = log->Close();
        if (!io_s.ok()) {
          break;
        }
      }
    }
    if (io_s.ok()) {
      io_s = directories_.GetWalDir()->Fsync(IOOptions(), nullptr);
    }

    mutex_.Lock();

    // "number <= current_log_number - 1" is equivalent to
    // "number < current_log_number".
    if (io_s.ok()) {
      io_s = status_to_io_status(MarkLogsSynced(current_log_number - 1, true));
    } else {
      MarkLogsNotSynced(current_log_number - 1);
    }
    if (!io_s.ok()) {
      TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Failed");
      return io_s;
    }
  }
  TEST_SYNC_POINT("DBImpl::SyncClosedLogs:end");
  return io_s;
}

Status DBImpl::FlushMemTableToOutputFile(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    bool* made_progress, JobContext* job_context,
    SuperVersionContext* superversion_context,
    std::vector<SequenceNumber>& snapshot_seqs,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, LogBuffer* log_buffer,
    Env::Priority thread_pri) {
  mutex_.AssertHeld();
  assert(cfd);
  assert(cfd->imm()->NumNotFlushed() != 0);
  assert(cfd->imm()->IsFlushPending());

  FlushJob flush_job(
      dbname_, cfd, immutable_db_options_, mutable_cf_options,
      port::kMaxUint64 /* memtable_id */, file_options_for_compaction_,
      versions_.get(), &mutex_, &shutting_down_, snapshot_seqs,
      earliest_write_conflict_snapshot, snapshot_checker, job_context,
      log_buffer, directories_.GetDbDir(), GetDataDir(cfd, 0U),
      GetCompressionFlush(*cfd->ioptions(), mutable_cf_options), stats_,
      &event_logger_, mutable_cf_options.report_bg_io_stats,
      true /* sync_output_directory */, true /* write_manifest */, thread_pri,
      io_tracer_, db_id_, db_session_id_, cfd->GetFullHistoryTsLow(),
      &blob_callback_);
  FileMetaData file_meta;

#ifndef ROCKSDB_LITE
  // may temporarily unlock and lock the mutex.
  NotifyOnFlushBegin(cfd, &file_meta, mutable_cf_options, job_context->job_id);
#endif  // ROCKSDB_LITE

  Status s;
  bool need_cancel = false;
  IOStatus log_io_s = IOStatus::OK();
  if (logfile_number_ > 0 &&
      versions_->GetColumnFamilySet()->NumberOfColumnFamilies() > 1) {
    // If there are more than one column families, we need to make sure that
    // all the log files except the most recent one are synced. Otherwise if
    // the host crashes after flushing and before WAL is persistent, the
    // flushed SST may contain data from write batches whose updates to
    // other column families are missing.
    // SyncClosedLogs() may unlock and re-lock the db_mutex.
    log_io_s = SyncClosedLogs(job_context);
    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
    }
  } else {
    TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Skip");
  }
  s = log_io_s;

  // If the log sync failed, we do not need to pick memtable. Otherwise,
  // num_flush_not_started_ needs to be rollback.
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:BeforePickMemtables");
  if (s.ok()) {
    flush_job.PickMemTable();
    need_cancel = true;
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:AfterPickMemtables");

  // Within flush_job.Run, rocksdb may call event listener to notify
  // file creation and deletion.
  //
  // Note that flush_job.Run will unlock and lock the db_mutex,
  // and EventListener callback will be called when the db_mutex
  // is unlocked by the current thread.
  if (s.ok()) {
    s = flush_job.Run(&logs_with_prep_tracker_, &file_meta);
    need_cancel = false;
  }

  if (!s.ok() && need_cancel) {
    flush_job.Cancel();
  }
  IOStatus io_s = IOStatus::OK();
  io_s = flush_job.io_status();
  if (s.ok()) {
    s = io_s;
  }

  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, superversion_context,
                                       mutable_cf_options);
    if (made_progress) {
      *made_progress = true;
    }

    const std::string& column_family_name = cfd->GetName();

    Version* const current = cfd->current();
    assert(current);

    const VersionStorageInfo* const storage_info = current->storage_info();
    assert(storage_info);

    VersionStorageInfo::LevelSummaryStorage tmp;
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                     column_family_name.c_str(),
                     storage_info->LevelSummary(&tmp));

    const auto& blob_files = storage_info->GetBlobFiles();
    if (!blob_files.empty()) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64
                       "\n",
                       column_family_name.c_str(), blob_files.begin()->first,
                       blob_files.rbegin()->first);
    }
  }

  if (!s.ok() && !s.IsShutdownInProgress() && !s.IsColumnFamilyDropped()) {
    if (!io_s.ok() && !io_s.IsShutdownInProgress() &&
        !io_s.IsColumnFamilyDropped()) {
      assert(log_io_s.ok());
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      if (!versions_->io_status().ok()) {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the Manifest write will be map to soft error.
        // TODO: kManifestWriteNoWAL and kFlushNoWAL are misleading. Refactor is
        // needed.
        error_handler_.SetBGError(io_s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the other SST file write errors will be set as
        // kFlushNoWAL.
        error_handler_.SetBGError(io_s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      if (log_io_s.ok()) {
        Status new_bg_error = s;
        error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
      }
    }
  } else {
    // If we got here, then we decided not to care about the i_os status (either
    // from never needing it or ignoring the flush job status
    io_s.PermitUncheckedError();
  }
  if (s.ok()) {
#ifndef ROCKSDB_LITE
    // may temporarily unlock and lock the mutex.
    NotifyOnFlushCompleted(cfd, mutable_cf_options,
                           flush_job.GetCommittedFlushJobsInfo());
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm) {
      // Notify sst_file_manager that a new file was added
      std::string file_path = MakeTableFileName(
          cfd->ioptions()->cf_paths[0].path, file_meta.fd.GetNumber());
      // TODO (PR7798).  We should only add the file to the FileManager if it
      // exists. Otherwise, some tests may fail.  Ignore the error in the
      // interim.
      sfm->OnAddFile(file_path).PermitUncheckedError();
      if (sfm->IsMaxAllowedSpaceReached()) {
        Status new_bg_error =
            Status::SpaceLimit("Max allowed space was reached");
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
            &new_bg_error);
        error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
      }
    }
#endif  // ROCKSDB_LITE
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:Finish");
  return s;
}

Status DBImpl::FlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
  if (immutable_db_options_.atomic_flush) {
    return AtomicFlushMemTablesToOutputFiles(
        bg_flush_args, made_progress, job_context, log_buffer, thread_pri);
  }
  assert(bg_flush_args.size() == 1);
  std::vector<SequenceNumber> snapshot_seqs;
  SequenceNumber earliest_write_conflict_snapshot;
  SnapshotChecker* snapshot_checker;
  GetSnapshotContext(job_context, &snapshot_seqs,
                     &earliest_write_conflict_snapshot, &snapshot_checker);
  const auto& bg_flush_arg = bg_flush_args[0];
  ColumnFamilyData* cfd = bg_flush_arg.cfd_;
  MutableCFOptions mutable_cf_options = *cfd->GetLatestMutableCFOptions();
  SuperVersionContext* superversion_context =
      bg_flush_arg.superversion_context_;
  Status s = FlushMemTableToOutputFile(
      cfd, mutable_cf_options, made_progress, job_context, superversion_context,
      snapshot_seqs, earliest_write_conflict_snapshot, snapshot_checker,
      log_buffer, thread_pri);
  return s;
}

/*
 * Atomically flushes multiple column families.
 *
 * For each column family, all memtables with ID smaller than or equal to the
 * ID specified in bg_flush_args will be flushed. Only after all column
 * families finish flush will this function commit to MANIFEST. If any of the
 * column families are not flushed successfully, this function does not have
 * any side-effect on the state of the database.
 */
Status DBImpl::AtomicFlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
  mutex_.AssertHeld();

  autovector<ColumnFamilyData*> cfds;
  for (const auto& arg : bg_flush_args) {
    cfds.emplace_back(arg.cfd_);
  }

#ifndef NDEBUG
  for (const auto cfd : cfds) {
    assert(cfd->imm()->NumNotFlushed() != 0);
    assert(cfd->imm()->IsFlushPending());
  }
#endif /* !NDEBUG */

  std::vector<SequenceNumber> snapshot_seqs;
  SequenceNumber earliest_write_conflict_snapshot;
  SnapshotChecker* snapshot_checker;
  GetSnapshotContext(job_context, &snapshot_seqs,
                     &earliest_write_conflict_snapshot, &snapshot_checker);

  autovector<FSDirectory*> distinct_output_dirs;
  autovector<std::string> distinct_output_dir_paths;
  std::vector<std::unique_ptr<FlushJob>> jobs;
  std::vector<MutableCFOptions> all_mutable_cf_options;
  int num_cfs = static_cast<int>(cfds.size());
  all_mutable_cf_options.reserve(num_cfs);
  for (int i = 0; i < num_cfs; ++i) {
    auto cfd = cfds[i];
    FSDirectory* data_dir = GetDataDir(cfd, 0U);
    const std::string& curr_path = cfd->ioptions()->cf_paths[0].path;

    // Add to distinct output directories if eligible. Use linear search. Since
    // the number of elements in the vector is not large, performance should be
    // tolerable.
    bool found = false;
    for (const auto& path : distinct_output_dir_paths) {
      if (path == curr_path) {
        found = true;
        break;
      }
    }
    if (!found) {
      distinct_output_dir_paths.emplace_back(curr_path);
      distinct_output_dirs.emplace_back(data_dir);
    }

    all_mutable_cf_options.emplace_back(*cfd->GetLatestMutableCFOptions());
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.back();
    uint64_t max_memtable_id = bg_flush_args[i].max_memtable_id_;
    jobs.emplace_back(new FlushJob(
        dbname_, cfd, immutable_db_options_, mutable_cf_options,
        max_memtable_id, file_options_for_compaction_, versions_.get(), &mutex_,
        &shutting_down_, snapshot_seqs, earliest_write_conflict_snapshot,
        snapshot_checker, job_context, log_buffer, directories_.GetDbDir(),
        data_dir, GetCompressionFlush(*cfd->ioptions(), mutable_cf_options),
        stats_, &event_logger_, mutable_cf_options.report_bg_io_stats,
        false /* sync_output_directory */, false /* write_manifest */,
        thread_pri, io_tracer_, db_id_, db_session_id_,
        cfd->GetFullHistoryTsLow()));
  }

  std::vector<FileMetaData> file_meta(num_cfs);
  Status s;
  IOStatus log_io_s = IOStatus::OK();
  assert(num_cfs == static_cast<int>(jobs.size()));

#ifndef ROCKSDB_LITE
  for (int i = 0; i != num_cfs; ++i) {
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.at(i);
    // may temporarily unlock and lock the mutex.
    NotifyOnFlushBegin(cfds[i], &file_meta[i], mutable_cf_options,
                       job_context->job_id);
  }
#endif /* !ROCKSDB_LITE */

  if (logfile_number_ > 0) {
    // TODO (yanqin) investigate whether we should sync the closed logs for
    // single column family case.
    log_io_s = SyncClosedLogs(job_context);
    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      if (total_log_size_ > 0) {
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
      } else {
        // If the WAL is empty, we use different error reason
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlushNoWAL);
      }
    }
  }
  s = log_io_s;

  // exec_status stores the execution status of flush_jobs as
  // <bool /* executed */, Status /* status code */>
  autovector<std::pair<bool, Status>> exec_status;
  autovector<IOStatus> io_status;
  std::vector<bool> pick_status;
  for (int i = 0; i != num_cfs; ++i) {
    // Initially all jobs are not executed, with status OK.
    exec_status.emplace_back(false, Status::OK());
    io_status.emplace_back(IOStatus::OK());
    pick_status.push_back(false);
  }

  if (s.ok()) {
    for (int i = 0; i != num_cfs; ++i) {
      jobs[i]->PickMemTable();
      pick_status[i] = true;
    }
  }

  if (s.ok()) {
    // TODO (yanqin): parallelize jobs with threads.
    for (int i = 1; i != num_cfs; ++i) {
      exec_status[i].second =
          jobs[i]->Run(&logs_with_prep_tracker_, &file_meta[i]);
      exec_status[i].first = true;
      io_status[i] = jobs[i]->io_status();
    }
    if (num_cfs > 1) {
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1");
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2");
    }
    assert(exec_status.size() > 0);
    assert(!file_meta.empty());
    exec_status[0].second =
        jobs[0]->Run(&logs_with_prep_tracker_, &file_meta[0]);
    exec_status[0].first = true;
    io_status[0] = jobs[0]->io_status();

    Status error_status;
    for (const auto& e : exec_status) {
      if (!e.second.ok()) {
        s = e.second;
        if (!e.second.IsShutdownInProgress() &&
            !e.second.IsColumnFamilyDropped()) {
          // If a flush job did not return OK, and the CF is not dropped, and
          // the DB is not shutting down, then we have to return this result to
          // caller later.
          error_status = e.second;
        }
      }
    }

    s = error_status.ok() ? s : error_status;
  }

  IOStatus io_s = IOStatus::OK();
  if (io_s.ok()) {
    IOStatus io_error = IOStatus::OK();
    for (int i = 0; i != static_cast<int>(io_status.size()); i++) {
      if (!io_status[i].ok() && !io_status[i].IsShutdownInProgress() &&
          !io_status[i].IsColumnFamilyDropped()) {
        io_error = io_status[i];
      }
    }
    io_s = io_error;
    if (s.ok() && !io_s.ok()) {
      s = io_s;
    }
  }

  if (s.IsColumnFamilyDropped()) {
    s = Status::OK();
  }

  if (s.ok() || s.IsShutdownInProgress()) {
    // Sync on all distinct output directories.
    for (auto dir : distinct_output_dirs) {
      if (dir != nullptr) {
        Status error_status = dir->Fsync(IOOptions(), nullptr);
        if (!error_status.ok()) {
          s = error_status;
          break;
        }
      }
    }
  } else {
    // Need to undo atomic flush if something went wrong, i.e. s is not OK and
    // it is not because of CF drop.
    // Have to cancel the flush jobs that have NOT executed because we need to
    // unref the versions.
    for (int i = 0; i != num_cfs; ++i) {
      if (pick_status[i] && !exec_status[i].first) {
        jobs[i]->Cancel();
      }
    }
    for (int i = 0; i != num_cfs; ++i) {
      if (exec_status[i].second.ok() && exec_status[i].first) {
        auto& mems = jobs[i]->GetMemTables();
        cfds[i]->imm()->RollbackMemtableFlush(mems,
                                              file_meta[i].fd.GetNumber());
      }
    }
  }

  if (s.ok()) {
    auto wait_to_install_func = [&]() {
      bool ready = true;
      for (size_t i = 0; i != cfds.size(); ++i) {
        const auto& mems = jobs[i]->GetMemTables();
        if (cfds[i]->IsDropped()) {
          // If the column family is dropped, then do not wait.
          continue;
        } else if (!mems.empty() &&
                   cfds[i]->imm()->GetEarliestMemTableID() < mems[0]->GetID()) {
          // If a flush job needs to install the flush result for mems and
          // mems[0] is not the earliest memtable, it means another thread must
          // be installing flush results for the same column family, then the
          // current thread needs to wait.
          ready = false;
          break;
        } else if (mems.empty() && cfds[i]->imm()->GetEarliestMemTableID() <=
                                       bg_flush_args[i].max_memtable_id_) {
          // If a flush job does not need to install flush results, then it has
          // to wait until all memtables up to max_memtable_id_ (inclusive) are
          // installed.
          ready = false;
          break;
        }
      }
      return ready;
    };

    bool resuming_from_bg_err = error_handler_.IsDBStopped();
    while ((!error_handler_.IsDBStopped() ||
            error_handler_.GetRecoveryError().ok()) &&
           !wait_to_install_func()) {
      atomic_flush_install_cv_.Wait();
    }

    s = resuming_from_bg_err ? error_handler_.GetRecoveryError()
                             : error_handler_.GetBGError();
  }

  if (s.ok()) {
    autovector<ColumnFamilyData*> tmp_cfds;
    autovector<const autovector<MemTable*>*> mems_list;
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    autovector<FileMetaData*> tmp_file_meta;
    for (int i = 0; i != num_cfs; ++i) {
      const auto& mems = jobs[i]->GetMemTables();
      if (!cfds[i]->IsDropped() && !mems.empty()) {
        tmp_cfds.emplace_back(cfds[i]);
        mems_list.emplace_back(&mems);
        mutable_cf_options_list.emplace_back(&all_mutable_cf_options[i]);
        tmp_file_meta.emplace_back(&file_meta[i]);
      }
    }

    s = InstallMemtableAtomicFlushResults(
        nullptr /* imm_lists */, tmp_cfds, mutable_cf_options_list, mems_list,
        versions_.get(), &logs_with_prep_tracker_, &mutex_, tmp_file_meta,
        &job_context->memtables_to_free, directories_.GetDbDir(), log_buffer);
  }

  if (s.ok()) {
    assert(num_cfs ==
           static_cast<int>(job_context->superversion_contexts.size()));
    for (int i = 0; i != num_cfs; ++i) {
      assert(cfds[i]);

      if (cfds[i]->IsDropped()) {
        continue;
      }
      InstallSuperVersionAndScheduleWork(cfds[i],
                                         &job_context->superversion_contexts[i],
                                         all_mutable_cf_options[i]);

      const std::string& column_family_name = cfds[i]->GetName();

      Version* const current = cfds[i]->current();
      assert(current);

      const VersionStorageInfo* const storage_info = current->storage_info();
      assert(storage_info);

      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                       column_family_name.c_str(),
                       storage_info->LevelSummary(&tmp));

      const auto& blob_files = storage_info->GetBlobFiles();
      if (!blob_files.empty()) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Blob file summary: head=%" PRIu64
                         ", tail=%" PRIu64 "\n",
                         column_family_name.c_str(), blob_files.begin()->first,
                         blob_files.rbegin()->first);
      }
    }
    if (made_progress) {
      *made_progress = true;
    }
#ifndef ROCKSDB_LITE
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    assert(all_mutable_cf_options.size() == static_cast<size_t>(num_cfs));
    for (int i = 0; s.ok() && i != num_cfs; ++i) {
      if (cfds[i]->IsDropped()) {
        continue;
      }
      NotifyOnFlushCompleted(cfds[i], all_mutable_cf_options[i],
                             jobs[i]->GetCommittedFlushJobsInfo());
      if (sfm) {
        std::string file_path = MakeTableFileName(
            cfds[i]->ioptions()->cf_paths[0].path, file_meta[i].fd.GetNumber());
        // TODO (PR7798).  We should only add the file to the FileManager if it
        // exists. Otherwise, some tests may fail.  Ignore the error in the
        // interim.
        sfm->OnAddFile(file_path).PermitUncheckedError();
        if (sfm->IsMaxAllowedSpaceReached() &&
            error_handler_.GetBGError().ok()) {
          Status new_bg_error =
              Status::SpaceLimit("Max allowed space was reached");
          error_handler_.SetBGError(new_bg_error,
                                    BackgroundErrorReason::kFlush);
        }
      }
    }
#endif  // ROCKSDB_LITE
  }

  // Need to undo atomic flush if something went wrong, i.e. s is not OK and
  // it is not because of CF drop.
  if (!s.ok() && !s.IsColumnFamilyDropped()) {
    if (!io_s.ok() && !io_s.IsColumnFamilyDropped()) {
      assert(log_io_s.ok());
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      if (!versions_->io_status().ok()) {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the Manifest write will be map to soft error.
        // TODO: kManifestWriteNoWAL and kFlushNoWAL are misleading. Refactor
        // is needed.
        error_handler_.SetBGError(io_s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the other SST file write errors will be set as
        // kFlushNoWAL.
        error_handler_.SetBGError(io_s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      if (log_io_s.ok()) {
        Status new_bg_error = s;
        error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
      }
    }
  }

  return s;
}

void DBImpl::NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                                const MutableCFOptions& mutable_cf_options,
                                int job_id) {
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
    FlushJobInfo info{};
    info.cf_id = cfd->GetID();
    info.cf_name = cfd->GetName();
    // TODO(yhchiang): make db_paths dynamic in case flush does not
    //                 go to L0 in the future.
    const uint64_t file_number = file_meta->fd.GetNumber();
    info.file_path =
        MakeTableFileName(cfd->ioptions()->cf_paths[0].path, file_number);
    info.file_number = file_number;
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.triggered_writes_slowdown = triggered_writes_slowdown;
    info.triggered_writes_stop = triggered_writes_stop;
    info.smallest_seqno = file_meta->fd.smallest_seqno;
    info.largest_seqno = file_meta->fd.largest_seqno;
    info.flush_reason = cfd->GetFlushReason();
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnFlushBegin(this, info);
    }
  }
  mutex_.Lock();
// no need to signal bg_cv_ as it will be signaled at the end of the
// flush process.
#else
  (void)cfd;
  (void)file_meta;
  (void)mutable_cf_options;
  (void)job_id;
#endif  // ROCKSDB_LITE
}

void DBImpl::NotifyOnFlushCompleted(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    std::list<std::unique_ptr<FlushJobInfo>>* flush_jobs_info) {
#ifndef ROCKSDB_LITE
  assert(flush_jobs_info != nullptr);
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
    for (auto& info : *flush_jobs_info) {
      info->triggered_writes_slowdown = triggered_writes_slowdown;
      info->triggered_writes_stop = triggered_writes_stop;
      for (auto listener : immutable_db_options_.listeners) {
        listener->OnFlushCompleted(this, *info);
      }
    }
    flush_jobs_info->clear();
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
#else
  (void)cfd;
  (void)mutable_cf_options;
  (void)flush_jobs_info;
#endif  // ROCKSDB_LITE
}

Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice* begin_without_ts,
                            const Slice* end_without_ts) {
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0) {
    return CompactRangeInternal(options, column_family, begin_without_ts,
                                end_without_ts);
  }

  std::string begin_str;
  std::string end_str;

  // CompactRange compact all keys: [begin, end] inclusively. Add maximum
  // timestamp to include all `begin` keys, and add minimal timestamp to include
  // all `end` keys.
  if (begin_without_ts != nullptr) {
    AppendKeyWithMaxTimestamp(&begin_str, *begin_without_ts, ts_sz);
  }
  if (end_without_ts != nullptr) {
    AppendKeyWithMinTimestamp(&end_str, *end_without_ts, ts_sz);
  }
  Slice begin(begin_str);
  Slice end(end_str);

  Slice* begin_with_ts = begin_without_ts ? &begin : nullptr;
  Slice* end_with_ts = end_without_ts ? &end : nullptr;

  return CompactRangeInternal(options, column_family, begin_with_ts,
                              end_with_ts);
}

Status DBImpl::IncreaseFullHistoryTsLow(ColumnFamilyData* cfd,
                                        std::string ts_low) {
  VersionEdit edit;
  edit.SetColumnFamily(cfd->GetID());
  edit.SetFullHistoryTsLow(ts_low);

  InstrumentedMutexLock l(&mutex_);
  std::string current_ts_low = cfd->GetFullHistoryTsLow();
  const Comparator* ucmp = cfd->user_comparator();
  if (!current_ts_low.empty() &&
      ucmp->CompareTimestamp(ts_low, current_ts_low) < 0) {
    return Status::InvalidArgument(
        "Cannot decrease full_history_timestamp_low");
  }

  return versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(), &edit,
                                &mutex_);
}

Status DBImpl::CompactRangeInternal(const CompactRangeOptions& options,
                                    ColumnFamilyHandle* column_family,
                                    const Slice* begin, const Slice* end) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  if (options.target_path_id >= cfd->ioptions()->cf_paths.size()) {
    return Status::InvalidArgument("Invalid target path ID");
  }

  bool flush_needed = true;

  // Update full_history_ts_low if it's set
  if (options.full_history_ts_low != nullptr &&
      !options.full_history_ts_low->empty()) {
    std::string ts_low = options.full_history_ts_low->ToString();
    if (begin != nullptr || end != nullptr) {
      return Status::InvalidArgument(
          "Cannot specify compaction range with full_history_ts_low");
    }
    Status s = IncreaseFullHistoryTsLow(cfd, ts_low);
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  Status s;
  if (begin != nullptr && end != nullptr) {
    // TODO(ajkr): We could also optimize away the flush in certain cases where
    // one/both sides of the interval are unbounded. But it requires more
    // changes to RangesOverlapWithMemtables.
    Range range(*begin, *end);
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
    s = cfd->RangesOverlapWithMemtables(
        {range}, super_version, immutable_db_options_.allow_data_in_errors,
        &flush_needed);
    CleanupSuperVersion(super_version);
  }

  if (s.ok() && flush_needed) {
    FlushOptions fo;
    fo.allow_write_stall = options.allow_write_stall;
    if (immutable_db_options_.atomic_flush) {
      autovector<ColumnFamilyData*> cfds;
      mutex_.Lock();
      SelectColumnFamiliesForAtomicFlush(&cfds);
      mutex_.Unlock();
      s = AtomicFlushMemTables(cfds, fo, FlushReason::kManualCompaction,
                               false /* writes_stopped */);
    } else {
      s = FlushMemTable(cfd, fo, FlushReason::kManualCompaction,
                        false /* writes_stopped*/);
    }
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  constexpr int kInvalidLevel = -1;
  int final_output_level = kInvalidLevel;
  bool exclusive = options.exclusive_manual_compaction;
  if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal &&
      cfd->NumberLevels() > 1) {
    // Always compact all files together.
    final_output_level = cfd->NumberLevels() - 1;
    // if bottom most level is reserved
    if (immutable_db_options_.allow_ingest_behind) {
      final_output_level--;
    }
    s = RunManualCompaction(cfd, ColumnFamilyData::kCompactAllLevels,
                            final_output_level, options, begin, end, exclusive,
                            false, port::kMaxUint64);
  } else {
    int first_overlapped_level = kInvalidLevel;
    int max_overlapped_level = kInvalidLevel;
    {
      SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
      Version* current_version = super_version->current;
      ReadOptions ro;
      ro.total_order_seek = true;
      bool overlap;
      for (int level = 0;
           level < current_version->storage_info()->num_non_empty_levels();
           level++) {
        overlap = true;
        if (begin != nullptr && end != nullptr) {
          Status status = current_version->OverlapWithLevelIterator(
              ro, file_options_, *begin, *end, level, &overlap);
          if (!status.ok()) {
            overlap = current_version->storage_info()->OverlapInLevel(
                level, begin, end);
          }
        } else {
          overlap = current_version->storage_info()->OverlapInLevel(level,
                                                                    begin, end);
        }
        if (overlap) {
          if (first_overlapped_level == kInvalidLevel) {
            first_overlapped_level = level;
          }
          max_overlapped_level = level;
        }
      }
      CleanupSuperVersion(super_version);
    }
    if (s.ok() && first_overlapped_level != kInvalidLevel) {
      // max_file_num_to_ignore can be used to filter out newly created SST
      // files, useful for bottom level compaction in a manual compaction
      uint64_t max_file_num_to_ignore = port::kMaxUint64;
      uint64_t next_file_number = versions_->current_next_file_number();
      final_output_level = max_overlapped_level;
      int output_level;
      for (int level = first_overlapped_level; level <= max_overlapped_level;
           level++) {
        bool disallow_trivial_move = false;
        // in case the compaction is universal or if we're compacting the
        // bottom-most level, the output level will be the same as input one.
        // level 0 can never be the bottommost level (i.e. if all files are in
        // level 0, we will compact to level 1)
        if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
            cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
          output_level = level;
        } else if (level == max_overlapped_level && level > 0) {
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
          // update max_file_num_to_ignore only for bottom level compaction
          // because data in newly compacted files in middle levels may still
          // need to be pushed down
          max_file_num_to_ignore = next_file_number;
        } else {
          output_level = level + 1;
          if (cfd->ioptions()->compaction_style == kCompactionStyleLevel &&
              cfd->ioptions()->level_compaction_dynamic_level_bytes &&
              level == 0) {
            output_level = ColumnFamilyData::kCompactToBaseLevel;
          }
          // if it's a BottommostLevel compaction and `kForce*` compaction is
          // set, disallow trivial move
          if (level == max_overlapped_level &&
              (options.bottommost_level_compaction ==
                   BottommostLevelCompaction::kForce ||
               options.bottommost_level_compaction ==
                   BottommostLevelCompaction::kForceOptimized)) {
            disallow_trivial_move = true;
          }
        }
        s = RunManualCompaction(cfd, level, output_level, options, begin, end,
                                exclusive, disallow_trivial_move,
                                max_file_num_to_ignore);
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
  }
  if (!s.ok() || final_output_level == kInvalidLevel) {
    LogFlush(immutable_db_options_.info_log);
    return s;
  }

  if (options.change_level) {
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:1");
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:2");

    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[RefitLevel] waiting for background threads to stop");
    DisableManualCompaction();
    s = PauseBackgroundWork();
    if (s.ok()) {
      TEST_SYNC_POINT("DBImpl::CompactRange:PreRefitLevel");
      s = ReFitLevel(cfd, final_output_level, options.target_level);
      TEST_SYNC_POINT("DBImpl::CompactRange:PostRefitLevel");
      // ContinueBackgroundWork always return Status::OK().
      Status temp_s = ContinueBackgroundWork();
      assert(temp_s.ok());
    }
    EnableManualCompaction();
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

Status DBImpl::CompactFiles(const CompactionOptions& compact_options,
                            ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& input_file_names,
                            const int output_level, const int output_path_id,
                            std::vector<std::string>* const output_file_names,
                            CompactionJobInfo* compaction_job_info) {
#ifdef ROCKSDB_LITE
  (void)compact_options;
  (void)column_family;
  (void)input_file_names;
  (void)output_level;
  (void)output_path_id;
  (void)output_file_names;
  (void)compaction_job_info;
  // not supported in lite version
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  if (column_family == nullptr) {
    return Status::InvalidArgument("ColumnFamilyHandle must be non-null.");
  }

  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  assert(cfd);

  Status s;
  JobContext job_context(0, true);
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  // Perform CompactFiles
  TEST_SYNC_POINT("TestCompactFiles::IngestExternalFile2");
  {
    InstrumentedMutexLock l(&mutex_);

    // This call will unlock/lock the mutex to wait for current running
    // IngestExternalFile() calls to finish.
    WaitForIngestFile();

    // We need to get current after `WaitForIngestFile`, because
    // `IngestExternalFile` may add files that overlap with `input_file_names`
    auto* current = cfd->current();
    current->Ref();

    s = CompactFilesImpl(compact_options, cfd, current, input_file_names,
                         output_file_names, output_level, output_path_id,
                         &job_context, &log_buffer, compaction_job_info);

    current->Unref();
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
  if (job_context.HaveSomethingToClean() ||
      job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
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
    std::vector<std::string>* const output_file_names, const int output_level,
    int output_path_id, JobContext* job_context, LogBuffer* log_buffer,
    CompactionJobInfo* compaction_job_info) {
  mutex_.AssertHeld();

  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }
  if (manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input_file_names) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  ColumnFamilyMetaData cf_meta;
  // TODO(yhchiang): can directly use version here if none of the
  // following functions call is pluggable to external developers.
  version->GetColumnFamilyMetaData(&cf_meta);

  if (output_path_id < 0) {
    if (cfd->ioptions()->cf_paths.size() == 1U) {
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

  for (const auto& inputs : input_files) {
    if (cfd->compaction_picker()->AreFilesInCompaction(inputs.files)) {
      return Status::Aborted(
          "Some of the necessary compaction input "
          "files are already being compacted");
    }
  }
  bool sfm_reserved_compact_space = false;
  // First check if we have enough room to do the compaction
  bool enough_room = EnoughRoomForCompaction(
      cfd, input_files, &sfm_reserved_compact_space, log_buffer);

  if (!enough_room) {
    // m's vars will get set properly at the end of this function,
    // as long as status == CompactionTooLarge
    return Status::CompactionTooLarge();
  }

  // At this point, CompactFiles will be run.
  bg_compaction_scheduled_++;

  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  c.reset(cfd->compaction_picker()->CompactFiles(
      compact_options, input_files, output_level, version->storage_info(),
      *cfd->GetLatestMutableCFOptions(), mutable_db_options_, output_path_id));
  // we already sanitized the set of input files and checked for conflicts
  // without releasing the lock, so we're guaranteed a compaction can be formed.
  assert(c != nullptr);

  c->SetInputVersion(version);
  // deletion compaction currently not allowed in CompactFiles.
  assert(!c->deletion_compaction());

  std::vector<SequenceNumber> snapshot_seqs;
  SequenceNumber earliest_write_conflict_snapshot;
  SnapshotChecker* snapshot_checker;
  GetSnapshotContext(job_context, &snapshot_seqs,
                     &earliest_write_conflict_snapshot, &snapshot_checker);

  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem(
      new std::list<uint64_t>::iterator(
          CaptureCurrentFileNumberInPendingOutputs()));

  assert(is_snapshot_supported_ || snapshots_.empty());
  CompactionJobStats compaction_job_stats;
  CompactionJob compaction_job(
      job_context->job_id, c.get(), immutable_db_options_,
      file_options_for_compaction_, versions_.get(), &shutting_down_,
      preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
      GetDataDir(c->column_family_data(), c->output_path_id()),
      GetDataDir(c->column_family_data(), 0), stats_, &mutex_, &error_handler_,
      snapshot_seqs, earliest_write_conflict_snapshot, snapshot_checker,
      table_cache_, &event_logger_,
      c->mutable_cf_options()->paranoid_file_checks,
      c->mutable_cf_options()->report_bg_io_stats, dbname_,
      &compaction_job_stats, Env::Priority::USER, io_tracer_,
      &manual_compaction_paused_, db_id_, db_session_id_,
      c->column_family_data()->GetFullHistoryTsLow());

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
  // Ignore the status here, as it will be checked in the Install down below...
  compaction_job.Run().PermitUncheckedError();
  TEST_SYNC_POINT("CompactFilesImpl:2");
  TEST_SYNC_POINT("CompactFilesImpl:3");
  mutex_.Lock();

  Status status = compaction_job.Install(*c->mutable_cf_options());
  if (status.ok()) {
    assert(compaction_job.io_status().ok());
    InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                       &job_context->superversion_contexts[0],
                                       *c->mutable_cf_options());
  }
  // status above captures any error during compaction_job.Install, so its ok
  // not check compaction_job.io_status() explicitly if we're not calling
  // SetBGError
  compaction_job.io_status().PermitUncheckedError();
  c->ReleaseCompactionFiles(s);
#ifndef ROCKSDB_LITE
  // Need to make sure SstFileManager does its bookkeeping
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm && sfm_reserved_compact_space) {
    sfm->OnCompactionCompletion(c.get());
  }
#endif  // ROCKSDB_LITE

  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  if (compaction_job_info != nullptr) {
    BuildCompactionJobInfo(cfd, c.get(), s, compaction_job_stats,
                           job_context->job_id, version, compaction_job_info);
  }

  if (status.ok()) {
    // Done
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else if (status.IsManualCompactionPaused()) {
    // Don't report stopping manual compaction as error
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Stopping manual compaction",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id);
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Compaction error: %s",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id, status.ToString().c_str());
    IOStatus io_s = compaction_job.io_status();
    if (!io_s.ok()) {
      error_handler_.SetBGError(io_s, BackgroundErrorReason::kCompaction);
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }
  }

  if (output_file_names != nullptr) {
    for (const auto& newf : c->edit()->GetNewFiles()) {
      (*output_file_names)
          .push_back(TableFileName(c->immutable_cf_options()->cf_paths,
                                   newf.second.fd.GetNumber(),
                                   newf.second.fd.GetPathId()));
    }
  }

  c.reset();

  bg_compaction_scheduled_--;
  if (bg_compaction_scheduled_ == 0) {
    bg_cv_.SignalAll();
  }
  MaybeScheduleFlushOrCompaction();
  TEST_SYNC_POINT("CompactFilesImpl:End");

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

void DBImpl::NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                                     const Status& st,
                                     const CompactionJobStats& job_stats,
                                     int job_id) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.empty()) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  if (c->is_manual_compaction() &&
      manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return;
  }
  Version* current = cfd->current();
  current->Ref();
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionBegin::UnlockMutex");
  {
    CompactionJobInfo info{};
    BuildCompactionJobInfo(cfd, c, st, job_stats, job_id, current, &info);
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnCompactionBegin(this, info);
    }
    info.status.PermitUncheckedError();
  }
  mutex_.Lock();
  current->Unref();
#else
  (void)cfd;
  (void)c;
  (void)st;
  (void)job_stats;
  (void)job_id;
#endif  // ROCKSDB_LITE
}

void DBImpl::NotifyOnCompactionCompleted(
    ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id) {
#ifndef ROCKSDB_LITE
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  if (c->is_manual_compaction() &&
      manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return;
  }
  Version* current = cfd->current();
  current->Ref();
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionCompleted::UnlockMutex");
  {
    CompactionJobInfo info{};
    BuildCompactionJobInfo(cfd, c, st, compaction_job_stats, job_id, current,
                           &info);
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnCompactionCompleted(this, info);
    }
  }
  mutex_.Lock();
  current->Unref();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
#else
  (void)cfd;
  (void)c;
  (void)st;
  (void)compaction_job_stats;
  (void)job_id;
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
  if (to_level != level) {
    if (to_level > level) {
      if (level == 0) {
        refitting_level_ = false;
        return Status::NotSupported(
            "Cannot change from level 0 to other levels.");
      }
      // Check levels are empty for a trivial move
      for (int l = level + 1; l <= to_level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
      }
    } else {
      // to_level < level
      // Check levels are empty for a trivial move
      for (int l = to_level; l < level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
      }
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Before refitting:\n%s", cfd->GetName().c_str(),
                    cfd->current()->DebugString().data());

    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    for (const auto& f : vstorage->LevelFiles(level)) {
      edit.DeleteFile(level, f->fd.GetNumber());
      edit.AddFile(to_level, f->fd.GetNumber(), f->fd.GetPathId(),
                   f->fd.GetFileSize(), f->smallest, f->largest,
                   f->fd.smallest_seqno, f->fd.largest_seqno,
                   f->marked_for_compaction, f->oldest_blob_file_number,
                   f->oldest_ancester_time, f->file_creation_time,
                   f->file_checksum, f->file_checksum_func_name);
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Apply version edit:\n%s", cfd->GetName().c_str(),
                    edit.DebugString().data());

    Status status = versions_->LogAndApply(cfd, mutable_cf_options, &edit,
                                           &mutex_, directories_.GetDbDir());

    InstallSuperVersionAndScheduleWork(cfd, &sv_context, mutable_cf_options);

    ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] LogAndApply: %s\n",
                    cfd->GetName().c_str(), status.ToString().data());

    if (status.ok()) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] After refitting:\n%s", cfd->GetName().c_str(),
                      cfd->current()->DebugString().data());
    }
    sv_context.Clean();
    refitting_level_ = false;

    return status;
  }

  refitting_level_ = false;
  return Status::OK();
}

int DBImpl::NumberLevels(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  return cfh->cfd()->NumberLevels();
}

int DBImpl::MaxMemCompactionLevel(ColumnFamilyHandle* /*column_family*/) {
  return 0;
}

int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  InstrumentedMutexLock l(&mutex_);
  return cfh->cfd()
      ->GetSuperVersion()
      ->mutable_cf_options.level0_stop_writes_trigger;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "[%s] Manual flush start.",
                 cfh->GetName().c_str());
  Status s;
  if (immutable_db_options_.atomic_flush) {
    s = AtomicFlushMemTables({cfh->cfd()}, flush_options,
                             FlushReason::kManualFlush);
  } else {
    s = FlushMemTable(cfh->cfd(), flush_options, FlushReason::kManualFlush);
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Manual flush finished, status: %s\n",
                 cfh->GetName().c_str(), s.ToString().c_str());
  return s;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  if (!immutable_db_options_.atomic_flush) {
    for (auto cfh : column_families) {
      s = Flush(flush_options, cfh);
      if (!s.ok()) {
        break;
      }
    }
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush start.\n"
                   "=====Column families:=====");
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
    autovector<ColumnFamilyData*> cfds;
    std::for_each(column_families.begin(), column_families.end(),
                  [&cfds](ColumnFamilyHandle* elem) {
                    auto cfh = static_cast<ColumnFamilyHandleImpl*>(elem);
                    cfds.emplace_back(cfh->cfd());
                  });
    s = AtomicFlushMemTables(cfds, flush_options, FlushReason::kManualFlush);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush finished, status: %s\n"
                   "=====Column families:=====",
                   s.ToString().c_str());
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
  }
  return s;
}

Status DBImpl::RunManualCompaction(
    ColumnFamilyData* cfd, int input_level, int output_level,
    const CompactRangeOptions& compact_range_options, const Slice* begin,
    const Slice* end, bool exclusive, bool disallow_trivial_move,
    uint64_t max_file_num_to_ignore) {
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
  manual.output_path_id = compact_range_options.target_path_id;
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

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
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
               *manual.cfd->GetLatestMutableCFOptions(), mutable_db_options_,
               manual.input_level, manual.output_level, compact_range_options,
               manual.begin, manual.end, &manual.manual_end, &manual_conflict,
               max_file_num_to_ignore)) == nullptr &&
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
      if (!RequestCompactionToken(
              cfd, true, &ca->prepicked_compaction->task_token, &log_buffer)) {
        // Don't throttle manual compaction, only count outstanding tasks.
        assert(false);
      }
      manual.incomplete = false;
      bg_compaction_scheduled_++;
      Env::Priority thread_pool_pri = Env::Priority::LOW;
      if (compaction->bottommost_level() &&
          env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
        thread_pool_pri = Env::Priority::BOTTOM;
      }
      env_->Schedule(&DBImpl::BGWorkCompaction, ca, thread_pool_pri, this,
                     &DBImpl::UnscheduleCompactionCallback);
      scheduled = true;
    }
  }

  log_buffer.FlushBufferToLog();
  assert(!manual.in_progress);
  assert(HasPendingManualCompaction());
  RemoveManualCompaction(&manual);
  bg_cv_.SignalAll();
  return manual.status;
}

void DBImpl::GenerateFlushRequest(const autovector<ColumnFamilyData*>& cfds,
                                  FlushRequest* req) {
  assert(req != nullptr);
  req->reserve(cfds.size());
  for (const auto cfd : cfds) {
    if (nullptr == cfd) {
      // cfd may be null, see DBImpl::ScheduleFlushes
      continue;
    }
    uint64_t max_memtable_id = cfd->imm()->GetLatestMemTableID();
    req->emplace_back(cfd, max_memtable_id);
  }
}

Status DBImpl::FlushMemTable(ColumnFamilyData* cfd,
                             const FlushOptions& flush_options,
                             FlushReason flush_reason, bool writes_stopped) {
  // This method should not be called if atomic_flush is true.
  assert(!immutable_db_options_.atomic_flush);
  Status s;
  if (!flush_options.allow_write_stall) {
    bool flush_needed = true;
    s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
    TEST_SYNC_POINT("DBImpl::FlushMemTable:StallWaitDone");
    if (!s.ok() || !flush_needed) {
      return s;
    }
  }

  autovector<FlushRequest> flush_reqs;
  autovector<uint64_t> memtable_ids_to_wait;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (!writes_stopped) {
      write_thread_.EnterUnbatched(&w, &mutex_);
      if (two_write_queues_) {
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
      }
    }
    WaitForPendingWrites();

    if (flush_reason != FlushReason::kErrorRecoveryRetryFlush &&
        (!cfd->mem()->IsEmpty() || !cached_recoverable_state_empty_.load())) {
      // Note that, when flush reason is kErrorRecoveryRetryFlush, during the
      // auto retry resume, we want to avoid creating new small memtables.
      // Therefore, SwitchMemtable will not be called. Also, since ResumeImpl
      // will iterate through all the CFs and call FlushMemtable during auto
      // retry resume, it is possible that in some CFs,
      // cfd->imm()->NumNotFlushed() = 0. In this case, so no flush request will
      // be created and scheduled, status::OK() will be returned.
      s = SwitchMemtable(cfd, &context);
    }
    const uint64_t flush_memtable_id = port::kMaxUint64;
    if (s.ok()) {
      if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
          !cached_recoverable_state_empty_.load()) {
        FlushRequest req{{cfd, flush_memtable_id}};
        flush_reqs.emplace_back(std::move(req));
        memtable_ids_to_wait.emplace_back(cfd->imm()->GetLatestMemTableID());
      }
      if (immutable_db_options_.persist_stats_to_disk &&
          flush_reason != FlushReason::kErrorRecoveryRetryFlush) {
        ColumnFamilyData* cfd_stats =
            versions_->GetColumnFamilySet()->GetColumnFamily(
                kPersistentStatsColumnFamilyName);
        if (cfd_stats != nullptr && cfd_stats != cfd &&
            !cfd_stats->mem()->IsEmpty()) {
          // only force flush stats CF when it will be the only CF lagging
          // behind after the current flush
          bool stats_cf_flush_needed = true;
          for (auto* loop_cfd : *versions_->GetColumnFamilySet()) {
            if (loop_cfd == cfd_stats || loop_cfd == cfd) {
              continue;
            }
            if (loop_cfd->GetLogNumber() <= cfd_stats->GetLogNumber()) {
              stats_cf_flush_needed = false;
            }
          }
          if (stats_cf_flush_needed) {
            ROCKS_LOG_INFO(immutable_db_options_.info_log,
                           "Force flushing stats CF with manual flush of %s "
                           "to avoid holding old logs",
                           cfd->GetName().c_str());
            s = SwitchMemtable(cfd_stats, &context);
            FlushRequest req{{cfd_stats, flush_memtable_id}};
            flush_reqs.emplace_back(std::move(req));
            memtable_ids_to_wait.emplace_back(
                cfd->imm()->GetLatestMemTableID());
          }
        }
      }
    }

    if (s.ok() && !flush_reqs.empty()) {
      for (const auto& req : flush_reqs) {
        assert(req.size() == 1);
        ColumnFamilyData* loop_cfd = req[0].first;
        loop_cfd->imm()->FlushRequested();
      }
      // If the caller wants to wait for this flush to complete, it indicates
      // that the caller expects the ColumnFamilyData not to be free'ed by
      // other threads which may drop the column family concurrently.
      // Therefore, we increase the cfd's ref count.
      if (flush_options.wait) {
        for (const auto& req : flush_reqs) {
          assert(req.size() == 1);
          ColumnFamilyData* loop_cfd = req[0].first;
          loop_cfd->Ref();
        }
      }
      for (const auto& req : flush_reqs) {
        SchedulePendingFlush(req, flush_reason);
      }
      MaybeScheduleFlushOrCompaction();
    }

    if (!writes_stopped) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::FlushMemTable:BeforeWaitForBgFlush");
  if (s.ok() && flush_options.wait) {
    autovector<ColumnFamilyData*> cfds;
    autovector<const uint64_t*> flush_memtable_ids;
    assert(flush_reqs.size() == memtable_ids_to_wait.size());
    for (size_t i = 0; i < flush_reqs.size(); ++i) {
      assert(flush_reqs[i].size() == 1);
      cfds.push_back(flush_reqs[i][0].first);
      flush_memtable_ids.push_back(&(memtable_ids_to_wait[i]));
    }
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        (flush_reason == FlushReason::kErrorRecovery ||
         flush_reason == FlushReason::kErrorRecoveryRetryFlush));
    InstrumentedMutexLock lock_guard(&mutex_);
    for (auto* tmp_cfd : cfds) {
      tmp_cfd->UnrefAndTryDelete();
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:FlushMemTableFinished");
  return s;
}

// Flush all elements in 'column_family_datas'
// and atomically record the result to the MANIFEST.
Status DBImpl::AtomicFlushMemTables(
    const autovector<ColumnFamilyData*>& column_family_datas,
    const FlushOptions& flush_options, FlushReason flush_reason,
    bool writes_stopped) {
  Status s;
  if (!flush_options.allow_write_stall) {
    int num_cfs_to_flush = 0;
    for (auto cfd : column_family_datas) {
      bool flush_needed = true;
      s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
      if (!s.ok()) {
        return s;
      } else if (flush_needed) {
        ++num_cfs_to_flush;
      }
    }
    if (0 == num_cfs_to_flush) {
      return s;
    }
  }
  FlushRequest flush_req;
  autovector<ColumnFamilyData*> cfds;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (!writes_stopped) {
      write_thread_.EnterUnbatched(&w, &mutex_);
      if (two_write_queues_) {
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
      }
    }
    WaitForPendingWrites();

    for (auto cfd : column_family_datas) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
          !cached_recoverable_state_empty_.load()) {
        cfds.emplace_back(cfd);
      }
    }
    for (auto cfd : cfds) {
      if ((cfd->mem()->IsEmpty() && cached_recoverable_state_empty_.load()) ||
          flush_reason == FlushReason::kErrorRecoveryRetryFlush) {
        continue;
      }
      cfd->Ref();
      s = SwitchMemtable(cfd, &context);
      cfd->UnrefAndTryDelete();
      if (!s.ok()) {
        break;
      }
    }
    if (s.ok()) {
      AssignAtomicFlushSeq(cfds);
      for (auto cfd : cfds) {
        cfd->imm()->FlushRequested();
      }
      // If the caller wants to wait for this flush to complete, it indicates
      // that the caller expects the ColumnFamilyData not to be free'ed by
      // other threads which may drop the column family concurrently.
      // Therefore, we increase the cfd's ref count.
      if (flush_options.wait) {
        for (auto cfd : cfds) {
          cfd->Ref();
        }
      }
      GenerateFlushRequest(cfds, &flush_req);
      SchedulePendingFlush(flush_req, flush_reason);
      MaybeScheduleFlushOrCompaction();
    }

    if (!writes_stopped) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush");
  if (s.ok() && flush_options.wait) {
    autovector<const uint64_t*> flush_memtable_ids;
    for (auto& iter : flush_req) {
      flush_memtable_ids.push_back(&(iter.second));
    }
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        (flush_reason == FlushReason::kErrorRecovery ||
         flush_reason == FlushReason::kErrorRecoveryRetryFlush));
    InstrumentedMutexLock lock_guard(&mutex_);
    for (auto* cfd : cfds) {
      cfd->UnrefAndTryDelete();
    }
  }
  return s;
}

// Calling FlushMemTable(), whether from DB::Flush() or from Backup Engine, can
// cause write stall, for example if one memtable is being flushed already.
// This method tries to avoid write stall (similar to CompactRange() behavior)
// it emulates how the SuperVersion / LSM would change if flush happens, checks
// it against various constrains and delays flush if it'd cause write stall.
// Called should check status and flush_needed to see if flush already happened.
Status DBImpl::WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                                 bool* flush_needed) {
  {
    *flush_needed = true;
    InstrumentedMutexLock l(&mutex_);
    uint64_t orig_active_memtable_id = cfd->mem()->GetID();
    WriteStallCondition write_stall_condition = WriteStallCondition::kNormal;
    do {
      if (write_stall_condition != WriteStallCondition::kNormal) {
        // Same error handling as user writes: Don't wait if there's a
        // background error, even if it's a soft error. We might wait here
        // indefinitely as the pending flushes/compactions may never finish
        // successfully, resulting in the stall condition lasting indefinitely
        if (error_handler_.IsBGWorkStopped()) {
          return error_handler_.GetBGError();
        }

        TEST_SYNC_POINT("DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait");
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[%s] WaitUntilFlushWouldNotStallWrites"
                       " waiting on stall conditions to clear",
                       cfd->GetName().c_str());
        bg_cv_.Wait();
      }
      if (cfd->IsDropped()) {
        return Status::ColumnFamilyDropped();
      }
      if (shutting_down_.load(std::memory_order_acquire)) {
        return Status::ShutdownInProgress();
      }

      uint64_t earliest_memtable_id =
          std::min(cfd->mem()->GetID(), cfd->imm()->GetEarliestMemTableID());
      if (earliest_memtable_id > orig_active_memtable_id) {
        // We waited so long that the memtable we were originally waiting on was
        // flushed.
        *flush_needed = false;
        return Status::OK();
      }

      const auto& mutable_cf_options = *cfd->GetLatestMutableCFOptions();
      const auto* vstorage = cfd->current()->storage_info();

      // Skip stalling check if we're below auto-flush and auto-compaction
      // triggers. If it stalled in these conditions, that'd mean the stall
      // triggers are so low that stalling is needed for any background work. In
      // that case we shouldn't wait since background work won't be scheduled.
      if (cfd->imm()->NumNotFlushed() <
              cfd->ioptions()->min_write_buffer_number_to_merge &&
          vstorage->l0_delay_trigger_count() <
              mutable_cf_options.level0_file_num_compaction_trigger) {
        break;
      }

      // check whether one extra immutable memtable or an extra L0 file would
      // cause write stalling mode to be entered. It could still enter stall
      // mode due to pending compaction bytes, but that's less common
      write_stall_condition = ColumnFamilyData::GetWriteStallConditionAndCause(
                                  cfd->imm()->NumNotFlushed() + 1,
                                  vstorage->l0_delay_trigger_count() + 1,
                                  vstorage->estimated_compaction_needed_bytes(),
                                  mutable_cf_options, *cfd->ioptions())
                                  .first;
    } while (write_stall_condition != WriteStallCondition::kNormal);
  }
  return Status::OK();
}

// Wait for memtables to be flushed for multiple column families.
// let N = cfds.size()
// for i in [0, N),
//  1) if flush_memtable_ids[i] is not null, then the memtables with lower IDs
//     have to be flushed for THIS column family;
//  2) if flush_memtable_ids[i] is null, then all memtables in THIS column
//     family have to be flushed.
// Finish waiting when ALL column families finish flushing memtables.
// resuming_from_bg_err indicates whether the caller is trying to resume from
// background error or in normal processing.
Status DBImpl::WaitForFlushMemTables(
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const uint64_t*>& flush_memtable_ids,
    bool resuming_from_bg_err) {
  int num = static_cast<int>(cfds.size());
  // Wait until the compaction completes
  InstrumentedMutexLock l(&mutex_);
  // If the caller is trying to resume from bg error, then
  // error_handler_.IsDBStopped() is true.
  while (resuming_from_bg_err || !error_handler_.IsDBStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress();
    }
    // If an error has occurred during resumption, then no need to wait.
    if (!error_handler_.GetRecoveryError().ok()) {
      break;
    }
    // If BGWorkStopped, which indicate that there is a BG error and
    // 1) soft error but requires no BG work, 2) no in auto_recovery_
    if (!resuming_from_bg_err && error_handler_.IsBGWorkStopped() &&
        error_handler_.GetBGError().severity() < Status::Severity::kHardError) {
      return error_handler_.GetBGError();
    }

    // Number of column families that have been dropped.
    int num_dropped = 0;
    // Number of column families that have finished flush.
    int num_finished = 0;
    for (int i = 0; i < num; ++i) {
      if (cfds[i]->IsDropped()) {
        ++num_dropped;
      } else if (cfds[i]->imm()->NumNotFlushed() == 0 ||
                 (flush_memtable_ids[i] != nullptr &&
                  cfds[i]->imm()->GetEarliestMemTableID() >
                      *flush_memtable_ids[i])) {
        ++num_finished;
      }
    }
    if (1 == num_dropped && 1 == num) {
      return Status::ColumnFamilyDropped();
    }
    // Column families involved in this flush request have either been dropped
    // or finished flush. Then it's time to finish waiting.
    if (num_dropped + num_finished == num) {
      break;
    }
    bg_cv_.Wait();
  }
  Status s;
  // If not resuming from bg error, and an error has caused the DB to stop,
  // then report the bg error to caller.
  if (!resuming_from_bg_err && error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
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

void DBImpl::DisableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  manual_compaction_paused_.fetch_add(1, std::memory_order_release);
  // Wait for any pending manual compactions to finish (typically through
  // failing with `Status::Incomplete`) prior to returning. This way we are
  // guaranteed no pending manual compaction will commit while manual
  // compactions are "disabled".
  while (HasPendingManualCompaction()) {
    bg_cv_.Wait();
  }
}

void DBImpl::EnableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  assert(manual_compaction_paused_ > 0);
  manual_compaction_paused_.fetch_sub(1, std::memory_order_release);
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
  } else if (error_handler_.IsBGWorkStopped() &&
             !error_handler_.IsRecoveryInProgress()) {
    // There has been a hard error and this call is not part of the recovery
    // sequence. Bail out here so we don't get into an endless loop of
    // scheduling BG work which will again call this function
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
    bg_flush_scheduled_++;
    FlushThreadArg* fta = new FlushThreadArg;
    fta->db_ = this;
    fta->thread_pri_ = Env::Priority::HIGH;
    env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::HIGH, this,
                   &DBImpl::UnscheduleFlushCallback);
    --unscheduled_flushes_;
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::MaybeScheduleFlushOrCompaction:AfterSchedule:0",
        &unscheduled_flushes_);
  }

  // special case -- if high-pri (flush) thread pool is empty, then schedule
  // flushes in low-pri (compaction) thread pool.
  if (is_flush_pool_empty) {
    while (unscheduled_flushes_ > 0 &&
           bg_flush_scheduled_ + bg_compaction_scheduled_ <
               bg_job_limits.max_flushes) {
      bg_flush_scheduled_++;
      FlushThreadArg* fta = new FlushThreadArg;
      fta->db_ = this;
      fta->thread_pri_ = Env::Priority::LOW;
      env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::LOW, this,
                     &DBImpl::UnscheduleFlushCallback);
      --unscheduled_flushes_;
    }
  }

  if (bg_compaction_paused_ > 0) {
    // we paused the background compaction
    return;
  } else if (error_handler_.IsBGWorkStopped()) {
    // Compaction is not part of the recovery sequence from a hard error. We
    // might get here because recovery might do a flush and install a new
    // super version, which will try to schedule pending compactions. Bail
    // out here and let the higher level recovery handle compactions
    return;
  }

  if (HasExclusiveManualCompaction()) {
    // only manual compactions are allowed to run. don't schedule automatic
    // compactions
    TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Conflict");
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
                   &DBImpl::UnscheduleCompactionCallback);
  }
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits() const {
  mutex_.AssertHeld();
  return GetBGJobLimits(mutable_db_options_.max_background_flushes,
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
  assert(!cfd->queued_for_compaction());
  cfd->Ref();
  compaction_queue_.push_back(cfd);
  cfd->set_queued_for_compaction(true);
}

ColumnFamilyData* DBImpl::PopFirstFromCompactionQueue() {
  assert(!compaction_queue_.empty());
  auto cfd = *compaction_queue_.begin();
  compaction_queue_.pop_front();
  assert(cfd->queued_for_compaction());
  cfd->set_queued_for_compaction(false);
  return cfd;
}

DBImpl::FlushRequest DBImpl::PopFirstFromFlushQueue() {
  assert(!flush_queue_.empty());
  FlushRequest flush_req = flush_queue_.front();
  flush_queue_.pop_front();
  if (!immutable_db_options_.atomic_flush) {
    assert(flush_req.size() == 1);
  }
  for (const auto& elem : flush_req) {
    if (!immutable_db_options_.atomic_flush) {
      ColumnFamilyData* cfd = elem.first;
      assert(cfd);
      assert(cfd->queued_for_flush());
      cfd->set_queued_for_flush(false);
    }
  }
  // TODO: need to unset flush reason?
  return flush_req;
}

ColumnFamilyData* DBImpl::PickCompactionFromQueue(
    std::unique_ptr<TaskLimiterToken>* token, LogBuffer* log_buffer) {
  assert(!compaction_queue_.empty());
  assert(*token == nullptr);
  autovector<ColumnFamilyData*> throttled_candidates;
  ColumnFamilyData* cfd = nullptr;
  while (!compaction_queue_.empty()) {
    auto first_cfd = *compaction_queue_.begin();
    compaction_queue_.pop_front();
    assert(first_cfd->queued_for_compaction());
    if (!RequestCompactionToken(first_cfd, false, token, log_buffer)) {
      throttled_candidates.push_back(first_cfd);
      continue;
    }
    cfd = first_cfd;
    cfd->set_queued_for_compaction(false);
    break;
  }
  // Add throttled compaction candidates back to queue in the original order.
  for (auto iter = throttled_candidates.rbegin();
       iter != throttled_candidates.rend(); ++iter) {
    compaction_queue_.push_front(*iter);
  }
  return cfd;
}

void DBImpl::SchedulePendingFlush(const FlushRequest& flush_req,
                                  FlushReason flush_reason) {
  mutex_.AssertHeld();
  if (flush_req.empty()) {
    return;
  }
  if (!immutable_db_options_.atomic_flush) {
    // For the non-atomic flush case, we never schedule multiple column
    // families in the same flush request.
    assert(flush_req.size() == 1);
    ColumnFamilyData* cfd = flush_req[0].first;
    assert(cfd);
    if (!cfd->queued_for_flush() && cfd->imm()->IsFlushPending()) {
      cfd->Ref();
      cfd->set_queued_for_flush(true);
      cfd->SetFlushReason(flush_reason);
      ++unscheduled_flushes_;
      flush_queue_.push_back(flush_req);
    }
  } else {
    for (auto& iter : flush_req) {
      ColumnFamilyData* cfd = iter.first;
      cfd->Ref();
      cfd->SetFlushReason(flush_reason);
    }
    ++unscheduled_flushes_;
    flush_queue_.push_back(flush_req);
  }
}

void DBImpl::SchedulePendingCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  if (!cfd->queued_for_compaction() && cfd->NeedsCompaction()) {
    AddToCompactionQueue(cfd);
    ++unscheduled_compactions_;
  }
}

void DBImpl::SchedulePendingPurge(std::string fname, std::string dir_to_sync,
                                  FileType type, uint64_t number, int job_id) {
  mutex_.AssertHeld();
  PurgeFileInfo file_info(fname, dir_to_sync, type, number, job_id);
  purge_files_.insert({{number, std::move(file_info)}});
}

void DBImpl::BGWorkFlush(void* arg) {
  FlushThreadArg fta = *(reinterpret_cast<FlushThreadArg*>(arg));
  delete reinterpret_cast<FlushThreadArg*>(arg);

  IOSTATS_SET_THREAD_POOL_ID(fta.thread_pri_);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush");
  static_cast_with_check<DBImpl>(fta.db_)->BackgroundCallFlush(fta.thread_pri_);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush:done");
}

void DBImpl::BGWorkCompaction(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkCompaction");
  auto prepicked_compaction =
      static_cast<PrepickedCompaction*>(ca.prepicked_compaction);
  static_cast_with_check<DBImpl>(ca.db)->BackgroundCallCompaction(
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

void DBImpl::UnscheduleCompactionCallback(void* arg) {
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));
  delete reinterpret_cast<CompactionArg*>(arg);
  if (ca.prepicked_compaction != nullptr) {
    if (ca.prepicked_compaction->compaction != nullptr) {
      delete ca.prepicked_compaction->compaction;
    }
    delete ca.prepicked_compaction;
  }
  TEST_SYNC_POINT("DBImpl::UnscheduleCompactionCallback");
}

void DBImpl::UnscheduleFlushCallback(void* arg) {
  delete reinterpret_cast<FlushThreadArg*>(arg);
  TEST_SYNC_POINT("DBImpl::UnscheduleFlushCallback");
}

Status DBImpl::BackgroundFlush(bool* made_progress, JobContext* job_context,
                               LogBuffer* log_buffer, FlushReason* reason,
                               Env::Priority thread_pri) {
  mutex_.AssertHeld();

  Status status;
  *reason = FlushReason::kOthers;
  // If BG work is stopped due to an error, but a recovery is in progress,
  // that means this flush is part of the recovery. So allow it to go through
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    }
  } else if (!error_handler_.IsRecoveryInProgress()) {
    status = error_handler_.GetBGError();
  }

  if (!status.ok()) {
    return status;
  }

  autovector<BGFlushArg> bg_flush_args;
  std::vector<SuperVersionContext>& superversion_contexts =
      job_context->superversion_contexts;
  autovector<ColumnFamilyData*> column_families_not_to_flush;
  while (!flush_queue_.empty()) {
    // This cfd is already referenced
    const FlushRequest& flush_req = PopFirstFromFlushQueue();
    superversion_contexts.clear();
    superversion_contexts.reserve(flush_req.size());

    for (const auto& iter : flush_req) {
      ColumnFamilyData* cfd = iter.first;
      if (cfd->IsDropped() || !cfd->imm()->IsFlushPending()) {
        // can't flush this CF, try next one
        column_families_not_to_flush.push_back(cfd);
        continue;
      }
      superversion_contexts.emplace_back(SuperVersionContext(true));
      bg_flush_args.emplace_back(cfd, iter.second,
                                 &(superversion_contexts.back()));
    }
    if (!bg_flush_args.empty()) {
      break;
    }
  }

  if (!bg_flush_args.empty()) {
    auto bg_job_limits = GetBGJobLimits();
    for (const auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "Calling FlushMemTableToOutputFile with column "
          "family [%s], flush slots available %d, compaction slots available "
          "%d, "
          "flush slots scheduled %d, compaction slots scheduled %d",
          cfd->GetName().c_str(), bg_job_limits.max_flushes,
          bg_job_limits.max_compactions, bg_flush_scheduled_,
          bg_compaction_scheduled_);
    }
    status = FlushMemTablesToOutputFiles(bg_flush_args, made_progress,
                                         job_context, log_buffer, thread_pri);
    TEST_SYNC_POINT("DBImpl::BackgroundFlush:BeforeFlush");
    // All the CFDs in the FlushReq must have the same flush reason, so just
    // grab the first one
    *reason = bg_flush_args[0].cfd_->GetFlushReason();
    for (auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      if (cfd->UnrefAndTryDelete()) {
        arg.cfd_ = nullptr;
      }
    }
  }
  for (auto cfd : column_families_not_to_flush) {
    cfd->UnrefAndTryDelete();
  }
  return status;
}

void DBImpl::BackgroundCallFlush(Env::Priority thread_pri) {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);

  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:start");

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);
    assert(bg_flush_scheduled_);
    num_running_flushes_++;

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs()));
    FlushReason reason;

    Status s = BackgroundFlush(&made_progress, &job_context, &log_buffer,
                               &reason, thread_pri);
    if (!s.ok() && !s.IsShutdownInProgress() && !s.IsColumnFamilyDropped() &&
        reason != FlushReason::kErrorRecovery) {
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
      immutable_db_options_.clock->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FlushFinish:0");
    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If flush failed, we want to delete all temporary files that we might have
    // created. Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                        !s.IsColumnFamilyDropped());
    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FilesFound");
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
    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:ContextCleanedUp");

    assert(num_running_flushes_ > 0);
    num_running_flushes_--;
    bg_flush_scheduled_--;
    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();
    atomic_flush_install_cv_.SignalAll();
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
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);

    // This call will unlock/lock the mutex to wait for current running
    // IngestExternalFile() calls to finish.
    WaitForIngestFile();

    num_running_compactions_++;

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs()));

    assert((bg_thread_pri == Env::Priority::BOTTOM &&
            bg_bottom_compaction_scheduled_) ||
           (bg_thread_pri == Env::Priority::LOW && bg_compaction_scheduled_));
    Status s = BackgroundCompaction(&made_progress, &job_context, &log_buffer,
                                    prepicked_compaction, bg_thread_pri);
    TEST_SYNC_POINT("BackgroundCallCompaction:1");
    if (s.IsBusy()) {
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      immutable_db_options_.clock->SleepForMicroseconds(
          10000);  // prevent hot loop
      mutex_.Lock();
    } else if (!s.ok() && !s.IsShutdownInProgress() &&
               !s.IsManualCompactionPaused() && !s.IsColumnFamilyDropped()) {
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
      immutable_db_options_.clock->SleepForMicroseconds(1000000);
      mutex_.Lock();
    } else if (s.IsManualCompactionPaused()) {
      ManualCompactionState* m = prepicked_compaction->manual_compaction_state;
      assert(m);
      ROCKS_LOG_BUFFER(&log_buffer, "[%s] [JOB %d] Manual compaction paused",
                       m->cfd->GetName().c_str(), job_context.job_id);
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If compaction failed, we want to delete all temporary files that we might
    // have created (they might not be all recorded in job_context in case of a
    // failure). Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                        !s.IsManualCompactionPaused() &&
                                        !s.IsColumnFamilyDropped() &&
                                        !s.IsBusy());
    TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:FoundObsoleteFiles");

    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_compaction_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
        TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles");
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
        HasPendingManualCompaction() || unscheduled_compactions_ == 0) {
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
                                    PrepickedCompaction* prepicked_compaction,
                                    Env::Priority thread_pri) {
  ManualCompactionState* manual_compaction =
      prepicked_compaction == nullptr
          ? nullptr
          : prepicked_compaction->manual_compaction_state;
  *made_progress = false;
  mutex_.AssertHeld();
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Start");

  bool is_manual = (manual_compaction != nullptr);
  std::unique_ptr<Compaction> c;
  if (prepicked_compaction != nullptr &&
      prepicked_compaction->compaction != nullptr) {
    c.reset(prepicked_compaction->compaction);
  }
  bool is_prepicked = is_manual || c;

  // (manual_compaction->in_progress == false);
  bool trivial_move_disallowed =
      is_manual && manual_compaction->disallow_trivial_move;

  CompactionJobStats compaction_job_stats;
  Status status;
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    } else if (is_manual &&
               manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
      status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    }
  } else {
    status = error_handler_.GetBGError();
    // If we get here, it means a hard error happened after this compaction
    // was scheduled by MaybeScheduleFlushOrCompaction(), but before it got
    // a chance to execute. Since we didn't pop a cfd from the compaction
    // queue, increment unscheduled_compactions_
    unscheduled_compactions_++;
  }

  if (!status.ok()) {
    if (is_manual) {
      manual_compaction->status = status;
      manual_compaction->done = true;
      manual_compaction->in_progress = false;
      manual_compaction = nullptr;
    }
    if (c) {
      c->ReleaseCompactionFiles(status);
      c.reset();
    }
    return status;
  }

  if (is_manual) {
    // another thread cannot pick up the same work
    manual_compaction->in_progress = true;
  }

  std::unique_ptr<TaskLimiterToken> task_token;

  // InternalKey manual_end_storage;
  // InternalKey* manual_end = &manual_end_storage;
  bool sfm_reserved_compact_space = false;
  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    assert(m->in_progress);
    if (!c) {
      m->done = true;
      m->manual_end = nullptr;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Manual compaction from level-%d from %s .. "
          "%s; nothing to do\n",
          m->cfd->GetName().c_str(), m->input_level,
          (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
          (m->end ? m->end->DebugString(true).c_str() : "(end)"));
    } else {
      // First check if we have enough room to do the compaction
      bool enough_room = EnoughRoomForCompaction(
          m->cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

      if (!enough_room) {
        // Then don't do the compaction
        c->ReleaseCompactionFiles(status);
        c.reset();
        // m's vars will get set properly at the end of this function,
        // as long as status == CompactionTooLarge
        status = Status::CompactionTooLarge();
      } else {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Manual compaction from level-%d to level-%d from %s .. "
            "%s; will stop at %s\n",
            m->cfd->GetName().c_str(), m->input_level, c->output_level(),
            (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
            (m->end ? m->end->DebugString(true).c_str() : "(end)"),
            ((m->done || m->manual_end == nullptr)
                 ? "(end)"
                 : m->manual_end->DebugString(true).c_str()));
      }
    }
  } else if (!is_prepicked && !compaction_queue_.empty()) {
    if (HasExclusiveManualCompaction()) {
      // Can't compact right now, but try again later
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction()::Conflict");

      // Stay in the compaction queue.
      unscheduled_compactions_++;

      return Status::OK();
    }

    auto cfd = PickCompactionFromQueue(&task_token, log_buffer);
    if (cfd == nullptr) {
      // Can't find any executable task from the compaction queue.
      // All tasks have been throttled by compaction thread limiter.
      ++unscheduled_compactions_;
      return Status::Busy();
    }

    // We unreference here because the following code will take a Ref() on
    // this cfd if it is going to use it (Compaction class holds a
    // reference).
    // This will all happen under a mutex so we don't have to be afraid of
    // somebody else deleting it.
    if (cfd->UnrefAndTryDelete()) {
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
      c.reset(cfd->PickCompaction(*mutable_cf_options, mutable_db_options_,
                                  log_buffer));
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():AfterPickCompaction");

      if (c != nullptr) {
        bool enough_room = EnoughRoomForCompaction(
            cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

        if (!enough_room) {
          // Then don't do the compaction
          c->ReleaseCompactionFiles(status);
          c->column_family_data()
              ->current()
              ->storage_info()
              ->ComputeCompactionScore(*(c->immutable_cf_options()),
                                       *(c->mutable_cf_options()));
          AddToCompactionQueue(cfd);
          ++unscheduled_compactions_;

          c.reset();
          // Don't need to sleep here, because BackgroundCallCompaction
          // will sleep if !s.ok()
          status = Status::CompactionTooLarge();
        } else {
          // update statistics
          RecordInHistogram(stats_, NUM_FILES_IN_SINGLE_COMPACTION,
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
          // compaction, we might be able to execute it in parallel, so we add
          // it to the queue and schedule a new thread.
          if (cfd->NeedsCompaction()) {
            // Yes, we need more compactions!
            AddToCompactionQueue(cfd);
            ++unscheduled_compactions_;
            MaybeScheduleFlushOrCompaction();
          }
        }
      }
    }
  }

  IOStatus io_s;
  if (!c) {
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "Compaction nothing to do");
  } else if (c->deletion_compaction()) {
    // TODO(icanadi) Do we want to honor snapshots here? i.e. not delete old
    // file if there is alive snapshot pointing to it
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    assert(c->num_input_files(1) == 0);
    assert(c->level() == 0);
    assert(c->column_family_data()->ioptions()->compaction_style ==
           kCompactionStyleFIFO);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    for (const auto& f : *c->inputs(0)) {
      c->edit()->DeleteFile(c->level(), f->fd.GetNumber());
    }
    status = versions_->LogAndApply(c->column_family_data(),
                                    *c->mutable_cf_options(), c->edit(),
                                    &mutex_, directories_.GetDbDir());
    io_s = versions_->io_status();
    InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                       &job_context->superversion_contexts[0],
                                       *c->mutable_cf_options());
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Deleted %d files\n",
                     c->column_family_data()->GetName().c_str(),
                     c->num_input_files(0));
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  } else if (!trivial_move_disallowed && c->IsTrivialMove()) {
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:TrivialMove");
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    // Instrument for event update
    // TODO(yhchiang): add op details for showing trivial-move.
    ThreadStatusUtil::SetColumnFamily(
        c->column_family_data(), c->column_family_data()->ioptions()->env,
        immutable_db_options_.enable_thread_tracking);
    ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

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
                           f->largest, f->fd.smallest_seqno,
                           f->fd.largest_seqno, f->marked_for_compaction,
                           f->oldest_blob_file_number, f->oldest_ancester_time,
                           f->file_creation_time, f->file_checksum,
                           f->file_checksum_func_name);

        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Moving #%" PRIu64 " to level-%d %" PRIu64 " bytes\n",
            c->column_family_data()->GetName().c_str(), f->fd.GetNumber(),
            c->output_level(), f->fd.GetFileSize());
        ++moved_files;
        moved_bytes += f->fd.GetFileSize();
      }
    }

    status = versions_->LogAndApply(c->column_family_data(),
                                    *c->mutable_cf_options(), c->edit(),
                                    &mutex_, directories_.GetDbDir());
    io_s = versions_->io_status();
    // Use latest MutableCFOptions
    InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                       &job_context->superversion_contexts[0],
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
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  } else if (!is_prepicked && c->output_level() > 0 &&
             c->output_level() ==
                 c->column_family_data()
                     ->current()
                     ->storage_info()
                     ->MaxOutputLevel(
                         immutable_db_options_.allow_ingest_behind) &&
             env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
    // Forward compactions involving last level to the bottom pool if it exists,
    // such that compactions unlikely to contribute to write stalls can be
    // delayed or deprioritized.
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:ForwardToBottomPriPool");
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->prepicked_compaction = new PrepickedCompaction;
    ca->prepicked_compaction->compaction = c.release();
    ca->prepicked_compaction->manual_compaction_state = nullptr;
    // Transfer requested token, so it doesn't need to do it again.
    ca->prepicked_compaction->task_token = std::move(task_token);
    ++bg_bottom_compaction_scheduled_;
    env_->Schedule(&DBImpl::BGWorkBottomCompaction, ca, Env::Priority::BOTTOM,
                   this, &DBImpl::UnscheduleCompactionCallback);
  } else {
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    int output_level __attribute__((__unused__));
    output_level = c->output_level();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:NonTrivial",
                             &output_level);
    std::vector<SequenceNumber> snapshot_seqs;
    SequenceNumber earliest_write_conflict_snapshot;
    SnapshotChecker* snapshot_checker;
    GetSnapshotContext(job_context, &snapshot_seqs,
                       &earliest_write_conflict_snapshot, &snapshot_checker);
    assert(is_snapshot_supported_ || snapshots_.empty());
    CompactionJob compaction_job(
        job_context->job_id, c.get(), immutable_db_options_,
        file_options_for_compaction_, versions_.get(), &shutting_down_,
        preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
        GetDataDir(c->column_family_data(), c->output_path_id()),
        GetDataDir(c->column_family_data(), 0), stats_, &mutex_,
        &error_handler_, snapshot_seqs, earliest_write_conflict_snapshot,
        snapshot_checker, table_cache_, &event_logger_,
        c->mutable_cf_options()->paranoid_file_checks,
        c->mutable_cf_options()->report_bg_io_stats, dbname_,
        &compaction_job_stats, thread_pri, io_tracer_,
        is_manual ? &manual_compaction_paused_ : nullptr, db_id_,
        db_session_id_, c->column_family_data()->GetFullHistoryTsLow());
    compaction_job.Prepare();

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);
    mutex_.Unlock();
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun", nullptr);
    // Should handle erorr?
    compaction_job.Run().PermitUncheckedError();
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:NonTrivial:AfterRun");
    mutex_.Lock();

    status = compaction_job.Install(*c->mutable_cf_options());
    io_s = compaction_job.io_status();
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                         &job_context->superversion_contexts[0],
                                         *c->mutable_cf_options());
    }
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  }

  if (status.ok() && !io_s.ok()) {
    status = io_s;
  } else {
    io_s.PermitUncheckedError();
  }

  if (c != nullptr) {
    c->ReleaseCompactionFiles(status);
    *made_progress = true;

#ifndef ROCKSDB_LITE
    // Need to make sure SstFileManager does its bookkeeping
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm && sfm_reserved_compact_space) {
      sfm->OnCompactionCompletion(c.get());
    }
#endif  // ROCKSDB_LITE

    NotifyOnCompactionCompleted(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
  }

  if (status.ok() || status.IsCompactionTooLarge() ||
      status.IsManualCompactionPaused()) {
    // Done
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Compaction error: %s",
                   status.ToString().c_str());
    if (!io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      auto err_reason = versions_->io_status().ok()
                            ? BackgroundErrorReason::kCompaction
                            : BackgroundErrorReason::kManifestWrite;
      error_handler_.SetBGError(io_s, err_reason);
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }
    if (c != nullptr && !is_manual && !error_handler_.IsBGWorkStopped()) {
      // Put this cfd back in the compaction queue so we can retry after some
      // time
      auto cfd = c->column_family_data();
      assert(cfd != nullptr);
      // Since this compaction failed, we need to recompute the score so it
      // takes the original input files into account
      c->column_family_data()
          ->current()
          ->storage_info()
          ->ComputeCompactionScore(*(c->immutable_cf_options()),
                                   *(c->mutable_cf_options()));
      if (!cfd->queued_for_compaction()) {
        AddToCompactionQueue(cfd);
        ++unscheduled_compactions_;
      }
    }
  }
  // this will unref its input_version and column_family_data
  c.reset();

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
    m->in_progress = false;  // not being processed anymore
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
    ++it;
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
      ++it;
      seen = true;
      continue;
    } else if (MCOverlap(m, (*it)) && (!seen && !(*it)->in_progress)) {
      // Consider the other manual compaction *it, conflicts if:
      // overlaps with m
      // and (*it) is ahead in the queue and is not yet in progress
      return true;
    }
    ++it;
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
    ++it;
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
    ++it;
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

#ifndef ROCKSDB_LITE
void DBImpl::BuildCompactionJobInfo(
    const ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id,
    const Version* current, CompactionJobInfo* compaction_job_info) const {
  assert(compaction_job_info != nullptr);
  compaction_job_info->cf_id = cfd->GetID();
  compaction_job_info->cf_name = cfd->GetName();
  compaction_job_info->status = st;
  compaction_job_info->thread_id = env_->GetThreadID();
  compaction_job_info->job_id = job_id;
  compaction_job_info->base_input_level = c->start_level();
  compaction_job_info->output_level = c->output_level();
  compaction_job_info->stats = compaction_job_stats;
  compaction_job_info->table_properties = c->GetOutputTableProperties();
  compaction_job_info->compaction_reason = c->compaction_reason();
  compaction_job_info->compression = c->output_compression();
  for (size_t i = 0; i < c->num_input_levels(); ++i) {
    for (const auto fmd : *c->inputs(i)) {
      const FileDescriptor& desc = fmd->fd;
      const uint64_t file_number = desc.GetNumber();
      auto fn = TableFileName(c->immutable_cf_options()->cf_paths, file_number,
                              desc.GetPathId());
      compaction_job_info->input_files.push_back(fn);
      compaction_job_info->input_file_infos.push_back(CompactionFileInfo{
          static_cast<int>(i), file_number, fmd->oldest_blob_file_number});
      if (compaction_job_info->table_properties.count(fn) == 0) {
        std::shared_ptr<const TableProperties> tp;
        auto s = current->GetTableProperties(&tp, fmd, &fn);
        if (s.ok()) {
          compaction_job_info->table_properties[fn] = tp;
        }
      }
    }
  }
  for (const auto& newf : c->edit()->GetNewFiles()) {
    const FileMetaData& meta = newf.second;
    const FileDescriptor& desc = meta.fd;
    const uint64_t file_number = desc.GetNumber();
    compaction_job_info->output_files.push_back(TableFileName(
        c->immutable_cf_options()->cf_paths, file_number, desc.GetPathId()));
    compaction_job_info->output_file_infos.push_back(CompactionFileInfo{
        newf.first, file_number, meta.oldest_blob_file_number});
  }
}
#endif

// SuperVersionContext gets created and destructed outside of the lock --
// we use this conveniently to:
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

  // this branch is unlikely to step in
  if (UNLIKELY(sv_context->new_superversion == nullptr)) {
    sv_context->NewSuperVersion();
  }
  cfd->InstallSuperVersion(sv_context, &mutex_, mutable_cf_options);

  // There may be a small data race here. The snapshot tricking bottommost
  // compaction may already be released here. But assuming there will always be
  // newer snapshot created and released frequently, the compaction will be
  // triggered soon anyway.
  bottommost_files_mark_threshold_ = kMaxSequenceNumber;
  for (auto* my_cfd : *versions_->GetColumnFamilySet()) {
    bottommost_files_mark_threshold_ = std::min(
        bottommost_files_mark_threshold_,
        my_cfd->current()->storage_info()->bottommost_files_mark_threshold());
  }

  // Whenever we install new SuperVersion, we might need to issue new flushes or
  // compactions.
  SchedulePendingCompaction(cfd);
  MaybeScheduleFlushOrCompaction();

  // Update max_total_in_memory_state_
  max_total_in_memory_state_ = max_total_in_memory_state_ - old_memtable_size +
                               mutable_cf_options.write_buffer_size *
                                   mutable_cf_options.max_write_buffer_number;
}

// ShouldPurge is called by FindObsoleteFiles when doing a full scan,
// and db mutex (mutex_) should already be held.
// Actually, the current implementation of FindObsoleteFiles with
// full_scan=true can issue I/O requests to obtain list of files in
// directories, e.g. env_->getChildren while holding db mutex.
bool DBImpl::ShouldPurge(uint64_t file_number) const {
  return files_grabbed_for_purge_.find(file_number) ==
             files_grabbed_for_purge_.end() &&
         purge_files_.find(file_number) == purge_files_.end();
}

// MarkAsGrabbedForPurge is called by FindObsoleteFiles, and db mutex
// (mutex_) should already be held.
void DBImpl::MarkAsGrabbedForPurge(uint64_t file_number) {
  files_grabbed_for_purge_.insert(file_number);
}

void DBImpl::SetSnapshotChecker(SnapshotChecker* snapshot_checker) {
  InstrumentedMutexLock l(&mutex_);
  // snapshot_checker_ should only set once. If we need to set it multiple
  // times, we need to make sure the old one is not deleted while it is still
  // using by a compaction job.
  assert(!snapshot_checker_);
  snapshot_checker_.reset(snapshot_checker);
}

void DBImpl::GetSnapshotContext(
    JobContext* job_context, std::vector<SequenceNumber>* snapshot_seqs,
    SequenceNumber* earliest_write_conflict_snapshot,
    SnapshotChecker** snapshot_checker_ptr) {
  mutex_.AssertHeld();
  assert(job_context != nullptr);
  assert(snapshot_seqs != nullptr);
  assert(earliest_write_conflict_snapshot != nullptr);
  assert(snapshot_checker_ptr != nullptr);

  *snapshot_checker_ptr = snapshot_checker_.get();
  if (use_custom_gc_ && *snapshot_checker_ptr == nullptr) {
    *snapshot_checker_ptr = DisableGCSnapshotChecker::Instance();
  }
  if (*snapshot_checker_ptr != nullptr) {
    // If snapshot_checker is used, that means the flush/compaction may
    // contain values not visible to snapshot taken after
    // flush/compaction job starts. Take a snapshot and it will appear
    // in snapshot_seqs and force compaction iterator to consider such
    // snapshots.
    const Snapshot* job_snapshot =
        GetSnapshotImpl(false /*write_conflict_boundary*/, false /*lock*/);
    job_context->job_snapshot.reset(new ManagedSnapshot(this, job_snapshot));
  }
  *snapshot_seqs = snapshots_.GetAll(earliest_write_conflict_snapshot);
}
}  // namespace ROCKSDB_NAMESPACE
