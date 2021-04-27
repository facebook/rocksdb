//  Copyright (c) 2019-present, Rockset, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

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

//
// This does the meat of the compaction. This function runs on the
// compaction tier and the logic closely follows DBImpl::CompactFilesImpl
//
Status DBImpl::doCompact(const CompactionOptions& compact_options,
                         ColumnFamilyData* cfd, Version* version,
                         const std::vector<FilesInOneLevel>& input_file_names,
                         int output_level,
                         const std::vector<SequenceNumber>& existing_snapshots,
                         bool sanitize __attribute__((unused)),
                         JobContext* job_context, LogBuffer* log_buffer,
                         PluggableCompactionResult* result) {
  mutex_.AssertHeld();

  // No need for using the levels specified inside input_file_names
  // because we will find the levels by inspecting the DB version.
  std::unordered_set<uint64_t> input_set;
  for (const auto& onelevel : input_file_names) {
    for (auto file_name : onelevel.files) {
      input_set.insert(TableFileNameToNumber(file_name));
    }
  }

  // get column family info
  ColumnFamilyMetaData cf_meta;
  version->GetColumnFamilyMetaData(&cf_meta);

  if (output_level >= static_cast<int>(cf_meta.levels.size())) {
    return Status::InvalidArgument(
        "Output level for column family " + cf_meta.name +
        " must between [0, " +
        ToString(cf_meta.levels[cf_meta.levels.size() - 1].level) + "].");
  }

  auto max_output_level = cfd->compaction_picker()->MaxOutputLevel();
  if (output_level > max_output_level) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm --- " +
        ToString(max_output_level));
  }

  if (output_level < 0) {
    return Status::InvalidArgument("Output level cannot be negative.");
  }

  if (input_set.empty()) {
    return Status::InvalidArgument(
        "A compaction must contain at least one file.");
  }

  // Validate that these files actually belong to this database. We do
  // not explicitly need to state the level where these files reside
  // because the DB should auto find the level of the specified file#.
  std::vector<CompactionInputFiles> input_files;
  Status s = cfd->compaction_picker()->GetCompactionInputsFromFileNumbers(
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

  // these are dummies and should not be needed
  const int output_path_id = 0;

  // At this point, CompactFiles will be run.
  bg_compaction_scheduled_++;

  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());

  c.reset(cfd->compaction_picker()->CompactFiles(
      compact_options, input_files, output_level, version->storage_info(),
      *cfd->GetLatestMutableCFOptions(), mutable_db_options_, output_path_id));
  assert(c != nullptr);
  c->SetInputVersion(version);

  // do we need to remember the iterator of our insert?
  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem(
      new std::list<uint64_t>::iterator(
          CaptureCurrentFileNumberInPendingOutputs()));

  CompactionJobStats compaction_job_stats;

  auto snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && snapshot_checker == nullptr) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }

  // Verify that we have no existing snapshots. Otherwise the compaction
  // process might erroneously retain keys in the output
  SequenceNumber earliest_write_conflict_snapshot;
  std::vector<SequenceNumber> snapshot_seqs =
      snapshots_.GetAll(&earliest_write_conflict_snapshot);
  assert(earliest_write_conflict_snapshot == kMaxSequenceNumber);
  assert(snapshot_seqs.size() == 0);

  // create compaction job
  CompactionJob compaction_job(
      job_context->job_id, c.get(), immutable_db_options_,
      file_options_for_compaction_, versions_.get(), &shutting_down_,
      preserve_deletes_seqnum_.load(), log_buffer, directories_.GetDbDir(),
      GetDataDir(c->column_family_data(), c->output_path_id()), stats_, &mutex_,
      &error_handler_, existing_snapshots, earliest_write_conflict_snapshot,
      snapshot_checker, table_cache_, &event_logger_,
      c->mutable_cf_options()->paranoid_file_checks,
      c->mutable_cf_options()->report_bg_io_stats, dbname_,
      &compaction_job_stats, Env::Priority::USER, nullptr);

  compaction_job.Prepare();
  mutex_.Unlock();

  // run the compaction job here
  Status status = compaction_job.Run();

  mutex_.Lock();

  // Get the results back.
  compaction_job.RetrieveResultsAndCleanup(result);

  // remove this compaction job from the list of compactions
  c->ReleaseCompactionFiles(s);

#ifndef ROCKSDB_LITE
  // Make sure SstFileManager does its bookkeeping
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm) {
    sfm->OnCompactionCompletion(c.get());
  }
#endif // ROCKSDB_LITE
  
  // remove our iterator
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
    error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
  }

  c.reset();
  bg_compaction_scheduled_--;
  if (bg_compaction_scheduled_ == 0) {
    bg_cv_.SignalAll();
  }
  return status;
}

//
// Entry point to execute a remote compaction request on local db.
//
Status DBImpl::ExecuteRemoteCompactionRequest(
    const PluggableCompactionParam& input, PluggableCompactionResult* result,
    bool sanitize) {
  Status s;
  JobContext job_context(0, true);
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  ColumnFamilyData* cfd = nullptr;
  {
    InstrumentedMutexLock l(&mutex_);
    cfd = versions_->GetColumnFamilySet()->GetColumnFamily(
        input.column_family_name);
    if (!cfd) {
      s = Status::InvalidArgument("Invalid column family: " +
                                  input.column_family_name);
      return s;
    }

    // get a referece on the version
    auto* current = cfd->current();
    current->Ref();

    s = doCompact(input.compact_options, cfd, current, input.input_files,
                  input.output_level, input.existing_snapshots, sanitize,
                  &job_context, &log_buffer, result);

    current->Unref();
  }

  // We could Find and delete obsolete files. But we choose to skip
  // it because we use this api only from the compactor tier and that
  // tier never does more than one compaction after opening a db.
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

  // flush log buffer after releasing mutex
  if (!log_buffer.IsEmpty()) {
    log_buffer.FlushBufferToLog();
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
