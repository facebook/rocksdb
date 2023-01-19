//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl/db_impl.h"

#include <stdint.h>
#ifdef OS_SOLARIS
#include <alloca.h>
#endif

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/arena_wrapped_db_iter.h"
#include "db/builder.h"
#include "db/compaction/compaction_job.h"
#include "db/db_info_dumper.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/flush_job.h"
#include "db/forward_iterator.h"
#include "db/import_column_family_job.h"
#include "db/job_context.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/malloc_stats.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/periodic_task_scheduler.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/transaction_log_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "db/write_callback.h"
#include "env/unique_id_gen.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/in_memory_stats_history.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/persistent_stats_history.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/get_context.h"
#include "table/merging_iterator.h"
#include "table/multiget_context.h"
#include "table/sst_file_dumper.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "trace_replay/trace_replay.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/distributed_mutex.h"
#include "util/hash_containers.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "utilities/trace/replayer_impl.h"

namespace ROCKSDB_NAMESPACE {

const std::string kDefaultColumnFamilyName("default");
const std::string kPersistentStatsColumnFamilyName(
    "___rocksdb_stats_history___");
void DumpRocksDBBuildVersion(Logger* log);

CompressionType GetCompressionFlush(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options) {
  // Compressing memtable flushes might not help unless the sequential load
  // optimization is used for leveled compaction. Otherwise the CPU and
  // latency overhead is not offset by saving much space.
  if (ioptions.compaction_style == kCompactionStyleUniversal &&
      mutable_cf_options.compaction_options_universal
              .compression_size_percent >= 0) {
    return kNoCompression;
  }
  if (mutable_cf_options.compression_per_level.empty()) {
    return mutable_cf_options.compression;
  } else {
    // For leveled compress when min_level_to_compress != 0.
    return mutable_cf_options.compression_per_level[0];
  }
}

namespace {
void DumpSupportInfo(Logger* logger) {
  ROCKS_LOG_HEADER(logger, "Compression algorithms supported:");
  for (auto& compression : OptionsHelper::compression_type_string_map) {
    if (compression.second != kNoCompression &&
        compression.second != kDisableCompressionOption) {
      ROCKS_LOG_HEADER(logger, "\t%s supported: %d", compression.first.c_str(),
                       CompressionTypeSupported(compression.second));
    }
  }
  ROCKS_LOG_HEADER(logger, "Fast CRC32 supported: %s",
                   crc32c::IsFastCrc32Supported().c_str());

  ROCKS_LOG_HEADER(logger, "DMutex implementation: %s", DMutex::kName());
}
}  // namespace

DBImpl::DBImpl(const DBOptions& options, const std::string& dbname,
               const bool seq_per_batch, const bool batch_per_txn,
               bool read_only)
    : dbname_(dbname),
      own_info_log_(options.info_log == nullptr),
      init_logger_creation_s_(),
      initial_db_options_(SanitizeOptions(dbname, options, read_only,
                                          &init_logger_creation_s_)),
      env_(initial_db_options_.env),
      io_tracer_(std::make_shared<IOTracer>()),
      immutable_db_options_(initial_db_options_),
      fs_(immutable_db_options_.fs, io_tracer_),
      mutable_db_options_(initial_db_options_),
      stats_(immutable_db_options_.stats),
#ifdef COERCE_CONTEXT_SWITCH
      mutex_(stats_, immutable_db_options_.clock, DB_MUTEX_WAIT_MICROS, &bg_cv_,
             immutable_db_options_.use_adaptive_mutex),
#else   // COERCE_CONTEXT_SWITCH
      mutex_(stats_, immutable_db_options_.clock, DB_MUTEX_WAIT_MICROS,
             immutable_db_options_.use_adaptive_mutex),
#endif  // COERCE_CONTEXT_SWITCH
      default_cf_handle_(nullptr),
      error_handler_(this, immutable_db_options_, &mutex_),
      event_logger_(immutable_db_options_.info_log.get()),
      max_total_in_memory_state_(0),
      file_options_(BuildDBOptions(immutable_db_options_, mutable_db_options_)),
      file_options_for_compaction_(fs_->OptimizeForCompactionTableWrite(
          file_options_, immutable_db_options_)),
      seq_per_batch_(seq_per_batch),
      batch_per_txn_(batch_per_txn),
      next_job_id_(1),
      shutting_down_(false),
      db_lock_(nullptr),
      manual_compaction_paused_(false),
      bg_cv_(&mutex_),
      logfile_number_(0),
      log_dir_synced_(false),
      log_empty_(true),
      persist_stats_cf_handle_(nullptr),
      log_sync_cv_(&log_write_mutex_),
      total_log_size_(0),
      is_snapshot_supported_(true),
      write_buffer_manager_(immutable_db_options_.write_buffer_manager.get()),
      write_thread_(immutable_db_options_),
      nonmem_write_thread_(immutable_db_options_),
      write_controller_(mutable_db_options_.delayed_write_rate),
      last_batch_group_size_(0),
      unscheduled_flushes_(0),
      unscheduled_compactions_(0),
      bg_bottom_compaction_scheduled_(0),
      bg_compaction_scheduled_(0),
      num_running_compactions_(0),
      bg_flush_scheduled_(0),
      num_running_flushes_(0),
      bg_purge_scheduled_(0),
      disable_delete_obsolete_files_(0),
      pending_purge_obsolete_files_(0),
      delete_obsolete_files_last_run_(immutable_db_options_.clock->NowMicros()),
      last_stats_dump_time_microsec_(0),
      has_unpersisted_data_(false),
      unable_to_release_oldest_log_(false),
      num_running_ingest_file_(0),
#ifndef ROCKSDB_LITE
      wal_manager_(immutable_db_options_, file_options_, io_tracer_,
                   seq_per_batch),
#endif  // ROCKSDB_LITE
      bg_work_paused_(0),
      bg_compaction_paused_(0),
      refitting_level_(false),
      opened_successfully_(false),
#ifndef ROCKSDB_LITE
      periodic_task_scheduler_(),
#endif  // ROCKSDB_LITE
      two_write_queues_(options.two_write_queues),
      manual_wal_flush_(options.manual_wal_flush),
      // last_sequencee_ is always maintained by the main queue that also writes
      // to the memtable. When two_write_queues_ is disabled last seq in
      // memtable is the same as last seq published to the readers. When it is
      // enabled but seq_per_batch_ is disabled, last seq in memtable still
      // indicates last published seq since wal-only writes that go to the 2nd
      // queue do not consume a sequence number. Otherwise writes performed by
      // the 2nd queue could change what is visible to the readers. In this
      // cases, last_seq_same_as_publish_seq_==false, the 2nd queue maintains a
      // separate variable to indicate the last published sequence.
      last_seq_same_as_publish_seq_(
          !(seq_per_batch && options.two_write_queues)),
      // Since seq_per_batch_ is currently set only by WritePreparedTxn which
      // requires a custom gc for compaction, we use that to set use_custom_gc_
      // as well.
      use_custom_gc_(seq_per_batch),
      shutdown_initiated_(false),
      own_sfm_(options.sst_file_manager == nullptr),
      closed_(false),
      atomic_flush_install_cv_(&mutex_),
      blob_callback_(immutable_db_options_.sst_file_manager.get(), &mutex_,
                     &error_handler_, &event_logger_,
                     immutable_db_options_.listeners, dbname_) {
  // !batch_per_trx_ implies seq_per_batch_ because it is only unset for
  // WriteUnprepared, which should use seq_per_batch_.
  assert(batch_per_txn_ || seq_per_batch_);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  // Give a large number for setting of "infinite" open files.
  const int table_cache_size = (mutable_db_options_.max_open_files == -1)
                                   ? TableCache::kInfiniteCapacity
                                   : mutable_db_options_.max_open_files - 10;
  LRUCacheOptions co;
  co.capacity = table_cache_size;
  co.num_shard_bits = immutable_db_options_.table_cache_numshardbits;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  table_cache_ = NewLRUCache(co);
  SetDbSessionId();
  assert(!db_session_id_.empty());

#ifndef ROCKSDB_LITE
  periodic_task_functions_.emplace(PeriodicTaskType::kDumpStats,
                                   [this]() { this->DumpStats(); });
  periodic_task_functions_.emplace(PeriodicTaskType::kPersistStats,
                                   [this]() { this->PersistStats(); });
  periodic_task_functions_.emplace(PeriodicTaskType::kFlushInfoLog,
                                   [this]() { this->FlushInfoLog(); });
  periodic_task_functions_.emplace(
      PeriodicTaskType::kRecordSeqnoTime,
      [this]() { this->RecordSeqnoToTimeMapping(); });
#endif  // ROCKSDB_LITE

  versions_.reset(new VersionSet(dbname_, &immutable_db_options_, file_options_,
                                 table_cache_.get(), write_buffer_manager_,
                                 &write_controller_, &block_cache_tracer_,
                                 io_tracer_, db_id_, db_session_id_));
  column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(versions_->GetColumnFamilySet()));

  DumpRocksDBBuildVersion(immutable_db_options_.info_log.get());
  DumpDBFileSummary(immutable_db_options_, dbname_, db_session_id_);
  immutable_db_options_.Dump(immutable_db_options_.info_log.get());
  mutable_db_options_.Dump(immutable_db_options_.info_log.get());
  DumpSupportInfo(immutable_db_options_.info_log.get());

  max_total_wal_size_.store(mutable_db_options_.max_total_wal_size,
                            std::memory_order_relaxed);
  if (write_buffer_manager_) {
    wbm_stall_.reset(new WBMStallInterface());
  }
}

Status DBImpl::Resume() {
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Resuming DB");

  InstrumentedMutexLock db_mutex(&mutex_);

  if (!error_handler_.IsDBStopped() && !error_handler_.IsBGWorkStopped()) {
    // Nothing to do
    return Status::OK();
  }

  if (error_handler_.IsRecoveryInProgress()) {
    // Don't allow a mix of manual and automatic recovery
    return Status::Busy();
  }

  mutex_.Unlock();
  Status s = error_handler_.RecoverFromBGError(true);
  mutex_.Lock();
  return s;
}

// This function implements the guts of recovery from a background error. It
// is eventually called for both manual as well as automatic recovery. It does
// the following -
// 1. Wait for currently scheduled background flush/compaction to exit, in
//    order to inadvertently causing an error and thinking recovery failed
// 2. Flush memtables if there's any data for all the CFs. This may result
//    another error, which will be saved by error_handler_ and reported later
//    as the recovery status
// 3. Find and delete any obsolete files
// 4. Schedule compactions if needed for all the CFs. This is needed as the
//    flush in the prior step might have been a no-op for some CFs, which
//    means a new super version wouldn't have been installed
Status DBImpl::ResumeImpl(DBRecoverContext context) {
  mutex_.AssertHeld();
  WaitForBackgroundWork();

  Status s;
  if (shutdown_initiated_) {
    // Returning shutdown status to SFM during auto recovery will cause it
    // to abort the recovery and allow the shutdown to progress
    s = Status::ShutdownInProgress();
  }

  if (s.ok()) {
    Status bg_error = error_handler_.GetBGError();
    if (bg_error.severity() > Status::Severity::kHardError) {
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "DB resume requested but failed due to Fatal/Unrecoverable error");
      s = bg_error;
    }
  }

  // Make sure the IO Status stored in version set is set to OK.
  bool file_deletion_disabled = !IsFileDeletionsEnabled();
  if (s.ok()) {
    IOStatus io_s = versions_->io_status();
    if (io_s.IsIOError()) {
      // If resuming from IOError resulted from MANIFEST write, then assert
      // that we must have already set the MANIFEST writer to nullptr during
      // clean-up phase MANIFEST writing. We must have also disabled file
      // deletions.
      assert(!versions_->descriptor_log_);
      assert(file_deletion_disabled);
      // Since we are trying to recover from MANIFEST write error, we need to
      // switch to a new MANIFEST anyway. The old MANIFEST can be corrupted.
      // Therefore, force writing a dummy version edit because we do not know
      // whether there are flush jobs with non-empty data to flush, triggering
      // appends to MANIFEST.
      VersionEdit edit;
      auto cfh =
          static_cast_with_check<ColumnFamilyHandleImpl>(default_cf_handle_);
      assert(cfh);
      ColumnFamilyData* cfd = cfh->cfd();
      const MutableCFOptions& cf_opts = *cfd->GetLatestMutableCFOptions();
      s = versions_->LogAndApply(cfd, cf_opts, &edit, &mutex_,
                                 directories_.GetDbDir());
      if (!s.ok()) {
        io_s = versions_->io_status();
        if (!io_s.ok()) {
          s = error_handler_.SetBGError(io_s,
                                        BackgroundErrorReason::kManifestWrite);
        }
      }
    }
  }

  // We cannot guarantee consistency of the WAL. So force flush Memtables of
  // all the column families
  if (s.ok()) {
    FlushOptions flush_opts;
    // We allow flush to stall write since we are trying to resume from error.
    flush_opts.allow_write_stall = true;
    if (immutable_db_options_.atomic_flush) {
      autovector<ColumnFamilyData*> cfds;
      SelectColumnFamiliesForAtomicFlush(&cfds);
      mutex_.Unlock();
      s = AtomicFlushMemTables(cfds, flush_opts, context.flush_reason);
      mutex_.Lock();
    } else {
      for (auto cfd : versions_->GetRefedColumnFamilySet()) {
        if (cfd->IsDropped()) {
          continue;
        }
        InstrumentedMutexUnlock u(&mutex_);
        s = FlushMemTable(cfd, flush_opts, context.flush_reason);
        if (!s.ok()) {
          break;
        }
      }
    }
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "DB resume requested but failed due to Flush failure [%s]",
                     s.ToString().c_str());
    }
  }

  JobContext job_context(0);
  FindObsoleteFiles(&job_context, true);
  mutex_.Unlock();

  job_context.manifest_file_number = 1;
  if (job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();

  if (s.ok()) {
    assert(versions_->io_status().ok());
    // If we reach here, we should re-enable file deletions if it was disabled
    // during previous error handling.
    if (file_deletion_disabled) {
      // Always return ok
      s = EnableFileDeletions(/*force=*/true);
      if (!s.ok()) {
        ROCKS_LOG_INFO(
            immutable_db_options_.info_log,
            "DB resume requested but could not enable file deletions [%s]",
            s.ToString().c_str());
        assert(false);
      }
    }
  }

  mutex_.Lock();
  if (s.ok()) {
    // This will notify and unblock threads waiting for error recovery to
    // finish. Those previouly waiting threads can now proceed, which may
    // include closing the db.
    s = error_handler_.ClearBGError();
  } else {
    // NOTE: this is needed to pass ASSERT_STATUS_CHECKED
    // in the DBSSTTest.DBWithMaxSpaceAllowedRandomized test.
    // See https://github.com/facebook/rocksdb/pull/7715#issuecomment-754947952
    error_handler_.GetRecoveryError().PermitUncheckedError();
  }

  if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Successfully resumed DB");
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Failed to resume DB [%s]",
                   s.ToString().c_str());
  }

  // Check for shutdown again before scheduling further compactions,
  // since we released and re-acquired the lock above
  if (shutdown_initiated_) {
    s = Status::ShutdownInProgress();
  }
  if (s.ok()) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      SchedulePendingCompaction(cfd);
    }
    MaybeScheduleFlushOrCompaction();
  }

  // Wake up any waiters - in this case, it could be the shutdown thread
  bg_cv_.SignalAll();

  // No need to check BGError again. If something happened, event listener would
  // be notified and the operation causing it would have failed
  return s;
}

void DBImpl::WaitForBackgroundWork() {
  // Wait for background work to finish
  while (bg_bottom_compaction_scheduled_ || bg_compaction_scheduled_ ||
         bg_flush_scheduled_) {
    bg_cv_.Wait();
  }
}

// Will lock the mutex_,  will wait for completion if wait is true
void DBImpl::CancelAllBackgroundWork(bool wait) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Shutdown: canceling all background work");

#ifndef ROCKSDB_LITE
  for (uint8_t task_type = 0;
       task_type < static_cast<uint8_t>(PeriodicTaskType::kMax); task_type++) {
    Status s = periodic_task_scheduler_.Unregister(
        static_cast<PeriodicTaskType>(task_type));
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to unregister periodic task %d, status: %s",
                     task_type, s.ToString().c_str());
    }
  }
#endif  // !ROCKSDB_LITE

  InstrumentedMutexLock l(&mutex_);
  if (!shutting_down_.load(std::memory_order_acquire) &&
      has_unpersisted_data_.load(std::memory_order_relaxed) &&
      !mutable_db_options_.avoid_flush_during_shutdown) {
    if (immutable_db_options_.atomic_flush) {
      autovector<ColumnFamilyData*> cfds;
      SelectColumnFamiliesForAtomicFlush(&cfds);
      mutex_.Unlock();
      Status s =
          AtomicFlushMemTables(cfds, FlushOptions(), FlushReason::kShutDown);
      s.PermitUncheckedError();  //**TODO: What to do on error?
      mutex_.Lock();
    } else {
      for (auto cfd : versions_->GetRefedColumnFamilySet()) {
        if (!cfd->IsDropped() && cfd->initialized() && !cfd->mem()->IsEmpty()) {
          InstrumentedMutexUnlock u(&mutex_);
          Status s = FlushMemTable(cfd, FlushOptions(), FlushReason::kShutDown);
          s.PermitUncheckedError();  //**TODO: What to do on error?
        }
      }
    }
  }

  shutting_down_.store(true, std::memory_order_release);
  bg_cv_.SignalAll();
  if (!wait) {
    return;
  }
  WaitForBackgroundWork();
}

Status DBImpl::MaybeReleaseTimestampedSnapshotsAndCheck() {
  size_t num_snapshots = 0;
  ReleaseTimestampedSnapshotsOlderThan(std::numeric_limits<uint64_t>::max(),
                                       &num_snapshots);

  // If there is unreleased snapshot, fail the close call
  if (num_snapshots > 0) {
    return Status::Aborted("Cannot close DB with unreleased snapshot.");
  }

  return Status::OK();
}

Status DBImpl::CloseHelper() {
  // Guarantee that there is no background error recovery in progress before
  // continuing with the shutdown
  mutex_.Lock();
  shutdown_initiated_ = true;
  error_handler_.CancelErrorRecovery();
  while (error_handler_.IsRecoveryInProgress()) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  // Below check is added as recovery_error_ is not checked and it causes crash
  // in DBSSTTest.DBWithMaxSpaceAllowedWithBlobFiles when space limit is
  // reached.
  error_handler_.GetRecoveryError().PermitUncheckedError();

  // CancelAllBackgroundWork called with false means we just set the shutdown
  // marker. After this we do a variant of the waiting and unschedule work
  // (to consider: moving all the waiting into CancelAllBackgroundWork(true))
  CancelAllBackgroundWork(false);

  // Cancel manual compaction if there's any
  if (HasPendingManualCompaction()) {
    DisableManualCompaction();
  }
  mutex_.Lock();
  // Unschedule all tasks for this DB
  for (uint8_t i = 0; i < static_cast<uint8_t>(TaskType::kCount); i++) {
    env_->UnSchedule(GetTaskTag(i), Env::Priority::BOTTOM);
    env_->UnSchedule(GetTaskTag(i), Env::Priority::LOW);
    env_->UnSchedule(GetTaskTag(i), Env::Priority::HIGH);
  }

  Status ret = Status::OK();

  // Wait for background work to finish
  while (bg_bottom_compaction_scheduled_ || bg_compaction_scheduled_ ||
         bg_flush_scheduled_ || bg_purge_scheduled_ ||
         pending_purge_obsolete_files_ ||
         error_handler_.IsRecoveryInProgress()) {
    TEST_SYNC_POINT("DBImpl::~DBImpl:WaitJob");
    bg_cv_.Wait();
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::CloseHelper:PendingPurgeFinished",
                           &files_grabbed_for_purge_);
  EraseThreadStatusDbInfo();
  flush_scheduler_.Clear();
  trim_history_scheduler_.Clear();

  while (!flush_queue_.empty()) {
    const FlushRequest& flush_req = PopFirstFromFlushQueue();
    for (const auto& iter : flush_req.cfd_to_max_mem_id_to_persist) {
      iter.first->UnrefAndTryDelete();
    }
  }

  while (!compaction_queue_.empty()) {
    auto cfd = PopFirstFromCompactionQueue();
    cfd->UnrefAndTryDelete();
  }

  if (default_cf_handle_ != nullptr || persist_stats_cf_handle_ != nullptr) {
    // we need to delete handle outside of lock because it does its own locking
    mutex_.Unlock();
    if (default_cf_handle_) {
      delete default_cf_handle_;
      default_cf_handle_ = nullptr;
    }
    if (persist_stats_cf_handle_) {
      delete persist_stats_cf_handle_;
      persist_stats_cf_handle_ = nullptr;
    }
    mutex_.Lock();
  }

  // Clean up obsolete files due to SuperVersion release.
  // (1) Need to delete to obsolete files before closing because RepairDB()
  // scans all existing files in the file system and builds manifest file.
  // Keeping obsolete files confuses the repair process.
  // (2) Need to check if we Open()/Recover() the DB successfully before
  // deleting because if VersionSet recover fails (may be due to corrupted
  // manifest file), it is not able to identify live files correctly. As a
  // result, all "live" files can get deleted by accident. However, corrupted
  // manifest is recoverable by RepairDB().
  if (opened_successfully_) {
    JobContext job_context(next_job_id_.fetch_add(1));
    FindObsoleteFiles(&job_context, true);

    mutex_.Unlock();
    // manifest number starting from 2
    job_context.manifest_file_number = 1;
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
    mutex_.Lock();
  }
  {
    InstrumentedMutexLock lock(&log_write_mutex_);
    for (auto l : logs_to_free_) {
      delete l;
    }
    for (auto& log : logs_) {
      uint64_t log_number = log.writer->get_log_number();
      Status s = log.ClearWriter();
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            immutable_db_options_.info_log,
            "Unable to Sync WAL file %s with error -- %s",
            LogFileName(immutable_db_options_.GetWalDir(), log_number).c_str(),
            s.ToString().c_str());
        // Retain the first error
        if (ret.ok()) {
          ret = s;
        }
      }
    }
    logs_.clear();
  }

  // Table cache may have table handles holding blocks from the block cache.
  // We need to release them before the block cache is destroyed. The block
  // cache may be destroyed inside versions_.reset(), when column family data
  // list is destroyed, so leaving handles in table cache after
  // versions_.reset() may cause issues.
  // Here we clean all unreferenced handles in table cache.
  // Now we assume all user queries have finished, so only version set itself
  // can possibly hold the blocks from block cache. After releasing unreferenced
  // handles here, only handles held by version set left and inside
  // versions_.reset(), we will release them. There, we need to make sure every
  // time a handle is released, we erase it from the cache too. By doing that,
  // we can guarantee that after versions_.reset(), table cache is empty
  // so the cache can be safely destroyed.
  table_cache_->EraseUnRefEntries();

  for (auto& txn_entry : recovered_transactions_) {
    delete txn_entry.second;
  }

  // versions need to be destroyed before table_cache since it can hold
  // references to table_cache.
  versions_.reset();
  mutex_.Unlock();
  if (db_lock_ != nullptr) {
    // TODO: Check for unlock error
    env_->UnlockFile(db_lock_).PermitUncheckedError();
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Shutdown complete");
  LogFlush(immutable_db_options_.info_log);

#ifndef ROCKSDB_LITE
  // If the sst_file_manager was allocated by us during DB::Open(), ccall
  // Close() on it before closing the info_log. Otherwise, background thread
  // in SstFileManagerImpl might try to log something
  if (immutable_db_options_.sst_file_manager && own_sfm_) {
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    sfm->Close();
  }
#endif  // ROCKSDB_LITE

  if (immutable_db_options_.info_log && own_info_log_) {
    Status s = immutable_db_options_.info_log->Close();
    if (!s.ok() && !s.IsNotSupported() && ret.ok()) {
      ret = s;
    }
  }

  if (write_buffer_manager_ && wbm_stall_) {
    write_buffer_manager_->RemoveDBFromQueue(wbm_stall_.get());
  }

  IOStatus io_s = directories_.Close(IOOptions(), nullptr /* dbg */);
  if (!io_s.ok()) {
    ret = io_s;
  }
  if (ret.IsAborted()) {
    // Reserve IsAborted() error for those where users didn't release
    // certain resource and they can release them and come back and
    // retry. In this case, we wrap this exception to something else.
    return Status::Incomplete(ret.ToString());
  }

  return ret;
}

Status DBImpl::CloseImpl() { return CloseHelper(); }

DBImpl::~DBImpl() {
  // TODO: remove this.
  init_logger_creation_s_.PermitUncheckedError();

  InstrumentedMutexLock closing_lock_guard(&closing_mutex_);
  if (closed_) {
    return;
  }

  closed_ = true;

  {
    const Status s = MaybeReleaseTimestampedSnapshotsAndCheck();
    s.PermitUncheckedError();
  }

  closing_status_ = CloseImpl();
  closing_status_.PermitUncheckedError();
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || immutable_db_options_.paranoid_checks) {
    // No change needed
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Ignoring error %s",
                   s->ToString().c_str());
    *s = Status::OK();
  }
}

const Status DBImpl::CreateArchivalDirectory() {
  if (immutable_db_options_.WAL_ttl_seconds > 0 ||
      immutable_db_options_.WAL_size_limit_MB > 0) {
    std::string archivalPath =
        ArchivalDirectory(immutable_db_options_.GetWalDir());
    return env_->CreateDirIfMissing(archivalPath);
  }
  return Status::OK();
}

void DBImpl::PrintStatistics() {
  auto dbstats = immutable_db_options_.stats;
  if (dbstats) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "STATISTICS:\n %s",
                   dbstats->ToString().c_str());
  }
}

Status DBImpl::StartPeriodicTaskScheduler() {
#ifndef ROCKSDB_LITE

#ifndef NDEBUG
  // It only used by test to disable scheduler
  bool disable_scheduler = false;
  TEST_SYNC_POINT_CALLBACK(
      "DBImpl::StartPeriodicTaskScheduler:DisableScheduler",
      &disable_scheduler);
  if (disable_scheduler) {
    return Status::OK();
  }

  {
    InstrumentedMutexLock l(&mutex_);
    TEST_SYNC_POINT_CALLBACK("DBImpl::StartPeriodicTaskScheduler:Init",
                             &periodic_task_scheduler_);
  }

#endif  // !NDEBUG
  if (mutable_db_options_.stats_dump_period_sec > 0) {
    Status s = periodic_task_scheduler_.Register(
        PeriodicTaskType::kDumpStats,
        periodic_task_functions_.at(PeriodicTaskType::kDumpStats),
        mutable_db_options_.stats_dump_period_sec);
    if (!s.ok()) {
      return s;
    }
  }
  if (mutable_db_options_.stats_persist_period_sec > 0) {
    Status s = periodic_task_scheduler_.Register(
        PeriodicTaskType::kPersistStats,
        periodic_task_functions_.at(PeriodicTaskType::kPersistStats),
        mutable_db_options_.stats_persist_period_sec);
    if (!s.ok()) {
      return s;
    }
  }

  Status s = periodic_task_scheduler_.Register(
      PeriodicTaskType::kFlushInfoLog,
      periodic_task_functions_.at(PeriodicTaskType::kFlushInfoLog));

  return s;
#else
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

Status DBImpl::RegisterRecordSeqnoTimeWorker() {
#ifndef ROCKSDB_LITE
  uint64_t min_time_duration = std::numeric_limits<uint64_t>::max();
  uint64_t max_time_duration = std::numeric_limits<uint64_t>::min();
  {
    InstrumentedMutexLock l(&mutex_);

    for (auto cfd : *versions_->GetColumnFamilySet()) {
      // preserve time is the max of 2 options.
      uint64_t preserve_time_duration =
          std::max(cfd->ioptions()->preserve_internal_time_seconds,
                   cfd->ioptions()->preclude_last_level_data_seconds);
      if (!cfd->IsDropped() && preserve_time_duration > 0) {
        min_time_duration = std::min(preserve_time_duration, min_time_duration);
        max_time_duration = std::max(preserve_time_duration, max_time_duration);
      }
    }
    if (min_time_duration == std::numeric_limits<uint64_t>::max()) {
      seqno_time_mapping_.Resize(0, 0);
    } else {
      seqno_time_mapping_.Resize(min_time_duration, max_time_duration);
    }
  }

  uint64_t seqno_time_cadence = 0;
  if (min_time_duration != std::numeric_limits<uint64_t>::max()) {
    // round up to 1 when the time_duration is smaller than
    // kMaxSeqnoTimePairsPerCF
    seqno_time_cadence =
        (min_time_duration + SeqnoToTimeMapping::kMaxSeqnoTimePairsPerCF - 1) /
        SeqnoToTimeMapping::kMaxSeqnoTimePairsPerCF;
  }

  Status s;
  if (seqno_time_cadence == 0) {
    s = periodic_task_scheduler_.Unregister(PeriodicTaskType::kRecordSeqnoTime);
  } else {
    s = periodic_task_scheduler_.Register(
        PeriodicTaskType::kRecordSeqnoTime,
        periodic_task_functions_.at(PeriodicTaskType::kRecordSeqnoTime),
        seqno_time_cadence);
  }

  return s;
#else
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

// esitmate the total size of stats_history_
size_t DBImpl::EstimateInMemoryStatsHistorySize() const {
  size_t size_total =
      sizeof(std::map<uint64_t, std::map<std::string, uint64_t>>);
  if (stats_history_.size() == 0) return size_total;
  size_t size_per_slice =
      sizeof(uint64_t) + sizeof(std::map<std::string, uint64_t>);
  // non-empty map, stats_history_.begin() guaranteed to exist
  for (const auto& pairs : stats_history_.begin()->second) {
    size_per_slice +=
        pairs.first.capacity() + sizeof(pairs.first) + sizeof(pairs.second);
  }
  size_total = size_per_slice * stats_history_.size();
  return size_total;
}

void DBImpl::PersistStats() {
  TEST_SYNC_POINT("DBImpl::PersistStats:Entry");
#ifndef ROCKSDB_LITE
  if (shutdown_initiated_) {
    return;
  }
  TEST_SYNC_POINT("DBImpl::PersistStats:StartRunning");
  uint64_t now_seconds =
      immutable_db_options_.clock->NowMicros() / kMicrosInSecond;

  Statistics* statistics = immutable_db_options_.stats;
  if (!statistics) {
    return;
  }
  size_t stats_history_size_limit = 0;
  {
    InstrumentedMutexLock l(&mutex_);
    stats_history_size_limit = mutable_db_options_.stats_history_buffer_size;
  }

  std::map<std::string, uint64_t> stats_map;
  if (!statistics->getTickerMap(&stats_map)) {
    return;
  }
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "------- PERSISTING STATS -------");

  if (immutable_db_options_.persist_stats_to_disk) {
    WriteBatch batch;
    Status s = Status::OK();
    if (stats_slice_initialized_) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Reading %" ROCKSDB_PRIszt " stats from statistics\n",
                     stats_slice_.size());
      for (const auto& stat : stats_map) {
        if (s.ok()) {
          char key[100];
          int length =
              EncodePersistentStatsKey(now_seconds, stat.first, 100, key);
          // calculate the delta from last time
          if (stats_slice_.find(stat.first) != stats_slice_.end()) {
            uint64_t delta = stat.second - stats_slice_[stat.first];
            s = batch.Put(persist_stats_cf_handle_,
                          Slice(key, std::min(100, length)),
                          std::to_string(delta));
          }
        }
      }
    }
    stats_slice_initialized_ = true;
    std::swap(stats_slice_, stats_map);
    if (s.ok()) {
      WriteOptions wo;
      wo.low_pri = true;
      wo.no_slowdown = true;
      wo.sync = false;
      s = Write(wo, &batch);
    }
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Writing to persistent stats CF failed -- %s",
                     s.ToString().c_str());
    } else {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Writing %" ROCKSDB_PRIszt " stats with timestamp %" PRIu64
                     " to persistent stats CF succeeded",
                     stats_slice_.size(), now_seconds);
    }
    // TODO(Zhongyi): add purging for persisted data
  } else {
    InstrumentedMutexLock l(&stats_history_mutex_);
    // calculate the delta from last time
    if (stats_slice_initialized_) {
      std::map<std::string, uint64_t> stats_delta;
      for (const auto& stat : stats_map) {
        if (stats_slice_.find(stat.first) != stats_slice_.end()) {
          stats_delta[stat.first] = stat.second - stats_slice_[stat.first];
        }
      }
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Storing %" ROCKSDB_PRIszt " stats with timestamp %" PRIu64
                     " to in-memory stats history",
                     stats_slice_.size(), now_seconds);
      stats_history_[now_seconds] = stats_delta;
    }
    stats_slice_initialized_ = true;
    std::swap(stats_slice_, stats_map);
    TEST_SYNC_POINT("DBImpl::PersistStats:StatsCopied");

    // delete older stats snapshots to control memory consumption
    size_t stats_history_size = EstimateInMemoryStatsHistorySize();
    bool purge_needed = stats_history_size > stats_history_size_limit;
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[Pre-GC] In-memory stats history size: %" ROCKSDB_PRIszt
                   " bytes, slice count: %" ROCKSDB_PRIszt,
                   stats_history_size, stats_history_.size());
    while (purge_needed && !stats_history_.empty()) {
      stats_history_.erase(stats_history_.begin());
      purge_needed =
          EstimateInMemoryStatsHistorySize() > stats_history_size_limit;
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[Post-GC] In-memory stats history size: %" ROCKSDB_PRIszt
                   " bytes, slice count: %" ROCKSDB_PRIszt,
                   stats_history_size, stats_history_.size());
  }
  TEST_SYNC_POINT("DBImpl::PersistStats:End");
#endif  // !ROCKSDB_LITE
}

bool DBImpl::FindStatsByTime(uint64_t start_time, uint64_t end_time,
                             uint64_t* new_time,
                             std::map<std::string, uint64_t>* stats_map) {
  assert(new_time);
  assert(stats_map);
  if (!new_time || !stats_map) return false;
  // lock when search for start_time
  {
    InstrumentedMutexLock l(&stats_history_mutex_);
    auto it = stats_history_.lower_bound(start_time);
    if (it != stats_history_.end() && it->first < end_time) {
      // make a copy for timestamp and stats_map
      *new_time = it->first;
      *stats_map = it->second;
      return true;
    } else {
      return false;
    }
  }
}

Status DBImpl::GetStatsHistory(
    uint64_t start_time, uint64_t end_time,
    std::unique_ptr<StatsHistoryIterator>* stats_iterator) {
  if (!stats_iterator) {
    return Status::InvalidArgument("stats_iterator not preallocated.");
  }
  if (immutable_db_options_.persist_stats_to_disk) {
    stats_iterator->reset(
        new PersistentStatsHistoryIterator(start_time, end_time, this));
  } else {
    stats_iterator->reset(
        new InMemoryStatsHistoryIterator(start_time, end_time, this));
  }
  return (*stats_iterator)->status();
}

void DBImpl::DumpStats() {
  TEST_SYNC_POINT("DBImpl::DumpStats:1");
#ifndef ROCKSDB_LITE
  std::string stats;
  if (shutdown_initiated_) {
    return;
  }

  // Also probe block cache(s) for problems, dump to info log
  UnorderedSet<Cache*> probed_caches;
  TEST_SYNC_POINT("DBImpl::DumpStats:StartRunning");
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto cfd : versions_->GetRefedColumnFamilySet()) {
      if (!cfd->initialized()) {
        continue;
      }

      // Release DB mutex for gathering cache entry stats. Pass over all
      // column families for this first so that other stats are dumped
      // near-atomically.
      InstrumentedMutexUnlock u(&mutex_);
      cfd->internal_stats()->CollectCacheEntryStats(/*foreground=*/false);

      // Probe block cache for problems (if not already via another CF)
      if (immutable_db_options_.info_log) {
        auto* table_factory = cfd->ioptions()->table_factory.get();
        assert(table_factory != nullptr);
        Cache* cache =
            table_factory->GetOptions<Cache>(TableFactory::kBlockCacheOpts());
        if (cache && probed_caches.insert(cache).second) {
          cache->ReportProblems(immutable_db_options_.info_log);
        }
      }
    }

    const std::string* property = &DB::Properties::kDBStats;
    const DBPropertyInfo* property_info = GetPropertyInfo(*property);
    assert(property_info != nullptr);
    assert(!property_info->need_out_of_mutex);
    default_cf_internal_stats_->GetStringProperty(*property_info, *property,
                                                  &stats);

    property = &InternalStats::kPeriodicCFStats;
    property_info = GetPropertyInfo(*property);
    assert(property_info != nullptr);
    assert(!property_info->need_out_of_mutex);
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->initialized()) {
        cfd->internal_stats()->GetStringProperty(*property_info, *property,
                                                 &stats);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::DumpStats:2");
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "------- DUMPING STATS -------");
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s", stats.c_str());
  if (immutable_db_options_.dump_malloc_stats) {
    stats.clear();
    DumpMallocStats(&stats);
    if (!stats.empty()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "------- Malloc STATS -------");
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s", stats.c_str());
    }
  }
#endif  // !ROCKSDB_LITE

  PrintStatistics();
}

// Periodically flush info log out of application buffer at a low frequency.
// This improves debuggability in case of RocksDB hanging since it ensures the
// log messages leading up to the hang will eventually become visible in the
// log.
void DBImpl::FlushInfoLog() {
  if (shutdown_initiated_) {
    return;
  }
  TEST_SYNC_POINT("DBImpl::FlushInfoLog:StartRunning");
  LogFlush(immutable_db_options_.info_log);
}

Status DBImpl::TablesRangeTombstoneSummary(ColumnFamilyHandle* column_family,
                                           int max_entries_to_print,
                                           std::string* out_str) {
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();

  SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
  Version* version = super_version->current;

  Status s =
      version->TablesRangeTombstoneSummary(max_entries_to_print, out_str);

  CleanupSuperVersion(super_version);
  return s;
}

void DBImpl::ScheduleBgLogWriterClose(JobContext* job_context) {
  mutex_.AssertHeld();
  if (!job_context->logs_to_free.empty()) {
    for (auto l : job_context->logs_to_free) {
      AddToLogsToFreeQueue(l);
    }
    job_context->logs_to_free.clear();
  }
}

FSDirectory* DBImpl::GetDataDir(ColumnFamilyData* cfd, size_t path_id) const {
  assert(cfd);
  FSDirectory* ret_dir = cfd->GetDataDir(path_id);
  if (ret_dir == nullptr) {
    return directories_.GetDataDir(path_id);
  }
  return ret_dir;
}

Status DBImpl::SetOptions(
    ColumnFamilyHandle* column_family,
    const std::unordered_map<std::string, std::string>& options_map) {
#ifdef ROCKSDB_LITE
  (void)column_family;
  (void)options_map;
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  if (options_map.empty()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "SetOptions() on column family [%s], empty input",
                   cfd->GetName().c_str());
    return Status::InvalidArgument("empty input");
  }

  MutableCFOptions new_options;
  Status s;
  Status persist_options_status;
  SuperVersionContext sv_context(/* create_superversion */ true);
  {
    auto db_options = GetDBOptions();
    InstrumentedMutexLock l(&mutex_);
    s = cfd->SetOptions(db_options, options_map);
    if (s.ok()) {
      new_options = *cfd->GetLatestMutableCFOptions();
      // Append new version to recompute compaction score.
      VersionEdit dummy_edit;
      s = versions_->LogAndApply(cfd, new_options, &dummy_edit, &mutex_,
                                 directories_.GetDbDir());
      // Trigger possible flush/compactions. This has to be before we persist
      // options to file, otherwise there will be a deadlock with writer
      // thread.
      InstallSuperVersionAndScheduleWork(cfd, &sv_context, new_options);

      persist_options_status = WriteOptionsFile(
          false /*need_mutex_lock*/, true /*need_enter_write_thread*/);
      bg_cv_.SignalAll();
    }
  }
  sv_context.Clean();

  ROCKS_LOG_INFO(
      immutable_db_options_.info_log,
      "SetOptions() on column family [%s], inputs:", cfd->GetName().c_str());
  for (const auto& o : options_map) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s: %s\n", o.first.c_str(),
                   o.second.c_str());
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] SetOptions() succeeded", cfd->GetName().c_str());
    new_options.Dump(immutable_db_options_.info_log.get());
    if (!persist_options_status.ok()) {
      // NOTE: WriteOptionsFile already logs on failure
      s = persist_options_status;
    }
  } else {
    persist_options_status.PermitUncheckedError();  // less important
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "[%s] SetOptions() failed",
                   cfd->GetName().c_str());
  }
  LogFlush(immutable_db_options_.info_log);
  return s;
#endif  // ROCKSDB_LITE
}

Status DBImpl::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
#ifdef ROCKSDB_LITE
  (void)options_map;
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  if (options_map.empty()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "SetDBOptions(), empty input.");
    return Status::InvalidArgument("empty input");
  }

  MutableDBOptions new_options;
  Status s;
  Status persist_options_status = Status::OK();
  bool wal_changed = false;
  WriteContext write_context;
  {
    InstrumentedMutexLock l(&mutex_);
    s = GetMutableDBOptionsFromStrings(mutable_db_options_, options_map,
                                       &new_options);

    if (new_options.bytes_per_sync == 0) {
      new_options.bytes_per_sync = 1024 * 1024;
    }

    if (MutableDBOptionsAreEqual(mutable_db_options_, new_options)) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "SetDBOptions(), input option value is not changed, "
                     "skipping updating.");
      persist_options_status.PermitUncheckedError();
      return s;
    }

    DBOptions new_db_options =
        BuildDBOptions(immutable_db_options_, new_options);
    if (s.ok()) {
      s = ValidateOptions(new_db_options);
    }
    if (s.ok()) {
      for (auto c : *versions_->GetColumnFamilySet()) {
        if (!c->IsDropped()) {
          auto cf_options = c->GetLatestCFOptions();
          s = ColumnFamilyData::ValidateOptions(new_db_options, cf_options);
          if (!s.ok()) {
            break;
          }
        }
      }
    }
    if (s.ok()) {
      const BGJobLimits current_bg_job_limits =
          GetBGJobLimits(mutable_db_options_.max_background_flushes,
                         mutable_db_options_.max_background_compactions,
                         mutable_db_options_.max_background_jobs,
                         /* parallelize_compactions */ true);
      const BGJobLimits new_bg_job_limits = GetBGJobLimits(
          new_options.max_background_flushes,
          new_options.max_background_compactions,
          new_options.max_background_jobs, /* parallelize_compactions */ true);

      const bool max_flushes_increased =
          new_bg_job_limits.max_flushes > current_bg_job_limits.max_flushes;
      const bool max_compactions_increased =
          new_bg_job_limits.max_compactions >
          current_bg_job_limits.max_compactions;

      if (max_flushes_increased || max_compactions_increased) {
        if (max_flushes_increased) {
          env_->IncBackgroundThreadsIfNeeded(new_bg_job_limits.max_flushes,
                                             Env::Priority::HIGH);
        }

        if (max_compactions_increased) {
          env_->IncBackgroundThreadsIfNeeded(new_bg_job_limits.max_compactions,
                                             Env::Priority::LOW);
        }

        MaybeScheduleFlushOrCompaction();
      }

      mutex_.Unlock();
      if (new_options.stats_dump_period_sec == 0) {
        s = periodic_task_scheduler_.Unregister(PeriodicTaskType::kDumpStats);
      } else {
        s = periodic_task_scheduler_.Register(
            PeriodicTaskType::kDumpStats,
            periodic_task_functions_.at(PeriodicTaskType::kDumpStats),
            new_options.stats_dump_period_sec);
      }
      if (new_options.max_total_wal_size !=
          mutable_db_options_.max_total_wal_size) {
        max_total_wal_size_.store(new_options.max_total_wal_size,
                                  std::memory_order_release);
      }
      if (s.ok()) {
        if (new_options.stats_persist_period_sec == 0) {
          s = periodic_task_scheduler_.Unregister(
              PeriodicTaskType::kPersistStats);
        } else {
          s = periodic_task_scheduler_.Register(
              PeriodicTaskType::kPersistStats,
              periodic_task_functions_.at(PeriodicTaskType::kPersistStats),
              new_options.stats_persist_period_sec);
        }
      }
      mutex_.Lock();
      if (!s.ok()) {
        return s;
      }

      write_controller_.set_max_delayed_write_rate(
          new_options.delayed_write_rate);
      table_cache_.get()->SetCapacity(new_options.max_open_files == -1
                                          ? TableCache::kInfiniteCapacity
                                          : new_options.max_open_files - 10);
      wal_changed = mutable_db_options_.wal_bytes_per_sync !=
                    new_options.wal_bytes_per_sync;
      mutable_db_options_ = new_options;
      file_options_for_compaction_ = FileOptions(new_db_options);
      file_options_for_compaction_ = fs_->OptimizeForCompactionTableWrite(
          file_options_for_compaction_, immutable_db_options_);
      versions_->ChangeFileOptions(mutable_db_options_);
      // TODO(xiez): clarify why apply optimize for read to write options
      file_options_for_compaction_ = fs_->OptimizeForCompactionTableRead(
          file_options_for_compaction_, immutable_db_options_);
      file_options_for_compaction_.compaction_readahead_size =
          mutable_db_options_.compaction_readahead_size;
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      if (total_log_size_ > GetMaxTotalWalSize() || wal_changed) {
        Status purge_wal_status = SwitchWAL(&write_context);
        if (!purge_wal_status.ok()) {
          ROCKS_LOG_WARN(immutable_db_options_.info_log,
                         "Unable to purge WAL files in SetDBOptions() -- %s",
                         purge_wal_status.ToString().c_str());
        }
      }
      persist_options_status = WriteOptionsFile(
          false /*need_mutex_lock*/, false /*need_enter_write_thread*/);
      write_thread_.ExitUnbatched(&w);
    } else {
      // To get here, we must have had invalid options and will not attempt to
      // persist the options, which means the status is "OK/Uninitialized.
      persist_options_status.PermitUncheckedError();
    }
  }
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "SetDBOptions(), inputs:");
  for (const auto& o : options_map) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s: %s\n", o.first.c_str(),
                   o.second.c_str());
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "SetDBOptions() succeeded");
    new_options.Dump(immutable_db_options_.info_log.get());
    if (!persist_options_status.ok()) {
      if (immutable_db_options_.fail_if_options_file_error) {
        s = Status::IOError(
            "SetDBOptions() succeeded, but unable to persist options",
            persist_options_status.ToString());
      }
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Unable to persist options in SetDBOptions() -- %s",
                     persist_options_status.ToString().c_str());
    }
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "SetDBOptions failed");
  }
  LogFlush(immutable_db_options_.info_log);
  return s;
#endif  // ROCKSDB_LITE
}

// return the same level if it cannot be moved
int DBImpl::FindMinimumEmptyLevelFitting(
    ColumnFamilyData* cfd, const MutableCFOptions& /*mutable_cf_options*/,
    int level) {
  mutex_.AssertHeld();
  const auto* vstorage = cfd->current()->storage_info();
  int minimum_level = level;
  for (int i = level - 1; i > 0; --i) {
    // stop if level i is not empty
    if (vstorage->NumLevelFiles(i) > 0) break;
    // stop if level i is too small (cannot fit the level files)
    if (vstorage->MaxBytesForLevel(i) < vstorage->NumLevelBytes(level)) {
      break;
    }

    minimum_level = i;
  }
  return minimum_level;
}

Status DBImpl::FlushWAL(bool sync) {
  if (manual_wal_flush_) {
    IOStatus io_s;
    {
      // We need to lock log_write_mutex_ since logs_ might change concurrently
      InstrumentedMutexLock wl(&log_write_mutex_);
      log::Writer* cur_log_writer = logs_.back().writer;
      io_s = cur_log_writer->WriteBuffer();
    }
    if (!io_s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log, "WAL flush error %s",
                      io_s.ToString().c_str());
      // In case there is a fs error we should set it globally to prevent the
      // future writes
      IOStatusCheck(io_s);
      // whether sync or not, we should abort the rest of function upon error
      return static_cast<Status>(io_s);
    }
    if (!sync) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "FlushWAL sync=false");
      return static_cast<Status>(io_s);
    }
  }
  if (!sync) {
    return Status::OK();
  }
  // sync = true
  ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "FlushWAL sync=true");
  return SyncWAL();
}

bool DBImpl::WALBufferIsEmpty(bool lock) {
  if (lock) {
    log_write_mutex_.Lock();
  }
  log::Writer* cur_log_writer = logs_.back().writer;
  auto res = cur_log_writer->BufferIsEmpty();
  if (lock) {
    log_write_mutex_.Unlock();
  }
  return res;
}

Status DBImpl::SyncWAL() {
  TEST_SYNC_POINT("DBImpl::SyncWAL:Begin");
  autovector<log::Writer*, 1> logs_to_sync;
  bool need_log_dir_sync;
  uint64_t current_log_number;

  {
    InstrumentedMutexLock l(&log_write_mutex_);
    assert(!logs_.empty());

    // This SyncWAL() call only cares about logs up to this number.
    current_log_number = logfile_number_;

    while (logs_.front().number <= current_log_number &&
           logs_.front().IsSyncing()) {
      log_sync_cv_.Wait();
    }
    // First check that logs are safe to sync in background.
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      if (!it->writer->file()->writable_file()->IsSyncThreadSafe()) {
        return Status::NotSupported(
            "SyncWAL() is not supported for this implementation of WAL file",
            immutable_db_options_.allow_mmap_writes
                ? "try setting Options::allow_mmap_writes to false"
                : Slice());
      }
    }
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      auto& log = *it;
      log.PrepareForSync();
      logs_to_sync.push_back(log.writer);
    }

    need_log_dir_sync = !log_dir_synced_;
  }

  TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:1");
  RecordTick(stats_, WAL_FILE_SYNCED);
  Status status;
  IOStatus io_s;
  for (log::Writer* log : logs_to_sync) {
    io_s = log->file()->SyncWithoutFlush(immutable_db_options_.use_fsync);
    if (!io_s.ok()) {
      status = io_s;
      break;
    }
  }
  if (!io_s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log, "WAL Sync error %s",
                    io_s.ToString().c_str());
    // In case there is a fs error we should set it globally to prevent the
    // future writes
    IOStatusCheck(io_s);
  }
  if (status.ok() && need_log_dir_sync) {
    status = directories_.GetWalDir()->FsyncWithDirOptions(
        IOOptions(), nullptr,
        DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
  }
  TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:2");

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:1");
  VersionEdit synced_wals;
  {
    InstrumentedMutexLock l(&log_write_mutex_);
    if (status.ok()) {
      MarkLogsSynced(current_log_number, need_log_dir_sync, &synced_wals);
    } else {
      MarkLogsNotSynced(current_log_number);
    }
  }
  if (status.ok() && synced_wals.IsWalAddition()) {
    InstrumentedMutexLock l(&mutex_);
    status = ApplyWALToManifest(&synced_wals);
  }

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:2");

  return status;
}

Status DBImpl::ApplyWALToManifest(VersionEdit* synced_wals) {
  // not empty, write to MANIFEST.
  mutex_.AssertHeld();
  Status status = versions_->LogAndApplyToDefaultColumnFamily(
      synced_wals, &mutex_, directories_.GetDbDir());
  if (!status.ok() && versions_->io_status().IsIOError()) {
    status = error_handler_.SetBGError(versions_->io_status(),
                                       BackgroundErrorReason::kManifestWrite);
  }
  return status;
}

Status DBImpl::LockWAL() {
  {
    InstrumentedMutexLock lock(&mutex_);
    WriteThread::Writer w;
    write_thread_.EnterUnbatched(&w, &mutex_);
    WriteThread::Writer nonmem_w;
    if (two_write_queues_) {
      nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
    }

    lock_wal_write_token_ = write_controller_.GetStopToken();

    if (two_write_queues_) {
      nonmem_write_thread_.ExitUnbatched(&nonmem_w);
    }
    write_thread_.ExitUnbatched(&w);
  }
  return FlushWAL(/*sync=*/false);
}

Status DBImpl::UnlockWAL() {
  {
    InstrumentedMutexLock lock(&mutex_);
    lock_wal_write_token_.reset();
  }
  bg_cv_.SignalAll();
  return Status::OK();
}

void DBImpl::MarkLogsSynced(uint64_t up_to, bool synced_dir,
                            VersionEdit* synced_wals) {
  log_write_mutex_.AssertHeld();
  if (synced_dir && logfile_number_ == up_to) {
    log_dir_synced_ = true;
  }
  for (auto it = logs_.begin(); it != logs_.end() && it->number <= up_to;) {
    auto& wal = *it;
    assert(wal.IsSyncing());

    if (wal.number < logs_.back().number) {
      // Inactive WAL
      if (immutable_db_options_.track_and_verify_wals_in_manifest &&
          wal.GetPreSyncSize() > 0) {
        synced_wals->AddWal(wal.number, WalMetadata(wal.GetPreSyncSize()));
      }
      if (wal.GetPreSyncSize() == wal.writer->file()->GetFlushedSize()) {
        // Fully synced
        logs_to_free_.push_back(wal.ReleaseWriter());
        it = logs_.erase(it);
      } else {
        assert(wal.GetPreSyncSize() < wal.writer->file()->GetFlushedSize());
        wal.FinishSync();
        ++it;
      }
    } else {
      assert(wal.number == logs_.back().number);
      // Active WAL
      wal.FinishSync();
      ++it;
    }
  }
  log_sync_cv_.SignalAll();
}

void DBImpl::MarkLogsNotSynced(uint64_t up_to) {
  log_write_mutex_.AssertHeld();
  for (auto it = logs_.begin(); it != logs_.end() && it->number <= up_to;
       ++it) {
    auto& wal = *it;
    wal.FinishSync();
  }
  log_sync_cv_.SignalAll();
}

SequenceNumber DBImpl::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

void DBImpl::SetLastPublishedSequence(SequenceNumber seq) {
  versions_->SetLastPublishedSequence(seq);
}

Status DBImpl::GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                   std::string* ts_low) {
  if (ts_low == nullptr) {
    return Status::InvalidArgument("ts_low is nullptr");
  }
  ColumnFamilyData* cfd = nullptr;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    assert(cfh != nullptr);
    cfd = cfh->cfd();
  }
  assert(cfd != nullptr && cfd->user_comparator() != nullptr);
  if (cfd->user_comparator()->timestamp_size() == 0) {
    return Status::InvalidArgument(
        "Timestamp is not enabled in this column family");
  }
  InstrumentedMutexLock l(&mutex_);
  *ts_low = cfd->GetFullHistoryTsLow();
  assert(cfd->user_comparator()->timestamp_size() == ts_low->size());
  return Status::OK();
}

InternalIterator* DBImpl::NewInternalIterator(const ReadOptions& read_options,
                                              Arena* arena,
                                              SequenceNumber sequence,
                                              ColumnFamilyHandle* column_family,
                                              bool allow_unprepared_value) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    cfd = cfh->cfd();
  }

  mutex_.Lock();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  mutex_.Unlock();
  return NewInternalIterator(read_options, cfd, super_version, arena, sequence,
                             allow_unprepared_value);
}

void DBImpl::SchedulePurge() {
  mutex_.AssertHeld();
  assert(opened_successfully_);

  // Purge operations are put into High priority queue
  bg_purge_scheduled_++;
  env_->Schedule(&DBImpl::BGWorkPurge, this, Env::Priority::HIGH, nullptr);
}

void DBImpl::BackgroundCallPurge() {
  mutex_.Lock();

  while (!logs_to_free_queue_.empty()) {
    assert(!logs_to_free_queue_.empty());
    log::Writer* log_writer = *(logs_to_free_queue_.begin());
    logs_to_free_queue_.pop_front();
    mutex_.Unlock();
    delete log_writer;
    mutex_.Lock();
  }
  while (!superversions_to_free_queue_.empty()) {
    assert(!superversions_to_free_queue_.empty());
    SuperVersion* sv = superversions_to_free_queue_.front();
    superversions_to_free_queue_.pop_front();
    mutex_.Unlock();
    delete sv;
    mutex_.Lock();
  }

  assert(bg_purge_scheduled_ > 0);

  // Can't use iterator to go over purge_files_ because inside the loop we're
  // unlocking the mutex that protects purge_files_.
  while (!purge_files_.empty()) {
    auto it = purge_files_.begin();
    // Need to make a copy of the PurgeFilesInfo before unlocking the mutex.
    PurgeFileInfo purge_file = it->second;

    const std::string& fname = purge_file.fname;
    const std::string& dir_to_sync = purge_file.dir_to_sync;
    FileType type = purge_file.type;
    uint64_t number = purge_file.number;
    int job_id = purge_file.job_id;

    purge_files_.erase(it);

    mutex_.Unlock();
    DeleteObsoleteFileImpl(job_id, fname, dir_to_sync, type, number);
    mutex_.Lock();
  }

  bg_purge_scheduled_--;

  bg_cv_.SignalAll();
  // IMPORTANT:there should be no code after calling SignalAll. This call may
  // signal the DB destructor that it's OK to proceed with destruction. In
  // that case, all DB variables will be dealloacated and referencing them
  // will cause trouble.
  mutex_.Unlock();
}

namespace {

// A `SuperVersionHandle` holds a non-null `SuperVersion*` pointing at a
// `SuperVersion` referenced once for this object. It also contains the state
// needed to clean up the `SuperVersion` reference from outside of `DBImpl`
// using `CleanupSuperVersionHandle()`.
struct SuperVersionHandle {
  // `_super_version` must be non-nullptr and `Ref()`'d once as long as the
  // `SuperVersionHandle` may use it.
  SuperVersionHandle(DBImpl* _db, InstrumentedMutex* _mu,
                     SuperVersion* _super_version, bool _background_purge)
      : db(_db),
        mu(_mu),
        super_version(_super_version),
        background_purge(_background_purge) {}

  DBImpl* db;
  InstrumentedMutex* mu;
  SuperVersion* super_version;
  bool background_purge;
};

static void CleanupSuperVersionHandle(void* arg1, void* /*arg2*/) {
  SuperVersionHandle* sv_handle = reinterpret_cast<SuperVersionHandle*>(arg1);

  if (sv_handle->super_version->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);

    sv_handle->mu->Lock();
    sv_handle->super_version->Cleanup();
    sv_handle->db->FindObsoleteFiles(&job_context, false, true);
    if (sv_handle->background_purge) {
      sv_handle->db->ScheduleBgLogWriterClose(&job_context);
      sv_handle->db->AddSuperVersionsToFreeQueue(sv_handle->super_version);
      sv_handle->db->SchedulePurge();
    }
    sv_handle->mu->Unlock();

    if (!sv_handle->background_purge) {
      delete sv_handle->super_version;
    }
    if (job_context.HaveSomethingToDelete()) {
      sv_handle->db->PurgeObsoleteFiles(job_context,
                                        sv_handle->background_purge);
    }
    job_context.Clean();
  }

  delete sv_handle;
}

struct GetMergeOperandsState {
  MergeContext merge_context;
  PinnedIteratorsManager pinned_iters_mgr;
  SuperVersionHandle* sv_handle;
};

static void CleanupGetMergeOperandsState(void* arg1, void* /*arg2*/) {
  GetMergeOperandsState* state = static_cast<GetMergeOperandsState*>(arg1);
  CleanupSuperVersionHandle(state->sv_handle /* arg1 */, nullptr /* arg2 */);
  delete state;
}

}  // namespace

InternalIterator* DBImpl::NewInternalIterator(
    const ReadOptions& read_options, ColumnFamilyData* cfd,
    SuperVersion* super_version, Arena* arena, SequenceNumber sequence,
    bool allow_unprepared_value, ArenaWrappedDBIter* db_iter) {
  InternalIterator* internal_iter;
  assert(arena != nullptr);
  // Need to create internal iterator from the arena.
  MergeIteratorBuilder merge_iter_builder(
      &cfd->internal_comparator(), arena,
      !read_options.total_order_seek &&
          super_version->mutable_cf_options.prefix_extractor != nullptr,
      read_options.iterate_upper_bound);
  // Collect iterator for mutable memtable
  auto mem_iter = super_version->mem->NewIterator(read_options, arena);
  Status s;
  if (!read_options.ignore_range_deletions) {
    TruncatedRangeDelIterator* mem_tombstone_iter = nullptr;
    auto range_del_iter = super_version->mem->NewRangeTombstoneIterator(
        read_options, sequence, false /* immutable_memtable */);
    if (range_del_iter == nullptr || range_del_iter->empty()) {
      delete range_del_iter;
    } else {
      mem_tombstone_iter = new TruncatedRangeDelIterator(
          std::unique_ptr<FragmentedRangeTombstoneIterator>(range_del_iter),
          &cfd->ioptions()->internal_comparator, nullptr /* smallest */,
          nullptr /* largest */);
    }
    merge_iter_builder.AddPointAndTombstoneIterator(mem_iter,
                                                    mem_tombstone_iter);
  } else {
    merge_iter_builder.AddIterator(mem_iter);
  }

  // Collect all needed child iterators for immutable memtables
  if (s.ok()) {
    super_version->imm->AddIterators(read_options, &merge_iter_builder,
                                     !read_options.ignore_range_deletions);
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::NewInternalIterator:StatusCallback", &s);
  if (s.ok()) {
    // Collect iterators for files in L0 - Ln
    if (read_options.read_tier != kMemtableTier) {
      super_version->current->AddIterators(read_options, file_options_,
                                           &merge_iter_builder,
                                           allow_unprepared_value);
    }
    internal_iter = merge_iter_builder.Finish(
        read_options.ignore_range_deletions ? nullptr : db_iter);
    SuperVersionHandle* cleanup = new SuperVersionHandle(
        this, &mutex_, super_version,
        read_options.background_purge_on_iterator_cleanup ||
            immutable_db_options_.avoid_unnecessary_blocking_io);
    internal_iter->RegisterCleanup(CleanupSuperVersionHandle, cleanup, nullptr);

    return internal_iter;
  } else {
    CleanupSuperVersion(super_version);
  }
  return NewErrorInternalIterator<Slice>(s, arena);
}

ColumnFamilyHandle* DBImpl::DefaultColumnFamily() const {
  return default_cf_handle_;
}

ColumnFamilyHandle* DBImpl::PersistentStatsColumnFamily() const {
  return persist_stats_cf_handle_;
}

Status DBImpl::Get(const ReadOptions& read_options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableSlice* value) {
  return Get(read_options, column_family, key, value, /*timestamp=*/nullptr);
}

Status DBImpl::Get(const ReadOptions& read_options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableSlice* value, std::string* timestamp) {
  assert(value != nullptr);
  value->Reset();
  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.value = value;
  get_impl_options.timestamp = timestamp;
  Status s = GetImpl(read_options, key, get_impl_options);
  return s;
}

Status DBImpl::GetEntity(const ReadOptions& read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableWideColumns* columns) {
  if (!column_family) {
    return Status::InvalidArgument(
        "Cannot call GetEntity without a column family handle");
  }

  if (!columns) {
    return Status::InvalidArgument(
        "Cannot call GetEntity without a PinnableWideColumns object");
  }

  columns->Reset();

  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.columns = columns;

  return GetImpl(read_options, key, get_impl_options);
}

bool DBImpl::ShouldReferenceSuperVersion(const MergeContext& merge_context) {
  // If both thresholds are reached, a function returning merge operands as
  // `PinnableSlice`s should reference the `SuperVersion` to avoid large and/or
  // numerous `memcpy()`s.
  //
  // The below constants enable the optimization conservatively. They are
  // verified to not regress `GetMergeOperands()` latency in the following
  // scenarios.
  //
  // - CPU: two socket Intel(R) Xeon(R) Gold 6138 CPU @ 2.00GHz
  // - `GetMergeOperands()` threads: 1 - 32
  // - Entry size: 32 bytes - 4KB
  // - Merges per key: 1 - 16K
  // - LSM component: memtable
  //
  // TODO(ajkr): expand measurement to SST files.
  static const size_t kNumBytesForSvRef = 32768;
  static const size_t kLog2AvgBytesForSvRef = 8;  // 256 bytes

  size_t num_bytes = 0;
  for (const Slice& sl : merge_context.GetOperands()) {
    num_bytes += sl.size();
  }
  return num_bytes >= kNumBytesForSvRef &&
         (num_bytes >> kLog2AvgBytesForSvRef) >=
             merge_context.GetOperands().size();
}

Status DBImpl::GetImpl(const ReadOptions& read_options, const Slice& key,
                       GetImplOptions& get_impl_options) {
  assert(get_impl_options.value != nullptr ||
         get_impl_options.merge_operands != nullptr ||
         get_impl_options.columns != nullptr);

  assert(get_impl_options.column_family);

  if (read_options.timestamp) {
    const Status s = FailIfTsMismatchCf(get_impl_options.column_family,
                                        *(read_options.timestamp),
                                        /*ts_for_read=*/true);
    if (!s.ok()) {
      return s;
    }
  } else {
    const Status s = FailIfCfHasTs(get_impl_options.column_family);
    if (!s.ok()) {
      return s;
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (get_impl_options.timestamp) {
    get_impl_options.timestamp->clear();
  }

  GetWithTimestampReadCallback read_cb(0);  // Will call Refresh

  PERF_CPU_TIMER_GUARD(get_cpu_nanos, immutable_db_options_.clock);
  StopWatch sw(immutable_db_options_.clock, stats_, DB_GET);
  PERF_TIMER_GUARD(get_snapshot_time);

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      get_impl_options.column_family);
  auto cfd = cfh->cfd();

  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      // TODO: maybe handle the tracing status?
      tracer_->Get(get_impl_options.column_family, key).PermitUncheckedError();
    }
  }

  if (get_impl_options.get_merge_operands_options != nullptr) {
    for (int i = 0; i < get_impl_options.get_merge_operands_options
                            ->expected_max_number_of_operands;
         ++i) {
      get_impl_options.merge_operands[i].Reset();
    }
  }

  // Acquire SuperVersion
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  TEST_SYNC_POINT("DBImpl::GetImpl:1");
  TEST_SYNC_POINT("DBImpl::GetImpl:2");

  SequenceNumber snapshot;
  if (read_options.snapshot != nullptr) {
    if (get_impl_options.callback) {
      // Already calculated based on read_options.snapshot
      snapshot = get_impl_options.callback->max_visible_seq();
    } else {
      snapshot =
          reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
    }
  } else {
    // Note that the snapshot is assigned AFTER referencing the super
    // version because otherwise a flush happening in between may compact away
    // data for the snapshot, so the reader would see neither data that was be
    // visible to the snapshot before compaction nor the newer data inserted
    // afterwards.
    snapshot = GetLastPublishedSequence();
    if (get_impl_options.callback) {
      // The unprep_seqs are not published for write unprepared, so it could be
      // that max_visible_seq is larger. Seek to the std::max of the two.
      // However, we still want our callback to contain the actual snapshot so
      // that it can do the correct visibility filtering.
      get_impl_options.callback->Refresh(snapshot);

      // Internally, WriteUnpreparedTxnReadCallback::Refresh would set
      // max_visible_seq = max(max_visible_seq, snapshot)
      //
      // Currently, the commented out assert is broken by
      // InvalidSnapshotReadCallback, but if write unprepared recovery followed
      // the regular transaction flow, then this special read callback would not
      // be needed.
      //
      // assert(callback->max_visible_seq() >= snapshot);
      snapshot = get_impl_options.callback->max_visible_seq();
    }
  }
  // If timestamp is used, we use read callback to ensure <key,t,s> is returned
  // only if t <= read_opts.timestamp and s <= snapshot.
  // HACK: temporarily overwrite input struct field but restore
  SaveAndRestore<ReadCallback*> restore_callback(&get_impl_options.callback);
  const Comparator* ucmp = get_impl_options.column_family->GetComparator();
  assert(ucmp);
  if (ucmp->timestamp_size() > 0) {
    assert(!get_impl_options
                .callback);  // timestamp with callback is not supported
    read_cb.Refresh(snapshot);
    get_impl_options.callback = &read_cb;
  }
  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");

  // Prepare to store a list of merge operations if merge occurs.
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;

  Status s;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key, snapshot, read_options.timestamp);
  PERF_TIMER_STOP(get_snapshot_time);

  bool skip_memtable = (read_options.read_tier == kPersistedTier &&
                        has_unpersisted_data_.load(std::memory_order_relaxed));
  bool done = false;
  std::string* timestamp =
      ucmp->timestamp_size() > 0 ? get_impl_options.timestamp : nullptr;
  if (!skip_memtable) {
    // Get value associated with key
    if (get_impl_options.get_value) {
      if (sv->mem->Get(
              lkey,
              get_impl_options.value ? get_impl_options.value->GetSelf()
                                     : nullptr,
              get_impl_options.columns, timestamp, &s, &merge_context,
              &max_covering_tombstone_seq, read_options,
              false /* immutable_memtable */, get_impl_options.callback,
              get_impl_options.is_blob_index)) {
        done = true;

        if (get_impl_options.value) {
          get_impl_options.value->PinSelf();
        }

        RecordTick(stats_, MEMTABLE_HIT);
      } else if ((s.ok() || s.IsMergeInProgress()) &&
                 sv->imm->Get(lkey,
                              get_impl_options.value
                                  ? get_impl_options.value->GetSelf()
                                  : nullptr,
                              get_impl_options.columns, timestamp, &s,
                              &merge_context, &max_covering_tombstone_seq,
                              read_options, get_impl_options.callback,
                              get_impl_options.is_blob_index)) {
        done = true;

        if (get_impl_options.value) {
          get_impl_options.value->PinSelf();
        }

        RecordTick(stats_, MEMTABLE_HIT);
      }
    } else {
      // Get Merge Operands associated with key, Merge Operands should not be
      // merged and raw values should be returned to the user.
      if (sv->mem->Get(lkey, /*value=*/nullptr, /*columns=*/nullptr,
                       /*timestamp=*/nullptr, &s, &merge_context,
                       &max_covering_tombstone_seq, read_options,
                       false /* immutable_memtable */, nullptr, nullptr,
                       false)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      } else if ((s.ok() || s.IsMergeInProgress()) &&
                 sv->imm->GetMergeOperands(lkey, &s, &merge_context,
                                           &max_covering_tombstone_seq,
                                           read_options)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      }
    }
    if (!done && !s.ok() && !s.IsMergeInProgress()) {
      ReturnAndCleanupSuperVersion(cfd, sv);
      return s;
    }
  }
  TEST_SYNC_POINT("DBImpl::GetImpl:PostMemTableGet:0");
  TEST_SYNC_POINT("DBImpl::GetImpl:PostMemTableGet:1");
  PinnedIteratorsManager pinned_iters_mgr;
  if (!done) {
    PERF_TIMER_GUARD(get_from_output_files_time);
    sv->current->Get(
        read_options, lkey, get_impl_options.value, get_impl_options.columns,
        timestamp, &s, &merge_context, &max_covering_tombstone_seq,
        &pinned_iters_mgr,
        get_impl_options.get_value ? get_impl_options.value_found : nullptr,
        nullptr, nullptr,
        get_impl_options.get_value ? get_impl_options.callback : nullptr,
        get_impl_options.get_value ? get_impl_options.is_blob_index : nullptr,
        get_impl_options.get_value);
    RecordTick(stats_, MEMTABLE_MISS);
  }

  {
    PERF_TIMER_GUARD(get_post_process_time);

    RecordTick(stats_, NUMBER_KEYS_READ);
    size_t size = 0;
    if (s.ok()) {
      if (get_impl_options.get_value) {
        if (get_impl_options.value) {
          size = get_impl_options.value->size();
        } else if (get_impl_options.columns) {
          size = get_impl_options.columns->serialized_size();
        }
      } else {
        // Return all merge operands for get_impl_options.key
        *get_impl_options.number_of_operands =
            static_cast<int>(merge_context.GetNumOperands());
        if (*get_impl_options.number_of_operands >
            get_impl_options.get_merge_operands_options
                ->expected_max_number_of_operands) {
          s = Status::Incomplete(
              Status::SubCode::KMergeOperandsInsufficientCapacity);
        } else {
          // Each operand depends on one of the following resources: `sv`,
          // `pinned_iters_mgr`, or `merge_context`. It would be crazy expensive
          // to reference `sv` for each operand relying on it because `sv` is
          // (un)ref'd in all threads using the DB. Furthermore, we do not track
          // on which resource each operand depends.
          //
          // To solve this, we bundle the resources in a `GetMergeOperandsState`
          // and manage them with a `SharedCleanablePtr` shared among the
          // `PinnableSlice`s we return. This bundle includes one `sv` reference
          // and ownership of the `merge_context` and `pinned_iters_mgr`
          // objects.
          bool ref_sv = ShouldReferenceSuperVersion(merge_context);
          if (ref_sv) {
            assert(!merge_context.GetOperands().empty());
            SharedCleanablePtr shared_cleanable;
            GetMergeOperandsState* state = nullptr;
            state = new GetMergeOperandsState();
            state->merge_context = std::move(merge_context);
            state->pinned_iters_mgr = std::move(pinned_iters_mgr);

            sv->Ref();

            state->sv_handle = new SuperVersionHandle(
                this, &mutex_, sv,
                immutable_db_options_.avoid_unnecessary_blocking_io);

            shared_cleanable.Allocate();
            shared_cleanable->RegisterCleanup(CleanupGetMergeOperandsState,
                                              state /* arg1 */,
                                              nullptr /* arg2 */);
            for (size_t i = 0; i < state->merge_context.GetOperands().size();
                 ++i) {
              const Slice& sl = state->merge_context.GetOperands()[i];
              size += sl.size();

              get_impl_options.merge_operands->PinSlice(
                  sl, nullptr /* cleanable */);
              if (i == state->merge_context.GetOperands().size() - 1) {
                shared_cleanable.MoveAsCleanupTo(
                    get_impl_options.merge_operands);
              } else {
                shared_cleanable.RegisterCopyWith(
                    get_impl_options.merge_operands);
              }
              get_impl_options.merge_operands++;
            }
          } else {
            for (const Slice& sl : merge_context.GetOperands()) {
              size += sl.size();
              get_impl_options.merge_operands->PinSelf(sl);
              get_impl_options.merge_operands++;
            }
          }
        }
      }
      RecordTick(stats_, BYTES_READ, size);
      PERF_COUNTER_ADD(get_read_bytes, size);
    }

    ReturnAndCleanupSuperVersion(cfd, sv);

    RecordInHistogram(stats_, BYTES_PER_READ, size);
  }
  return s;
}

std::vector<Status> DBImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  return MultiGet(read_options, column_family, keys, values,
                  /*timestamps=*/nullptr);
}

std::vector<Status> DBImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values,
    std::vector<std::string>* timestamps) {
  PERF_CPU_TIMER_GUARD(get_cpu_nanos, immutable_db_options_.clock);
  StopWatch sw(immutable_db_options_.clock, stats_, DB_MULTIGET);
  PERF_TIMER_GUARD(get_snapshot_time);

  size_t num_keys = keys.size();
  assert(column_family.size() == num_keys);
  std::vector<Status> stat_list(num_keys);

  bool should_fail = false;
  for (size_t i = 0; i < num_keys; ++i) {
    assert(column_family[i]);
    if (read_options.timestamp) {
      stat_list[i] = FailIfTsMismatchCf(
          column_family[i], *(read_options.timestamp), /*ts_for_read=*/true);
      if (!stat_list[i].ok()) {
        should_fail = true;
      }
    } else {
      stat_list[i] = FailIfCfHasTs(column_family[i]);
      if (!stat_list[i].ok()) {
        should_fail = true;
      }
    }
  }

  if (should_fail) {
    for (auto& s : stat_list) {
      if (s.ok()) {
        s = Status::Incomplete(
            "DB not queried due to invalid argument(s) in the same MultiGet");
      }
    }
    return stat_list;
  }

  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      // TODO: maybe handle the tracing status?
      tracer_->MultiGet(column_family, keys).PermitUncheckedError();
    }
  }

  SequenceNumber consistent_seqnum;

  UnorderedMap<uint32_t, MultiGetColumnFamilyData> multiget_cf_data(
      column_family.size());
  for (auto cf : column_family) {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(cf);
    auto cfd = cfh->cfd();
    if (multiget_cf_data.find(cfd->GetID()) == multiget_cf_data.end()) {
      multiget_cf_data.emplace(cfd->GetID(),
                               MultiGetColumnFamilyData(cfh, nullptr));
    }
  }

  std::function<MultiGetColumnFamilyData*(
      UnorderedMap<uint32_t, MultiGetColumnFamilyData>::iterator&)>
      iter_deref_lambda =
          [](UnorderedMap<uint32_t, MultiGetColumnFamilyData>::iterator&
                 cf_iter) { return &cf_iter->second; };

  bool unref_only =
      MultiCFSnapshot<UnorderedMap<uint32_t, MultiGetColumnFamilyData>>(
          read_options, nullptr, iter_deref_lambda, &multiget_cf_data,
          &consistent_seqnum);

  TEST_SYNC_POINT("DBImpl::MultiGet:AfterGetSeqNum1");
  TEST_SYNC_POINT("DBImpl::MultiGet:AfterGetSeqNum2");

  // Contain a list of merge operations if merge occurs.
  MergeContext merge_context;

  // Note: this always resizes the values array
  values->resize(num_keys);
  if (timestamps) {
    timestamps->resize(num_keys);
  }

  // Keep track of bytes that we read for statistics-recording later
  uint64_t bytes_read = 0;
  PERF_TIMER_STOP(get_snapshot_time);

  // For each of the given keys, apply the entire "get" process as follows:
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  size_t num_found = 0;
  size_t keys_read;
  uint64_t curr_value_size = 0;

  GetWithTimestampReadCallback timestamp_read_callback(0);
  ReadCallback* read_callback = nullptr;
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    timestamp_read_callback.Refresh(consistent_seqnum);
    read_callback = &timestamp_read_callback;
  }

  for (keys_read = 0; keys_read < num_keys; ++keys_read) {
    merge_context.Clear();
    Status& s = stat_list[keys_read];
    std::string* value = &(*values)[keys_read];
    std::string* timestamp = timestamps ? &(*timestamps)[keys_read] : nullptr;

    LookupKey lkey(keys[keys_read], consistent_seqnum, read_options.timestamp);
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
        column_family[keys_read]);
    SequenceNumber max_covering_tombstone_seq = 0;
    auto mgd_iter = multiget_cf_data.find(cfh->cfd()->GetID());
    assert(mgd_iter != multiget_cf_data.end());
    auto mgd = mgd_iter->second;
    auto super_version = mgd.super_version;
    bool skip_memtable =
        (read_options.read_tier == kPersistedTier &&
         has_unpersisted_data_.load(std::memory_order_relaxed));
    bool done = false;
    if (!skip_memtable) {
      if (super_version->mem->Get(
              lkey, value, /*columns=*/nullptr, timestamp, &s, &merge_context,
              &max_covering_tombstone_seq, read_options,
              false /* immutable_memtable */, read_callback)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      } else if (super_version->imm->Get(lkey, value, /*columns=*/nullptr,
                                         timestamp, &s, &merge_context,
                                         &max_covering_tombstone_seq,
                                         read_options, read_callback)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      }
    }
    if (!done) {
      PinnableSlice pinnable_val;
      PERF_TIMER_GUARD(get_from_output_files_time);
      PinnedIteratorsManager pinned_iters_mgr;
      super_version->current->Get(read_options, lkey, &pinnable_val,
                                  /*columns=*/nullptr, timestamp, &s,
                                  &merge_context, &max_covering_tombstone_seq,
                                  &pinned_iters_mgr, /*value_found=*/nullptr,
                                  /*key_exists=*/nullptr,
                                  /*seq=*/nullptr, read_callback);
      value->assign(pinnable_val.data(), pinnable_val.size());
      RecordTick(stats_, MEMTABLE_MISS);
    }

    if (s.ok()) {
      bytes_read += value->size();
      num_found++;
      curr_value_size += value->size();
      if (curr_value_size > read_options.value_size_soft_limit) {
        while (++keys_read < num_keys) {
          stat_list[keys_read] = Status::Aborted();
        }
        break;
      }
    }
    if (read_options.deadline.count() &&
        immutable_db_options_.clock->NowMicros() >
            static_cast<uint64_t>(read_options.deadline.count())) {
      break;
    }
  }

  if (keys_read < num_keys) {
    // The only reason to break out of the loop is when the deadline is
    // exceeded
    assert(immutable_db_options_.clock->NowMicros() >
           static_cast<uint64_t>(read_options.deadline.count()));
    for (++keys_read; keys_read < num_keys; ++keys_read) {
      stat_list[keys_read] = Status::TimedOut();
    }
  }

  // Post processing (decrement reference counts and record statistics)
  PERF_TIMER_GUARD(get_post_process_time);
  autovector<SuperVersion*> superversions_to_delete;

  for (auto mgd_iter : multiget_cf_data) {
    auto mgd = mgd_iter.second;
    if (!unref_only) {
      ReturnAndCleanupSuperVersion(mgd.cfd, mgd.super_version);
    } else {
      mgd.cfd->GetSuperVersion()->Unref();
    }
  }
  RecordTick(stats_, NUMBER_MULTIGET_CALLS);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_READ, num_keys);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_FOUND, num_found);
  RecordTick(stats_, NUMBER_MULTIGET_BYTES_READ, bytes_read);
  RecordInHistogram(stats_, BYTES_PER_MULTIGET, bytes_read);
  PERF_COUNTER_ADD(multiget_read_bytes, bytes_read);
  PERF_TIMER_STOP(get_post_process_time);

  return stat_list;
}

template <class T>
bool DBImpl::MultiCFSnapshot(
    const ReadOptions& read_options, ReadCallback* callback,
    std::function<MultiGetColumnFamilyData*(typename T::iterator&)>&
        iter_deref_func,
    T* cf_list, SequenceNumber* snapshot) {
  PERF_TIMER_GUARD(get_snapshot_time);

  bool last_try = false;
  if (cf_list->size() == 1) {
    // Fast path for a single column family. We can simply get the thread loca
    // super version
    auto cf_iter = cf_list->begin();
    auto node = iter_deref_func(cf_iter);
    node->super_version = GetAndRefSuperVersion(node->cfd);
    if (read_options.snapshot != nullptr) {
      // Note: In WritePrepared txns this is not necessary but not harmful
      // either.  Because prep_seq > snapshot => commit_seq > snapshot so if
      // a snapshot is specified we should be fine with skipping seq numbers
      // that are greater than that.
      //
      // In WriteUnprepared, we cannot set snapshot in the lookup key because we
      // may skip uncommitted data that should be visible to the transaction for
      // reading own writes.
      *snapshot =
          static_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
      if (callback) {
        *snapshot = std::max(*snapshot, callback->max_visible_seq());
      }
    } else {
      // Since we get and reference the super version before getting
      // the snapshot number, without a mutex protection, it is possible
      // that a memtable switch happened in the middle and not all the
      // data for this snapshot is available. But it will contain all
      // the data available in the super version we have, which is also
      // a valid snapshot to read from.
      // We shouldn't get snapshot before finding and referencing the super
      // version because a flush happening in between may compact away data for
      // the snapshot, but the snapshot is earlier than the data overwriting it,
      // so users may see wrong results.
      *snapshot = GetLastPublishedSequence();
    }
  } else {
    // If we end up with the same issue of memtable geting sealed during 2
    // consecutive retries, it means the write rate is very high. In that case
    // its probably ok to take the mutex on the 3rd try so we can succeed for
    // sure
    constexpr int num_retries = 3;
    for (int i = 0; i < num_retries; ++i) {
      last_try = (i == num_retries - 1);
      bool retry = false;

      if (i > 0) {
        for (auto cf_iter = cf_list->begin(); cf_iter != cf_list->end();
             ++cf_iter) {
          auto node = iter_deref_func(cf_iter);
          SuperVersion* super_version = node->super_version;
          ColumnFamilyData* cfd = node->cfd;
          if (super_version != nullptr) {
            ReturnAndCleanupSuperVersion(cfd, super_version);
          }
          node->super_version = nullptr;
        }
      }
      if (read_options.snapshot == nullptr) {
        if (last_try) {
          TEST_SYNC_POINT("DBImpl::MultiGet::LastTry");
          // We're close to max number of retries. For the last retry,
          // acquire the lock so we're sure to succeed
          mutex_.Lock();
        }
        *snapshot = GetLastPublishedSequence();
      } else {
        *snapshot =
            static_cast_with_check<const SnapshotImpl>(read_options.snapshot)
                ->number_;
      }
      for (auto cf_iter = cf_list->begin(); cf_iter != cf_list->end();
           ++cf_iter) {
        auto node = iter_deref_func(cf_iter);
        if (!last_try) {
          node->super_version = GetAndRefSuperVersion(node->cfd);
        } else {
          node->super_version = node->cfd->GetSuperVersion()->Ref();
        }
        TEST_SYNC_POINT("DBImpl::MultiGet::AfterRefSV");
        if (read_options.snapshot != nullptr || last_try) {
          // If user passed a snapshot, then we don't care if a memtable is
          // sealed or compaction happens because the snapshot would ensure
          // that older key versions are kept around. If this is the last
          // retry, then we have the lock so nothing bad can happen
          continue;
        }
        // We could get the earliest sequence number for the whole list of
        // memtables, which will include immutable memtables as well, but that
        // might be tricky to maintain in case we decide, in future, to do
        // memtable compaction.
        if (!last_try) {
          SequenceNumber seq =
              node->super_version->mem->GetEarliestSequenceNumber();
          if (seq > *snapshot) {
            retry = true;
            break;
          }
        }
      }
      if (!retry) {
        if (last_try) {
          mutex_.Unlock();
        }
        break;
      }
    }
  }

  // Keep track of bytes that we read for statistics-recording later
  PERF_TIMER_STOP(get_snapshot_time);

  return last_try;
}

void DBImpl::MultiGet(const ReadOptions& read_options, const size_t num_keys,
                      ColumnFamilyHandle** column_families, const Slice* keys,
                      PinnableSlice* values, Status* statuses,
                      const bool sorted_input) {
  return MultiGet(read_options, num_keys, column_families, keys, values,
                  /*timestamps=*/nullptr, statuses, sorted_input);
}

void DBImpl::MultiGet(const ReadOptions& read_options, const size_t num_keys,
                      ColumnFamilyHandle** column_families, const Slice* keys,
                      PinnableSlice* values, std::string* timestamps,
                      Status* statuses, const bool sorted_input) {
  if (num_keys == 0) {
    return;
  }

  bool should_fail = false;
  for (size_t i = 0; i < num_keys; ++i) {
    ColumnFamilyHandle* cfh = column_families[i];
    assert(cfh);
    if (read_options.timestamp) {
      statuses[i] = FailIfTsMismatchCf(cfh, *(read_options.timestamp),
                                       /*ts_for_read=*/true);
      if (!statuses[i].ok()) {
        should_fail = true;
      }
    } else {
      statuses[i] = FailIfCfHasTs(cfh);
      if (!statuses[i].ok()) {
        should_fail = true;
      }
    }
  }
  if (should_fail) {
    for (size_t i = 0; i < num_keys; ++i) {
      if (statuses[i].ok()) {
        statuses[i] = Status::Incomplete(
            "DB not queried due to invalid argument(s) in the same MultiGet");
      }
    }
    return;
  }

  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      // TODO: maybe handle the tracing status?
      tracer_->MultiGet(num_keys, column_families, keys).PermitUncheckedError();
    }
  }

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  sorted_keys.resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    values[i].Reset();
    key_context.emplace_back(column_families[i], keys[i], &values[i],
                             timestamps ? &timestamps[i] : nullptr,
                             &statuses[i]);
  }
  for (size_t i = 0; i < num_keys; ++i) {
    sorted_keys[i] = &key_context[i];
  }
  PrepareMultiGetKeys(num_keys, sorted_input, &sorted_keys);

  autovector<MultiGetColumnFamilyData, MultiGetContext::MAX_BATCH_SIZE>
      multiget_cf_data;
  size_t cf_start = 0;
  ColumnFamilyHandle* cf = sorted_keys[0]->column_family;

  for (size_t i = 0; i < num_keys; ++i) {
    KeyContext* key_ctx = sorted_keys[i];
    if (key_ctx->column_family != cf) {
      multiget_cf_data.emplace_back(cf, cf_start, i - cf_start, nullptr);
      cf_start = i;
      cf = key_ctx->column_family;
    }
  }

  multiget_cf_data.emplace_back(cf, cf_start, num_keys - cf_start, nullptr);

  std::function<MultiGetColumnFamilyData*(
      autovector<MultiGetColumnFamilyData,
                 MultiGetContext::MAX_BATCH_SIZE>::iterator&)>
      iter_deref_lambda =
          [](autovector<MultiGetColumnFamilyData,
                        MultiGetContext::MAX_BATCH_SIZE>::iterator& cf_iter) {
            return &(*cf_iter);
          };

  SequenceNumber consistent_seqnum;
  bool unref_only = MultiCFSnapshot<
      autovector<MultiGetColumnFamilyData, MultiGetContext::MAX_BATCH_SIZE>>(
      read_options, nullptr, iter_deref_lambda, &multiget_cf_data,
      &consistent_seqnum);

  GetWithTimestampReadCallback timestamp_read_callback(0);
  ReadCallback* read_callback = nullptr;
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    timestamp_read_callback.Refresh(consistent_seqnum);
    read_callback = &timestamp_read_callback;
  }

  Status s;
  auto cf_iter = multiget_cf_data.begin();
  for (; cf_iter != multiget_cf_data.end(); ++cf_iter) {
    s = MultiGetImpl(read_options, cf_iter->start, cf_iter->num_keys,
                     &sorted_keys, cf_iter->super_version, consistent_seqnum,
                     read_callback);
    if (!s.ok()) {
      break;
    }
  }
  if (!s.ok()) {
    assert(s.IsTimedOut() || s.IsAborted());
    for (++cf_iter; cf_iter != multiget_cf_data.end(); ++cf_iter) {
      for (size_t i = cf_iter->start; i < cf_iter->start + cf_iter->num_keys;
           ++i) {
        *sorted_keys[i]->s = s;
      }
    }
  }

  for (const auto& iter : multiget_cf_data) {
    if (!unref_only) {
      ReturnAndCleanupSuperVersion(iter.cfd, iter.super_version);
    } else {
      iter.cfd->GetSuperVersion()->Unref();
    }
  }
}

namespace {
// Order keys by CF ID, followed by key contents
struct CompareKeyContext {
  inline bool operator()(const KeyContext* lhs, const KeyContext* rhs) {
    ColumnFamilyHandleImpl* cfh =
        static_cast<ColumnFamilyHandleImpl*>(lhs->column_family);
    uint32_t cfd_id1 = cfh->cfd()->GetID();
    const Comparator* comparator = cfh->cfd()->user_comparator();
    cfh = static_cast<ColumnFamilyHandleImpl*>(rhs->column_family);
    uint32_t cfd_id2 = cfh->cfd()->GetID();

    if (cfd_id1 < cfd_id2) {
      return true;
    } else if (cfd_id1 > cfd_id2) {
      return false;
    }

    // Both keys are from the same column family
    int cmp = comparator->CompareWithoutTimestamp(
        *(lhs->key), /*a_has_ts=*/false, *(rhs->key), /*b_has_ts=*/false);
    if (cmp < 0) {
      return true;
    }
    return false;
  }
};

}  // anonymous namespace

void DBImpl::PrepareMultiGetKeys(
    size_t num_keys, bool sorted_input,
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys) {
  if (sorted_input) {
#ifndef NDEBUG
    assert(std::is_sorted(sorted_keys->begin(), sorted_keys->end(),
                          CompareKeyContext()));
#endif
    return;
  }

  std::sort(sorted_keys->begin(), sorted_keys->begin() + num_keys,
            CompareKeyContext());
}

void DBImpl::MultiGet(const ReadOptions& read_options,
                      ColumnFamilyHandle* column_family, const size_t num_keys,
                      const Slice* keys, PinnableSlice* values,
                      Status* statuses, const bool sorted_input) {
  return MultiGet(read_options, column_family, num_keys, keys, values,
                  /*timestamp=*/nullptr, statuses, sorted_input);
}

void DBImpl::MultiGet(const ReadOptions& read_options,
                      ColumnFamilyHandle* column_family, const size_t num_keys,
                      const Slice* keys, PinnableSlice* values,
                      std::string* timestamps, Status* statuses,
                      const bool sorted_input) {
  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      // TODO: maybe handle the tracing status?
      tracer_->MultiGet(num_keys, column_family, keys).PermitUncheckedError();
    }
  }
  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  sorted_keys.resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    values[i].Reset();
    key_context.emplace_back(column_family, keys[i], &values[i],
                             timestamps ? &timestamps[i] : nullptr,
                             &statuses[i]);
  }
  for (size_t i = 0; i < num_keys; ++i) {
    sorted_keys[i] = &key_context[i];
  }
  PrepareMultiGetKeys(num_keys, sorted_input, &sorted_keys);
  MultiGetWithCallback(read_options, column_family, nullptr, &sorted_keys);
}

void DBImpl::MultiGetWithCallback(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    ReadCallback* callback,
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys) {
  std::array<MultiGetColumnFamilyData, 1> multiget_cf_data;
  multiget_cf_data[0] = MultiGetColumnFamilyData(column_family, nullptr);
  std::function<MultiGetColumnFamilyData*(
      std::array<MultiGetColumnFamilyData, 1>::iterator&)>
      iter_deref_lambda =
          [](std::array<MultiGetColumnFamilyData, 1>::iterator& cf_iter) {
            return &(*cf_iter);
          };

  size_t num_keys = sorted_keys->size();
  SequenceNumber consistent_seqnum;
  bool unref_only = MultiCFSnapshot<std::array<MultiGetColumnFamilyData, 1>>(
      read_options, callback, iter_deref_lambda, &multiget_cf_data,
      &consistent_seqnum);
#ifndef NDEBUG
  assert(!unref_only);
#else
  // Silence unused variable warning
  (void)unref_only;
#endif  // NDEBUG

  if (callback && read_options.snapshot == nullptr) {
    // The unprep_seqs are not published for write unprepared, so it could be
    // that max_visible_seq is larger. Seek to the std::max of the two.
    // However, we still want our callback to contain the actual snapshot so
    // that it can do the correct visibility filtering.
    callback->Refresh(consistent_seqnum);

    // Internally, WriteUnpreparedTxnReadCallback::Refresh would set
    // max_visible_seq = max(max_visible_seq, snapshot)
    //
    // Currently, the commented out assert is broken by
    // InvalidSnapshotReadCallback, but if write unprepared recovery followed
    // the regular transaction flow, then this special read callback would not
    // be needed.
    //
    // assert(callback->max_visible_seq() >= snapshot);
    consistent_seqnum = callback->max_visible_seq();
  }

  GetWithTimestampReadCallback timestamp_read_callback(0);
  ReadCallback* read_callback = callback;
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    assert(!read_callback);  // timestamp with callback is not supported
    timestamp_read_callback.Refresh(consistent_seqnum);
    read_callback = &timestamp_read_callback;
  }

  Status s = MultiGetImpl(read_options, 0, num_keys, sorted_keys,
                          multiget_cf_data[0].super_version, consistent_seqnum,
                          read_callback);
  assert(s.ok() || s.IsTimedOut() || s.IsAborted());
  ReturnAndCleanupSuperVersion(multiget_cf_data[0].cfd,
                               multiget_cf_data[0].super_version);
}

// The actual implementation of batched MultiGet. Parameters -
// start_key - Index in the sorted_keys vector to start processing from
// num_keys - Number of keys to lookup, starting with sorted_keys[start_key]
// sorted_keys - The entire batch of sorted keys for this CF
//
// The per key status is returned in the KeyContext structures pointed to by
// sorted_keys. An overall Status is also returned, with the only possible
// values being Status::OK() and Status::TimedOut(). The latter indicates
// that the call exceeded read_options.deadline
Status DBImpl::MultiGetImpl(
    const ReadOptions& read_options, size_t start_key, size_t num_keys,
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys,
    SuperVersion* super_version, SequenceNumber snapshot,
    ReadCallback* callback) {
  PERF_CPU_TIMER_GUARD(get_cpu_nanos, immutable_db_options_.clock);
  StopWatch sw(immutable_db_options_.clock, stats_, DB_MULTIGET);

  assert(sorted_keys);
  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  for (auto* kctx : *sorted_keys) {
    assert(kctx);
    if (kctx->timestamp) {
      kctx->timestamp->clear();
    }
  }

  // For each of the given keys, apply the entire "get" process as follows:
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  size_t keys_left = num_keys;
  Status s;
  uint64_t curr_value_size = 0;
  while (keys_left) {
    if (read_options.deadline.count() &&
        immutable_db_options_.clock->NowMicros() >
            static_cast<uint64_t>(read_options.deadline.count())) {
      s = Status::TimedOut();
      break;
    }

    size_t batch_size = (keys_left > MultiGetContext::MAX_BATCH_SIZE)
                            ? MultiGetContext::MAX_BATCH_SIZE
                            : keys_left;
    MultiGetContext ctx(sorted_keys, start_key + num_keys - keys_left,
                        batch_size, snapshot, read_options, GetFileSystem(),
                        stats_);
    MultiGetRange range = ctx.GetMultiGetRange();
    range.AddValueSize(curr_value_size);
    bool lookup_current = false;

    keys_left -= batch_size;
    for (auto mget_iter = range.begin(); mget_iter != range.end();
         ++mget_iter) {
      mget_iter->merge_context.Clear();
      *mget_iter->s = Status::OK();
    }

    bool skip_memtable =
        (read_options.read_tier == kPersistedTier &&
         has_unpersisted_data_.load(std::memory_order_relaxed));
    if (!skip_memtable) {
      super_version->mem->MultiGet(read_options, &range, callback,
                                   false /* immutable_memtable */);
      if (!range.empty()) {
        super_version->imm->MultiGet(read_options, &range, callback);
      }
      if (!range.empty()) {
        lookup_current = true;
        uint64_t left = range.KeysLeft();
        RecordTick(stats_, MEMTABLE_MISS, left);
      }
    }
    if (lookup_current) {
      PERF_TIMER_GUARD(get_from_output_files_time);
      super_version->current->MultiGet(read_options, &range, callback);
    }
    curr_value_size = range.GetValueSize();
    if (curr_value_size > read_options.value_size_soft_limit) {
      s = Status::Aborted();
      break;
    }
  }

  // Post processing (decrement reference counts and record statistics)
  PERF_TIMER_GUARD(get_post_process_time);
  size_t num_found = 0;
  uint64_t bytes_read = 0;
  for (size_t i = start_key; i < start_key + num_keys - keys_left; ++i) {
    KeyContext* key = (*sorted_keys)[i];
    if (key->s->ok()) {
      bytes_read += key->value->size();
      num_found++;
    }
  }
  if (keys_left) {
    assert(s.IsTimedOut() || s.IsAborted());
    for (size_t i = start_key + num_keys - keys_left; i < start_key + num_keys;
         ++i) {
      KeyContext* key = (*sorted_keys)[i];
      *key->s = s;
    }
  }

  RecordTick(stats_, NUMBER_MULTIGET_CALLS);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_READ, num_keys);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_FOUND, num_found);
  RecordTick(stats_, NUMBER_MULTIGET_BYTES_READ, bytes_read);
  RecordInHistogram(stats_, BYTES_PER_MULTIGET, bytes_read);
  PERF_COUNTER_ADD(multiget_read_bytes, bytes_read);
  PERF_TIMER_STOP(get_post_process_time);

  return s;
}

Status DBImpl::CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                  const std::string& column_family,
                                  ColumnFamilyHandle** handle) {
  assert(handle != nullptr);
  Status s = CreateColumnFamilyImpl(cf_options, column_family, handle);
  if (s.ok()) {
    s = WriteOptionsFile(true /*need_mutex_lock*/,
                         true /*need_enter_write_thread*/);
  }
  return s;
}

Status DBImpl::CreateColumnFamilies(
    const ColumnFamilyOptions& cf_options,
    const std::vector<std::string>& column_family_names,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  handles->clear();
  size_t num_cf = column_family_names.size();
  Status s;
  bool success_once = false;
  for (size_t i = 0; i < num_cf; i++) {
    ColumnFamilyHandle* handle;
    s = CreateColumnFamilyImpl(cf_options, column_family_names[i], &handle);
    if (!s.ok()) {
      break;
    }
    handles->push_back(handle);
    success_once = true;
  }
  if (success_once) {
    Status persist_options_status = WriteOptionsFile(
        true /*need_mutex_lock*/, true /*need_enter_write_thread*/);
    if (s.ok() && !persist_options_status.ok()) {
      s = persist_options_status;
    }
  }
  return s;
}

Status DBImpl::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  handles->clear();
  size_t num_cf = column_families.size();
  Status s;
  bool success_once = false;
  for (size_t i = 0; i < num_cf; i++) {
    ColumnFamilyHandle* handle;
    s = CreateColumnFamilyImpl(column_families[i].options,
                               column_families[i].name, &handle);
    if (!s.ok()) {
      break;
    }
    handles->push_back(handle);
    success_once = true;
  }
  if (success_once) {
    Status persist_options_status = WriteOptionsFile(
        true /*need_mutex_lock*/, true /*need_enter_write_thread*/);
    if (s.ok() && !persist_options_status.ok()) {
      s = persist_options_status;
    }
  }
  return s;
}

Status DBImpl::CreateColumnFamilyImpl(const ColumnFamilyOptions& cf_options,
                                      const std::string& column_family_name,
                                      ColumnFamilyHandle** handle) {
  Status s;
  *handle = nullptr;

  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  s = ColumnFamilyData::ValidateOptions(db_options, cf_options);
  if (s.ok()) {
    for (auto& cf_path : cf_options.cf_paths) {
      s = env_->CreateDirIfMissing(cf_path.path);
      if (!s.ok()) {
        break;
      }
    }
  }
  if (!s.ok()) {
    return s;
  }

  SuperVersionContext sv_context(/* create_superversion */ true);
  {
    InstrumentedMutexLock l(&mutex_);

    if (versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name) !=
        nullptr) {
      return Status::InvalidArgument("Column family already exists");
    }
    VersionEdit edit;
    edit.AddColumnFamily(column_family_name);
    uint32_t new_id = versions_->GetColumnFamilySet()->GetNextColumnFamilyID();
    edit.SetColumnFamily(new_id);
    edit.SetLogNumber(logfile_number_);
    edit.SetComparatorName(cf_options.comparator->Name());

    // LogAndApply will both write the creation in MANIFEST and create
    // ColumnFamilyData object
    {  // write thread
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      // LogAndApply will both write the creation in MANIFEST and create
      // ColumnFamilyData object
      s = versions_->LogAndApply(nullptr, MutableCFOptions(cf_options), &edit,
                                 &mutex_, directories_.GetDbDir(), false,
                                 &cf_options);
      write_thread_.ExitUnbatched(&w);
    }
    if (s.ok()) {
      auto* cfd =
          versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name);
      assert(cfd != nullptr);
      std::map<std::string, std::shared_ptr<FSDirectory>> dummy_created_dirs;
      s = cfd->AddDirectories(&dummy_created_dirs);
    }
    if (s.ok()) {
      auto* cfd =
          versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name);
      assert(cfd != nullptr);
      InstallSuperVersionAndScheduleWork(cfd, &sv_context,
                                         *cfd->GetLatestMutableCFOptions());

      if (!cfd->mem()->IsSnapshotSupported()) {
        is_snapshot_supported_ = false;
      }

      cfd->set_initialized();

      *handle = new ColumnFamilyHandleImpl(cfd, this, &mutex_);
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Created column family [%s] (ID %u)",
                     column_family_name.c_str(), (unsigned)cfd->GetID());
    } else {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Creating column family [%s] FAILED -- %s",
                      column_family_name.c_str(), s.ToString().c_str());
    }
  }  // InstrumentedMutexLock l(&mutex_)

  if (cf_options.preserve_internal_time_seconds > 0 ||
      cf_options.preclude_last_level_data_seconds > 0) {
    s = RegisterRecordSeqnoTimeWorker();
  }
  sv_context.Clean();
  // this is outside the mutex
  if (s.ok()) {
    NewThreadStatusCfInfo(
        static_cast_with_check<ColumnFamilyHandleImpl>(*handle)->cfd());
  }
  return s;
}

Status DBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
  assert(column_family != nullptr);
  Status s = DropColumnFamilyImpl(column_family);
  if (s.ok()) {
    s = WriteOptionsFile(true /*need_mutex_lock*/,
                         true /*need_enter_write_thread*/);
  }
  return s;
}

Status DBImpl::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  bool success_once = false;
  for (auto* handle : column_families) {
    s = DropColumnFamilyImpl(handle);
    if (!s.ok()) {
      break;
    }
    success_once = true;
  }
  if (success_once) {
    Status persist_options_status = WriteOptionsFile(
        true /*need_mutex_lock*/, true /*need_enter_write_thread*/);
    if (s.ok() && !persist_options_status.ok()) {
      s = persist_options_status;
    }
  }
  return s;
}

Status DBImpl::DropColumnFamilyImpl(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  if (cfd->GetID() == 0) {
    return Status::InvalidArgument("Can't drop default column family");
  }

  bool cf_support_snapshot = cfd->mem()->IsSnapshotSupported();

  VersionEdit edit;
  edit.DropColumnFamily();
  edit.SetColumnFamily(cfd->GetID());

  Status s;
  {
    InstrumentedMutexLock l(&mutex_);
    if (cfd->IsDropped()) {
      s = Status::InvalidArgument("Column family already dropped!\n");
    }
    if (s.ok()) {
      // we drop column family from a single write thread
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      s = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(), &edit,
                                 &mutex_, directories_.GetDbDir());
      write_thread_.ExitUnbatched(&w);
    }
    if (s.ok()) {
      auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
      max_total_in_memory_state_ -= mutable_cf_options->write_buffer_size *
                                    mutable_cf_options->max_write_buffer_number;
    }

    if (!cf_support_snapshot) {
      // Dropped Column Family doesn't support snapshot. Need to recalculate
      // is_snapshot_supported_.
      bool new_is_snapshot_supported = true;
      for (auto c : *versions_->GetColumnFamilySet()) {
        if (!c->IsDropped() && !c->mem()->IsSnapshotSupported()) {
          new_is_snapshot_supported = false;
          break;
        }
      }
      is_snapshot_supported_ = new_is_snapshot_supported;
    }
    bg_cv_.SignalAll();
  }

  if (cfd->ioptions()->preserve_internal_time_seconds > 0 ||
      cfd->ioptions()->preclude_last_level_data_seconds > 0) {
    s = RegisterRecordSeqnoTimeWorker();
  }

  if (s.ok()) {
    // Note that here we erase the associated cf_info of the to-be-dropped
    // cfd before its ref-count goes to zero to avoid having to erase cf_info
    // later inside db_mutex.
    EraseThreadStatusCfInfo(cfd);
    assert(cfd->IsDropped());
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Dropped column family with id %u\n", cfd->GetID());
  } else {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Dropping column family with id %u FAILED -- %s\n",
                    cfd->GetID(), s.ToString().c_str());
  }

  return s;
}

bool DBImpl::KeyMayExist(const ReadOptions& read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         std::string* value, std::string* timestamp,
                         bool* value_found) {
  assert(value != nullptr);
  if (value_found != nullptr) {
    // falsify later if key-may-exist but can't fetch value
    *value_found = true;
  }
  ReadOptions roptions = read_options;
  roptions.read_tier = kBlockCacheTier;  // read from block cache only
  PinnableSlice pinnable_val;
  GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.value = &pinnable_val;
  get_impl_options.value_found = value_found;
  get_impl_options.timestamp = timestamp;
  auto s = GetImpl(roptions, key, get_impl_options);
  value->assign(pinnable_val.data(), pinnable_val.size());

  // If block_cache is enabled and the index block of the table didn't
  // not present in block_cache, the return value will be Status::Incomplete.
  // In this case, key may still exist in the table.
  return s.ok() || s.IsIncomplete();
}

Iterator* DBImpl::NewIterator(const ReadOptions& read_options,
                              ColumnFamilyHandle* column_family) {
  if (read_options.managed) {
    return NewErrorIterator(
        Status::NotSupported("Managed iterator is not supported anymore."));
  }
  Iterator* result = nullptr;
  if (read_options.read_tier == kPersistedTier) {
    return NewErrorIterator(Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators."));
  }

  assert(column_family);

  if (read_options.timestamp) {
    const Status s = FailIfTsMismatchCf(
        column_family, *(read_options.timestamp), /*ts_for_read=*/true);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  } else {
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  }

  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  assert(cfd != nullptr);
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  if (read_options.tailing) {
#ifdef ROCKSDB_LITE
    // not supported in lite version
    result = nullptr;

#else
    SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
    auto iter = new ForwardIterator(this, read_options, cfd, sv,
                                    /* allow_unprepared_value */ true);
    result = NewDBIterator(
        env_, read_options, *cfd->ioptions(), sv->mutable_cf_options,
        cfd->user_comparator(), iter, sv->current, kMaxSequenceNumber,
        sv->mutable_cf_options.max_sequential_skip_in_iterations, read_callback,
        this, cfd);
#endif
  } else {
    // Note: no need to consider the special case of
    // last_seq_same_as_publish_seq_==false since NewIterator is overridden in
    // WritePreparedTxnDB
    result = NewIteratorImpl(read_options, cfd,
                             (read_options.snapshot != nullptr)
                                 ? read_options.snapshot->GetSequenceNumber()
                                 : kMaxSequenceNumber,
                             read_callback);
  }
  return result;
}

ArenaWrappedDBIter* DBImpl::NewIteratorImpl(const ReadOptions& read_options,
                                            ColumnFamilyData* cfd,
                                            SequenceNumber snapshot,
                                            ReadCallback* read_callback,
                                            bool expose_blob_index,
                                            bool allow_refresh) {
  SuperVersion* sv = cfd->GetReferencedSuperVersion(this);

  TEST_SYNC_POINT("DBImpl::NewIterator:1");
  TEST_SYNC_POINT("DBImpl::NewIterator:2");

  if (snapshot == kMaxSequenceNumber) {
    // Note that the snapshot is assigned AFTER referencing the super
    // version because otherwise a flush happening in between may compact away
    // data for the snapshot, so the reader would see neither data that was be
    // visible to the snapshot before compaction nor the newer data inserted
    // afterwards.
    // Note that the super version might not contain all the data available
    // to this snapshot, but in that case it can see all the data in the
    // super version, which is a valid consistent state after the user
    // calls NewIterator().
    snapshot = versions_->LastSequence();
    TEST_SYNC_POINT("DBImpl::NewIterator:3");
    TEST_SYNC_POINT("DBImpl::NewIterator:4");
  }

  // Try to generate a DB iterator tree in continuous memory area to be
  // cache friendly. Here is an example of result:
  // +-------------------------------+
  // |                               |
  // | ArenaWrappedDBIter            |
  // |  +                            |
  // |  +---> Inner Iterator   ------------+
  // |  |                            |     |
  // |  |    +-- -- -- -- -- -- -- --+     |
  // |  +--- | Arena                 |     |
  // |       |                       |     |
  // |          Allocated Memory:    |     |
  // |       |   +-------------------+     |
  // |       |   | DBIter            | <---+
  // |           |  +                |
  // |       |   |  +-> iter_  ------------+
  // |       |   |                   |     |
  // |       |   +-------------------+     |
  // |       |   | MergingIterator   | <---+
  // |           |  +                |
  // |       |   |  +->child iter1  ------------+
  // |       |   |  |                |          |
  // |           |  +->child iter2  ----------+ |
  // |       |   |  |                |        | |
  // |       |   |  +->child iter3  --------+ | |
  // |           |                   |      | | |
  // |       |   +-------------------+      | | |
  // |       |   | Iterator1         | <--------+
  // |       |   +-------------------+      | |
  // |       |   | Iterator2         | <------+
  // |       |   +-------------------+      |
  // |       |   | Iterator3         | <----+
  // |       |   +-------------------+
  // |       |                       |
  // +-------+-----------------------+
  //
  // ArenaWrappedDBIter inlines an arena area where all the iterators in
  // the iterator tree are allocated in the order of being accessed when
  // querying.
  // Laying out the iterators in the order of being accessed makes it more
  // likely that any iterator pointer is close to the iterator it points to so
  // that they are likely to be in the same cache line and/or page.
  ArenaWrappedDBIter* db_iter = NewArenaWrappedDbIterator(
      env_, read_options, *cfd->ioptions(), sv->mutable_cf_options, sv->current,
      snapshot, sv->mutable_cf_options.max_sequential_skip_in_iterations,
      sv->version_number, read_callback, this, cfd, expose_blob_index,
      read_options.snapshot != nullptr ? false : allow_refresh);

  InternalIterator* internal_iter = NewInternalIterator(
      db_iter->GetReadOptions(), cfd, sv, db_iter->GetArena(), snapshot,
      /* allow_unprepared_value */ true, db_iter);
  db_iter->SetIterUnderDBIter(internal_iter);

  return db_iter;
}

Status DBImpl::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (read_options.managed) {
    return Status::NotSupported("Managed iterator is not supported anymore.");
  }
  if (read_options.read_tier == kPersistedTier) {
    return Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators.");
  }

  if (read_options.timestamp) {
    for (auto* cf : column_families) {
      assert(cf);
      const Status s = FailIfTsMismatchCf(cf, *(read_options.timestamp),
                                          /*ts_for_read=*/true);
      if (!s.ok()) {
        return s;
      }
    }
  } else {
    for (auto* cf : column_families) {
      assert(cf);
      const Status s = FailIfCfHasTs(cf);
      if (!s.ok()) {
        return s;
      }
    }
  }

  ReadCallback* read_callback = nullptr;  // No read callback provided.
  iterators->clear();
  iterators->reserve(column_families.size());
  if (read_options.tailing) {
#ifdef ROCKSDB_LITE
    return Status::InvalidArgument(
        "Tailing iterator not supported in RocksDB lite");
#else
    for (auto cfh : column_families) {
      auto cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
      SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
      auto iter = new ForwardIterator(this, read_options, cfd, sv,
                                      /* allow_unprepared_value */ true);
      iterators->push_back(NewDBIterator(
          env_, read_options, *cfd->ioptions(), sv->mutable_cf_options,
          cfd->user_comparator(), iter, sv->current, kMaxSequenceNumber,
          sv->mutable_cf_options.max_sequential_skip_in_iterations,
          read_callback, this, cfd));
    }
#endif
  } else {
    // Note: no need to consider the special case of
    // last_seq_same_as_publish_seq_==false since NewIterators is overridden in
    // WritePreparedTxnDB
    auto snapshot = read_options.snapshot != nullptr
                        ? read_options.snapshot->GetSequenceNumber()
                        : versions_->LastSequence();
    for (size_t i = 0; i < column_families.size(); ++i) {
      auto* cfd =
          static_cast_with_check<ColumnFamilyHandleImpl>(column_families[i])
              ->cfd();
      iterators->push_back(
          NewIteratorImpl(read_options, cfd, snapshot, read_callback));
    }
  }

  return Status::OK();
}

const Snapshot* DBImpl::GetSnapshot() { return GetSnapshotImpl(false); }

#ifndef ROCKSDB_LITE
const Snapshot* DBImpl::GetSnapshotForWriteConflictBoundary() {
  return GetSnapshotImpl(true);
}
#endif  // ROCKSDB_LITE

std::pair<Status, std::shared_ptr<const Snapshot>>
DBImpl::CreateTimestampedSnapshot(SequenceNumber snapshot_seq, uint64_t ts) {
  assert(ts != std::numeric_limits<uint64_t>::max());

  auto ret = CreateTimestampedSnapshotImpl(snapshot_seq, ts, /*lock=*/true);
  return ret;
}

std::shared_ptr<const SnapshotImpl> DBImpl::GetTimestampedSnapshot(
    uint64_t ts) const {
  InstrumentedMutexLock lock_guard(&mutex_);
  return timestamped_snapshots_.GetSnapshot(ts);
}

void DBImpl::ReleaseTimestampedSnapshotsOlderThan(uint64_t ts,
                                                  size_t* remaining_total_ss) {
  autovector<std::shared_ptr<const SnapshotImpl>> snapshots_to_release;
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    timestamped_snapshots_.ReleaseSnapshotsOlderThan(ts, snapshots_to_release);
  }
  snapshots_to_release.clear();

  if (remaining_total_ss) {
    InstrumentedMutexLock lock_guard(&mutex_);
    *remaining_total_ss = static_cast<size_t>(snapshots_.count());
  }
}

Status DBImpl::GetTimestampedSnapshots(
    uint64_t ts_lb, uint64_t ts_ub,
    std::vector<std::shared_ptr<const Snapshot>>& timestamped_snapshots) const {
  if (ts_lb >= ts_ub) {
    return Status::InvalidArgument(
        "timestamp lower bound must be smaller than upper bound");
  }
  timestamped_snapshots.clear();
  InstrumentedMutexLock lock_guard(&mutex_);
  timestamped_snapshots_.GetSnapshots(ts_lb, ts_ub, timestamped_snapshots);
  return Status::OK();
}

SnapshotImpl* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary,
                                      bool lock) {
  int64_t unix_time = 0;
  immutable_db_options_.clock->GetCurrentTime(&unix_time)
      .PermitUncheckedError();  // Ignore error
  SnapshotImpl* s = new SnapshotImpl;

  if (lock) {
    mutex_.Lock();
  } else {
    mutex_.AssertHeld();
  }
  // returns null if the underlying memtable does not support snapshot.
  if (!is_snapshot_supported_) {
    if (lock) {
      mutex_.Unlock();
    }
    delete s;
    return nullptr;
  }
  auto snapshot_seq = GetLastPublishedSequence();
  SnapshotImpl* snapshot =
      snapshots_.New(s, snapshot_seq, unix_time, is_write_conflict_boundary);
  if (lock) {
    mutex_.Unlock();
  }
  return snapshot;
}

std::pair<Status, std::shared_ptr<const SnapshotImpl>>
DBImpl::CreateTimestampedSnapshotImpl(SequenceNumber snapshot_seq, uint64_t ts,
                                      bool lock) {
  int64_t unix_time = 0;
  immutable_db_options_.clock->GetCurrentTime(&unix_time)
      .PermitUncheckedError();  // Ignore error
  SnapshotImpl* s = new SnapshotImpl;

  const bool need_update_seq = (snapshot_seq != kMaxSequenceNumber);

  if (lock) {
    mutex_.Lock();
  } else {
    mutex_.AssertHeld();
  }
  // returns null if the underlying memtable does not support snapshot.
  if (!is_snapshot_supported_) {
    if (lock) {
      mutex_.Unlock();
    }
    delete s;
    return std::make_pair(
        Status::NotSupported("Memtable does not support snapshot"), nullptr);
  }

  // Caller is not write thread, thus didn't provide a valid snapshot_seq.
  // Obtain seq from db.
  if (!need_update_seq) {
    snapshot_seq = GetLastPublishedSequence();
  }

  std::shared_ptr<const SnapshotImpl> latest =
      timestamped_snapshots_.GetSnapshot(std::numeric_limits<uint64_t>::max());

  // If there is already a latest timestamped snapshot, then we need to do some
  // checks.
  if (latest) {
    uint64_t latest_snap_ts = latest->GetTimestamp();
    SequenceNumber latest_snap_seq = latest->GetSequenceNumber();
    assert(latest_snap_seq <= snapshot_seq);
    bool needs_create_snap = true;
    Status status;
    std::shared_ptr<const SnapshotImpl> ret;
    if (latest_snap_ts > ts) {
      // A snapshot created later cannot have smaller timestamp than a previous
      // timestamped snapshot.
      needs_create_snap = false;
      std::ostringstream oss;
      oss << "snapshot exists with larger timestamp " << latest_snap_ts << " > "
          << ts;
      status = Status::InvalidArgument(oss.str());
    } else if (latest_snap_ts == ts) {
      if (latest_snap_seq == snapshot_seq) {
        // We are requesting the same sequence number and timestamp, thus can
        // safely reuse (share) the current latest timestamped snapshot.
        needs_create_snap = false;
        ret = latest;
      } else if (latest_snap_seq < snapshot_seq) {
        // There may have been writes to the database since the latest
        // timestamped snapshot, yet we are still requesting the same
        // timestamp. In this case, we cannot create the new timestamped
        // snapshot.
        needs_create_snap = false;
        std::ostringstream oss;
        oss << "Allocated seq is " << snapshot_seq
            << ", while snapshot exists with smaller seq " << latest_snap_seq
            << " but same timestamp " << ts;
        status = Status::InvalidArgument(oss.str());
      }
    }
    if (!needs_create_snap) {
      if (lock) {
        mutex_.Unlock();
      }
      delete s;
      return std::make_pair(status, ret);
    } else {
      status.PermitUncheckedError();
    }
  }

  SnapshotImpl* snapshot =
      snapshots_.New(s, snapshot_seq, unix_time,
                     /*is_write_conflict_boundary=*/true, ts);

  std::shared_ptr<const SnapshotImpl> ret(
      snapshot,
      std::bind(&DBImpl::ReleaseSnapshot, this, std::placeholders::_1));
  timestamped_snapshots_.AddSnapshot(ret);

  // Caller is from write thread, and we need to update database's sequence
  // number.
  if (need_update_seq) {
    assert(versions_);
    if (last_seq_same_as_publish_seq_) {
      versions_->SetLastSequence(snapshot_seq);
    } else {
      // TODO: support write-prepared/write-unprepared transactions with two
      // write queues.
      assert(false);
    }
  }

  if (lock) {
    mutex_.Unlock();
  }
  return std::make_pair(Status::OK(), ret);
}

namespace {
using CfdList = autovector<ColumnFamilyData*, 2>;
bool CfdListContains(const CfdList& list, ColumnFamilyData* cfd) {
  for (const ColumnFamilyData* t : list) {
    if (t == cfd) {
      return true;
    }
  }
  return false;
}
}  //  namespace

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  if (s == nullptr) {
    // DBImpl::GetSnapshot() can return nullptr when snapshot
    // not supported by specifying the condition:
    // inplace_update_support enabled.
    return;
  }
  const SnapshotImpl* casted_s = reinterpret_cast<const SnapshotImpl*>(s);
  {
    InstrumentedMutexLock l(&mutex_);
    snapshots_.Delete(casted_s);
    uint64_t oldest_snapshot;
    if (snapshots_.empty()) {
      oldest_snapshot = GetLastPublishedSequence();
    } else {
      oldest_snapshot = snapshots_.oldest()->number_;
    }
    // Avoid to go through every column family by checking a global threshold
    // first.
    if (oldest_snapshot > bottommost_files_mark_threshold_) {
      CfdList cf_scheduled;
      for (auto* cfd : *versions_->GetColumnFamilySet()) {
        if (!cfd->ioptions()->allow_ingest_behind) {
          cfd->current()->storage_info()->UpdateOldestSnapshot(oldest_snapshot);
          if (!cfd->current()
                   ->storage_info()
                   ->BottommostFilesMarkedForCompaction()
                   .empty()) {
            SchedulePendingCompaction(cfd);
            MaybeScheduleFlushOrCompaction();
            cf_scheduled.push_back(cfd);
          }
        }
      }

      // Calculate a new threshold, skipping those CFs where compactions are
      // scheduled. We do not do the same pass as the previous loop because
      // mutex might be unlocked during the loop, making the result inaccurate.
      SequenceNumber new_bottommost_files_mark_threshold = kMaxSequenceNumber;
      for (auto* cfd : *versions_->GetColumnFamilySet()) {
        if (CfdListContains(cf_scheduled, cfd) ||
            cfd->ioptions()->allow_ingest_behind) {
          continue;
        }
        new_bottommost_files_mark_threshold = std::min(
            new_bottommost_files_mark_threshold,
            cfd->current()->storage_info()->bottommost_files_mark_threshold());
      }
      bottommost_files_mark_threshold_ = new_bottommost_files_mark_threshold;
    }
  }
  delete casted_s;
}

#ifndef ROCKSDB_LITE
Status DBImpl::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                        TablePropertiesCollection* props) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfAllTables(props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

Status DBImpl::GetPropertiesOfTablesInRange(ColumnFamilyHandle* column_family,
                                            const Range* range, std::size_t n,
                                            TablePropertiesCollection* props) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfTablesInRange(range, n, props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

#endif  // ROCKSDB_LITE

const std::string& DBImpl::GetName() const { return dbname_; }

Env* DBImpl::GetEnv() const { return env_; }

FileSystem* DB::GetFileSystem() const {
  const auto& fs = GetEnv()->GetFileSystem();
  return fs.get();
}

FileSystem* DBImpl::GetFileSystem() const {
  return immutable_db_options_.fs.get();
}

SystemClock* DBImpl::GetSystemClock() const {
  return immutable_db_options_.clock;
}

#ifndef ROCKSDB_LITE

Status DBImpl::StartIOTrace(const TraceOptions& trace_options,
                            std::unique_ptr<TraceWriter>&& trace_writer) {
  assert(trace_writer != nullptr);
  return io_tracer_->StartIOTrace(GetSystemClock(), trace_options,
                                  std::move(trace_writer));
}

Status DBImpl::EndIOTrace() {
  io_tracer_->EndIOTrace();
  return Status::OK();
}

#endif  // ROCKSDB_LITE

Options DBImpl::GetOptions(ColumnFamilyHandle* column_family) const {
  InstrumentedMutexLock l(&mutex_);
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  return Options(BuildDBOptions(immutable_db_options_, mutable_db_options_),
                 cfh->cfd()->GetLatestCFOptions());
}

DBOptions DBImpl::GetDBOptions() const {
  InstrumentedMutexLock l(&mutex_);
  return BuildDBOptions(immutable_db_options_, mutable_db_options_);
}

bool DBImpl::GetProperty(ColumnFamilyHandle* column_family,
                         const Slice& property, std::string* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_int) {
    uint64_t int_value;
    bool ret_value =
        GetIntPropertyInternal(cfd, *property_info, false, &int_value);
    if (ret_value) {
      *value = std::to_string(int_value);
    }
    return ret_value;
  } else if (property_info->handle_string) {
    if (property_info->need_out_of_mutex) {
      return cfd->internal_stats()->GetStringProperty(*property_info, property,
                                                      value);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetStringProperty(*property_info, property,
                                                      value);
    }
  } else if (property_info->handle_string_dbimpl) {
    if (property_info->need_out_of_mutex) {
      return (this->*(property_info->handle_string_dbimpl))(value);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return (this->*(property_info->handle_string_dbimpl))(value);
    }
  }
  // Shouldn't reach here since exactly one of handle_string and handle_int
  // should be non-nullptr.
  assert(false);
  return false;
}

bool DBImpl::GetMapProperty(ColumnFamilyHandle* column_family,
                            const Slice& property,
                            std::map<std::string, std::string>* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_map) {
    if (property_info->need_out_of_mutex) {
      return cfd->internal_stats()->GetMapProperty(*property_info, property,
                                                   value);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetMapProperty(*property_info, property,
                                                   value);
    }
  }
  // If we reach this point it means that handle_map is not provided for the
  // requested property
  return false;
}

bool DBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, uint64_t* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }
  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  return GetIntPropertyInternal(cfd, *property_info, false, value);
}

bool DBImpl::GetIntPropertyInternal(ColumnFamilyData* cfd,
                                    const DBPropertyInfo& property_info,
                                    bool is_locked, uint64_t* value) {
  assert(property_info.handle_int != nullptr);
  if (!property_info.need_out_of_mutex) {
    if (is_locked) {
      mutex_.AssertHeld();
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    }
  } else {
    SuperVersion* sv = nullptr;
    if (is_locked) {
      mutex_.Unlock();
    }
    sv = GetAndRefSuperVersion(cfd);

    bool ret = cfd->internal_stats()->GetIntPropertyOutOfMutex(
        property_info, sv->current, value);

    ReturnAndCleanupSuperVersion(cfd, sv);
    if (is_locked) {
      mutex_.Lock();
    }

    return ret;
  }
}

bool DBImpl::GetPropertyHandleOptionsStatistics(std::string* value) {
  assert(value != nullptr);
  Statistics* statistics = immutable_db_options_.stats;
  if (!statistics) {
    return false;
  }
  *value = statistics->ToString();
  return true;
}

#ifndef ROCKSDB_LITE
Status DBImpl::ResetStats() {
  InstrumentedMutexLock l(&mutex_);
  for (auto* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->initialized()) {
      cfd->internal_stats()->Clear();
    }
  }
  return Status::OK();
}
#endif  // ROCKSDB_LITE

bool DBImpl::GetAggregatedIntProperty(const Slice& property,
                                      uint64_t* aggregated_value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }

  uint64_t sum = 0;
  bool ret = true;
  {
    // Needs mutex to protect the list of column families.
    InstrumentedMutexLock l(&mutex_);
    uint64_t value;
    for (auto* cfd : versions_->GetRefedColumnFamilySet()) {
      if (!cfd->initialized()) {
        continue;
      }
      ret = GetIntPropertyInternal(cfd, *property_info, true, &value);
      // GetIntPropertyInternal may release db mutex and re-acquire it.
      mutex_.AssertHeld();
      if (ret) {
        sum += value;
      } else {
        ret = false;
        break;
      }
    }
  }
  *aggregated_value = sum;
  return ret;
}

SuperVersion* DBImpl::GetAndRefSuperVersion(ColumnFamilyData* cfd) {
  // TODO(ljin): consider using GetReferencedSuperVersion() directly
  return cfd->GetThreadLocalSuperVersion(this);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
SuperVersion* DBImpl::GetAndRefSuperVersion(uint32_t column_family_id) {
  auto column_family_set = versions_->GetColumnFamilySet();
  auto cfd = column_family_set->GetColumnFamily(column_family_id);
  if (!cfd) {
    return nullptr;
  }

  return GetAndRefSuperVersion(cfd);
}

void DBImpl::CleanupSuperVersion(SuperVersion* sv) {
  // Release SuperVersion
  if (sv->Unref()) {
    bool defer_purge = immutable_db_options().avoid_unnecessary_blocking_io;
    {
      InstrumentedMutexLock l(&mutex_);
      sv->Cleanup();
      if (defer_purge) {
        AddSuperVersionsToFreeQueue(sv);
        SchedulePurge();
      }
    }
    if (!defer_purge) {
      delete sv;
    }
    RecordTick(stats_, NUMBER_SUPERVERSION_CLEANUPS);
  }
  RecordTick(stats_, NUMBER_SUPERVERSION_RELEASES);
}

void DBImpl::ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd,
                                          SuperVersion* sv) {
  if (!cfd->ReturnThreadLocalSuperVersion(sv)) {
    CleanupSuperVersion(sv);
  }
}

// REQUIRED: this function should only be called on the write thread.
void DBImpl::ReturnAndCleanupSuperVersion(uint32_t column_family_id,
                                          SuperVersion* sv) {
  auto column_family_set = versions_->GetColumnFamilySet();
  auto cfd = column_family_set->GetColumnFamily(column_family_id);

  // If SuperVersion is held, and we successfully fetched a cfd using
  // GetAndRefSuperVersion(), it must still exist.
  assert(cfd != nullptr);
  ReturnAndCleanupSuperVersion(cfd, sv);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
ColumnFamilyHandle* DBImpl::GetColumnFamilyHandle(uint32_t column_family_id) {
  ColumnFamilyMemTables* cf_memtables = column_family_memtables_.get();

  if (!cf_memtables->Seek(column_family_id)) {
    return nullptr;
  }

  return cf_memtables->GetColumnFamilyHandle();
}

// REQUIRED: mutex is NOT held.
std::unique_ptr<ColumnFamilyHandle> DBImpl::GetColumnFamilyHandleUnlocked(
    uint32_t column_family_id) {
  InstrumentedMutexLock l(&mutex_);

  auto* cfd =
      versions_->GetColumnFamilySet()->GetColumnFamily(column_family_id);
  if (cfd == nullptr) {
    return nullptr;
  }

  return std::unique_ptr<ColumnFamilyHandleImpl>(
      new ColumnFamilyHandleImpl(cfd, this, &mutex_));
}

void DBImpl::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                         const Range& range,
                                         uint64_t* const count,
                                         uint64_t* const size) {
  ColumnFamilyHandleImpl* cfh =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  // Convert user_key into a corresponding internal key.
  InternalKey k1(range.start, kMaxSequenceNumber, kValueTypeForSeek);
  InternalKey k2(range.limit, kMaxSequenceNumber, kValueTypeForSeek);
  MemTable::MemTableStats memStats =
      sv->mem->ApproximateStats(k1.Encode(), k2.Encode());
  MemTable::MemTableStats immStats =
      sv->imm->ApproximateStats(k1.Encode(), k2.Encode());
  *count = memStats.count + immStats.count;
  *size = memStats.size + immStats.size;

  ReturnAndCleanupSuperVersion(cfd, sv);
}

Status DBImpl::GetApproximateSizes(const SizeApproximationOptions& options,
                                   ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes) {
  if (!options.include_memtables && !options.include_files) {
    return Status::InvalidArgument("Invalid options");
  }

  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();

  Version* v;
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);
  v = sv->current;

  for (int i = 0; i < n; i++) {
    Slice start = range[i].start;
    Slice limit = range[i].limit;

    // Add timestamp if needed
    std::string start_with_ts, limit_with_ts;
    if (ts_sz > 0) {
      // Maximum timestamp means including all key with any timestamp
      AppendKeyWithMaxTimestamp(&start_with_ts, start, ts_sz);
      // Append a maximum timestamp as the range limit is exclusive:
      // [start, limit)
      AppendKeyWithMaxTimestamp(&limit_with_ts, limit, ts_sz);
      start = start_with_ts;
      limit = limit_with_ts;
    }
    // Convert user_key into a corresponding internal key.
    InternalKey k1(start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(limit, kMaxSequenceNumber, kValueTypeForSeek);
    sizes[i] = 0;
    if (options.include_files) {
      sizes[i] += versions_->ApproximateSize(
          options, v, k1.Encode(), k2.Encode(), /*start_level=*/0,
          /*end_level=*/-1, TableReaderCaller::kUserApproximateSize);
    }
    if (options.include_memtables) {
      sizes[i] += sv->mem->ApproximateStats(k1.Encode(), k2.Encode()).size;
      sizes[i] += sv->imm->ApproximateStats(k1.Encode(), k2.Encode()).size;
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);
  return Status::OK();
}

std::list<uint64_t>::iterator
DBImpl::CaptureCurrentFileNumberInPendingOutputs() {
  // We need to remember the iterator of our insert, because after the
  // background job is done, we need to remove that element from
  // pending_outputs_.
  pending_outputs_.push_back(versions_->current_next_file_number());
  auto pending_outputs_inserted_elem = pending_outputs_.end();
  --pending_outputs_inserted_elem;
  return pending_outputs_inserted_elem;
}

void DBImpl::ReleaseFileNumberFromPendingOutputs(
    std::unique_ptr<std::list<uint64_t>::iterator>& v) {
  if (v.get() != nullptr) {
    pending_outputs_.erase(*v.get());
    v.reset();
  }
}

#ifndef ROCKSDB_LITE
Status DBImpl::GetUpdatesSince(
    SequenceNumber seq, std::unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {
  RecordTick(stats_, GET_UPDATES_SINCE_CALLS);
  if (seq_per_batch_) {
    return Status::NotSupported(
        "This API is not yet compatible with write-prepared/write-unprepared "
        "transactions");
  }
  if (seq > versions_->LastSequence()) {
    return Status::NotFound("Requested sequence not yet written in the db");
  }
  return wal_manager_.GetUpdatesSince(seq, iter, read_options, versions_.get());
}

Status DBImpl::DeleteFile(std::string name) {
  uint64_t number;
  FileType type;
  WalFileType log_type;
  if (!ParseFileName(name, &number, &type, &log_type) ||
      (type != kTableFile && type != kWalFile)) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log, "DeleteFile %s failed.\n",
                    name.c_str());
    return Status::InvalidArgument("Invalid file name");
  }

  if (type == kWalFile) {
    // Only allow deleting archived log files
    if (log_type != kArchivedLogFile) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "DeleteFile %s failed - not archived log.\n",
                      name.c_str());
      return Status::NotSupported("Delete only supported for archived logs");
    }
    Status status = wal_manager_.DeleteFile(name, number);
    if (!status.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "DeleteFile %s failed -- %s.\n", name.c_str(),
                      status.ToString().c_str());
    }
    return status;
  }

  Status status;
  int level;
  FileMetaData* metadata;
  ColumnFamilyData* cfd;
  VersionEdit edit;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  {
    InstrumentedMutexLock l(&mutex_);
    status = versions_->GetMetadataForFile(number, &level, &metadata, &cfd);
    if (!status.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "DeleteFile %s failed. File not found\n", name.c_str());
      job_context.Clean();
      return Status::InvalidArgument("File not found");
    }
    assert(level < cfd->NumberLevels());

    // If the file is being compacted no need to delete.
    if (metadata->being_compacted) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "DeleteFile %s Skipped. File about to be compacted\n",
                     name.c_str());
      job_context.Clean();
      return Status::OK();
    }

    // Only the files in the last level can be deleted externally.
    // This is to make sure that any deletion tombstones are not
    // lost. Check that the level passed is the last level.
    auto* vstoreage = cfd->current()->storage_info();
    for (int i = level + 1; i < cfd->NumberLevels(); i++) {
      if (vstoreage->NumLevelFiles(i) != 0) {
        ROCKS_LOG_WARN(immutable_db_options_.info_log,
                       "DeleteFile %s FAILED. File not in last level\n",
                       name.c_str());
        job_context.Clean();
        return Status::InvalidArgument("File not in last level");
      }
    }
    // if level == 0, it has to be the oldest file
    if (level == 0 &&
        vstoreage->LevelFiles(0).back()->fd.GetNumber() != number) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "DeleteFile %s failed ---"
                     " target file in level 0 must be the oldest.",
                     name.c_str());
      job_context.Clean();
      return Status::InvalidArgument("File in level 0, but not oldest");
    }
    edit.SetColumnFamily(cfd->GetID());
    edit.DeleteFile(level, number);
    status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                    &edit, &mutex_, directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(cfd,
                                         &job_context.superversion_contexts[0],
                                         *cfd->GetLatestMutableCFOptions());
    }
    FindObsoleteFiles(&job_context, false);
  }  // lock released here

  LogFlush(immutable_db_options_.info_log);
  // remove files outside the db-lock
  if (job_context.HaveSomethingToDelete()) {
    // Call PurgeObsoleteFiles() without holding mutex.
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
  return status;
}

Status DBImpl::DeleteFilesInRanges(ColumnFamilyHandle* column_family,
                                   const RangePtr* ranges, size_t n,
                                   bool include_end) {
  Status status = Status::OK();
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  VersionEdit edit;
  std::set<FileMetaData*> deleted_files;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  {
    InstrumentedMutexLock l(&mutex_);
    Version* input_version = cfd->current();

    auto* vstorage = input_version->storage_info();
    for (size_t r = 0; r < n; r++) {
      auto begin = ranges[r].start, end = ranges[r].limit;
      for (int i = 1; i < cfd->NumberLevels(); i++) {
        if (vstorage->LevelFiles(i).empty() ||
            !vstorage->OverlapInLevel(i, begin, end)) {
          continue;
        }
        std::vector<FileMetaData*> level_files;
        InternalKey begin_storage, end_storage, *begin_key, *end_key;
        if (begin == nullptr) {
          begin_key = nullptr;
        } else {
          begin_storage.SetMinPossibleForUserKey(*begin);
          begin_key = &begin_storage;
        }
        if (end == nullptr) {
          end_key = nullptr;
        } else {
          end_storage.SetMaxPossibleForUserKey(*end);
          end_key = &end_storage;
        }

        vstorage->GetCleanInputsWithinInterval(
            i, begin_key, end_key, &level_files, -1 /* hint_index */,
            nullptr /* file_index */);
        FileMetaData* level_file;
        for (uint32_t j = 0; j < level_files.size(); j++) {
          level_file = level_files[j];
          if (level_file->being_compacted) {
            continue;
          }
          if (deleted_files.find(level_file) != deleted_files.end()) {
            continue;
          }
          if (!include_end && end != nullptr &&
              cfd->user_comparator()->Compare(level_file->largest.user_key(),
                                              *end) == 0) {
            continue;
          }
          edit.SetColumnFamily(cfd->GetID());
          edit.DeleteFile(i, level_file->fd.GetNumber());
          deleted_files.insert(level_file);
          level_file->being_compacted = true;
        }
        vstorage->ComputeCompactionScore(*cfd->ioptions(),
                                         *cfd->GetLatestMutableCFOptions());
      }
    }
    if (edit.GetDeletedFiles().empty()) {
      job_context.Clean();
      return status;
    }
    input_version->Ref();
    status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                    &edit, &mutex_, directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(cfd,
                                         &job_context.superversion_contexts[0],
                                         *cfd->GetLatestMutableCFOptions());
    }
    for (auto* deleted_file : deleted_files) {
      deleted_file->being_compacted = false;
    }
    input_version->Unref();
    FindObsoleteFiles(&job_context, false);
  }  // lock released here

  LogFlush(immutable_db_options_.info_log);
  // remove files outside the db-lock
  if (job_context.HaveSomethingToDelete()) {
    // Call PurgeObsoleteFiles() without holding mutex.
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
  return status;
}

void DBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  InstrumentedMutexLock l(&mutex_);
  versions_->GetLiveFilesMetaData(metadata);
}

Status DBImpl::GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) {
  InstrumentedMutexLock l(&mutex_);
  return versions_->GetLiveFilesChecksumInfo(checksum_list);
}

void DBImpl::GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                                     ColumnFamilyMetaData* cf_meta) {
  assert(column_family);
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  auto* sv = GetAndRefSuperVersion(cfd);
  {
    // Without mutex, Version::GetColumnFamilyMetaData will have data race with
    // Compaction::MarkFilesBeingCompacted. One solution is to use mutex, but
    // this may cause regression. An alternative is to make
    // FileMetaData::being_compacted atomic, but it will make FileMetaData
    // non-copy-able. Another option is to separate these variables from
    // original FileMetaData struct, and this requires re-organization of data
    // structures. For now, we take the easy approach. If
    // DB::GetColumnFamilyMetaData is not called frequently, the regression
    // should not be big. We still need to keep an eye on it.
    InstrumentedMutexLock l(&mutex_);
    sv->current->GetColumnFamilyMetaData(cf_meta);
  }
  ReturnAndCleanupSuperVersion(cfd, sv);
}

void DBImpl::GetAllColumnFamilyMetaData(
    std::vector<ColumnFamilyMetaData>* metadata) {
  InstrumentedMutexLock l(&mutex_);
  for (auto cfd : *(versions_->GetColumnFamilySet())) {
    {
      metadata->emplace_back();
      cfd->current()->GetColumnFamilyMetaData(&metadata->back());
    }
  }
}

#endif  // ROCKSDB_LITE

Status DBImpl::CheckConsistency() {
  mutex_.AssertHeld();
  std::vector<LiveFileMetaData> metadata;
  versions_->GetLiveFilesMetaData(&metadata);
  TEST_SYNC_POINT("DBImpl::CheckConsistency:AfterGetLiveFilesMetaData");

  std::string corruption_messages;

  if (immutable_db_options_.skip_checking_sst_file_sizes_on_db_open) {
    // Instead of calling GetFileSize() for each expected file, call
    // GetChildren() for the DB directory and check that all expected files
    // are listed, without checking their sizes.
    // Since sst files might be in different directories, do it for each
    // directory separately.
    std::map<std::string, std::vector<std::string>> files_by_directory;
    for (const auto& md : metadata) {
      // md.name has a leading "/". Remove it.
      std::string fname = md.name;
      if (!fname.empty() && fname[0] == '/') {
        fname = fname.substr(1);
      }
      files_by_directory[md.db_path].push_back(fname);
    }

    IOOptions io_opts;
    io_opts.do_not_recurse = true;
    for (const auto& dir_files : files_by_directory) {
      std::string directory = dir_files.first;
      std::vector<std::string> existing_files;
      Status s = fs_->GetChildren(directory, io_opts, &existing_files,
                                  /*IODebugContext*=*/nullptr);
      if (!s.ok()) {
        corruption_messages +=
            "Can't list files in " + directory + ": " + s.ToString() + "\n";
        continue;
      }
      std::sort(existing_files.begin(), existing_files.end());

      for (const std::string& fname : dir_files.second) {
        if (!std::binary_search(existing_files.begin(), existing_files.end(),
                                fname) &&
            !std::binary_search(existing_files.begin(), existing_files.end(),
                                Rocks2LevelTableFileName(fname))) {
          corruption_messages +=
              "Missing sst file " + fname + " in " + directory + "\n";
        }
      }
    }
  } else {
    for (const auto& md : metadata) {
      // md.name has a leading "/".
      std::string file_path = md.db_path + md.name;

      uint64_t fsize = 0;
      TEST_SYNC_POINT("DBImpl::CheckConsistency:BeforeGetFileSize");
      Status s = env_->GetFileSize(file_path, &fsize);
      if (!s.ok() &&
          env_->GetFileSize(Rocks2LevelTableFileName(file_path), &fsize).ok()) {
        s = Status::OK();
      }
      if (!s.ok()) {
        corruption_messages +=
            "Can't access " + md.name + ": " + s.ToString() + "\n";
      } else if (fsize != md.size) {
        corruption_messages += "Sst file size mismatch: " + file_path +
                               ". Size recorded in manifest " +
                               std::to_string(md.size) + ", actual size " +
                               std::to_string(fsize) + "\n";
      }
    }
  }

  if (corruption_messages.size() == 0) {
    return Status::OK();
  } else {
    return Status::Corruption(corruption_messages);
  }
}

Status DBImpl::GetDbIdentity(std::string& identity) const {
  identity.assign(db_id_);
  return Status::OK();
}

Status DBImpl::GetDbIdentityFromIdentityFile(std::string* identity) const {
  std::string idfilename = IdentityFileName(dbname_);
  const FileOptions soptions;

  Status s = ReadFileToString(fs_.get(), idfilename, identity);
  if (!s.ok()) {
    return s;
  }

  // If last character is '\n' remove it from identity. (Old implementations
  // of Env::GenerateUniqueId() would include a trailing '\n'.)
  if (identity->size() > 0 && identity->back() == '\n') {
    identity->pop_back();
  }
  return s;
}

Status DBImpl::GetDbSessionId(std::string& session_id) const {
  session_id.assign(db_session_id_);
  return Status::OK();
}

namespace {
SemiStructuredUniqueIdGen* DbSessionIdGen() {
  static SemiStructuredUniqueIdGen gen;
  return &gen;
}
}  // namespace

void DBImpl::TEST_ResetDbSessionIdGen() { DbSessionIdGen()->Reset(); }

std::string DBImpl::GenerateDbSessionId(Env*) {
  // See SemiStructuredUniqueIdGen for its desirable properties.
  auto gen = DbSessionIdGen();

  uint64_t lo, hi;
  gen->GenerateNext(&hi, &lo);
  if (lo == 0) {
    // Avoid emitting session ID with lo==0, so that SST unique
    // IDs can be more easily ensured non-zero
    gen->GenerateNext(&hi, &lo);
    assert(lo != 0);
  }
  return EncodeSessionId(hi, lo);
}

void DBImpl::SetDbSessionId() {
  db_session_id_ = GenerateDbSessionId(env_);
  TEST_SYNC_POINT_CALLBACK("DBImpl::SetDbSessionId", &db_session_id_);
}

// Default implementation -- returns not supported status
Status DB::CreateColumnFamily(const ColumnFamilyOptions& /*cf_options*/,
                              const std::string& /*column_family_name*/,
                              ColumnFamilyHandle** /*handle*/) {
  return Status::NotSupported("");
}

Status DB::CreateColumnFamilies(
    const ColumnFamilyOptions& /*cf_options*/,
    const std::vector<std::string>& /*column_family_names*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("");
}

Status DB::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& /*column_families*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("");
}

Status DB::DropColumnFamily(ColumnFamilyHandle* /*column_family*/) {
  return Status::NotSupported("");
}

Status DB::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& /*column_families*/) {
  return Status::NotSupported("");
}

Status DB::DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) {
  if (DefaultColumnFamily() == column_family) {
    return Status::InvalidArgument(
        "Cannot destroy the handle returned by DefaultColumnFamily()");
  }
  delete column_family;
  return Status::OK();
}

DB::~DB() {}

Status DBImpl::Close() {
  InstrumentedMutexLock closing_lock_guard(&closing_mutex_);
  if (closed_) {
    return closing_status_;
  }

  {
    const Status s = MaybeReleaseTimestampedSnapshotsAndCheck();
    if (!s.ok()) {
      return s;
    }
  }

  closing_status_ = CloseImpl();
  closed_ = true;
  return closing_status_;
}

Status DB::ListColumnFamilies(const DBOptions& db_options,
                              const std::string& name,
                              std::vector<std::string>* column_families) {
  const std::shared_ptr<FileSystem>& fs = db_options.env->GetFileSystem();
  return VersionSet::ListColumnFamilies(column_families, name, fs.get());
}

Snapshot::~Snapshot() {}

Status DestroyDB(const std::string& dbname, const Options& options,
                 const std::vector<ColumnFamilyDescriptor>& column_families) {
  ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;
  std::vector<std::string> filenames;
  bool wal_in_db_path = soptions.IsWalDirSameAsDBPath();

  // Reset the logger because it holds a handle to the
  // log file and prevents cleanup and directory removal
  soptions.info_log.reset();
  IOOptions io_opts;
  // Ignore error in case directory does not exist
  soptions.fs
      ->GetChildren(dbname, io_opts, &filenames,
                    /*IODebugContext*=*/nullptr)
      .PermitUncheckedError();

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    InfoLogPrefix info_log_prefix(!soptions.db_log_dir.empty(), dbname);
    for (const auto& fname : filenames) {
      if (ParseFileName(fname, &number, info_log_prefix.prefix, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        std::string path_to_delete = dbname + "/" + fname;
        if (type == kMetaDatabase) {
          del = DestroyDB(path_to_delete, options);
        } else if (type == kTableFile || type == kWalFile ||
                   type == kBlobFile) {
          del = DeleteDBFile(
              &soptions, path_to_delete, dbname,
              /*force_bg=*/false,
              /*force_fg=*/(type == kWalFile) ? !wal_in_db_path : false);
        } else {
          del = env->DeleteFile(path_to_delete);
        }
        if (!del.ok() && result.ok()) {
          result = del;
        }
      }
    }

    std::set<std::string> paths;
    for (const DbPath& db_path : options.db_paths) {
      paths.insert(db_path.path);
    }
    for (const ColumnFamilyDescriptor& cf : column_families) {
      for (const DbPath& cf_path : cf.options.cf_paths) {
        paths.insert(cf_path.path);
      }
    }

    for (const auto& path : paths) {
      if (soptions.fs
              ->GetChildren(path, io_opts, &filenames,
                            /*IODebugContext*=*/nullptr)
              .ok()) {
        for (const auto& fname : filenames) {
          if (ParseFileName(fname, &number, &type) &&
              (type == kTableFile ||
               type == kBlobFile)) {  // Lock file will be deleted at end
            std::string file_path = path + "/" + fname;
            Status del = DeleteDBFile(&soptions, file_path, dbname,
                                      /*force_bg=*/false, /*force_fg=*/false);
            if (!del.ok() && result.ok()) {
              result = del;
            }
          }
        }
        // TODO: Should we return an error if we cannot delete the directory?
        env->DeleteDir(path).PermitUncheckedError();
      }
    }

    std::vector<std::string> walDirFiles;
    std::string archivedir = ArchivalDirectory(dbname);
    bool wal_dir_exists = false;
    if (!soptions.IsWalDirSameAsDBPath(dbname)) {
      wal_dir_exists =
          soptions.fs
              ->GetChildren(soptions.wal_dir, io_opts, &walDirFiles,
                            /*IODebugContext*=*/nullptr)
              .ok();
      archivedir = ArchivalDirectory(soptions.wal_dir);
    }

    // Archive dir may be inside wal dir or dbname and should be
    // processed and removed before those otherwise we have issues
    // removing them
    std::vector<std::string> archiveFiles;
    if (soptions.fs
            ->GetChildren(archivedir, io_opts, &archiveFiles,
                          /*IODebugContext*=*/nullptr)
            .ok()) {
      // Delete archival files.
      for (const auto& file : archiveFiles) {
        if (ParseFileName(file, &number, &type) && type == kWalFile) {
          Status del =
              DeleteDBFile(&soptions, archivedir + "/" + file, archivedir,
                           /*force_bg=*/false, /*force_fg=*/!wal_in_db_path);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        }
      }
      // Ignore error in case dir contains other files
      env->DeleteDir(archivedir).PermitUncheckedError();
    }

    // Delete log files in the WAL dir
    if (wal_dir_exists) {
      for (const auto& file : walDirFiles) {
        if (ParseFileName(file, &number, &type) && type == kWalFile) {
          Status del =
              DeleteDBFile(&soptions, LogFileName(soptions.wal_dir, number),
                           soptions.wal_dir, /*force_bg=*/false,
                           /*force_fg=*/!wal_in_db_path);
          if (!del.ok() && result.ok()) {
            result = del;
          }
        }
      }
      // Ignore error in case dir contains other files
      env->DeleteDir(soptions.wal_dir).PermitUncheckedError();
    }

    // Ignore error since state is already gone
    env->UnlockFile(lock).PermitUncheckedError();
    env->DeleteFile(lockname).PermitUncheckedError();

    // sst_file_manager holds a ref to the logger. Make sure the logger is
    // gone before trying to remove the directory.
    soptions.sst_file_manager.reset();

    // Ignore error in case dir contains other files
    env->DeleteDir(dbname).PermitUncheckedError();
    ;
  }
  return result;
}

Status DBImpl::WriteOptionsFile(bool need_mutex_lock,
                                bool need_enter_write_thread) {
#ifndef ROCKSDB_LITE
  WriteThread::Writer w;
  if (need_mutex_lock) {
    mutex_.Lock();
  } else {
    mutex_.AssertHeld();
  }
  if (need_enter_write_thread) {
    write_thread_.EnterUnbatched(&w, &mutex_);
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;

  // This part requires mutex to protect the column family options
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cf_names.push_back(cfd->GetName());
    cf_opts.push_back(cfd->GetLatestCFOptions());
  }

  // Unlock during expensive operations.  New writes cannot get here
  // because the single write thread ensures all new writes get queued.
  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  mutex_.Unlock();

  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:1");
  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:2");
  TEST_SYNC_POINT_CALLBACK("DBImpl::WriteOptionsFile:PersistOptions",
                           &db_options);

  std::string file_name =
      TempOptionsFileName(GetName(), versions_->NewFileNumber());
  Status s = PersistRocksDBOptions(db_options, cf_names, cf_opts, file_name,
                                   fs_.get());

  if (s.ok()) {
    s = RenameTempFileToOptionsFile(file_name);
  }
  // restore lock
  if (!need_mutex_lock) {
    mutex_.Lock();
  }
  if (need_enter_write_thread) {
    write_thread_.ExitUnbatched(&w);
  }
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Unnable to persist options -- %s", s.ToString().c_str());
    if (immutable_db_options_.fail_if_options_file_error) {
      return Status::IOError("Unable to persist options.",
                             s.ToString().c_str());
    }
  }
#else
  (void)need_mutex_lock;
  (void)need_enter_write_thread;
#endif  // !ROCKSDB_LITE
  return Status::OK();
}

#ifndef ROCKSDB_LITE
namespace {
void DeleteOptionsFilesHelper(const std::map<uint64_t, std::string>& filenames,
                              const size_t num_files_to_keep,
                              const std::shared_ptr<Logger>& info_log,
                              Env* env) {
  if (filenames.size() <= num_files_to_keep) {
    return;
  }
  for (auto iter = std::next(filenames.begin(), num_files_to_keep);
       iter != filenames.end(); ++iter) {
    if (!env->DeleteFile(iter->second).ok()) {
      ROCKS_LOG_WARN(info_log, "Unable to delete options file %s",
                     iter->second.c_str());
    }
  }
}
}  // namespace
#endif  // !ROCKSDB_LITE

Status DBImpl::DeleteObsoleteOptionsFiles() {
#ifndef ROCKSDB_LITE
  std::vector<std::string> filenames;
  // use ordered map to store keep the filenames sorted from the newest
  // to the oldest.
  std::map<uint64_t, std::string> options_filenames;
  Status s;
  IOOptions io_opts;
  io_opts.do_not_recurse = true;
  s = fs_->GetChildren(GetName(), io_opts, &filenames,
                       /*IODebugContext*=*/nullptr);
  if (!s.ok()) {
    return s;
  }
  for (auto& filename : filenames) {
    uint64_t file_number;
    FileType type;
    if (ParseFileName(filename, &file_number, &type) && type == kOptionsFile) {
      options_filenames.insert(
          {std::numeric_limits<uint64_t>::max() - file_number,
           GetName() + "/" + filename});
    }
  }

  // Keeps the latest 2 Options file
  const size_t kNumOptionsFilesKept = 2;
  DeleteOptionsFilesHelper(options_filenames, kNumOptionsFilesKept,
                           immutable_db_options_.info_log, GetEnv());
  return Status::OK();
#else
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

Status DBImpl::RenameTempFileToOptionsFile(const std::string& file_name) {
#ifndef ROCKSDB_LITE
  Status s;

  uint64_t options_file_number = versions_->NewFileNumber();
  std::string options_file_name =
      OptionsFileName(GetName(), options_file_number);
  uint64_t options_file_size = 0;
  s = GetEnv()->GetFileSize(file_name, &options_file_size);
  if (s.ok()) {
    // Retry if the file name happen to conflict with an existing one.
    s = GetEnv()->RenameFile(file_name, options_file_name);
    std::unique_ptr<FSDirectory> dir_obj;
    if (s.ok()) {
      s = fs_->NewDirectory(GetName(), IOOptions(), &dir_obj, nullptr);
    }
    if (s.ok()) {
      s = dir_obj->FsyncWithDirOptions(IOOptions(), nullptr,
                                       DirFsyncOptions(options_file_name));
    }
    if (s.ok()) {
      Status temp_s = dir_obj->Close(IOOptions(), nullptr);
      // The default Close() could return "NotSupproted" and we bypass it
      // if it is not impelmented. Detailed explanations can be found in
      // db/db_impl/db_impl.h
      if (!temp_s.ok()) {
        if (temp_s.IsNotSupported()) {
          temp_s.PermitUncheckedError();
        } else {
          s = temp_s;
        }
      }
    }
  }
  if (s.ok()) {
    InstrumentedMutexLock l(&mutex_);
    versions_->options_file_number_ = options_file_number;
    versions_->options_file_size_ = options_file_size;
  }

  if (0 == disable_delete_obsolete_files_) {
    // TODO: Should we check for errors here?
    DeleteObsoleteOptionsFiles().PermitUncheckedError();
  }
  return s;
#else
  (void)file_name;
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

#ifdef ROCKSDB_USING_THREAD_STATUS

void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::NewColumnFamilyInfo(this, cfd, cfd->GetName(),
                                          cfd->ioptions()->env);
  }
}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseColumnFamilyInfo(cfd);
  }
}

void DBImpl::EraseThreadStatusDbInfo() const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseDatabaseInfo(this);
  }
}

#else
void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* /*cfd*/) const {}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* /*cfd*/) const {}

void DBImpl::EraseThreadStatusDbInfo() const {}
#endif  // ROCKSDB_USING_THREAD_STATUS

//
// A global method that can dump out the build version
void DumpRocksDBBuildVersion(Logger* log) {
  ROCKS_LOG_HEADER(log, "RocksDB version: %s\n",
                   GetRocksVersionAsString().c_str());
  const auto& props = GetRocksBuildProperties();
  const auto& sha = props.find("rocksdb_build_git_sha");
  if (sha != props.end()) {
    ROCKS_LOG_HEADER(log, "Git sha %s", sha->second.c_str());
  }
  const auto date = props.find("rocksdb_build_date");
  if (date != props.end()) {
    ROCKS_LOG_HEADER(log, "Compile date %s", date->second.c_str());
  }
}

#ifndef ROCKSDB_LITE
SequenceNumber DBImpl::GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                         bool include_history) {
  // Find the earliest sequence number that we know we can rely on reading
  // from the memtable without needing to check sst files.
  SequenceNumber earliest_seq =
      sv->imm->GetEarliestSequenceNumber(include_history);
  if (earliest_seq == kMaxSequenceNumber) {
    earliest_seq = sv->mem->GetEarliestSequenceNumber();
  }
  assert(sv->mem->GetEarliestSequenceNumber() >= earliest_seq);

  return earliest_seq;
}

Status DBImpl::GetLatestSequenceForKey(
    SuperVersion* sv, const Slice& key, bool cache_only,
    SequenceNumber lower_bound_seq, SequenceNumber* seq, std::string* timestamp,
    bool* found_record_for_key, bool* is_blob_index) {
  Status s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;

  ReadOptions read_options;
  SequenceNumber current_seq = versions_->LastSequence();

  ColumnFamilyData* cfd = sv->cfd;
  assert(cfd);
  const Comparator* const ucmp = cfd->user_comparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  std::string ts_buf;
  if (ts_sz > 0) {
    assert(timestamp);
    ts_buf.assign(ts_sz, '\xff');
  } else {
    assert(!timestamp);
  }
  Slice ts(ts_buf);

  LookupKey lkey(key, current_seq, ts_sz == 0 ? nullptr : &ts);

  *seq = kMaxSequenceNumber;
  *found_record_for_key = false;

  // Check if there is a record for this key in the latest memtable
  sv->mem->Get(lkey, /*value=*/nullptr, /*columns=*/nullptr, timestamp, &s,
               &merge_context, &max_covering_tombstone_seq, seq, read_options,
               false /* immutable_memtable */, nullptr /*read_callback*/,
               is_blob_index);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Unexpected status returned from MemTable::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }
  assert(!ts_sz ||
         (*seq != kMaxSequenceNumber &&
          *timestamp != std::string(ts_sz, '\xff')) ||
         (*seq == kMaxSequenceNumber && timestamp->empty()));

  TEST_SYNC_POINT_CALLBACK("DBImpl::GetLatestSequenceForKey:mem", timestamp);

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check immutable memtables
    *found_record_for_key = true;
    return Status::OK();
  }

  SequenceNumber lower_bound_in_mem = sv->mem->GetEarliestSequenceNumber();
  if (lower_bound_in_mem != kMaxSequenceNumber &&
      lower_bound_in_mem < lower_bound_seq) {
    *found_record_for_key = false;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->Get(lkey, /*value=*/nullptr, /*columns=*/nullptr, timestamp, &s,
               &merge_context, &max_covering_tombstone_seq, seq, read_options,
               nullptr /*read_callback*/, is_blob_index);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Unexpected status returned from MemTableList::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  assert(!ts_sz ||
         (*seq != kMaxSequenceNumber &&
          *timestamp != std::string(ts_sz, '\xff')) ||
         (*seq == kMaxSequenceNumber && timestamp->empty()));

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check memtable history
    *found_record_for_key = true;
    return Status::OK();
  }

  SequenceNumber lower_bound_in_imm = sv->imm->GetEarliestSequenceNumber();
  if (lower_bound_in_imm != kMaxSequenceNumber &&
      lower_bound_in_imm < lower_bound_seq) {
    *found_record_for_key = false;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->GetFromHistory(lkey, /*value=*/nullptr, /*columns=*/nullptr,
                          timestamp, &s, &merge_context,
                          &max_covering_tombstone_seq, seq, read_options,
                          is_blob_index);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(
        immutable_db_options_.info_log,
        "Unexpected status returned from MemTableList::GetFromHistory: %s\n",
        s.ToString().c_str());

    return s;
  }

  assert(!ts_sz ||
         (*seq != kMaxSequenceNumber &&
          *timestamp != std::string(ts_sz, '\xff')) ||
         (*seq == kMaxSequenceNumber && timestamp->empty()));

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check SST files
    assert(0 == ts_sz || *timestamp != std::string(ts_sz, '\xff'));
    *found_record_for_key = true;
    return Status::OK();
  }

  // We could do a sv->imm->GetEarliestSequenceNumber(/*include_history*/ true)
  // check here to skip the history if possible. But currently the caller
  // already does that. Maybe we should move the logic here later.

  // TODO(agiardullo): possible optimization: consider checking cached
  // SST files if cache_only=true?
  if (!cache_only) {
    // Check tables
    PinnedIteratorsManager pinned_iters_mgr;
    sv->current->Get(read_options, lkey, /*value=*/nullptr, /*columns=*/nullptr,
                     timestamp, &s, &merge_context, &max_covering_tombstone_seq,
                     &pinned_iters_mgr, nullptr /* value_found */,
                     found_record_for_key, seq, nullptr /*read_callback*/,
                     is_blob_index);

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      // unexpected error reading SST files
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Unexpected status returned from Version::Get: %s\n",
                      s.ToString().c_str());
    }
  }

  return s;
}

Status DBImpl::IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& ingestion_options) {
  IngestExternalFileArg arg;
  arg.column_family = column_family;
  arg.external_files = external_files;
  arg.options = ingestion_options;
  return IngestExternalFiles({arg});
}

Status DBImpl::IngestExternalFiles(
    const std::vector<IngestExternalFileArg>& args) {
  if (args.empty()) {
    return Status::InvalidArgument("ingestion arg list is empty");
  }
  {
    std::unordered_set<ColumnFamilyHandle*> unique_cfhs;
    for (const auto& arg : args) {
      if (arg.column_family == nullptr) {
        return Status::InvalidArgument("column family handle is null");
      } else if (unique_cfhs.count(arg.column_family) > 0) {
        return Status::InvalidArgument(
            "ingestion args have duplicate column families");
      }
      unique_cfhs.insert(arg.column_family);
    }
  }
  // Ingest multiple external SST files atomically.
  const size_t num_cfs = args.size();
  for (size_t i = 0; i != num_cfs; ++i) {
    if (args[i].external_files.empty()) {
      char err_msg[128] = {0};
      snprintf(err_msg, 128, "external_files[%zu] is empty", i);
      return Status::InvalidArgument(err_msg);
    }
  }
  for (const auto& arg : args) {
    const IngestExternalFileOptions& ingest_opts = arg.options;
    if (ingest_opts.ingest_behind &&
        !immutable_db_options_.allow_ingest_behind) {
      return Status::InvalidArgument(
          "can't ingest_behind file in DB with allow_ingest_behind=false");
    }
  }

  // TODO (yanqin) maybe handle the case in which column_families have
  // duplicates
  std::unique_ptr<std::list<uint64_t>::iterator> pending_output_elem;
  size_t total = 0;
  for (const auto& arg : args) {
    total += arg.external_files.size();
  }
  uint64_t next_file_number = 0;
  Status status = ReserveFileNumbersBeforeIngestion(
      static_cast<ColumnFamilyHandleImpl*>(args[0].column_family)->cfd(), total,
      pending_output_elem, &next_file_number);
  if (!status.ok()) {
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
    return status;
  }

  std::vector<ExternalSstFileIngestionJob> ingestion_jobs;
  for (const auto& arg : args) {
    auto* cfd = static_cast<ColumnFamilyHandleImpl*>(arg.column_family)->cfd();
    ingestion_jobs.emplace_back(versions_.get(), cfd, immutable_db_options_,
                                mutable_db_options_, file_options_, &snapshots_,
                                arg.options, &directories_, &event_logger_,
                                io_tracer_);
  }

  // TODO(yanqin) maybe make jobs run in parallel
  uint64_t start_file_number = next_file_number;
  for (size_t i = 1; i != num_cfs; ++i) {
    start_file_number += args[i - 1].external_files.size();
    auto* cfd =
        static_cast<ColumnFamilyHandleImpl*>(args[i].column_family)->cfd();
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
    Status es = ingestion_jobs[i].Prepare(
        args[i].external_files, args[i].files_checksums,
        args[i].files_checksum_func_names, args[i].file_temperature,
        start_file_number, super_version);
    // capture first error only
    if (!es.ok() && status.ok()) {
      status = es;
    }
    CleanupSuperVersion(super_version);
  }
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeLastJobPrepare:0");
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeLastJobPrepare:1");
  {
    auto* cfd =
        static_cast<ColumnFamilyHandleImpl*>(args[0].column_family)->cfd();
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
    Status es = ingestion_jobs[0].Prepare(
        args[0].external_files, args[0].files_checksums,
        args[0].files_checksum_func_names, args[0].file_temperature,
        next_file_number, super_version);
    if (!es.ok()) {
      status = es;
    }
    CleanupSuperVersion(super_version);
  }
  if (!status.ok()) {
    for (size_t i = 0; i != num_cfs; ++i) {
      ingestion_jobs[i].Cleanup(status);
    }
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
    return status;
  }

  std::vector<SuperVersionContext> sv_ctxs;
  for (size_t i = 0; i != num_cfs; ++i) {
    sv_ctxs.emplace_back(true /* create_superversion */);
  }
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeJobsRun:0");
  TEST_SYNC_POINT("DBImpl::IngestExternalFiles:BeforeJobsRun:1");
  TEST_SYNC_POINT("DBImpl::AddFile:Start");
  {
    InstrumentedMutexLock l(&mutex_);
    TEST_SYNC_POINT("DBImpl::AddFile:MutexLock");

    // Stop writes to the DB by entering both write threads
    WriteThread::Writer w;
    write_thread_.EnterUnbatched(&w, &mutex_);
    WriteThread::Writer nonmem_w;
    if (two_write_queues_) {
      nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
    }

    // When unordered_write is enabled, the keys are writing to memtable in an
    // unordered way. If the ingestion job checks memtable key range before the
    // key landing in memtable, the ingestion job may skip the necessary
    // memtable flush.
    // So wait here to ensure there is no pending write to memtable.
    WaitForPendingWrites();

    num_running_ingest_file_ += static_cast<int>(num_cfs);
    TEST_SYNC_POINT("DBImpl::IngestExternalFile:AfterIncIngestFileCounter");

    bool at_least_one_cf_need_flush = false;
    std::vector<bool> need_flush(num_cfs, false);
    for (size_t i = 0; i != num_cfs; ++i) {
      auto* cfd =
          static_cast<ColumnFamilyHandleImpl*>(args[i].column_family)->cfd();
      if (cfd->IsDropped()) {
        // TODO (yanqin) investigate whether we should abort ingestion or
        // proceed with other non-dropped column families.
        status = Status::InvalidArgument(
            "cannot ingest an external file into a dropped CF");
        break;
      }
      bool tmp = false;
      status = ingestion_jobs[i].NeedsFlush(&tmp, cfd->GetSuperVersion());
      need_flush[i] = tmp;
      at_least_one_cf_need_flush = (at_least_one_cf_need_flush || tmp);
      if (!status.ok()) {
        break;
      }
    }
    TEST_SYNC_POINT_CALLBACK("DBImpl::IngestExternalFile:NeedFlush",
                             &at_least_one_cf_need_flush);

    if (status.ok() && at_least_one_cf_need_flush) {
      FlushOptions flush_opts;
      flush_opts.allow_write_stall = true;
      if (immutable_db_options_.atomic_flush) {
        autovector<ColumnFamilyData*> cfds_to_flush;
        SelectColumnFamiliesForAtomicFlush(&cfds_to_flush);
        mutex_.Unlock();
        status = AtomicFlushMemTables(cfds_to_flush, flush_opts,
                                      FlushReason::kExternalFileIngestion,
                                      true /* entered_write_thread */);
        mutex_.Lock();
      } else {
        for (size_t i = 0; i != num_cfs; ++i) {
          if (need_flush[i]) {
            mutex_.Unlock();
            auto* cfd =
                static_cast<ColumnFamilyHandleImpl*>(args[i].column_family)
                    ->cfd();
            status = FlushMemTable(cfd, flush_opts,
                                   FlushReason::kExternalFileIngestion,
                                   true /* entered_write_thread */);
            mutex_.Lock();
            if (!status.ok()) {
              break;
            }
          }
        }
      }
    }
    // Run ingestion jobs.
    if (status.ok()) {
      for (size_t i = 0; i != num_cfs; ++i) {
        mutex_.AssertHeld();
        status = ingestion_jobs[i].Run();
        if (!status.ok()) {
          break;
        }
        ingestion_jobs[i].RegisterRange();
      }
    }
    if (status.ok()) {
      autovector<ColumnFamilyData*> cfds_to_commit;
      autovector<const MutableCFOptions*> mutable_cf_options_list;
      autovector<autovector<VersionEdit*>> edit_lists;
      uint32_t num_entries = 0;
      for (size_t i = 0; i != num_cfs; ++i) {
        auto* cfd =
            static_cast<ColumnFamilyHandleImpl*>(args[i].column_family)->cfd();
        if (cfd->IsDropped()) {
          continue;
        }
        cfds_to_commit.push_back(cfd);
        mutable_cf_options_list.push_back(cfd->GetLatestMutableCFOptions());
        autovector<VersionEdit*> edit_list;
        edit_list.push_back(ingestion_jobs[i].edit());
        edit_lists.push_back(edit_list);
        ++num_entries;
      }
      // Mark the version edits as an atomic group if the number of version
      // edits exceeds 1.
      if (cfds_to_commit.size() > 1) {
        for (auto& edits : edit_lists) {
          assert(edits.size() == 1);
          edits[0]->MarkAtomicGroup(--num_entries);
        }
        assert(0 == num_entries);
      }
      status =
          versions_->LogAndApply(cfds_to_commit, mutable_cf_options_list,
                                 edit_lists, &mutex_, directories_.GetDbDir());
      // It is safe to update VersionSet last seqno here after LogAndApply since
      // LogAndApply persists last sequence number from VersionEdits,
      // which are from file's largest seqno and not from VersionSet.
      //
      // It is necessary to update last seqno here since LogAndApply releases
      // mutex when persisting MANIFEST file, and the snapshots taken during
      // that period will not be stable if VersionSet last seqno is updated
      // before LogAndApply.
      int consumed_seqno_count =
          ingestion_jobs[0].ConsumedSequenceNumbersCount();
      for (size_t i = 1; i != num_cfs; ++i) {
        consumed_seqno_count =
            std::max(consumed_seqno_count,
                     ingestion_jobs[i].ConsumedSequenceNumbersCount());
      }
      if (consumed_seqno_count > 0) {
        const SequenceNumber last_seqno = versions_->LastSequence();
        versions_->SetLastAllocatedSequence(last_seqno + consumed_seqno_count);
        versions_->SetLastPublishedSequence(last_seqno + consumed_seqno_count);
        versions_->SetLastSequence(last_seqno + consumed_seqno_count);
      }
    }

    for (auto& job : ingestion_jobs) {
      job.UnregisterRange();
    }

    if (status.ok()) {
      for (size_t i = 0; i != num_cfs; ++i) {
        auto* cfd =
            static_cast<ColumnFamilyHandleImpl*>(args[i].column_family)->cfd();
        if (!cfd->IsDropped()) {
          InstallSuperVersionAndScheduleWork(cfd, &sv_ctxs[i],
                                             *cfd->GetLatestMutableCFOptions());
#ifndef NDEBUG
          if (0 == i && num_cfs > 1) {
            TEST_SYNC_POINT(
                "DBImpl::IngestExternalFiles:InstallSVForFirstCF:0");
            TEST_SYNC_POINT(
                "DBImpl::IngestExternalFiles:InstallSVForFirstCF:1");
          }
#endif  // !NDEBUG
        }
      }
    } else if (versions_->io_status().IsIOError()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      const IOStatus& io_s = versions_->io_status();
      // Should handle return error?
      error_handler_.SetBGError(io_s, BackgroundErrorReason::kManifestWrite);
    }

    // Resume writes to the DB
    if (two_write_queues_) {
      nonmem_write_thread_.ExitUnbatched(&nonmem_w);
    }
    write_thread_.ExitUnbatched(&w);

    if (status.ok()) {
      for (auto& job : ingestion_jobs) {
        job.UpdateStats();
      }
    }
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
    num_running_ingest_file_ -= static_cast<int>(num_cfs);
    if (0 == num_running_ingest_file_) {
      bg_cv_.SignalAll();
    }
    TEST_SYNC_POINT("DBImpl::AddFile:MutexUnlock");
  }
  // mutex_ is unlocked here

  // Cleanup
  for (size_t i = 0; i != num_cfs; ++i) {
    sv_ctxs[i].Clean();
    // This may rollback jobs that have completed successfully. This is
    // intended for atomicity.
    ingestion_jobs[i].Cleanup(status);
  }
  if (status.ok()) {
    for (size_t i = 0; i != num_cfs; ++i) {
      auto* cfd =
          static_cast<ColumnFamilyHandleImpl*>(args[i].column_family)->cfd();
      if (!cfd->IsDropped()) {
        NotifyOnExternalFileIngested(cfd, ingestion_jobs[i]);
      }
    }
  }
  return status;
}

Status DBImpl::CreateColumnFamilyWithImport(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    const ImportColumnFamilyOptions& import_options,
    const ExportImportFilesMetaData& metadata, ColumnFamilyHandle** handle) {
  assert(handle != nullptr);
  assert(*handle == nullptr);
  std::string cf_comparator_name = options.comparator->Name();
  if (cf_comparator_name != metadata.db_comparator_name) {
    return Status::InvalidArgument("Comparator name mismatch");
  }

  // Create column family.
  auto status = CreateColumnFamily(options, column_family_name, handle);
  if (!status.ok()) {
    return status;
  }

  // Import sst files from metadata.
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(*handle);
  auto cfd = cfh->cfd();
  ImportColumnFamilyJob import_job(versions_.get(), cfd, immutable_db_options_,
                                   file_options_, import_options,
                                   metadata.files, io_tracer_);

  SuperVersionContext dummy_sv_ctx(/* create_superversion */ true);
  VersionEdit dummy_edit;
  uint64_t next_file_number = 0;
  std::unique_ptr<std::list<uint64_t>::iterator> pending_output_elem;
  {
    // Lock db mutex
    InstrumentedMutexLock l(&mutex_);
    if (error_handler_.IsDBStopped()) {
      // Don't import files when there is a bg_error
      status = error_handler_.GetBGError();
    }

    // Make sure that bg cleanup wont delete the files that we are importing
    pending_output_elem.reset(new std::list<uint64_t>::iterator(
        CaptureCurrentFileNumberInPendingOutputs()));

    if (status.ok()) {
      // If crash happen after a hard link established, Recover function may
      // reuse the file number that has already assigned to the internal file,
      // and this will overwrite the external file. To protect the external
      // file, we have to make sure the file number will never being reused.
      next_file_number = versions_->FetchAddFileNumber(metadata.files.size());
      auto cf_options = cfd->GetLatestMutableCFOptions();
      status = versions_->LogAndApply(cfd, *cf_options, &dummy_edit, &mutex_,
                                      directories_.GetDbDir());
      if (status.ok()) {
        InstallSuperVersionAndScheduleWork(cfd, &dummy_sv_ctx, *cf_options);
      }
    }
  }
  dummy_sv_ctx.Clean();

  if (status.ok()) {
    SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
    status = import_job.Prepare(next_file_number, sv);
    CleanupSuperVersion(sv);
  }

  if (status.ok()) {
    SuperVersionContext sv_context(true /*create_superversion*/);
    {
      // Lock db mutex
      InstrumentedMutexLock l(&mutex_);

      // Stop writes to the DB by entering both write threads
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      WriteThread::Writer nonmem_w;
      if (two_write_queues_) {
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
      }

      num_running_ingest_file_++;
      assert(!cfd->IsDropped());
      mutex_.AssertHeld();
      status = import_job.Run();

      // Install job edit [Mutex will be unlocked here]
      if (status.ok()) {
        auto cf_options = cfd->GetLatestMutableCFOptions();
        status = versions_->LogAndApply(cfd, *cf_options, import_job.edit(),
                                        &mutex_, directories_.GetDbDir());
        if (status.ok()) {
          InstallSuperVersionAndScheduleWork(cfd, &sv_context, *cf_options);
        }
      }

      // Resume writes to the DB
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
      write_thread_.ExitUnbatched(&w);

      num_running_ingest_file_--;
      if (num_running_ingest_file_ == 0) {
        bg_cv_.SignalAll();
      }
    }
    // mutex_ is unlocked here

    sv_context.Clean();
  }

  {
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
  }

  import_job.Cleanup(status);
  if (!status.ok()) {
    Status temp_s = DropColumnFamily(*handle);
    if (!temp_s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "DropColumnFamily failed with error %s",
                      temp_s.ToString().c_str());
    }
    // Always returns Status::OK()
    temp_s = DestroyColumnFamilyHandle(*handle);
    assert(temp_s.ok());
    *handle = nullptr;
  }
  return status;
}

Status DBImpl::VerifyFileChecksums(const ReadOptions& read_options) {
  return VerifyChecksumInternal(read_options, /*use_file_checksum=*/true);
}

Status DBImpl::VerifyChecksum(const ReadOptions& read_options) {
  return VerifyChecksumInternal(read_options, /*use_file_checksum=*/false);
}

Status DBImpl::VerifyChecksumInternal(const ReadOptions& read_options,
                                      bool use_file_checksum) {
  // `bytes_read` stat is enabled based on compile-time support and cannot
  // be dynamically toggled. So we do not need to worry about `PerfLevel`
  // here, unlike many other `IOStatsContext` / `PerfContext` stats.
  uint64_t prev_bytes_read = IOSTATS(bytes_read);

  Status s;

  if (use_file_checksum) {
    FileChecksumGenFactory* const file_checksum_gen_factory =
        immutable_db_options_.file_checksum_gen_factory.get();
    if (!file_checksum_gen_factory) {
      s = Status::InvalidArgument(
          "Cannot verify file checksum if options.file_checksum_gen_factory is "
          "null");
      return s;
    }
  }

  // TODO: simplify using GetRefedColumnFamilySet?
  std::vector<ColumnFamilyData*> cfd_list;
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped() && cfd->initialized()) {
        cfd->Ref();
        cfd_list.push_back(cfd);
      }
    }
  }
  std::vector<SuperVersion*> sv_list;
  for (auto cfd : cfd_list) {
    sv_list.push_back(cfd->GetReferencedSuperVersion(this));
  }

  for (auto& sv : sv_list) {
    VersionStorageInfo* vstorage = sv->current->storage_info();
    ColumnFamilyData* cfd = sv->current->cfd();
    Options opts;
    if (!use_file_checksum) {
      InstrumentedMutexLock l(&mutex_);
      opts = Options(BuildDBOptions(immutable_db_options_, mutable_db_options_),
                     cfd->GetLatestCFOptions());
    }
    for (int i = 0; i < vstorage->num_non_empty_levels() && s.ok(); i++) {
      for (size_t j = 0; j < vstorage->LevelFilesBrief(i).num_files && s.ok();
           j++) {
        const auto& fd_with_krange = vstorage->LevelFilesBrief(i).files[j];
        const auto& fd = fd_with_krange.fd;
        const FileMetaData* fmeta = fd_with_krange.file_metadata;
        assert(fmeta);
        std::string fname = TableFileName(cfd->ioptions()->cf_paths,
                                          fd.GetNumber(), fd.GetPathId());
        if (use_file_checksum) {
          s = VerifyFullFileChecksum(fmeta->file_checksum,
                                     fmeta->file_checksum_func_name, fname,
                                     read_options);
        } else {
          s = ROCKSDB_NAMESPACE::VerifySstFileChecksum(
              opts, file_options_, read_options, fname, fd.largest_seqno);
        }
        RecordTick(stats_, VERIFY_CHECKSUM_READ_BYTES,
                   IOSTATS(bytes_read) - prev_bytes_read);
        prev_bytes_read = IOSTATS(bytes_read);
      }
    }

    if (s.ok() && use_file_checksum) {
      const auto& blob_files = vstorage->GetBlobFiles();
      for (const auto& meta : blob_files) {
        assert(meta);

        const uint64_t blob_file_number = meta->GetBlobFileNumber();

        const std::string blob_file_name = BlobFileName(
            cfd->ioptions()->cf_paths.front().path, blob_file_number);
        s = VerifyFullFileChecksum(meta->GetChecksumValue(),
                                   meta->GetChecksumMethod(), blob_file_name,
                                   read_options);
        RecordTick(stats_, VERIFY_CHECKSUM_READ_BYTES,
                   IOSTATS(bytes_read) - prev_bytes_read);
        prev_bytes_read = IOSTATS(bytes_read);
        if (!s.ok()) {
          break;
        }
      }
    }
    if (!s.ok()) {
      break;
    }
  }

  bool defer_purge = immutable_db_options().avoid_unnecessary_blocking_io;
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto sv : sv_list) {
      if (sv && sv->Unref()) {
        sv->Cleanup();
        if (defer_purge) {
          AddSuperVersionsToFreeQueue(sv);
        } else {
          delete sv;
        }
      }
    }
    if (defer_purge) {
      SchedulePurge();
    }
    for (auto cfd : cfd_list) {
      cfd->UnrefAndTryDelete();
    }
  }
  RecordTick(stats_, VERIFY_CHECKSUM_READ_BYTES,
             IOSTATS(bytes_read) - prev_bytes_read);
  return s;
}

Status DBImpl::VerifyFullFileChecksum(const std::string& file_checksum_expected,
                                      const std::string& func_name_expected,
                                      const std::string& fname,
                                      const ReadOptions& read_options) {
  Status s;
  if (file_checksum_expected == kUnknownFileChecksum) {
    return s;
  }
  std::string file_checksum;
  std::string func_name;
  s = ROCKSDB_NAMESPACE::GenerateOneFileChecksum(
      fs_.get(), fname, immutable_db_options_.file_checksum_gen_factory.get(),
      func_name_expected, &file_checksum, &func_name,
      read_options.readahead_size, immutable_db_options_.allow_mmap_reads,
      io_tracer_, immutable_db_options_.rate_limiter.get(),
      read_options.rate_limiter_priority);
  if (s.ok()) {
    assert(func_name_expected == func_name);
    if (file_checksum != file_checksum_expected) {
      std::ostringstream oss;
      oss << fname << " file checksum mismatch, ";
      oss << "expecting "
          << Slice(file_checksum_expected).ToString(/*hex=*/true);
      oss << ", but actual " << Slice(file_checksum).ToString(/*hex=*/true);
      s = Status::Corruption(oss.str());
      TEST_SYNC_POINT_CALLBACK("DBImpl::VerifyFullFileChecksum:mismatch", &s);
    }
  }
  return s;
}

void DBImpl::NotifyOnExternalFileIngested(
    ColumnFamilyData* cfd, const ExternalSstFileIngestionJob& ingestion_job) {
  if (immutable_db_options_.listeners.empty()) {
    return;
  }

  for (const IngestedFileInfo& f : ingestion_job.files_to_ingest()) {
    ExternalFileIngestionInfo info;
    info.cf_name = cfd->GetName();
    info.external_file_path = f.external_file_path;
    info.internal_file_path = f.internal_file_path;
    info.global_seqno = f.assigned_seqno;
    info.table_properties = f.table_properties;
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnExternalFileIngested(this, info);
    }
  }
}

Status DBImpl::StartTrace(const TraceOptions& trace_options,
                          std::unique_ptr<TraceWriter>&& trace_writer) {
  InstrumentedMutexLock lock(&trace_mutex_);
  tracer_.reset(new Tracer(immutable_db_options_.clock, trace_options,
                           std::move(trace_writer)));
  return Status::OK();
}

Status DBImpl::EndTrace() {
  InstrumentedMutexLock lock(&trace_mutex_);
  Status s;
  if (tracer_ != nullptr) {
    s = tracer_->Close();
    tracer_.reset();
  } else {
    s = Status::IOError("No trace file to close");
  }
  return s;
}

Status DBImpl::NewDefaultReplayer(
    const std::vector<ColumnFamilyHandle*>& handles,
    std::unique_ptr<TraceReader>&& reader,
    std::unique_ptr<Replayer>* replayer) {
  replayer->reset(new ReplayerImpl(this, handles, std::move(reader)));
  return Status::OK();
}

Status DBImpl::StartBlockCacheTrace(
    const TraceOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer) {
  BlockCacheTraceOptions block_trace_opts;
  block_trace_opts.sampling_frequency = trace_options.sampling_frequency;

  BlockCacheTraceWriterOptions trace_writer_opt;
  trace_writer_opt.max_trace_file_size = trace_options.max_trace_file_size;

  std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
      NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                               std::move(trace_writer));

  return block_cache_tracer_.StartTrace(block_trace_opts,
                                        std::move(block_cache_trace_writer));
}

Status DBImpl::StartBlockCacheTrace(
    const BlockCacheTraceOptions& trace_options,
    std::unique_ptr<BlockCacheTraceWriter>&& trace_writer) {
  return block_cache_tracer_.StartTrace(trace_options, std::move(trace_writer));
}

Status DBImpl::EndBlockCacheTrace() {
  block_cache_tracer_.EndTrace();
  return Status::OK();
}

Status DBImpl::TraceIteratorSeek(const uint32_t& cf_id, const Slice& key,
                                 const Slice& lower_bound,
                                 const Slice upper_bound) {
  Status s;
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      s = tracer_->IteratorSeek(cf_id, key, lower_bound, upper_bound);
    }
  }
  return s;
}

Status DBImpl::TraceIteratorSeekForPrev(const uint32_t& cf_id, const Slice& key,
                                        const Slice& lower_bound,
                                        const Slice upper_bound) {
  Status s;
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      s = tracer_->IteratorSeekForPrev(cf_id, key, lower_bound, upper_bound);
    }
  }
  return s;
}

Status DBImpl::ReserveFileNumbersBeforeIngestion(
    ColumnFamilyData* cfd, uint64_t num,
    std::unique_ptr<std::list<uint64_t>::iterator>& pending_output_elem,
    uint64_t* next_file_number) {
  Status s;
  SuperVersionContext dummy_sv_ctx(true /* create_superversion */);
  assert(nullptr != next_file_number);
  InstrumentedMutexLock l(&mutex_);
  if (error_handler_.IsDBStopped()) {
    // Do not ingest files when there is a bg_error
    return error_handler_.GetBGError();
  }
  pending_output_elem.reset(new std::list<uint64_t>::iterator(
      CaptureCurrentFileNumberInPendingOutputs()));
  *next_file_number = versions_->FetchAddFileNumber(static_cast<uint64_t>(num));
  auto cf_options = cfd->GetLatestMutableCFOptions();
  VersionEdit dummy_edit;
  // If crash happen after a hard link established, Recover function may
  // reuse the file number that has already assigned to the internal file,
  // and this will overwrite the external file. To protect the external
  // file, we have to make sure the file number will never being reused.
  s = versions_->LogAndApply(cfd, *cf_options, &dummy_edit, &mutex_,
                             directories_.GetDbDir());
  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, &dummy_sv_ctx, *cf_options);
  }
  dummy_sv_ctx.Clean();
  return s;
}

Status DBImpl::GetCreationTimeOfOldestFile(uint64_t* creation_time) {
  if (mutable_db_options_.max_open_files == -1) {
    uint64_t oldest_time = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped()) {
        uint64_t ctime;
        {
          SuperVersion* sv = GetAndRefSuperVersion(cfd);
          Version* version = sv->current;
          version->GetCreationTimeOfOldestFile(&ctime);
          ReturnAndCleanupSuperVersion(cfd, sv);
        }

        if (ctime < oldest_time) {
          oldest_time = ctime;
        }
        if (oldest_time == 0) {
          break;
        }
      }
    }
    *creation_time = oldest_time;
    return Status::OK();
  } else {
    return Status::NotSupported("This API only works if max_open_files = -1");
  }
}

void DBImpl::RecordSeqnoToTimeMapping() {
  // Get time first then sequence number, so the actual time of seqno is <=
  // unix_time recorded
  int64_t unix_time = 0;
  immutable_db_options_.clock->GetCurrentTime(&unix_time)
      .PermitUncheckedError();  // Ignore error
  SequenceNumber seqno = GetLatestSequenceNumber();
  bool appended = false;
  {
    InstrumentedMutexLock l(&mutex_);
    appended = seqno_time_mapping_.Append(seqno, unix_time);
  }
  if (!appended) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Failed to insert sequence number to time entry: %" PRIu64
                   " -> %" PRIu64,
                   seqno, unix_time);
  }
}
#endif  // ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
