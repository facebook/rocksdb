//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl_secondary.h"

#include <cinttypes>

#include "db/arena_wrapped_db_iter.h"
#include "db/merge_context.h"
#include "logging/auto_roll_logger.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "util/cast_util.h"
#include "util/write_batch_util.h"

namespace ROCKSDB_NAMESPACE {

DBImplSecondary::DBImplSecondary(const DBOptions& db_options,
                                 const std::string& dbname,
                                 std::string secondary_path)
    : DBImpl(db_options, dbname, false, true, true),
      secondary_path_(std::move(secondary_path)) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in secondary mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplSecondary::~DBImplSecondary() = default;

Status DBImplSecondary::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    bool /*readonly*/, bool /*error_if_wal_file_exists*/,
    bool /*error_if_data_exists_in_wals*/, bool /*is_retry*/, uint64_t*,
    RecoveryContext* /*recovery_ctx*/, bool* /*can_retry*/) {
  mutex_.AssertHeld();

  JobContext job_context(0);
  Status s;
  s = static_cast<ReactiveVersionSet*>(versions_.get())
          ->Recover(column_families, &manifest_reader_, &manifest_reporter_,
                    &manifest_reader_status_);
  if (!s.ok()) {
    if (manifest_reader_status_) {
      manifest_reader_status_->PermitUncheckedError();
    }
    return s;
  }
  if (immutable_db_options_.paranoid_checks && s.ok()) {
    s = CheckConsistency();
  }
  // Initial max_total_in_memory_state_ before recovery logs.
  max_total_in_memory_state_ = 0;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
    max_total_in_memory_state_ += mutable_cf_options->write_buffer_size *
                                  mutable_cf_options->max_write_buffer_number;
  }
  if (s.ok()) {
    default_cf_handle_ = new ColumnFamilyHandleImpl(
        versions_->GetColumnFamilySet()->GetDefault(), this, &mutex_);
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();

    std::unordered_set<ColumnFamilyData*> cfds_changed;
    s = FindAndRecoverLogFiles(&cfds_changed, &job_context);
  }

  if (s.IsPathNotFound()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Secondary tries to read WAL, but WAL file(s) have already "
                   "been purged by primary.");
    s = Status::OK();
  }
  // TODO: update options_file_number_ needed?

  job_context.Clean();
  return s;
}

// find new WAL and apply them in order to the secondary instance
Status DBImplSecondary::FindAndRecoverLogFiles(
    std::unordered_set<ColumnFamilyData*>* cfds_changed,
    JobContext* job_context) {
  assert(nullptr != cfds_changed);
  assert(nullptr != job_context);
  Status s;
  std::vector<uint64_t> logs;
  s = FindNewLogNumbers(&logs);
  if (s.ok() && !logs.empty()) {
    SequenceNumber next_sequence(kMaxSequenceNumber);
    s = RecoverLogFiles(logs, &next_sequence, cfds_changed, job_context);
  }
  return s;
}

// List wal_dir and find all new WALs, return these log numbers
Status DBImplSecondary::FindNewLogNumbers(std::vector<uint64_t>* logs) {
  assert(logs != nullptr);
  std::vector<std::string> filenames;
  Status s;
  IOOptions io_opts;
  io_opts.do_not_recurse = true;
  s = immutable_db_options_.fs->GetChildren(immutable_db_options_.GetWalDir(),
                                            io_opts, &filenames,
                                            /*IODebugContext*=*/nullptr);
  if (s.IsNotFound()) {
    return Status::InvalidArgument("Failed to open wal_dir",
                                   immutable_db_options_.GetWalDir());
  } else if (!s.ok()) {
    return s;
  }

  // if log_readers_ is non-empty, it means we have applied all logs with log
  // numbers smaller than the smallest log in log_readers_, so there is no
  // need to pass these logs to RecoverLogFiles
  uint64_t log_number_min = 0;
  if (!log_readers_.empty()) {
    log_number_min = log_readers_.begin()->first;
  }
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (ParseFileName(filenames[i], &number, &type) && type == kWalFile &&
        number >= log_number_min) {
      logs->push_back(number);
    }
  }
  // Recover logs in the order that they were generated
  if (!logs->empty()) {
    std::sort(logs->begin(), logs->end());
  }
  return s;
}

Status DBImplSecondary::MaybeInitLogReader(
    uint64_t log_number, log::FragmentBufferedReader** log_reader) {
  auto iter = log_readers_.find(log_number);
  // make sure the log file is still present
  if (iter == log_readers_.end() ||
      iter->second->reader_->GetLogNumber() != log_number) {
    // delete the obsolete log reader if log number mismatch
    if (iter != log_readers_.end()) {
      log_readers_.erase(iter);
    }
    // initialize log reader from log_number
    // TODO: min_log_number_to_keep_2pc check needed?
    // Open the log file
    std::string fname =
        LogFileName(immutable_db_options_.GetWalDir(), log_number);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Recovering log #%" PRIu64 " mode %d", log_number,
                   static_cast<int>(immutable_db_options_.wal_recovery_mode));

    std::unique_ptr<SequentialFileReader> file_reader;
    {
      std::unique_ptr<FSSequentialFile> file;
      Status status = fs_->NewSequentialFile(
          fname, fs_->OptimizeForLogRead(file_options_), &file, nullptr);
      if (!status.ok()) {
        *log_reader = nullptr;
        return status;
      }
      file_reader.reset(new SequentialFileReader(
          std::move(file), fname, immutable_db_options_.log_readahead_size,
          io_tracer_));
    }

    // Create the log reader.
    LogReaderContainer* log_reader_container = new LogReaderContainer(
        env_, immutable_db_options_.info_log, std::move(fname),
        std::move(file_reader), log_number);
    log_readers_.insert(std::make_pair(
        log_number, std::unique_ptr<LogReaderContainer>(log_reader_container)));
  }
  iter = log_readers_.find(log_number);
  assert(iter != log_readers_.end());
  *log_reader = iter->second->reader_;
  return Status::OK();
}

// After manifest recovery, replay WALs and refresh log_readers_ if necessary
// REQUIRES: log_numbers are sorted in ascending order
Status DBImplSecondary::RecoverLogFiles(
    const std::vector<uint64_t>& log_numbers, SequenceNumber* next_sequence,
    std::unordered_set<ColumnFamilyData*>* cfds_changed,
    JobContext* job_context) {
  assert(nullptr != cfds_changed);
  assert(nullptr != job_context);
  mutex_.AssertHeld();
  Status status;
  for (auto log_number : log_numbers) {
    log::FragmentBufferedReader* reader = nullptr;
    status = MaybeInitLogReader(log_number, &reader);
    if (!status.ok()) {
      return status;
    }
    assert(reader != nullptr);
  }

  const UnorderedMap<uint32_t, size_t>& running_ts_sz =
      versions_->GetRunningColumnFamiliesTimestampSize();
  for (auto log_number : log_numbers) {
    auto it = log_readers_.find(log_number);
    assert(it != log_readers_.end());
    log::FragmentBufferedReader* reader = it->second->reader_;
    Status* wal_read_status = it->second->status_;
    assert(wal_read_status);
    // Manually update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(log_number);

    // Determine if we should tolerate incomplete records at the tail end of the
    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;

    while (reader->ReadRecord(&record, &scratch,
                              immutable_db_options_.wal_recovery_mode) &&
           wal_read_status->ok() && status.ok()) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reader->GetReporter()->Corruption(
            record.size(), Status::Corruption("log record too small"));
        continue;
      }
      status = WriteBatchInternal::SetContents(&batch, record);
      if (!status.ok()) {
        break;
      }
      const UnorderedMap<uint32_t, size_t>& record_ts_sz =
          reader->GetRecordedTimestampSize();
      status = HandleWriteBatchTimestampSizeDifference(
          &batch, running_ts_sz, record_ts_sz,
          TimestampSizeConsistencyMode::kVerifyConsistency);
      if (!status.ok()) {
        break;
      }
      SequenceNumber seq_of_batch = WriteBatchInternal::Sequence(&batch);
      std::vector<uint32_t> column_family_ids;
      status = CollectColumnFamilyIdsFromWriteBatch(batch, &column_family_ids);
      if (status.ok()) {
        for (const auto id : column_family_ids) {
          ColumnFamilyData* cfd =
              versions_->GetColumnFamilySet()->GetColumnFamily(id);
          if (cfd == nullptr) {
            continue;
          }
          if (cfds_changed->count(cfd) == 0) {
            cfds_changed->insert(cfd);
          }
          const std::vector<FileMetaData*>& l0_files =
              cfd->current()->storage_info()->LevelFiles(0);
          SequenceNumber seq =
              l0_files.empty() ? 0 : l0_files.back()->fd.largest_seqno;
          // If the write batch's sequence number is smaller than the last
          // sequence number of the largest sequence persisted for this column
          // family, then its data must reside in an SST that has already been
          // added in the prior MANIFEST replay.
          if (seq_of_batch <= seq) {
            continue;
          }
          auto curr_log_num = std::numeric_limits<uint64_t>::max();
          if (cfd_to_current_log_.count(cfd) > 0) {
            curr_log_num = cfd_to_current_log_[cfd];
          }
          // If the active memtable contains records added by replaying an
          // earlier WAL, then we need to seal the memtable, add it to the
          // immutable memtable list and create a new active memtable.
          if (!cfd->mem()->IsEmpty() &&
              (curr_log_num == std::numeric_limits<uint64_t>::max() ||
               curr_log_num != log_number)) {
            const MutableCFOptions mutable_cf_options =
                *cfd->GetLatestMutableCFOptions();
            MemTable* new_mem =
                cfd->ConstructNewMemtable(mutable_cf_options, seq_of_batch);
            cfd->mem()->SetNextLogNumber(log_number);
            cfd->mem()->ConstructFragmentedRangeTombstones();
            cfd->imm()->Add(cfd->mem(), &job_context->memtables_to_free);
            new_mem->Ref();
            cfd->SetMemtable(new_mem);
          }
        }
        bool has_valid_writes = false;
        status = WriteBatchInternal::InsertInto(
            &batch, column_family_memtables_.get(),
            nullptr /* flush_scheduler */, nullptr /* trim_history_scheduler*/,
            true, log_number, this, false /* concurrent_memtable_writes */,
            next_sequence, &has_valid_writes, seq_per_batch_, batch_per_txn_);
      }
      // If column family was not found, it might mean that the WAL write
      // batch references to the column family that was dropped after the
      // insert. We don't want to fail the whole write batch in that case --
      // we just ignore the update.
      // That's why we set ignore missing column families to true
      // passing null flush_scheduler will disable memtable flushing which is
      // needed for secondary instances
      if (status.ok()) {
        for (const auto id : column_family_ids) {
          ColumnFamilyData* cfd =
              versions_->GetColumnFamilySet()->GetColumnFamily(id);
          if (cfd == nullptr) {
            continue;
          }
          std::unordered_map<ColumnFamilyData*, uint64_t>::iterator iter =
              cfd_to_current_log_.find(cfd);
          if (iter == cfd_to_current_log_.end()) {
            cfd_to_current_log_.insert({cfd, log_number});
          } else if (log_number > iter->second) {
            iter->second = log_number;
          }
        }
        auto last_sequence = *next_sequence - 1;
        if ((*next_sequence != kMaxSequenceNumber) &&
            (versions_->LastSequence() <= last_sequence)) {
          versions_->SetLastAllocatedSequence(last_sequence);
          versions_->SetLastPublishedSequence(last_sequence);
          versions_->SetLastSequence(last_sequence);
        }
      } else {
        // We are treating this as a failure while reading since we read valid
        // blocks that do not form coherent data
        reader->GetReporter()->Corruption(record.size(), status);
      }
    }
    if (status.ok() && !wal_read_status->ok()) {
      status = *wal_read_status;
    }
    if (!status.ok()) {
      wal_read_status->PermitUncheckedError();
      return status;
    }
  }
  // remove logreaders from map after successfully recovering the WAL
  if (log_readers_.size() > 1) {
    auto erase_iter = log_readers_.begin();
    std::advance(erase_iter, log_readers_.size() - 1);
    log_readers_.erase(log_readers_.begin(), erase_iter);
  }
  return status;
}

Status DBImplSecondary::GetImpl(const ReadOptions& read_options,
                                const Slice& key,
                                GetImplOptions& get_impl_options) {
  assert(get_impl_options.value != nullptr ||
         get_impl_options.columns != nullptr);
  assert(get_impl_options.column_family);

  Status s;

  if (read_options.timestamp) {
    s = FailIfTsMismatchCf(get_impl_options.column_family,
                           *(read_options.timestamp));
    if (!s.ok()) {
      return s;
    }
  } else {
    s = FailIfCfHasTs(get_impl_options.column_family);
    if (!s.ok()) {
      return s;
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (get_impl_options.timestamp) {
    get_impl_options.timestamp->clear();
  }

  PERF_CPU_TIMER_GUARD(get_cpu_nanos, immutable_db_options_.clock);
  StopWatch sw(immutable_db_options_.clock, stats_, DB_GET);
  PERF_TIMER_GUARD(get_snapshot_time);

  const Comparator* ucmp = get_impl_options.column_family->GetComparator();
  assert(ucmp);
  std::string* ts =
      ucmp->timestamp_size() > 0 ? get_impl_options.timestamp : nullptr;
  SequenceNumber snapshot = versions_->LastSequence();
  GetWithTimestampReadCallback read_cb(snapshot);
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      get_impl_options.column_family);
  auto cfd = cfh->cfd();
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      tracer_->Get(get_impl_options.column_family, key);
    }
  }

  // Acquire SuperVersion
  SuperVersion* super_version = GetAndRefSuperVersion(cfd);
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    s = FailIfReadCollapsedHistory(cfd, super_version,
                                   *(read_options.timestamp));
    if (!s.ok()) {
      ReturnAndCleanupSuperVersion(cfd, super_version);
      return s;
    }
  }
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey(key, snapshot, read_options.timestamp);
  PERF_TIMER_STOP(get_snapshot_time);
  bool done = false;

  // Look up starts here
  if (super_version->mem->Get(
          lkey,
          get_impl_options.value ? get_impl_options.value->GetSelf() : nullptr,
          get_impl_options.columns, ts, &s, &merge_context,
          &max_covering_tombstone_seq, read_options,
          false /* immutable_memtable */, &read_cb)) {
    done = true;
    if (get_impl_options.value) {
      get_impl_options.value->PinSelf();
    }
    RecordTick(stats_, MEMTABLE_HIT);
  } else if ((s.ok() || s.IsMergeInProgress()) &&
             super_version->imm->Get(
                 lkey,
                 get_impl_options.value ? get_impl_options.value->GetSelf()
                                        : nullptr,
                 get_impl_options.columns, ts, &s, &merge_context,
                 &max_covering_tombstone_seq, read_options, &read_cb)) {
    done = true;
    if (get_impl_options.value) {
      get_impl_options.value->PinSelf();
    }
    RecordTick(stats_, MEMTABLE_HIT);
  }
  if (!done && !s.ok() && !s.IsMergeInProgress()) {
    ReturnAndCleanupSuperVersion(cfd, super_version);
    return s;
  }
  if (!done) {
    PERF_TIMER_GUARD(get_from_output_files_time);
    PinnedIteratorsManager pinned_iters_mgr;
    super_version->current->Get(
        read_options, lkey, get_impl_options.value, get_impl_options.columns,
        ts, &s, &merge_context, &max_covering_tombstone_seq, &pinned_iters_mgr,
        /*value_found*/ nullptr,
        /*key_exists*/ nullptr, /*seq*/ nullptr, &read_cb, /*is_blob*/ nullptr,
        /*do_merge*/ true);
    RecordTick(stats_, MEMTABLE_MISS);
  }
  {
    PERF_TIMER_GUARD(get_post_process_time);
    ReturnAndCleanupSuperVersion(cfd, super_version);
    RecordTick(stats_, NUMBER_KEYS_READ);
    size_t size = 0;
    if (get_impl_options.value) {
      size = get_impl_options.value->size();
    } else if (get_impl_options.columns) {
      size = get_impl_options.columns->serialized_size();
    }
    RecordTick(stats_, BYTES_READ, size);
    RecordTimeToHistogram(stats_, BYTES_PER_READ, size);
    PERF_COUNTER_ADD(get_read_bytes, size);
  }
  return s;
}

Iterator* DBImplSecondary::NewIterator(const ReadOptions& _read_options,
                                       ColumnFamilyHandle* column_family) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kDBIterator) {
    return NewErrorIterator(Status::InvalidArgument(
        "Can only call NewIterator with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`"));
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kDBIterator;
  }
  if (read_options.managed) {
    return NewErrorIterator(
        Status::NotSupported("Managed iterator is not supported anymore."));
  }
  if (read_options.read_tier == kPersistedTier) {
    return NewErrorIterator(Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators."));
  }

  assert(column_family);
  if (read_options.timestamp) {
    const Status s =
        FailIfTsMismatchCf(column_family, *(read_options.timestamp));
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  } else {
    const Status s = FailIfCfHasTs(column_family);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
  }

  Iterator* result = nullptr;
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh != nullptr);
  auto cfd = cfh->cfd();
  if (read_options.tailing) {
    return NewErrorIterator(Status::NotSupported(
        "tailing iterator not supported in secondary mode"));
  } else if (read_options.snapshot != nullptr) {
    // TODO (yanqin) support snapshot.
    return NewErrorIterator(
        Status::NotSupported("snapshot not supported in secondary mode"));
  } else {
    SequenceNumber snapshot(kMaxSequenceNumber);
    SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
    if (read_options.timestamp && read_options.timestamp->size() > 0) {
      const Status s =
          FailIfReadCollapsedHistory(cfd, sv, *(read_options.timestamp));
      if (!s.ok()) {
        CleanupSuperVersion(sv);
        return NewErrorIterator(s);
      }
    }
    result = NewIteratorImpl(read_options, cfh, sv, snapshot,
                             nullptr /*read_callback*/);
  }
  return result;
}

ArenaWrappedDBIter* DBImplSecondary::NewIteratorImpl(
    const ReadOptions& read_options, ColumnFamilyHandleImpl* cfh,
    SuperVersion* super_version, SequenceNumber snapshot,
    ReadCallback* read_callback, bool expose_blob_index, bool allow_refresh) {
  assert(nullptr != cfh);
  assert(snapshot == kMaxSequenceNumber);
  snapshot = versions_->LastSequence();
  assert(snapshot != kMaxSequenceNumber);
  auto db_iter = NewArenaWrappedDbIterator(
      env_, read_options, *cfh->cfd()->ioptions(),
      super_version->mutable_cf_options, super_version->current, snapshot,
      super_version->mutable_cf_options.max_sequential_skip_in_iterations,
      super_version->version_number, read_callback, cfh, expose_blob_index,
      allow_refresh);
  auto internal_iter = NewInternalIterator(
      db_iter->GetReadOptions(), cfh->cfd(), super_version, db_iter->GetArena(),
      snapshot, /* allow_unprepared_value */ true, db_iter);
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}

Status DBImplSecondary::NewIterators(
    const ReadOptions& _read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kDBIterator) {
    return Status::InvalidArgument(
        "Can only call NewIterators with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kDBIterator`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kDBIterator;
  }
  if (read_options.managed) {
    return Status::NotSupported("Managed iterator is not supported anymore.");
  }
  if (read_options.read_tier == kPersistedTier) {
    return Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators.");
  }
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }

  if (read_options.timestamp) {
    for (auto* cf : column_families) {
      assert(cf);
      const Status s = FailIfTsMismatchCf(cf, *(read_options.timestamp));
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
  iterators->clear();
  iterators->reserve(column_families.size());
  if (read_options.tailing) {
    return Status::NotSupported(
        "tailing iterator not supported in secondary mode");
  } else if (read_options.snapshot != nullptr) {
    // TODO (yanqin) support snapshot.
    return Status::NotSupported("snapshot not supported in secondary mode");
  } else {
    SequenceNumber read_seq(kMaxSequenceNumber);
    autovector<std::tuple<ColumnFamilyHandleImpl*, SuperVersion*>> cfh_to_sv;
    const bool check_read_ts =
        read_options.timestamp && read_options.timestamp->size() > 0;
    for (auto cf : column_families) {
      auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(cf);
      auto cfd = cfh->cfd();
      SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
      cfh_to_sv.emplace_back(cfh, sv);
      if (check_read_ts) {
        const Status s =
            FailIfReadCollapsedHistory(cfd, sv, *(read_options.timestamp));
        if (!s.ok()) {
          for (auto prev_entry : cfh_to_sv) {
            CleanupSuperVersion(std::get<1>(prev_entry));
          }
          return s;
        }
      }
    }
    assert(cfh_to_sv.size() == column_families.size());
    for (auto [cfh, sv] : cfh_to_sv) {
      iterators->push_back(
          NewIteratorImpl(read_options, cfh, sv, read_seq, read_callback));
    }
  }
  return Status::OK();
}

Status DBImplSecondary::CheckConsistency() {
  mutex_.AssertHeld();
  Status s = DBImpl::CheckConsistency();
  // If DBImpl::CheckConsistency() which is stricter returns success, then we
  // do not need to give a second chance.
  if (s.ok()) {
    return s;
  }
  // It's possible that DBImpl::CheckConssitency() can fail because the primary
  // may have removed certain files, causing the GetFileSize(name) call to
  // fail and returning a PathNotFound. In this case, we take a best-effort
  // approach and just proceed.
  TEST_SYNC_POINT_CALLBACK(
      "DBImplSecondary::CheckConsistency:AfterFirstAttempt", &s);

  if (immutable_db_options_.skip_checking_sst_file_sizes_on_db_open) {
    return Status::OK();
  }

  std::vector<LiveFileMetaData> metadata;
  versions_->GetLiveFilesMetaData(&metadata);

  std::string corruption_messages;
  for (const auto& md : metadata) {
    // md.name has a leading "/".
    std::string file_path = md.db_path + md.name;

    uint64_t fsize = 0;
    s = env_->GetFileSize(file_path, &fsize);
    if (!s.ok() &&
        (env_->GetFileSize(Rocks2LevelTableFileName(file_path), &fsize).ok() ||
         s.IsPathNotFound())) {
      s = Status::OK();
    }
    if (!s.ok()) {
      corruption_messages +=
          "Can't access " + md.name + ": " + s.ToString() + "\n";
    }
  }
  return corruption_messages.empty() ? Status::OK()
                                     : Status::Corruption(corruption_messages);
}

Status DBImplSecondary::TryCatchUpWithPrimary() {
  assert(versions_.get() != nullptr);
  assert(manifest_reader_.get() != nullptr);
  Status s;
  // read the manifest and apply new changes to the secondary instance
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  JobContext job_context(0, true /*create_superversion*/);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    s = static_cast_with_check<ReactiveVersionSet>(versions_.get())
            ->ReadAndApply(&mutex_, &manifest_reader_,
                           manifest_reader_status_.get(), &cfds_changed,
                           /*files_to_delete=*/nullptr);

    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Last sequence is %" PRIu64,
                   static_cast<uint64_t>(versions_->LastSequence()));
    for (ColumnFamilyData* cfd : cfds_changed) {
      if (cfd->IsDropped()) {
        ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] is dropped\n",
                        cfd->GetName().c_str());
        continue;
      }
      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] Level summary: %s\n", cfd->GetName().c_str(),
                      cfd->current()->storage_info()->LevelSummary(&tmp));
    }

    // list wal_dir to discover new WALs and apply new changes to the secondary
    // instance
    if (s.ok()) {
      s = FindAndRecoverLogFiles(&cfds_changed, &job_context);
    }
    if (s.IsPathNotFound()) {
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "Secondary tries to read WAL, but WAL file(s) have already "
          "been purged by primary.");
      s = Status::OK();
    }
    if (s.ok()) {
      for (auto cfd : cfds_changed) {
        cfd->imm()->RemoveOldMemTables(cfd->GetLogNumber(),
                                       &job_context.memtables_to_free);
        auto& sv_context = job_context.superversion_contexts.back();
        cfd->InstallSuperVersion(&sv_context, &mutex_);
        sv_context.NewSuperVersion();
      }
    }
  }
  job_context.Clean();

  // Cleanup unused, obsolete files.
  JobContext purge_files_job_context(0);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    // Currently, secondary instance does not own the database files, thus it
    // is unnecessary for the secondary to force full scan.
    FindObsoleteFiles(&purge_files_job_context, /*force=*/false);
  }
  if (purge_files_job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(purge_files_job_context);
  }
  purge_files_job_context.Clean();
  return s;
}

Status DB::OpenAsSecondary(const Options& options, const std::string& dbname,
                           const std::string& secondary_path, DB** dbptr) {
  *dbptr = nullptr;

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;

  Status s = DB::OpenAsSecondary(db_options, dbname, secondary_path,
                                 column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    delete handles[0];
  }
  return s;
}

Status DB::OpenAsSecondary(
    const DBOptions& db_options, const std::string& dbname,
    const std::string& secondary_path,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  *dbptr = nullptr;

  DBOptions tmp_opts(db_options);
  Status s;
  if (nullptr == tmp_opts.info_log) {
    s = CreateLoggerFromOptions(secondary_path, tmp_opts, &tmp_opts.info_log);
    if (!s.ok()) {
      tmp_opts.info_log = nullptr;
      return s;
    }
  }

  assert(tmp_opts.info_log != nullptr);
  if (db_options.max_open_files != -1) {
    std::ostringstream oss;
    oss << "The primary instance may delete all types of files after they "
           "become obsolete. The application can coordinate the primary and "
           "secondary so that primary does not delete/rename files that are "
           "currently being used by the secondary. Alternatively, a custom "
           "Env/FS can be provided such that files become inaccessible only "
           "after all primary and secondaries indicate that they are obsolete "
           "and deleted. If the above two are not possible, you can open the "
           "secondary instance with `max_open_files==-1` so that secondary "
           "will eagerly keep all table files open. Even if a file is deleted, "
           "its content can still be accessed via a prior open file "
           "descriptor. This is a hacky workaround for only table files. If "
           "none of the above is done, then point lookup or "
           "range scan via the secondary instance can result in IOError: file "
           "not found. This can be resolved by retrying "
           "TryCatchUpWithPrimary().";
    ROCKS_LOG_WARN(tmp_opts.info_log, "%s", oss.str().c_str());
  }

  handles->clear();
  DBImplSecondary* impl = new DBImplSecondary(tmp_opts, dbname, secondary_path);
  impl->versions_.reset(new ReactiveVersionSet(
      dbname, &impl->immutable_db_options_, impl->file_options_,
      impl->table_cache_.get(), impl->write_buffer_manager_,
      &impl->write_controller_, impl->io_tracer_));
  impl->column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(impl->versions_->GetColumnFamilySet()));
  impl->wal_in_db_path_ = impl->immutable_db_options_.IsWalDirSameAsDBPath();

  impl->mutex_.Lock();
  s = impl->Recover(column_families, true, false, false);
  if (s.ok()) {
    for (const auto& cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (nullptr == cfd) {
        s = Status::InvalidArgument("Column family not found", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
  SuperVersionContext sv_context(true /* create_superversion */);
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      sv_context.NewSuperVersion();
      cfd->InstallSuperVersion(&sv_context, &impl->mutex_);
    }
  }
  impl->mutex_.Unlock();
  sv_context.Clean();
  if (s.ok()) {
    *dbptr = impl;
    for (auto h : *handles) {
      impl->NewThreadStatusCfInfo(
          static_cast_with_check<ColumnFamilyHandleImpl>(h)->cfd());
    }
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}

Status DBImplSecondary::CompactWithoutInstallation(
    const OpenAndCompactOptions& options, ColumnFamilyHandle* cfh,
    const CompactionServiceInput& input, CompactionServiceResult* result) {
  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
  InstrumentedMutexLock l(&mutex_);
  auto cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
  if (!cfd) {
    return Status::InvalidArgument("Cannot find column family" +
                                   cfh->GetName());
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input.input_files) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  auto* version = cfd->current();

  ColumnFamilyMetaData cf_meta;
  version->GetColumnFamilyMetaData(&cf_meta);

  const MutableCFOptions* mutable_cf_options = cfd->GetLatestMutableCFOptions();
  ColumnFamilyOptions cf_options = cfd->GetLatestCFOptions();
  VersionStorageInfo* vstorage = version->storage_info();

  // Use comp_options to reuse some CompactFiles functions
  CompactionOptions comp_options;
  comp_options.compression = kDisableCompressionOption;
  comp_options.output_file_size_limit = MaxFileSizeForLevel(
      *mutable_cf_options, input.output_level, cf_options.compaction_style,
      vstorage->base_level(), cf_options.level_compaction_dynamic_level_bytes);

  std::vector<CompactionInputFiles> input_files;
  Status s = cfd->compaction_picker()->GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage, comp_options);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  c.reset(cfd->compaction_picker()->CompactFiles(
      comp_options, input_files, input.output_level, vstorage,
      *mutable_cf_options, mutable_db_options_, 0));
  assert(c != nullptr);

  c->FinalizeInputInfo(version);

  // Create output directory if it's not existed yet
  std::unique_ptr<FSDirectory> output_dir;
  s = CreateAndNewDirectory(fs_.get(), secondary_path_, &output_dir);
  if (!s.ok()) {
    return s;
  }

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  const int job_id = next_job_id_.fetch_add(1);

  // use primary host's db_id for running the compaction, but db_session_id is
  // using the local one, which is to make sure the unique id is unique from
  // the remote compactors. Because the id is generated from db_id,
  // db_session_id and orig_file_number, unlike the local compaction, remote
  // compaction cannot guarantee the uniqueness of orig_file_number, the file
  // number is only assigned when compaction is done.
  CompactionServiceCompactionJob compaction_job(
      job_id, c.get(), immutable_db_options_, mutable_db_options_,
      file_options_for_compaction_, versions_.get(), &shutting_down_,
      &log_buffer, output_dir.get(), stats_, &mutex_, &error_handler_,
      input.snapshots, table_cache_, &event_logger_, dbname_, io_tracer_,
      options.canceled ? *options.canceled : kManualCompactionCanceledFalse_,
      input.db_id, db_session_id_, secondary_path_, input, result);

  mutex_.Unlock();
  s = compaction_job.Run();
  mutex_.Lock();

  // clean up
  compaction_job.io_status().PermitUncheckedError();
  compaction_job.CleanupCompaction();
  c->ReleaseCompactionFiles(s);
  c.reset();

  TEST_SYNC_POINT_CALLBACK("DBImplSecondary::CompactWithoutInstallation::End",
                           &s);
  result->status = s;
  return s;
}

Status DB::OpenAndCompact(
    const OpenAndCompactOptions& options, const std::string& name,
    const std::string& output_directory, const std::string& input,
    std::string* output,
    const CompactionServiceOptionsOverride& override_options) {
  // Check for cancellation
  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  // 1. Deserialize Compaction Input
  CompactionServiceInput compaction_input;
  Status s = CompactionServiceInput::Read(input, &compaction_input);
  if (!s.ok()) {
    return s;
  }

  // 2. Load the options
  DBOptions db_options;
  ConfigOptions config_options;
  config_options.env = override_options.env;
  std::vector<ColumnFamilyDescriptor> all_column_families;

  std::string options_file_name =
      OptionsFileName(name, compaction_input.options_file_number);

  s = LoadOptionsFromFile(config_options, options_file_name, &db_options,
                          &all_column_families);
  if (!s.ok()) {
    return s;
  }

  // 3. Override pointer configurations in DBOptions with
  // CompactionServiceOptionsOverride
  db_options.env = override_options.env;
  db_options.file_checksum_gen_factory =
      override_options.file_checksum_gen_factory;
  db_options.statistics = override_options.statistics;
  db_options.listeners = override_options.listeners;
  db_options.compaction_service = nullptr;
  // We will close the DB after the compaction anyway.
  // Open as many files as needed for the compaction.
  db_options.max_open_files = -1;

  // 4. Filter CFs that are needed for OpenAndCompact()
  // We do not need to open all column families for the remote compaction.
  // Only open default CF + target CF. If target CF == default CF, we will open
  // just the default CF (Due to current limitation, DB cannot open without the
  // default CF)
  std::vector<ColumnFamilyDescriptor> column_families;
  for (auto& cf : all_column_families) {
    if (cf.name == compaction_input.cf_name) {
      cf.options.comparator = override_options.comparator;
      cf.options.merge_operator = override_options.merge_operator;
      cf.options.compaction_filter = override_options.compaction_filter;
      cf.options.compaction_filter_factory =
          override_options.compaction_filter_factory;
      cf.options.prefix_extractor = override_options.prefix_extractor;
      cf.options.table_factory = override_options.table_factory;
      cf.options.sst_partitioner_factory =
          override_options.sst_partitioner_factory;
      cf.options.table_properties_collector_factories =
          override_options.table_properties_collector_factories;
      column_families.emplace_back(cf);
    } else if (cf.name == kDefaultColumnFamilyName) {
      column_families.emplace_back(cf);
    }
  }

  // 5. Open db As Secondary
  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  s = DB::OpenAsSecondary(db_options, name, output_directory, column_families,
                          &handles, &db);
  if (!s.ok()) {
    return s;
  }
  assert(db);

  // 6. Find the handle of the Column Family that this will compact
  ColumnFamilyHandle* cfh = nullptr;
  for (auto* handle : handles) {
    if (compaction_input.cf_name == handle->GetName()) {
      cfh = handle;
      break;
    }
  }
  assert(cfh);

  // 7. Run the compaction without installation.
  // Output will be stored in the directory specified by output_directory
  CompactionServiceResult compaction_result;
  DBImplSecondary* db_secondary = static_cast_with_check<DBImplSecondary>(db);
  s = db_secondary->CompactWithoutInstallation(options, cfh, compaction_input,
                                               &compaction_result);

  // 8. Serialize the result
  Status serialization_status = compaction_result.Write(output);

  // 9. Close the db and return
  for (auto& handle : handles) {
    delete handle;
  }
  delete db;
  if (s.ok()) {
    return serialization_status;
  } else {
    serialization_status.PermitUncheckedError();
  }
  return s;
}

Status DB::OpenAndCompact(
    const std::string& name, const std::string& output_directory,
    const std::string& input, std::string* output,
    const CompactionServiceOptionsOverride& override_options) {
  return OpenAndCompact(OpenAndCompactOptions(), name, output_directory, input,
                        output, override_options);
}


}  // namespace ROCKSDB_NAMESPACE
