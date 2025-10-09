//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl_secondary.h"

#include <cinttypes>

#include "db/arena_wrapped_db_iter.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/merge_context.h"
#include "db/version_edit.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
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
  // Initial max_total_in_memory_state_ before recovery logs.
  max_total_in_memory_state_ = 0;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
    max_total_in_memory_state_ += mutable_cf_options.write_buffer_size *
                                  mutable_cf_options.max_write_buffer_number;
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
          TimestampSizeConsistencyMode::kVerifyConsistency, seq_per_batch_,
          batch_per_txn_);
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
          cfds_changed->insert(cfd);
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
            MemTable* new_mem = cfd->ConstructNewMemtable(
                cfd->GetLatestMutableCFOptions(), seq_of_batch);
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
         get_impl_options.columns != nullptr ||
         get_impl_options.merge_operands != nullptr);
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
  // TODO - Large Result Optimization for Secondary DB
  // (https://github.com/facebook/rocksdb/pull/10458)

  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey(key, snapshot, read_options.timestamp);
  PERF_TIMER_STOP(get_snapshot_time);
  bool done = false;

  // Look up starts here
  if (get_impl_options.get_value) {
    if (super_version->mem->Get(
            lkey,
            get_impl_options.value ? get_impl_options.value->GetSelf()
                                   : nullptr,
            get_impl_options.columns, ts, &s, &merge_context,
            &max_covering_tombstone_seq, read_options,
            false /* immutable_memtable */, &read_cb,
            /*is_blob_index=*/nullptr, /*do_merge=*/true)) {
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
  } else {
    // GetMergeOperands
    if (super_version->mem->Get(
            lkey,
            get_impl_options.value ? get_impl_options.value->GetSelf()
                                   : nullptr,
            get_impl_options.columns, ts, &s, &merge_context,
            &max_covering_tombstone_seq, read_options,
            false /* immutable_memtable */, &read_cb,
            /*is_blob_index=*/nullptr, /*do_merge=*/false)) {
      done = true;
      RecordTick(stats_, MEMTABLE_HIT);
    } else if ((s.ok() || s.IsMergeInProgress()) &&
               super_version->imm->GetMergeOperands(lkey, &s, &merge_context,
                                                    &max_covering_tombstone_seq,
                                                    read_options)) {
      done = true;
      RecordTick(stats_, MEMTABLE_HIT);
    }
  }
  if (!s.ok() && !s.IsMergeInProgress() && !s.IsNotFound()) {
    assert(done);
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
    } else if (get_impl_options.merge_operands) {
      *get_impl_options.number_of_operands =
          static_cast<int>(merge_context.GetNumOperands());
      for (const Slice& sl : merge_context.GetOperands()) {
        size += sl.size();
        get_impl_options.merge_operands->PinSelf(sl);
        get_impl_options.merge_operands++;
      }
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
  return NewArenaWrappedDbIterator(env_, read_options, cfh, super_version,
                                   snapshot, read_callback, this,
                                   expose_blob_index, allow_refresh,
                                   /*allow_mark_memtable_for_flush=*/false);
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

Status DBImplSecondary::TryCatchUpWithPrimary() {
  assert(versions_.get() != nullptr);
  Status s;
  // read the manifest and apply new changes to the secondary instance
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  JobContext job_context(0, true /*create_superversion*/);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    assert(manifest_reader_.get() != nullptr);
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
      if (s.IsPathNotFound()) {
        ROCKS_LOG_INFO(
            immutable_db_options_.info_log,
            "Secondary tries to read WAL, but WAL file(s) have already "
            "been purged by primary.");
        s = Status::OK();
      }
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
                           const std::string& secondary_path,
                           std::unique_ptr<DB>* dbptr) {
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
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr) {
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
    dbptr->reset(impl);
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

Status DBImplSecondary::ScanCompactionProgressFiles(
    CompactionProgressFilesScan* scan_result) {
  assert(scan_result != nullptr);
  scan_result->Clear();

  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  std::vector<std::string> all_filenames;
  s = fs_->GetChildren(secondary_path_, opts, &all_filenames, nullptr /* dbg*/);
  if (!s.ok()) {
    return s;
  }

  for (const auto& filename : all_filenames) {
    if (filename == "." || filename == "..") {
      continue;
    }

    uint64_t number;
    FileType type;

    if (!ParseFileName(filename, &number, &type)) {
      continue;
    }

    // Categorize compaction progress files
    if (type == kCompactionProgressFile) {
      if (number > scan_result->latest_progress_timestamp) {
        // Found a newer progress file
        if (scan_result->HasLatestProgressFile()) {
          // Previous "latest" becomes "old"
          scan_result->old_progress_filenames.push_back(
              scan_result->latest_progress_filename.value());
        }
        scan_result->latest_progress_timestamp = number;
        scan_result->latest_progress_filename = filename;
      } else {
        // This is an older progress file
        scan_result->old_progress_filenames.push_back(filename);
      }
    } else if (type == kTempFile &&
               filename.find(kCompactionProgressFileNamePrefix) == 0) {
      // Temporary progress files
      scan_result->temp_progress_filenames.push_back(filename);
    } else if (type == kTableFile) {
      // Collect table file numbers for CleanupPhysicalCompactionOutputFiles
      scan_result->table_file_numbers.push_back(number);
    }
  }

  return Status::OK();
}

Status DBImplSecondary::DeleteCompactionProgressFiles(
    const std::vector<std::string>& filenames) {
  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  for (const auto& filename : filenames) {
    std::string file_path = secondary_path_ + "/" + filename;
    Status delete_status = fs_->DeleteFile(file_path, opts, nullptr /* dbg */);
    if (!delete_status.ok()) {
      return delete_status;
    }
  }

  return Status::OK();
}

Status DBImplSecondary::CleanupOldAndTemporaryCompactionProgressFiles(
    bool preserve_latest, const CompactionProgressFilesScan& scan_result) {
  std::vector<std::string> filenames_to_delete;

  // Always delete old progress files
  filenames_to_delete.insert(filenames_to_delete.end(),
                             scan_result.old_progress_filenames.begin(),
                             scan_result.old_progress_filenames.end());

  // Always delete temp files
  filenames_to_delete.insert(filenames_to_delete.end(),
                             scan_result.temp_progress_filenames.begin(),
                             scan_result.temp_progress_filenames.end());

  // Conditionally delete latest file
  if (!preserve_latest && scan_result.HasLatestProgressFile()) {
    filenames_to_delete.push_back(scan_result.latest_progress_filename.value());
  }

  return DeleteCompactionProgressFiles(filenames_to_delete);
}

// Loads compaction progress from a file and cleans up extra output
// files. After loading the progress, this function identifies and deletes any
// SST files in the output folder that are NOT tracked in the
// progress. This ensures consistency between the progress file and
// actual output files on disk.
Status DBImplSecondary::LoadCompactionProgressAndCleanupExtraOutputFiles(
    const std::string& compaction_progress_file_path,
    const CompactionProgressFilesScan& scan_result) {
  Status s = ParseCompactionProgressFile(compaction_progress_file_path,
                                         &compaction_progress_);
  if (s.ok()) {
    s = CleanupPhysicalCompactionOutputFiles(true /* preserve_tracked_files */,
                                             scan_result);
  }
  return s;
}

Status DBImplSecondary::ParseCompactionProgressFile(
    const std::string& compaction_progress_file_path,
    CompactionProgress* compaction_progress) {
  std::unique_ptr<FSSequentialFile> file;
  Status s = fs_->NewSequentialFile(compaction_progress_file_path,
                                    FileOptions(), &file, nullptr /* dbg */);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<SequentialFileReader> file_reader(new SequentialFileReader(
      std::move(file), compaction_progress_file_path,
      immutable_db_options_.log_readahead_size, io_tracer_, {} /* listeners */,
      immutable_db_options_.rate_limiter.get()));

  Status reader_status;

  struct CompactionProgressReaderReporter : public log::Reader::Reporter {
    Status* status;
    explicit CompactionProgressReaderReporter(Status* s) : status(s) {}

    void Corruption(size_t /*bytes*/, const Status& s,
                    uint64_t /*log_number*/) override {
      if (status->ok()) {
        *status = s;
      }
    }

    void OldLogRecord(size_t /*bytes*/) override {
      // Ignore old records
    }
  } progress_reporter(&reader_status);

  log::Reader compaction_progress_reader(
      immutable_db_options_.info_log, std::move(file_reader),
      &progress_reporter, true /* checksum */, 0 /* log_num */);

  // LIMITATION: Only supports resuming single subcompaction
  SubcompactionProgressBuilder progress_builder;
  Slice slice;
  std::string record;

  while (compaction_progress_reader.ReadRecord(&slice, &record)) {
    if (!reader_status.ok()) {
      return reader_status;
    }

    VersionEdit edit;
    s = edit.DecodeFrom(slice);
    if (!s.ok()) {
      break;
    }

    bool res = progress_builder.ProcessVersionEdit(edit);
    if (!res) {
      break;
    }
  }

  if (!s.ok()) {
    return s;
  }

  if (progress_builder.HasAccumulatedSubcompactionProgress()) {
    compaction_progress->clear();
    compaction_progress->push_back(
        progress_builder.GetAccumulatedSubcompactionProgress());
  } else {
    s = Status::NotFound("No compaction progress was persisted yet");
  }

  return s;
}

Status DBImplSecondary::RenameCompactionProgressFile(
    const std::string& temp_file_path, std::string* final_file_path) {
  uint64_t current_time = env_->NowMicros();
  *final_file_path = CompactionProgressFileName(secondary_path_, current_time);

  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  s = fs_->RenameFile(temp_file_path, *final_file_path, opts,
                      nullptr /* dbg */);

  return s;
}

Status DBImplSecondary::CleanupPhysicalCompactionOutputFiles(
    bool preserve_tracked_files,
    const CompactionProgressFilesScan& scan_result) {
  std::unordered_set<uint64_t> files_to_preserve;

  if (preserve_tracked_files) {
    for (const auto& subcompaction_progress : compaction_progress_) {
      for (const auto& file_metadata :
           subcompaction_progress.output_level_progress.GetOutputFiles()) {
        files_to_preserve.insert(file_metadata.fd.GetNumber());
      }
      for (const auto& file_metadata :
           subcompaction_progress.proximal_output_level_progress
               .GetOutputFiles()) {
        files_to_preserve.insert(file_metadata.fd.GetNumber());
      }
    }
  }

  WriteOptions write_options(Env::IOActivity::kCompaction);
  IOOptions opts;
  Status s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  for (uint64_t file_number : scan_result.table_file_numbers) {
    bool should_delete =
        !preserve_tracked_files ||
        (files_to_preserve.find(file_number) == files_to_preserve.end());

    if (should_delete) {
      std::string file_path = MakeTableFileName(secondary_path_, file_number);
      Status delete_status =
          fs_->DeleteFile(file_path, opts, nullptr /* dbg */);
      if (!delete_status.ok()) {
        return delete_status;
      }
    }
  }

  return Status::OK();
}

Status DBImplSecondary::InitializeCompactionWorkspace(
    bool allow_resumption, std::unique_ptr<FSDirectory>* output_dir,
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  // Create output directory if it doest exist yet
  Status s = CreateAndNewDirectory(fs_.get(), secondary_path_, output_dir);
  if (!s.ok() || !allow_resumption) {
    return s;
  }

  s = PrepareCompactionProgressState();

  if (!s.ok()) {
    return s;
  }

  s = FinalizeCompactionProgressWriter(compaction_progress_writer);

  if (!s.ok()) {
    return s;
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Initialized compaction workspace with %zu subcompaction "
                 "progress to resume",
                 compaction_progress_.size());

  return Status::OK();
}

// PrepareCompactionProgressState() manages compaction progress files and output
// files to ensure a clean, consistent state for resuming or starting fresh
// compaction.
//
// PRECONDITION:
// - This function is ONLY called when allow_resumption = true
// - The caller wants resumption support for this compaction attempt
//
// FILE SYSTEM STATE (before entering this function):
// - 0 or more compaction progress files may exist in `secondary_path_`:
//   * Latest progress file (from the most recent compaction attempt)
//   * Older progress files (left by crashing during a previous
//     InitializeCompactionWorkspace() call)
//   * Temporary progress files (left by crashing during a previous
//     InitializeCompactionWorkspace() call)
// - 0 or more compaction output files may exist in `secondary_path_`
//
// POSTCONDITIONS (after this function):
// - IF the latest progress file exists AND it parses successfully AND
//   actually contains valid compaction progress:
//   * Exactly one latest progress file remains
//   * All older and temporary compaction progress files are deleted
//   * All corresponding compaction output files are preserved
//   * All extra compaction output files are deleted (files left by
//   compaction
//     crashing before persisting the progress)
//   * Result: Ready to resume compaction from the saved progress
// - OTHERWISE (no latest progress file OR it fails to parse OR it's
// invalid):
//   * ALL compaction progress files are deleted (latest + older +
//   temporary)
//   * ALL compaction output files are deleted
//   * Result: Ready to start fresh compaction (despite allow_resumption =
//   true, we cannot resume because there's no valid progress to resume from)
//
// ERROR HANDLING:
// - ON ERROR (if any of the postconditions cannot be achieved):
//   * Function returns error status
//   * File system may be left in a partially modified state
//   * Caller should manually clean up secondary_path_ before retrying
//   * Subsequent OpenAndCompact() calls to this clean secondary_path_ will
//     effectively start fresh compaction
Status DBImplSecondary::PrepareCompactionProgressState() {
  Status s;

  // STEP 1: Scan directory ONCE (includes progress files + table files)
  CompactionProgressFilesScan scan_result;
  s = ScanCompactionProgressFiles(&scan_result);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Encountered error when scanning for compaction "
                    "progress files: %s",
                    s.ToString().c_str());
    return s;
  }

  std::optional<std::string> latest_progress_file =
      scan_result.latest_progress_filename;

  // STEP 2: Determine if we should resume
  bool should_resume = false;
  if (latest_progress_file.has_value()) {
    should_resume = true;
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Did not find any latest compaction progress file. "
                   "Will perform clean up to start fresh compaction");
  }

  // STEP 3: Cleanup using pre-scanned results
  if (should_resume) {
    // Keep latest, delete old/temp
    s = CleanupOldAndTemporaryCompactionProgressFiles(
        true /* preserve_latest */, scan_result);
  } else {
    // Delete everything including latest
    s = CleanupOldAndTemporaryCompactionProgressFiles(
        false /* preserve_latest */, scan_result);
    latest_progress_file.reset();
  }

  if (!s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Failed to clean up compaction progress file(s): %s. "
                    "Will fail the compaction",
                    s.ToString().c_str());
    return s;
  }

  // STEP 4: Load progress if resuming
  if (latest_progress_file.has_value()) {
    uint64_t timestamp = scan_result.latest_progress_timestamp;

    std::string compaction_progress_file_path =
        CompactionProgressFileName(secondary_path_, timestamp);

    s = LoadCompactionProgressAndCleanupExtraOutputFiles(
        compaction_progress_file_path, scan_result);

    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to load the latest compaction "
                     "progress from %s: %s. Will perform clean up "
                     "to start fresh compaction",
                     latest_progress_file.value().c_str(),
                     s.ToString().c_str());
      return HandleInvalidOrNoCompactionProgress(compaction_progress_file_path,
                                                 scan_result);
    }
    return s;
  } else {
    return HandleInvalidOrNoCompactionProgress(
        std::nullopt /* compaction_progress_file_path */, scan_result);
  }
}

uint64_t DBImplSecondary::CalculateResumedCompactionBytes(
    const CompactionProgress& compaction_progress) const {
  uint64_t total_resumed_bytes = 0;

  for (const auto& subcompaction_progress : compaction_progress) {
    for (const auto& file_meta :
         subcompaction_progress.output_level_progress.GetOutputFiles()) {
      total_resumed_bytes += file_meta.fd.file_size;
    }

    for (const auto& file_meta :
         subcompaction_progress.proximal_output_level_progress
             .GetOutputFiles()) {
      total_resumed_bytes += file_meta.fd.file_size;
    }
  }

  return total_resumed_bytes;
}

Status DBImplSecondary::HandleInvalidOrNoCompactionProgress(
    const std::optional<std::string>& compaction_progress_file_path,
    const CompactionProgressFilesScan& scan_result) {
  compaction_progress_.clear();

  Status s;
  if (compaction_progress_file_path.has_value()) {
    WriteOptions write_options(Env::IOActivity::kCompaction);
    IOOptions opts;
    s = WritableFileWriter::PrepareIOOptions(write_options, opts);
    if (s.ok()) {
      s = fs_->DeleteFile(compaction_progress_file_path.value(), opts,
                          nullptr /* dbg */);
    }
    if (!s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Failed to remove invalid progress file: %s",
                      s.ToString().c_str());
      return s;
    }
  }

  s = CleanupPhysicalCompactionOutputFiles(false /* preserve_tracked_files */,
                                           scan_result);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Failed to cleanup existing compaction output files: %s",
                    s.ToString().c_str());
    return s;
  }

  return Status::OK();
}

Status DBImplSecondary::CompactWithoutInstallation(
    const OpenAndCompactOptions& options, ColumnFamilyHandle* cfh,
    const CompactionServiceInput& input, CompactionServiceResult* result) {
  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  std::unique_ptr<FSDirectory> output_dir;
  std::unique_ptr<log::Writer> compaction_progress_writer;

  InstrumentedMutexLock l(&mutex_);

  auto cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
  if (!cfd) {
    return Status::InvalidArgument("Cannot find column family" +
                                   cfh->GetName());
  }
  Status s;

  // TODO(hx235): Resuming compaction is currently incompatible with
  // paranoid_file_checks=true because OutputValidator hash verification would
  // fail during compaction resumption. Before interruption, resuming
  // compaction needs to persist the hash of each output file to enable
  // validation after resumption. Alternatively and preferably, we could move
  // the output verification to happen immediately after each output file is
  // created. This workaround currently disables resuming compaction when
  // paranoid_file_checks is enabled. Note that paranoid_file_checks is
  // disabled by default.
  bool allow_resumption =
      options.allow_resumption &&
      !cfd->GetLatestMutableCFOptions().paranoid_file_checks;

  if (options.allow_resumption &&
      cfd->GetLatestMutableCFOptions().paranoid_file_checks) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Resume compaction configured but disabled due to "
                   "incompatible with paranoid_file_checks=true");
  }

  mutex_.Unlock();

  s = InitializeCompactionWorkspace(allow_resumption, &output_dir,
                                    &compaction_progress_writer);

  mutex_.Lock();

  if (!s.ok()) {
    return s;
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input.input_files) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  auto* version = cfd->current();

  ColumnFamilyMetaData cf_meta;
  version->GetColumnFamilyMetaData(&cf_meta);

  VersionStorageInfo* vstorage = version->storage_info();

  CompactionOptions comp_options;
  comp_options.compression = kDisableCompressionOption;
  comp_options.output_file_size_limit = MaxFileSizeForLevel(
      cfd->GetLatestMutableCFOptions(), input.output_level,
      cfd->ioptions().compaction_style, vstorage->base_level(),
      cfd->ioptions().level_compaction_dynamic_level_bytes);

  std::vector<CompactionInputFiles> input_files;
  s = cfd->compaction_picker()->GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage, comp_options);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        immutable_db_options_.info_log,
        "GetCompactionInputsFromFileNumbers() failed - %s.\n DebugString: %s",
        s.ToString().c_str(), version->DebugString(/*hex=*/true).c_str());
    return s;
  }

  const int job_id = next_job_id_.fetch_add(1);
  JobContext job_context(job_id, true /*create_superversion*/);
  std::vector<SequenceNumber> snapshots = input.snapshots;

  // TODO - snapshot_checker support in Remote Compaction
  job_context.InitSnapshotContext(/*checker=*/nullptr,
                                  /*managed_snapshot=*/nullptr,
                                  kMaxSequenceNumber, std::move(snapshots));

  // TODO - consider serializing the entire Compaction object and using it as
  // input instead of recreating it in the remote worker
  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  std::optional<SequenceNumber> earliest_snapshot = std::nullopt;
  // Standalone Range Deletion Optimization is only supported in Universal
  // Compactions - https://github.com/facebook/rocksdb/pull/13078
  if (cfd->GetLatestCFOptions().compaction_style ==
      CompactionStyle::kCompactionStyleUniversal) {
    earliest_snapshot = !job_context.snapshot_seqs.empty()
                            ? job_context.snapshot_seqs.front()
                            : kMaxSequenceNumber;
  }
  c.reset(cfd->compaction_picker()->PickCompactionForCompactFiles(
      comp_options, input_files, input.output_level, vstorage,
      cfd->GetLatestMutableCFOptions(), mutable_db_options_, 0,
      earliest_snapshot, job_context.snapshot_checker));
  assert(c != nullptr);
  c->FinalizeInputInfo(version);

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

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
      &job_context, table_cache_, &event_logger_, dbname_, io_tracer_,
      options.canceled ? *options.canceled : kManualCompactionCanceledFalse_,
      input.db_id, db_session_id_, secondary_path_, input, result);

  compaction_job.Prepare(compaction_progress_,
                         compaction_progress_writer.get());

  mutex_.Unlock();
  s = compaction_job.Run();
  mutex_.Lock();

  // These cleanup functions handle metadata and state cleanup only and
  // not the physical files
  compaction_job.io_status().PermitUncheckedError();
  compaction_job.CleanupCompaction();
  c->ReleaseCompactionFiles(s);
  c.reset();

  TEST_SYNC_POINT_CALLBACK("DBImplSecondary::CompactWithoutInstallation::End",
                           &s);

  if (!compaction_progress_.empty() && s.ok()) {
    uint64_t total_resumed_bytes =
        CalculateResumedCompactionBytes(compaction_progress_);

    if (total_resumed_bytes > 0 &&
        immutable_db_options_.statistics != nullptr) {
      RecordTick(immutable_db_options_.statistics.get(),
                 REMOTE_COMPACT_RESUMED_BYTES, total_resumed_bytes);
    }
  }

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
  DBOptions base_db_options;
  ConfigOptions config_options;
  config_options.env = override_options.env;
  config_options.ignore_unknown_options = true;
  std::vector<ColumnFamilyDescriptor> all_column_families;

  TEST_SYNC_POINT_CALLBACK(
      "DBImplSecondary::OpenAndCompact::BeforeLoadingOptions:0",
      &compaction_input.options_file_number);
  TEST_SYNC_POINT("DBImplSecondary::OpenAndCompact::BeforeLoadingOptions:1");
  std::string options_file_name =
      OptionsFileName(name, compaction_input.options_file_number);

  s = LoadOptionsFromFile(config_options, options_file_name, &base_db_options,
                          &all_column_families);
  if (!s.ok()) {
    return s;
  }

  // 3. Options to Override
  // Override serializable configurations from override_options.options_map
  DBOptions db_options;
  s = GetDBOptionsFromMap(config_options, base_db_options,
                          override_options.options_map, &db_options);
  if (!s.ok()) {
    return s;
  }

  // Override options that are directly set as shared ptrs in
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
  db_options.info_log = override_options.info_log;

  // 4. Filter CFs that are needed for OpenAndCompact()
  // We do not need to open all column families for the remote compaction.
  // Only open default CF + target CF. If target CF == default CF, we will open
  // just the default CF (Due to current limitation, DB cannot open without the
  // default CF)
  std::vector<ColumnFamilyDescriptor> column_families;
  for (auto& cf : all_column_families) {
    if (cf.name == compaction_input.cf_name) {
      ColumnFamilyOptions cf_options;
      // Override serializable configurations from override_options.options_map
      s = GetColumnFamilyOptionsFromMap(config_options, cf.options,
                                        override_options.options_map,
                                        &cf_options);
      if (!s.ok()) {
        return s;
      }
      cf.options = std::move(cf_options);

      // Override options that are directly set as shared ptrs in
      // CompactionServiceOptionsOverride
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

  TEST_SYNC_POINT_CALLBACK(
      "DBImplSecondary::OpenAndCompact::AfterOpenAsSecondary:0", db);

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

Status DBImplSecondary::CreateCompactionProgressWriter(
    const std::string& file_path,
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  std::unique_ptr<FSWritableFile> file;
  Status s =
      fs_->NewWritableFile(file_path, FileOptions(), &file, nullptr /* dbg */);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), file_path, FileOptions()));

  compaction_progress_writer->reset(
      new log::Writer(std::move(file_writer), 0 /* log_number */,
                      false /* recycle_log_files */));

  return Status::OK();
}

Status DBImplSecondary::PersistInitialCompactionProgress(
    log::Writer* compaction_progress_writer,
    const CompactionProgress& compaction_progress) {
  assert(compaction_progress_writer);

  // LIMITATION: Only supports resuming single subcompaction
  assert(compaction_progress.size() == 1);
  const SubcompactionProgress& subcompaction_progress = compaction_progress[0];

  VersionEdit edit;
  edit.SetSubcompactionProgress(subcompaction_progress);

  std::string record;
  if (!edit.EncodeTo(&record)) {
    return Status::IOError("Failed to encode the initial compaction progress");
  }

  WriteOptions write_options(Env::IOActivity::kCompaction);
  Status s = compaction_progress_writer->AddRecord(write_options, record);
  if (!s.ok()) {
    return s;
  }
  IOOptions opts;
  s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }

  s = compaction_progress_writer->file()->Sync(opts,
                                               immutable_db_options_.use_fsync);

  return s;
}

Status DBImplSecondary::HandleCompactionProgressWriterCreationFailure(
    const std::string& temp_file_path, const std::string& final_file_path,
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  compaction_progress_writer->reset();

  const std::vector<std::string> paths_to_delete = {final_file_path,
                                                    temp_file_path};

  Status s;
  for (const auto& file_path : paths_to_delete) {
    WriteOptions write_options(Env::IOActivity::kCompaction);
    IOOptions opts;
    s = WritableFileWriter::PrepareIOOptions(write_options, opts);
    if (s.ok()) {
      s = fs_->DeleteFile(file_path, opts, nullptr /* dbg */);
    }

    if (!s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Failed to cleanup the compaction progress file "
                      "during writer creation failure: %s",
                      s.ToString().c_str());
      return s;
    }
  }

  return s;
}

Status DBImplSecondary::FinalizeCompactionProgressWriter(
    std::unique_ptr<log::Writer>* compaction_progress_writer) {
  uint64_t timestamp = env_->NowMicros();
  const std::string temp_file_path =
      TempCompactionProgressFileName(secondary_path_, timestamp);

  Status s = CreateCompactionProgressWriter(temp_file_path,
                                            compaction_progress_writer);
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Failed to create compaction progress writer at "
                   "temp path %s: %s. Will perform clean up "
                   "to start compaction without progress persistence",
                   temp_file_path.c_str(), s.ToString().c_str());
    return HandleCompactionProgressWriterCreationFailure(
        temp_file_path, "" /* final_file_path */, compaction_progress_writer);
  }

  if (!compaction_progress_.empty()) {
    s = PersistInitialCompactionProgress(compaction_progress_writer->get(),
                                         compaction_progress_);
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to persist the initial copmaction "
                     "progress: %s. Will perform clean up "
                     "to start compaction without progress persistence",
                     s.ToString().c_str());
      return HandleCompactionProgressWriterCreationFailure(
          temp_file_path, "" /* final_file_path */, compaction_progress_writer);
    }
  }

  compaction_progress_writer->reset();

  std::string final_file_path;
  s = RenameCompactionProgressFile(temp_file_path, &final_file_path);

  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Failed to rename temporary compaction progress "
                   "file from %s to %s: %s.  Will perform clean up "
                   "to start compaction without progress persistence",
                   temp_file_path.c_str(), final_file_path.c_str(),
                   s.ToString().c_str());
    return HandleCompactionProgressWriterCreationFailure(
        temp_file_path, final_file_path, compaction_progress_writer);
  }

  s = CreateCompactionProgressWriter(final_file_path,
                                     compaction_progress_writer);
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Failed to create the final compaction progress "
                   "writer: %s. Will attempt clean to start the compaction "
                   "without progress persistence",
                   s.ToString().c_str());
    return HandleCompactionProgressWriterCreationFailure(
        "" /* temp_file_path */, final_file_path, compaction_progress_writer);
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
