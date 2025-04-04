//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl_readonly.h"

#include "db/arena_wrapped_db_iter.h"
#include "db/db_impl/compacted_db_impl.h"
#include "db/db_impl/db_impl.h"
#include "db/manifest_ops.h"
#include "db/merge_context.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

DBImplReadOnly::DBImplReadOnly(const DBOptions& db_options,
                               const std::string& dbname)
    : DBImpl(db_options, dbname, /*seq_per_batch*/ false,
             /*batch_per_txn*/ true, /*read_only*/ true) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in read only mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplReadOnly::~DBImplReadOnly() = default;

// Implementations of the DB interface
Status DBImplReadOnly::GetImpl(const ReadOptions& read_options,
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

  // In read-only mode Get(), no super version operation is needed (i.e.
  // GetAndRefSuperVersion and ReturnAndCleanupSuperVersion)
  SuperVersion* super_version = cfd->GetSuperVersion();
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    s = FailIfReadCollapsedHistory(cfd, super_version,
                                   *(read_options.timestamp));
    if (!s.ok()) {
      return s;
    }
  }
  // Prepare to store a list of merge operations if merge occurs.
  MergeContext merge_context;
  // TODO - Large Result Optimization for Read Only DB
  // (https://github.com/facebook/rocksdb/pull/10458)

  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey(key, snapshot, read_options.timestamp);
  PERF_TIMER_STOP(get_snapshot_time);

  // Look up starts here
  if (super_version->mem->Get(
          lkey,
          get_impl_options.value ? get_impl_options.value->GetSelf() : nullptr,
          get_impl_options.columns, ts, &s, &merge_context,
          &max_covering_tombstone_seq, read_options,
          false /* immutable_memtable */, &read_cb,
          /*is_blob_index=*/nullptr, /*do_merge=*/get_impl_options.get_value)) {
    if (get_impl_options.value) {
      get_impl_options.value->PinSelf();
    }
    RecordTick(stats_, MEMTABLE_HIT);
  } else {
    PERF_TIMER_GUARD(get_from_output_files_time);
    PinnedIteratorsManager pinned_iters_mgr;
    super_version->current->Get(
        read_options, lkey, get_impl_options.value, get_impl_options.columns,
        ts, &s, &merge_context, &max_covering_tombstone_seq, &pinned_iters_mgr,
        /*value_found*/ nullptr,
        /*key_exists*/ nullptr, /*seq*/ nullptr, &read_cb,
        /*is_blob*/ nullptr,
        /*do_merge=*/get_impl_options.get_value);
    RecordTick(stats_, MEMTABLE_MISS);
  }
  {
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
    RecordInHistogram(stats_, BYTES_PER_READ, size);
    PERF_COUNTER_ADD(get_read_bytes, size);
  }
  return s;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& _read_options,
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
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  if (read_options.timestamp && read_options.timestamp->size() > 0) {
    const Status s = FailIfReadCollapsedHistory(cfd, super_version,
                                                *(read_options.timestamp));
    if (!s.ok()) {
      cfd->GetSuperVersion()->Unref();
      return NewErrorIterator(s);
    }
  }
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SequenceNumber read_seq =
      read_options.snapshot != nullptr
          ? static_cast<const SnapshotImpl*>(read_options.snapshot)->number_
          : latest_snapshot;
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  return NewArenaWrappedDbIterator(
      env_, read_options, cfh, super_version, read_seq, read_callback, this,
      /*expose_blob_index=*/false, /*allow_refresh=*/false,
      /*allow_mark_memtable_for_flush=*/false);
}

Status DBImplReadOnly::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
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

  ReadCallback* read_callback = nullptr;  // No read callback provided.
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SequenceNumber read_seq =
      read_options.snapshot != nullptr
          ? static_cast<const SnapshotImpl*>(read_options.snapshot)->number_
          : latest_snapshot;

  autovector<std::tuple<ColumnFamilyHandleImpl*, SuperVersion*>> cfh_to_sv;

  const bool check_read_ts =
      read_options.timestamp && read_options.timestamp->size() > 0;
  for (auto cfh : column_families) {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
    auto* sv = cfd->GetSuperVersion()->Ref();
    cfh_to_sv.emplace_back(static_cast_with_check<ColumnFamilyHandleImpl>(cfh),
                           sv);
    if (check_read_ts) {
      const Status s =
          FailIfReadCollapsedHistory(cfd, sv, *(read_options.timestamp));
      if (!s.ok()) {
        for (auto prev_entry : cfh_to_sv) {
          std::get<1>(prev_entry)->Unref();
        }
        return s;
      }
    }
  }
  assert(cfh_to_sv.size() == column_families.size());
  for (auto [cfh, sv] : cfh_to_sv) {
    auto* db_iter = NewArenaWrappedDbIterator(
        env_, read_options, cfh, sv, read_seq, read_callback, this,
        /*expose_blob_index=*/false, /*allow_refresh=*/false,
        /*allow_mark_memtable_for_flush=*/false);
    iterators->push_back(db_iter);
  }

  return Status::OK();
}

namespace {
// Return OK if dbname exists in the file system or create it if
// create_if_missing
Status OpenForReadOnlyCheckExistence(const DBOptions& db_options,
                                     const std::string& dbname) {
  Status s;
  if (!db_options.create_if_missing) {
    // Attempt to read "CURRENT" file
    const std::shared_ptr<FileSystem>& fs = db_options.env->GetFileSystem();
    std::string manifest_path;
    uint64_t manifest_file_number;
    s = GetCurrentManifestPath(dbname, fs.get(), /*is_retry=*/false,
                               &manifest_path, &manifest_file_number);
  } else {
    // Historic behavior that doesn't necessarily make sense
    s = db_options.env->CreateDirIfMissing(dbname);
  }
  return s;
}
}  // namespace

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           std::unique_ptr<DB>* dbptr,
                           bool /*error_if_wal_file_exists*/) {
  Status s = OpenForReadOnlyCheckExistence(options, dbname);
  if (!s.ok()) {
    return s;
  }

  *dbptr = nullptr;

  // Try to first open DB as fully compacted DB
  s = CompactedDBImpl::Open(options, dbname, dbptr);
  if (s.ok()) {
    return s;
  }

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;

  s = DBImplReadOnly::OpenForReadOnlyWithoutCheck(
      db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a
    // reference to default column family
    delete handles[0];
  }
  return s;
}

Status DB::OpenForReadOnly(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr,
    bool error_if_wal_file_exists) {
  // If dbname does not exist in the file system, should not do anything
  Status s = OpenForReadOnlyCheckExistence(db_options, dbname);
  if (!s.ok()) {
    return s;
  }

  return DBImplReadOnly::OpenForReadOnlyWithoutCheck(
      db_options, dbname, column_families, handles, dbptr,
      error_if_wal_file_exists);
}

Status DBImplReadOnly::OpenForReadOnlyWithoutCheck(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr,
    bool error_if_wal_file_exists) {
  *dbptr = nullptr;
  handles->clear();

  SuperVersionContext sv_context(/* create_superversion */ true);
  DBImplReadOnly* impl = new DBImplReadOnly(db_options, dbname);
  impl->mutex_.Lock();
  Status s = impl->Recover(column_families, true /* read only */,
                           error_if_wal_file_exists);
  if (s.ok()) {
    // set column family handles
    for (const auto& cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (cfd == nullptr) {
        s = Status::InvalidArgument("Column family not found", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
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
    for (auto* h : *handles) {
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

}  // namespace ROCKSDB_NAMESPACE
