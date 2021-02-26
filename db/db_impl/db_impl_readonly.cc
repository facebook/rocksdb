//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl/db_impl_readonly.h"

#include "db/arena_wrapped_db_iter.h"
#include "db/compacted_db_impl.h"
#include "db/db_impl/db_impl.h"
#include "db/db_iter.h"
#include "db/merge_context.h"
#include "monitoring/perf_context_imp.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE

DBImplReadOnly::DBImplReadOnly(const DBOptions& db_options,
                               const std::string& dbname)
    : DBImpl(db_options, dbname) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in read only mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplReadOnly::~DBImplReadOnly() {}

// Implementations of the DB interface
Status DBImplReadOnly::Get(const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* pinnable_val) {
  assert(pinnable_val != nullptr);
  // TODO: stopwatch DB_GET needed?, perf timer needed?
  PERF_TIMER_GUARD(get_snapshot_time);
  Status s;
  SequenceNumber snapshot = versions_->LastSequence();
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      tracer_->Get(column_family, key);
    }
  }
  SuperVersion* super_version = cfd->GetSuperVersion();
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  LookupKey lkey(key, snapshot);
  PERF_TIMER_STOP(get_snapshot_time);
  if (super_version->mem->Get(lkey, pinnable_val->GetSelf(),
                              /*timestamp=*/nullptr, &s, &merge_context,
                              &max_covering_tombstone_seq, read_options)) {
    pinnable_val->PinSelf();
    RecordTick(stats_, MEMTABLE_HIT);
  } else {
    PERF_TIMER_GUARD(get_from_output_files_time);
    super_version->current->Get(read_options, lkey, pinnable_val,
                                /*timestamp=*/nullptr, &s, &merge_context,
                                &max_covering_tombstone_seq);
    RecordTick(stats_, MEMTABLE_MISS);
  }
  RecordTick(stats_, NUMBER_KEYS_READ);
  size_t size = pinnable_val->size();
  RecordTick(stats_, BYTES_READ, size);
  RecordInHistogram(stats_, BYTES_PER_READ, size);
  PERF_COUNTER_ADD(get_read_bytes, size);
  return s;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& read_options,
                                      ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SequenceNumber read_seq =
      read_options.snapshot != nullptr
          ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                ->number_
          : latest_snapshot;
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  auto db_iter = NewArenaWrappedDbIterator(
      env_, read_options, *cfd->ioptions(), super_version->mutable_cf_options,
      super_version->current, read_seq,
      super_version->mutable_cf_options.max_sequential_skip_in_iterations,
      super_version->version_number, read_callback);
  auto internal_iter = NewInternalIterator(
      db_iter->GetReadOptions(), cfd, super_version, db_iter->GetArena(),
      db_iter->GetRangeDelAggregator(), read_seq,
      /* allow_unprepared_value */ true);
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}

Status DBImplReadOnly::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  SequenceNumber latest_snapshot = versions_->LastSequence();
  SequenceNumber read_seq =
      read_options.snapshot != nullptr
          ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                ->number_
          : latest_snapshot;

  for (auto cfh : column_families) {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
    auto* sv = cfd->GetSuperVersion()->Ref();
    auto* db_iter = NewArenaWrappedDbIterator(
        env_, read_options, *cfd->ioptions(), sv->mutable_cf_options,
        sv->current, read_seq,
        sv->mutable_cf_options.max_sequential_skip_in_iterations,
        sv->version_number, read_callback);
    auto* internal_iter = NewInternalIterator(
        db_iter->GetReadOptions(), cfd, sv, db_iter->GetArena(),
        db_iter->GetRangeDelAggregator(), read_seq,
        /* allow_unprepared_value */ true);
    db_iter->SetIterUnderDBIter(internal_iter);
    iterators->push_back(db_iter);
  }

  return Status::OK();
}

namespace {
// Return OK if dbname exists in the file system
// or create_if_missing is false
Status OpenForReadOnlyCheckExistence(const DBOptions& db_options,
                                     const std::string& dbname) {
  Status s;
  if (!db_options.create_if_missing) {
    // Attempt to read "CURRENT" file
    const std::shared_ptr<FileSystem>& fs = db_options.env->GetFileSystem();
    std::string manifest_path;
    uint64_t manifest_file_number;
    s = VersionSet::GetCurrentManifestPath(dbname, fs.get(), &manifest_path,
                                           &manifest_file_number);
  }
  return s;
}
}  // namespace

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool /*error_if_wal_file_exists*/) {
  // If dbname does not exist in the file system, should not do anything
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
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
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
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
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
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
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
    for (auto cf : column_families) {
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
    *dbptr = impl;
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

#else   // !ROCKSDB_LITE

Status DB::OpenForReadOnly(const Options& /*options*/,
                           const std::string& /*dbname*/, DB** /*dbptr*/,
                           bool /*error_if_wal_file_exists*/) {
  return Status::NotSupported("Not supported in ROCKSDB_LITE.");
}

Status DB::OpenForReadOnly(
    const DBOptions& /*db_options*/, const std::string& /*dbname*/,
    const std::vector<ColumnFamilyDescriptor>& /*column_families*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/, DB** /*dbptr*/,
    bool /*error_if_wal_file_exists*/) {
  return Status::NotSupported("Not supported in ROCKSDB_LITE.");
}
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
