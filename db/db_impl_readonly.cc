//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_impl_readonly.h"

#include "db/compacted_db_impl.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/merge_context.h"
#include "db/range_del_aggregator.h"
#include "monitoring/perf_context_imp.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

DBImplReadOnly::DBImplReadOnly(const DBOptions& db_options,
                               const std::string& dbname)
    : DBImpl(db_options, dbname) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in read only mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplReadOnly::~DBImplReadOnly() {
}

// Implementations of the DB interface
Status DBImplReadOnly::Get(const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* pinnable_val) {
  assert(pinnable_val != nullptr);
  Status s;
  SequenceNumber snapshot = versions_->LastSequence();
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion();
  MergeContext merge_context;
  RangeDelAggregator range_del_agg(cfd->internal_comparator(), snapshot);
  LookupKey lkey(key, snapshot);
  if (super_version->mem->Get(lkey, pinnable_val->GetSelf(), &s, &merge_context,
                              &range_del_agg, read_options)) {
    pinnable_val->PinSelf();
  } else {
    PERF_TIMER_GUARD(get_from_output_files_time);
    super_version->current->Get(read_options, lkey, pinnable_val, &s,
                                &merge_context, &range_del_agg);
  }
  return s;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& read_options,
                                      ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  SequenceNumber latest_snapshot = versions_->LastSequence();
  auto db_iter = NewArenaWrappedDbIterator(
      env_, read_options, *cfd->ioptions(),
      (read_options.snapshot != nullptr
           ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                 ->number_
           : latest_snapshot),
      super_version->mutable_cf_options.max_sequential_skip_in_iterations,
      super_version->version_number);
  auto internal_iter =
      NewInternalIterator(read_options, cfd, super_version, db_iter->GetArena(),
                          db_iter->GetRangeDelAggregator());
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}

Status DBImplReadOnly::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  SequenceNumber latest_snapshot = versions_->LastSequence();

  for (auto cfh : column_families) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
    auto* sv = cfd->GetSuperVersion()->Ref();
    auto* db_iter = NewArenaWrappedDbIterator(
        env_, read_options, *cfd->ioptions(),
        (read_options.snapshot != nullptr
             ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                   ->number_
             : latest_snapshot),
        sv->mutable_cf_options.max_sequential_skip_in_iterations,
        sv->version_number);
    auto* internal_iter =
        NewInternalIterator(read_options, cfd, sv, db_iter->GetArena(),
                            db_iter->GetRangeDelAggregator());
    db_iter->SetIterUnderDBIter(internal_iter);
    iterators->push_back(db_iter);
  }

  return Status::OK();
}

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool error_if_log_file_exist) {
  *dbptr = nullptr;

  // Try to first open DB as fully compacted DB
  Status s;
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

  s = DB::OpenForReadOnly(db_options, dbname, column_families, &handles, dbptr);
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
    bool error_if_log_file_exist) {
  *dbptr = nullptr;
  handles->clear();

  DBImplReadOnly* impl = new DBImplReadOnly(db_options, dbname);
  impl->mutex_.Lock();
  Status s = impl->Recover(column_families, true /* read only */,
                           error_if_log_file_exist);
  if (s.ok()) {
    // set column family handles
    for (auto cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (cfd == nullptr) {
        s = Status::InvalidArgument("Column family not found: ", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      delete cfd->InstallSuperVersion(new SuperVersion(), &impl->mutex_);
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
    for (auto* h : *handles) {
      impl->NewThreadStatusCfInfo(
          reinterpret_cast<ColumnFamilyHandleImpl*>(h)->cfd());
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

#else  // !ROCKSDB_LITE

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool error_if_log_file_exist) {
  return Status::NotSupported("Not supported in ROCKSDB_LITE.");
}

Status DB::OpenForReadOnly(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    bool error_if_log_file_exist) {
  return Status::NotSupported("Not supported in ROCKSDB_LITE.");
}
#endif  // !ROCKSDB_LITE

}   // namespace rocksdb
