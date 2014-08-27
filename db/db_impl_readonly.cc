//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/db_impl_readonly.h"
#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/merge_operator.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/build_version.h"

namespace rocksdb {

DBImplReadOnly::DBImplReadOnly(const DBOptions& options,
                               const std::string& dbname)
    : DBImpl(options, dbname) {
  Log(options_.info_log, "Opening the db in read only mode");
}

DBImplReadOnly::~DBImplReadOnly() {
}

// Implementations of the DB interface
Status DBImplReadOnly::Get(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value) {
  Status s;
  SequenceNumber snapshot = versions_->LastSequence();
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion();
  MergeContext merge_context;
  LookupKey lkey(key, snapshot);
  if (super_version->mem->Get(lkey, value, &s, merge_context,
                              *cfd->options())) {
  } else {
    super_version->current->Get(options, lkey, value, &s, &merge_context);
  }
  return s;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& options,
                                      ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  SequenceNumber latest_snapshot = versions_->LastSequence();
  auto db_iter = NewArenaWrappedDbIterator(
      env_, *cfd->options(), cfd->user_comparator(),
      (options.snapshot != nullptr
           ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
           : latest_snapshot));
  auto internal_iter =
      NewInternalIterator(options, cfd, super_version, db_iter->GetArena());
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}

Status DBImplReadOnly::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  SequenceNumber latest_snapshot = versions_->LastSequence();

  for (auto cfh : column_families) {
    auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
    auto db_iter = NewArenaWrappedDbIterator(
        env_, *cfd->options(), cfd->user_comparator(),
        options.snapshot != nullptr
            ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
            : latest_snapshot);
    auto internal_iter = NewInternalIterator(
        options, cfd, cfd->GetSuperVersion()->Ref(), db_iter->GetArena());
    db_iter->SetIterUnderDBIter(internal_iter);
    iterators->push_back(db_iter);
  }

  return Status::OK();
}

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool error_if_log_file_exist) {
  *dbptr = nullptr;

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;

  Status s =
      DB::OpenForReadOnly(db_options, dbname, column_families, &handles, dbptr);
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
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}


}   // namespace rocksdb
