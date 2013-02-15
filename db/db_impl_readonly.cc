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
#include <algorithm>
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/build_version.h"

namespace leveldb {

DBImplReadOnly::DBImplReadOnly(const Options& options,
    const std::string& dbname)
    : DBImpl(options, dbname) {
  Log(options_.info_log, "Opening the db in read only mode");
}

DBImplReadOnly::~DBImplReadOnly() {
}

// Implementations of the DB interface
Status DBImplReadOnly::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  Version* current = versions_->current();
  SequenceNumber snapshot = versions_->LastSequence();
  LookupKey lkey(key, snapshot);
  Version::GetStats stats;
  s = current->Get(options, lkey, value, &stats);
  return s;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& options) {
  std::vector<Iterator*> list;
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  return NewDBIterator(
      &dbname_, env_, user_comparator(), internal_iter,
      (options.snapshot != NULL
      ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
      : versions_->LastSequence()));
}


Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                DB** dbptr, bool error_if_log_file_exist) {
  *dbptr = NULL;

  DBImplReadOnly* impl = new DBImplReadOnly(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit(impl->NumberLevels());
  Status s = impl->Recover(&edit, impl->GetMemTable(),
                           error_if_log_file_exist);
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

}
