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
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/build_version.h"

namespace rocksdb {

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
  MemTable* mem = GetMemTable();
  Version* current = versions_->current();
  SequenceNumber snapshot = versions_->LastSequence();
  std::deque<std::string> merge_operands;
  LookupKey lkey(key, snapshot);
  if (mem->Get(lkey, value, &s, &merge_operands, options_)) {
  } else {
    Version::GetStats stats;
    current->Get(options, lkey, value, &s, &merge_operands, &stats, options_);
  }
  return s;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
  return NewDBIterator(
    &dbname_, env_, options_,  user_comparator(),internal_iter,
      (options.snapshot != nullptr
      ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
      : latest_snapshot));
}


Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                DB** dbptr, bool error_if_log_file_exist) {
  *dbptr = nullptr;

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
