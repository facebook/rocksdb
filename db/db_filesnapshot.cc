//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <algorithm>
#include <string>
#include <stdint.h>
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"

namespace rocksdb {

Status DBImpl::DisableFileDeletions() {
  MutexLock l(&mutex_);
  ++disable_delete_obsolete_files_;
  if (disable_delete_obsolete_files_ == 1) {
    Log(options_.info_log, "File Deletions Disabled");
  } else {
    Log(options_.info_log,
        "File Deletions Disabled, but already disabled. Counter: %d",
        disable_delete_obsolete_files_);
  }
  return Status::OK();
}

Status DBImpl::EnableFileDeletions(bool force) {
  DeletionState deletion_state;
  bool should_purge_files = false;
  {
    MutexLock l(&mutex_);
    if (force) {
      // if force, we need to enable file deletions right away
      disable_delete_obsolete_files_ = 0;
    } else if (disable_delete_obsolete_files_ > 0) {
      --disable_delete_obsolete_files_;
    }
    if (disable_delete_obsolete_files_ == 0)  {
      Log(options_.info_log, "File Deletions Enabled");
      should_purge_files = true;
      FindObsoleteFiles(deletion_state, true);
    } else {
      Log(options_.info_log,
          "File Deletions Enable, but not really enabled. Counter: %d",
          disable_delete_obsolete_files_);
    }
  }
  if (should_purge_files)  {
    PurgeObsoleteFiles(deletion_state);
  }
  LogFlush(options_.info_log);
  return Status::OK();
}

int DBImpl::IsFileDeletionsEnabled() const {
  return disable_delete_obsolete_files_;
}

Status DBImpl::GetLiveFiles(std::vector<std::string>& ret,
                            uint64_t* manifest_file_size,
                            bool flush_memtable) {

  *manifest_file_size = 0;

  mutex_.Lock();

  if (flush_memtable) {
    // flush all dirty data to disk.
    Status status;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      cfd->Ref();
      mutex_.Unlock();
      status = FlushMemTable(cfd, FlushOptions());
      mutex_.Lock();
      cfd->Unref();
      if (!status.ok()) {
        break;
      }
    }
    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

    if (!status.ok()) {
      mutex_.Unlock();
      Log(options_.info_log, "Cannot Flush data %s\n",
          status.ToString().c_str());
      return status;
    }
  }

  // Make a set of all of the live *.sst files
  std::vector<FileDescriptor> live;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    cfd->current()->AddLiveFiles(&live);
  }

  ret.clear();
  ret.reserve(live.size() + 2); //*.sst + CURRENT + MANIFEST

  // create names of the live files. The names are not absolute
  // paths, instead they are relative to dbname_;
  for (auto live_file : live) {
    ret.push_back(MakeTableFileName("", live_file.GetNumber()));
  }

  ret.push_back(CurrentFileName(""));
  ret.push_back(DescriptorFileName("", versions_->ManifestFileNumber()));

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->ManifestFileSize();

  mutex_.Unlock();
  return Status::OK();
}

Status DBImpl::GetSortedWalFiles(VectorLogPtr& files) {
  // First get sorted files in db dir, then get sorted files from archived
  // dir, to avoid a race condition where a log file is moved to archived
  // dir in between.
  Status s;
  // list wal files in main db dir.
  VectorLogPtr logs;
  s = GetSortedWalsOfType(options_.wal_dir, logs, kAliveLogFile);
  if (!s.ok()) {
    return s;
  }

  // Reproduce the race condition where a log file is moved
  // to archived dir, between these two sync points, used in
  // (DBTest,TransactionLogIteratorRace)
  TEST_SYNC_POINT("DBImpl::GetSortedWalFiles:1");
  TEST_SYNC_POINT("DBImpl::GetSortedWalFiles:2");

  files.clear();
  // list wal files in archive dir.
  std::string archivedir = ArchivalDirectory(options_.wal_dir);
  if (env_->FileExists(archivedir)) {
    s = GetSortedWalsOfType(archivedir, files, kArchivedLogFile);
    if (!s.ok()) {
      return s;
    }
  }

  uint64_t latest_archived_log_number = 0;
  if (!files.empty()) {
    latest_archived_log_number = files.back()->LogNumber();
    Log(options_.info_log, "Latest Archived log: %" PRIu64,
        latest_archived_log_number);
  }

  files.reserve(files.size() + logs.size());
  for (auto& log : logs) {
    if (log->LogNumber() > latest_archived_log_number) {
      files.push_back(std::move(log));
    } else {
      // When the race condition happens, we could see the
      // same log in both db dir and archived dir. Simply
      // ignore the one in db dir. Note that, if we read
      // archived dir first, we would have missed the log file.
      Log(options_.info_log, "%s already moved to archive",
          log->PathName().c_str());
    }
  }

  return s;
}

}

#endif  // ROCKSDB_LITE
