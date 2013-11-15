//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

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

namespace rocksdb {

Status DBImpl::DisableFileDeletions() {
  MutexLock l(&mutex_);
  disable_delete_obsolete_files_ = true;
  Log(options_.info_log, "File Deletions Disabled");
  return Status::OK();
}

Status DBImpl::EnableFileDeletions() {
  DeletionState deletion_state;
  {
    MutexLock l(&mutex_);
    disable_delete_obsolete_files_ = false;
    Log(options_.info_log, "File Deletions Enabled");
    FindObsoleteFiles(deletion_state, true);
  }
  PurgeObsoleteFiles(deletion_state);
  LogFlush(options_.info_log);
  return Status::OK();
}

Status DBImpl::GetLiveFiles(std::vector<std::string>& ret,
                            uint64_t* manifest_file_size,
                            bool flush_memtable) {

  *manifest_file_size = 0;

  if (flush_memtable) {
    // flush all dirty data to disk.
    Status status =  Flush(FlushOptions());
    if (!status.ok()) {
      Log(options_.info_log, "Cannot Flush data %s\n",
          status.ToString().c_str());
      return status;
    }
  }

  MutexLock l(&mutex_);

  // Make a set of all of the live *.sst files
  std::set<uint64_t> live;
  versions_->AddLiveFilesCurrentVersion(&live);

  ret.clear();
  ret.reserve(live.size() + 2); //*.sst + CURRENT + MANIFEST

  // create names of the live files. The names are not absolute
  // paths, instead they are relative to dbname_;
  for (auto live_file : live) {
    ret.push_back(TableFileName("", live_file));
  }

  ret.push_back(CurrentFileName(""));
  ret.push_back(DescriptorFileName("", versions_->ManifestFileNumber()));

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->ManifestFileSize();

  return Status::OK();
}

Status DBImpl::GetSortedWalFiles(VectorLogPtr& files) {
  // First get sorted files in archive dir, then append sorted files from main
  // dir to maintain sorted order

  // list wal files in archive dir.
  Status s;
  std::string archivedir = ArchivalDirectory(options_.wal_dir);
  if (env_->FileExists(archivedir)) {
    s = AppendSortedWalsOfType(archivedir, files, kArchivedLogFile);
    if (!s.ok()) {
      return s;
    }
  }
  // list wal files in main db dir.
  return AppendSortedWalsOfType(options_.wal_dir, files, kAliveLogFile);
}

}
