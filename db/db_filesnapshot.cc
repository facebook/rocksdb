//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

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
#include "util/file_util.h"

namespace rocksdb {

Status DBImpl::DisableFileDeletions() {
  MutexLock l(&mutex_);
  ++disable_delete_obsolete_files_;
  if (disable_delete_obsolete_files_ == 1) {
    Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
        "File Deletions Disabled");
  } else {
    Log(InfoLogLevel::WARN_LEVEL, db_options_.info_log,
        "File Deletions Disabled, but already disabled. Counter: %d",
        disable_delete_obsolete_files_);
  }
  return Status::OK();
}

Status DBImpl::EnableFileDeletions(bool force) {
  JobContext job_context;
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
      Log(InfoLogLevel::INFO_LEVEL, db_options_.info_log,
          "File Deletions Enabled");
      should_purge_files = true;
      FindObsoleteFiles(&job_context, true);
    } else {
      Log(InfoLogLevel::WARN_LEVEL, db_options_.info_log,
          "File Deletions Enable, but not really enabled. Counter: %d",
          disable_delete_obsolete_files_);
    }
  }
  if (should_purge_files)  {
    PurgeObsoleteFiles(job_context);
  }
  LogFlush(db_options_.info_log);
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
      Log(InfoLogLevel::ERROR_LEVEL, db_options_.info_log,
          "Cannot Flush data %s\n", status.ToString().c_str());
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
  ret.push_back(DescriptorFileName("", versions_->manifest_file_number()));

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->manifest_file_size();

  mutex_.Unlock();
  return Status::OK();
}

Status DBImpl::GetSortedWalFiles(VectorLogPtr& files) {
  return wal_manager_.GetSortedWalFiles(files);
}

// Builds an openable snapshot of RocksDB
Status DBImpl::CreateCheckpoint(const std::string& snapshot_dir) {
  Status s;
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  uint64_t sequence_number = GetLatestSequenceNumber();
  bool same_fs = true;

  if (env_->FileExists(snapshot_dir)) {
    return Status::InvalidArgument("Directory exists");
  }

  s = DisableFileDeletions();
  if (s.ok()) {
    // this will return live_files prefixed with "/"
    s = GetLiveFiles(live_files, &manifest_file_size, true);
  }
  if (!s.ok()) {
    EnableFileDeletions(false);
    return s;
  }

  Log(db_options_.info_log,
      "Started the snapshot process -- creating snapshot in directory %s",
      snapshot_dir.c_str());

  std::string full_private_path = snapshot_dir + ".tmp";

  // create snapshot directory
  s = env_->CreateDir(full_private_path);

  // copy/hard link live_files
  for (size_t i = 0; s.ok() && i < live_files.size(); ++i) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(live_files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }
    // we should only get sst, manifest and current files here
    assert(type == kTableFile || type == kDescriptorFile ||
           type == kCurrentFile);
    assert(live_files[i].size() > 0 && live_files[i][0] == '/');
    std::string src_fname = live_files[i];

    // rules:
    // * if it's kTableFile, then it's shared
    // * if it's kDescriptorFile, limit the size to manifest_file_size
    // * always copy if cross-device link
    if ((type == kTableFile) && same_fs) {
      Log(db_options_.info_log, "Hard Linking %s", src_fname.c_str());
      s = env_->LinkFile(GetName() + src_fname, full_private_path + src_fname);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if ((type != kTableFile) || (!same_fs)) {
      Log(db_options_.info_log, "Copying %s", src_fname.c_str());
      s = CopyFile(env_, GetName() + src_fname, full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0);
    }
  }

  // we copied all the files, enable file deletions
  EnableFileDeletions(false);

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = env_->RenameFile(full_private_path, snapshot_dir);
  }
  if (s.ok()) {
    unique_ptr<Directory> snapshot_directory;
    env_->NewDirectory(snapshot_dir, &snapshot_directory);
    if (snapshot_directory != nullptr) {
      s = snapshot_directory->Fsync();
    }
  }

  if (!s.ok()) {
    // clean all the files we might have created
    Log(db_options_.info_log, "Snapshot failed -- %s", s.ToString().c_str());
    // we have to delete the dir and all its children
    std::vector<std::string> subchildren;
    env_->GetChildren(full_private_path, &subchildren);
    for (auto& subchild : subchildren) {
      Status s1 = env_->DeleteFile(full_private_path + subchild);
      if (s1.ok()) {
        Log(db_options_.info_log, "Deleted %s",
            (full_private_path + subchild).c_str());
      }
    }
    // finally delete the private dir
    Status s1 = env_->DeleteDir(full_private_path);
    Log(db_options_.info_log, "Deleted dir %s -- %s", full_private_path.c_str(),
        s1.ToString().c_str());
    return s;
  }

  // here we know that we succeeded and installed the new snapshot
  Log(db_options_.info_log, "Snapshot DONE. All is good");
  Log(db_options_.info_log, "Snapshot sequence number: %" PRIu64,
      sequence_number);

  return s;
}
}

#endif  // ROCKSDB_LITE
