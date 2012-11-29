// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "db/db_impl.h"
#include "db/filename.h"
#include <string>
#include <stdint.h>
#include "db/version_set.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"

namespace leveldb {

Status DBImpl::DisableFileDeletions() {
  MutexLock l(&mutex_);
  disable_delete_obsolete_files_ = true;
  return Status::OK();
}

Status DBImpl::EnableFileDeletions() {
  MutexLock l(&mutex_);
  disable_delete_obsolete_files_ = false;
  return Status::OK();
}

Status DBImpl::GetLiveFiles(std::vector<std::string>& ret,
                            uint64_t* manifest_file_size) {

  *manifest_file_size = 0;

  // flush all dirty data to disk.
  Status status =  Flush(FlushOptions());
  if (!status.ok()) {
    Log(options_.info_log, "Cannot Flush data %s\n",
        status.ToString().c_str());
    return status;
  }

  MutexLock l(&mutex_);

  // Make a set of all of the live *.sst files
  std::set<uint64_t> live;
  versions_->AddLiveFilesCurrentVersion(&live);

  ret.resize(live.size() + 2); //*.sst + CURRENT + MANIFEST

  // create names of the live files. The names are not absolute
  // paths, instead they are relative to dbname_;
  std::set<uint64_t>::iterator it = live.begin();
  for (unsigned int i = 0; i < live.size(); i++, it++) {
    ret[i] = TableFileName("", *it);
  }

  ret[live.size()] = CurrentFileName("");
  ret[live.size()+1] = DescriptorFileName("",
                                          versions_->ManifestFileNumber());

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->ManifestFileSize();

  return Status::OK();
}

}
