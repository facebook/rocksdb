//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db_impl.h"

#include "util/logging.h"
#include "util/mutexlock.h"

// BlobDBImpl methods to get snapshot of files, e.g. for replication.

namespace rocksdb {
namespace blob_db {

Status BlobDBImpl::DisableFileDeletions() {
  // Disable base DB file deletions.
  Status s = db_impl_->DisableFileDeletions();
  if (!s.ok()) {
    return s;
  }

  int count = 0;
  {
    // Hold delete_file_mutex_ to make sure no DeleteObsoleteFiles job
    // is running.
    MutexLock l(&delete_file_mutex_);
    count = ++disable_file_deletions_;
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Disalbed blob file deletions. count: %d", count);
  return Status::OK();
}

Status BlobDBImpl::EnableFileDeletions(bool force) {
  // Enable base DB file deletions.
  Status s = db_impl_->EnableFileDeletions(force);
  if (!s.ok()) {
    return s;
  }

  int count = 0;
  {
    MutexLock l(&delete_file_mutex_);
    if (force) {
      disable_file_deletions_ = 0;
    } else if (disable_file_deletions_ > 0) {
      count = --disable_file_deletions_;
    }
    assert(count >= 0);
  }

  ROCKS_LOG_INFO(db_options_.info_log, "Enabled blob file deletions. count: %d",
                 count);
  // Consider trigger DeleteobsoleteFiles once after re-enabled, if we are to
  // make DeleteobsoleteFiles re-run interval configuration.
  return Status::OK();
}

Status BlobDBImpl::GetLiveFiles(std::vector<std::string>& ret,
                                uint64_t* manifest_file_size,
                                bool flush_memtable) {
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  Status s = db_->GetLiveFiles(ret, manifest_file_size, flush_memtable);
  if (!s.ok()) {
    return s;
  }
  ret.reserve(ret.size() + blob_files_.size());
  for (auto bfile_pair : blob_files_) {
    auto blob_file = bfile_pair.second;
    ret.emplace_back(blob_file->PathName());
  }
  return Status::OK();
}

void BlobDBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  db_->GetLiveFilesMetaData(metadata);
  for (auto bfile_pair : blob_files_) {
    auto blob_file = bfile_pair.second;
    LiveFileMetaData filemetadata;
    filemetadata.size = blob_file->GetFileSize();
    filemetadata.name = blob_file->PathName();
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily());
    filemetadata.column_family_name = cfh->GetName();
    metadata->emplace_back(filemetadata);
  }
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
