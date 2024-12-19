//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/filename.h"
#include "logging/logging.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "utilities/blob_db/blob_db_impl.h"

// BlobDBImpl methods to get snapshot of files, e.g. for replication.

namespace ROCKSDB_NAMESPACE::blob_db {

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
                 "Disabled blob file deletions. count: %d", count);
  return Status::OK();
}

Status BlobDBImpl::EnableFileDeletions() {
  // Enable base DB file deletions.
  Status s = db_impl_->EnableFileDeletions();
  if (!s.ok()) {
    return s;
  }

  int count = 0;
  {
    MutexLock l(&delete_file_mutex_);
    if (disable_file_deletions_ > 0) {
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
  if (!bdb_options_.path_relative) {
    return Status::NotSupported(
        "Not able to get relative blob file path from absolute blob_dir.");
  }
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  Status s = db_->GetLiveFiles(ret, manifest_file_size, flush_memtable);
  if (!s.ok()) {
    return s;
  }
  ret.reserve(ret.size() + blob_files_.size());
  for (const auto& bfile_pair : blob_files_) {
    auto blob_file = bfile_pair.second;
    // Path should be relative to db_name, but begin with slash.
    ret.emplace_back(
        BlobFileName("", bdb_options_.blob_dir, blob_file->BlobFileNumber()));
  }
  return Status::OK();
}

void BlobDBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  // Path should be relative to db_name.
  assert(bdb_options_.path_relative);
  // Hold a lock in the beginning to avoid updates to base DB during the call
  ReadLock rl(&mutex_);
  db_->GetLiveFilesMetaData(metadata);
  for (const auto& bfile_pair : blob_files_) {
    auto blob_file = bfile_pair.second;
    LiveFileMetaData filemetadata;
    filemetadata.size = blob_file->GetFileSize();
    const uint64_t file_number = blob_file->BlobFileNumber();
    // Path should be relative to db_name, but begin with slash.
    filemetadata.name = BlobFileName("", bdb_options_.blob_dir, file_number);
    filemetadata.file_number = file_number;
    if (blob_file->HasTTL()) {
      filemetadata.oldest_ancester_time = blob_file->GetExpirationRange().first;
    }
    auto cfh =
        static_cast_with_check<ColumnFamilyHandleImpl>(DefaultColumnFamily());
    filemetadata.column_family_name = cfh->GetName();
    metadata->emplace_back(filemetadata);
  }
}

Status BlobDBImpl::GetLiveFilesStorageInfo(
    const LiveFilesStorageInfoOptions& opts,
    std::vector<LiveFileStorageInfo>* files) {
  ReadLock rl(&mutex_);
  Status s = db_->GetLiveFilesStorageInfo(opts, files);
  if (s.ok()) {
    files->reserve(files->size() + blob_files_.size());
    for (const auto& [blob_number, blob_file] : blob_files_) {
      LiveFileStorageInfo file;
      file.size = blob_file->GetFileSize();
      file.directory = blob_dir_;
      file.relative_filename = BlobFileName(blob_number);
      file.file_type = kBlobFile;
      file.trim_to_size = true;
      files->push_back(std::move(file));
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE::blob_db
