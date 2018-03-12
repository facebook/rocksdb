//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/sst_file_manager_impl.h"

#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_manager.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
SstFileManagerImpl::SstFileManagerImpl(Env* env, std::shared_ptr<Logger> logger,
                                       int64_t rate_bytes_per_sec,
                                       double max_trash_db_ratio)
    : env_(env),
      logger_(logger),
      total_files_size_(0),
      compaction_buffer_size_(0),
      cur_compactions_reserved_size_(0),
      max_allowed_space_(0),
      delete_scheduler_(env, rate_bytes_per_sec, logger.get(), this,
                        max_trash_db_ratio) {}

SstFileManagerImpl::~SstFileManagerImpl() {}

Status SstFileManagerImpl::OnAddFile(const std::string& file_path) {
  uint64_t file_size;
  Status s = env_->GetFileSize(file_path, &file_size);
  if (s.ok()) {
    MutexLock l(&mu_);
    OnAddFileImpl(file_path, file_size);
  }
  TEST_SYNC_POINT("SstFileManagerImpl::OnAddFile");
  return s;
}

Status SstFileManagerImpl::OnDeleteFile(const std::string& file_path) {
  {
    MutexLock l(&mu_);
    OnDeleteFileImpl(file_path);
  }
  TEST_SYNC_POINT("SstFileManagerImpl::OnDeleteFile");
  return Status::OK();
}

void SstFileManagerImpl::OnCompactionCompletion(Compaction* c) {
  MutexLock l(&mu_);
  uint64_t size_added_by_compaction = 0;
  for (size_t i = 0; i < c->num_input_levels(); i++) {
    for (size_t j = 0; j < c->num_input_files(i); j++) {
      FileMetaData* filemeta = c->input(i, j);
      size_added_by_compaction += filemeta->fd.GetFileSize();
    }
  }
  cur_compactions_reserved_size_ -= size_added_by_compaction;
}

Status SstFileManagerImpl::OnMoveFile(const std::string& old_path,
                                      const std::string& new_path,
                                      uint64_t* file_size) {
  {
    MutexLock l(&mu_);
    if (file_size != nullptr) {
      *file_size = tracked_files_[old_path];
    }
    OnAddFileImpl(new_path, tracked_files_[old_path]);
    OnDeleteFileImpl(old_path);
  }
  TEST_SYNC_POINT("SstFileManagerImpl::OnMoveFile");
  return Status::OK();
}

void SstFileManagerImpl::SetMaxAllowedSpaceUsage(uint64_t max_allowed_space) {
  MutexLock l(&mu_);
  max_allowed_space_ = max_allowed_space;
}

void SstFileManagerImpl::SetCompactionBufferSize(
    uint64_t compaction_buffer_size) {
  MutexLock l(&mu_);
  compaction_buffer_size_ = compaction_buffer_size;
}

bool SstFileManagerImpl::IsMaxAllowedSpaceReached() {
  MutexLock l(&mu_);
  if (max_allowed_space_ <= 0) {
    return false;
  }
  return total_files_size_ >= max_allowed_space_;
}

bool SstFileManagerImpl::IsMaxAllowedSpaceReachedIncludingCompactions() {
  MutexLock l(&mu_);
  if (max_allowed_space_ <= 0) {
    return false;
  }
  return total_files_size_ + cur_compactions_reserved_size_ >=
         max_allowed_space_;
}

bool SstFileManagerImpl::EnoughRoomForCompaction(Compaction* c) {
  MutexLock l(&mu_);
  uint64_t size_added_by_compaction = 0;
  // First check if we even have the space to do the compaction
  for (size_t i = 0; i < c->num_input_levels(); i++) {
    for (size_t j = 0; j < c->num_input_files(i); j++) {
      FileMetaData* filemeta = c->input(i, j);
      size_added_by_compaction += filemeta->fd.GetFileSize();
    }
  }

  if (max_allowed_space_ != 0 &&
      (size_added_by_compaction + cur_compactions_reserved_size_ +
           total_files_size_ + compaction_buffer_size_ >
       max_allowed_space_)) {
    return false;
  }
  // Update cur_compactions_reserved_size_ so concurrent compaction
  // don't max out space
  cur_compactions_reserved_size_ += size_added_by_compaction;
  return true;
}

uint64_t SstFileManagerImpl::GetCompactionsReservedSize() {
  MutexLock l(&mu_);
  return cur_compactions_reserved_size_;
}

uint64_t SstFileManagerImpl::GetTotalSize() {
  MutexLock l(&mu_);
  return total_files_size_;
}

std::unordered_map<std::string, uint64_t>
SstFileManagerImpl::GetTrackedFiles() {
  MutexLock l(&mu_);
  return tracked_files_;
}

int64_t SstFileManagerImpl::GetDeleteRateBytesPerSecond() {
  return delete_scheduler_.GetRateBytesPerSecond();
}

void SstFileManagerImpl::SetDeleteRateBytesPerSecond(int64_t delete_rate) {
  return delete_scheduler_.SetRateBytesPerSecond(delete_rate);
}

double SstFileManagerImpl::GetMaxTrashDBRatio() {
  return delete_scheduler_.GetMaxTrashDBRatio();
}

void SstFileManagerImpl::SetMaxTrashDBRatio(double r) {
  return delete_scheduler_.SetMaxTrashDBRatio(r);
}

Status SstFileManagerImpl::ScheduleFileDeletion(const std::string& file_path) {
  return delete_scheduler_.DeleteFile(file_path);
}

void SstFileManagerImpl::WaitForEmptyTrash() {
  delete_scheduler_.WaitForEmptyTrash();
}

void SstFileManagerImpl::OnAddFileImpl(const std::string& file_path,
                                       uint64_t file_size) {
  auto tracked_file = tracked_files_.find(file_path);
  if (tracked_file != tracked_files_.end()) {
    // File was added before, we will just update the size
    total_files_size_ -= tracked_file->second;
    total_files_size_ += file_size;
  } else {
    total_files_size_ += file_size;
  }
  tracked_files_[file_path] = file_size;
}

void SstFileManagerImpl::OnDeleteFileImpl(const std::string& file_path) {
  auto tracked_file = tracked_files_.find(file_path);
  if (tracked_file == tracked_files_.end()) {
    // File is not tracked
    return;
  }

  total_files_size_ -= tracked_file->second;
  tracked_files_.erase(tracked_file);
}

SstFileManager* NewSstFileManager(Env* env, std::shared_ptr<Logger> info_log,
                                  std::string trash_dir,
                                  int64_t rate_bytes_per_sec,
                                  bool delete_existing_trash, Status* status,
                                  double max_trash_db_ratio) {
  SstFileManagerImpl* res =
      new SstFileManagerImpl(env, info_log, rate_bytes_per_sec,
                             max_trash_db_ratio);

  // trash_dir is deprecated and not needed anymore, but if user passed it
  // we will still remove files in it.
  Status s;
  if (delete_existing_trash && trash_dir != "") {
    std::vector<std::string> files_in_trash;
    s = env->GetChildren(trash_dir, &files_in_trash);
    if (s.ok()) {
      for (const std::string& trash_file : files_in_trash) {
        if (trash_file == "." || trash_file == "..") {
          continue;
        }

        std::string path_in_trash = trash_dir + "/" + trash_file;
        res->OnAddFile(path_in_trash);
        Status file_delete = res->ScheduleFileDeletion(path_in_trash);
        if (s.ok() && !file_delete.ok()) {
          s = file_delete;
        }
      }
    }
  }

  if (status) {
    *status = s;
  }

  return res;
}

#else

SstFileManager* NewSstFileManager(Env* env, std::shared_ptr<Logger> info_log,
                                  std::string trash_dir,
                                  int64_t rate_bytes_per_sec,
                                  bool delete_existing_trash, Status* status,
                                  double max_trash_db_ratio) {
  if (status) {
    *status =
      Status::NotSupported("SstFileManager is not supported in ROCKSDB_LITE");
  }
  return nullptr;
}

#endif  // ROCKSDB_LITE

}  // namespace rocksdb
