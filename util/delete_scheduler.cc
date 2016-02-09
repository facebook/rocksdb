//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "util/delete_scheduler.h"

#include <thread>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "util/sst_file_manager_impl.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"

namespace rocksdb {

DeleteScheduler::DeleteScheduler(Env* env, const std::string& trash_dir,
                                 int64_t rate_bytes_per_sec, Logger* info_log,
                                 SstFileManagerImpl* sst_file_manager)
    : env_(env),
      trash_dir_(trash_dir),
      rate_bytes_per_sec_(rate_bytes_per_sec),
      pending_files_(0),
      closing_(false),
      cv_(&mu_),
      info_log_(info_log),
      sst_file_manager_(sst_file_manager) {
  if (rate_bytes_per_sec_ <= 0) {
    // Rate limiting is disabled
    bg_thread_.reset();
  } else {
    bg_thread_.reset(
        new std::thread(&DeleteScheduler::BackgroundEmptyTrash, this));
  }
}

DeleteScheduler::~DeleteScheduler() {
  {
    MutexLock l(&mu_);
    closing_ = true;
    cv_.SignalAll();
  }
  if (bg_thread_) {
    bg_thread_->join();
  }
}

Status DeleteScheduler::DeleteFile(const std::string& file_path) {
  Status s;
  if (rate_bytes_per_sec_ <= 0) {
    // Rate limiting is disabled
    s = env_->DeleteFile(file_path);
    if (s.ok() && sst_file_manager_) {
      sst_file_manager_->OnDeleteFile(file_path);
    }
    return s;
  }

  // Move file to trash
  std::string path_in_trash;
  s = MoveToTrash(file_path, &path_in_trash);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "Failed to move %s to trash directory (%s)", file_path.c_str(),
        trash_dir_.c_str());
    s = env_->DeleteFile(file_path);
    if (s.ok() && sst_file_manager_) {
      sst_file_manager_->OnDeleteFile(file_path);
    }
    return s;
  }

  // Add file to delete queue
  {
    MutexLock l(&mu_);
    queue_.push(path_in_trash);
    pending_files_++;
    if (pending_files_ == 1) {
      cv_.SignalAll();
    }
  }
  return s;
}

std::map<std::string, Status> DeleteScheduler::GetBackgroundErrors() {
  MutexLock l(&mu_);
  return bg_errors_;
}

Status DeleteScheduler::MoveToTrash(const std::string& file_path,
                                    std::string* path_in_trash) {
  Status s;
  // Figure out the name of the file in trash folder
  size_t idx = file_path.rfind("/");
  if (idx == std::string::npos || idx == file_path.size() - 1) {
    return Status::InvalidArgument("file_path is corrupted");
  }
  *path_in_trash = trash_dir_ + file_path.substr(idx);
  std::string unique_suffix = "";

  if (*path_in_trash == file_path) {
    // This file is already in trash
    return s;
  }

  // TODO(tec) : Implement Env::RenameFileIfNotExist and remove
  //             file_move_mu mutex.
  MutexLock l(&file_move_mu_);
  while (true) {
    s = env_->FileExists(*path_in_trash + unique_suffix);
    if (s.IsNotFound()) {
      // We found a path for our file in trash
      *path_in_trash += unique_suffix;
      s = env_->RenameFile(file_path, *path_in_trash);
      break;
    } else if (s.ok()) {
      // Name conflict, generate new random suffix
      unique_suffix = env_->GenerateUniqueId();
    } else {
      // Error during FileExists call, we cannot continue
      break;
    }
  }
  if (s.ok() && sst_file_manager_) {
    sst_file_manager_->OnMoveFile(file_path, *path_in_trash);
  }
  return s;
}

void DeleteScheduler::BackgroundEmptyTrash() {
  TEST_SYNC_POINT("DeleteScheduler::BackgroundEmptyTrash");

  while (true) {
    MutexLock l(&mu_);
    while (queue_.empty() && !closing_) {
      cv_.Wait();
    }

    if (closing_) {
      return;
    }

    // Delete all files in queue_
    uint64_t start_time = env_->NowMicros();
    uint64_t total_deleted_bytes = 0;
    while (!queue_.empty() && !closing_) {
      std::string path_in_trash = queue_.front();
      queue_.pop();

      // We dont need to hold the lock while deleting the file
      mu_.Unlock();
      uint64_t deleted_bytes = 0;
      // Delete file from trash and update total_penlty value
      Status s = DeleteTrashFile(path_in_trash,  &deleted_bytes);
      total_deleted_bytes += deleted_bytes;
      mu_.Lock();

      if (!s.ok()) {
        bg_errors_[path_in_trash] = s;
      }

      // Apply penlty if necessary
      uint64_t total_penlty =
          ((total_deleted_bytes * kMicrosInSecond) / rate_bytes_per_sec_);
      while (!closing_ && !cv_.TimedWait(start_time + total_penlty)) {}
      TEST_SYNC_POINT_CALLBACK("DeleteScheduler::BackgroundEmptyTrash:Wait",
                               &total_penlty);

      pending_files_--;
      if (pending_files_ == 0) {
        // Unblock WaitForEmptyTrash since there are no more files waiting
        // to be deleted
        cv_.SignalAll();
      }
    }
  }
}

Status DeleteScheduler::DeleteTrashFile(const std::string& path_in_trash,
                                        uint64_t* deleted_bytes) {
  uint64_t file_size;
  Status s = env_->GetFileSize(path_in_trash, &file_size);
  if (s.ok()) {
    TEST_SYNC_POINT("DeleteScheduler::DeleteTrashFile:DeleteFile");
    s = env_->DeleteFile(path_in_trash);
  }

  if (!s.ok()) {
    // Error while getting file size or while deleting
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "Failed to delete %s from trash -- %s", path_in_trash.c_str(),
        s.ToString().c_str());
    *deleted_bytes = 0;
  } else {
    *deleted_bytes = file_size;
    if (sst_file_manager_) {
      sst_file_manager_->OnDeleteFile(path_in_trash);
    }
  }

  return s;
}

void DeleteScheduler::WaitForEmptyTrash() {
  MutexLock l(&mu_);
  while (pending_files_ > 0 && !closing_) {
    cv_.Wait();
  }
}

}  // namespace rocksdb
