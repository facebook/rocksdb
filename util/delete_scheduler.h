//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <map>
#include <queue>
#include <string>
#include <thread>

#include "monitoring/instrumented_mutex.h"
#include "port/port.h"

#include "rocksdb/status.h"

namespace rocksdb {

class Env;
class Logger;
class SstFileManagerImpl;

// DeleteScheduler allows the DB to enforce a rate limit on file deletion,
// Instead of deleteing files immediately, files are moved to trash_dir
// and deleted in a background thread that apply sleep penlty between deletes
// if they are happening in a rate faster than rate_bytes_per_sec,
//
// Rate limiting can be turned off by setting rate_bytes_per_sec = 0, In this
// case DeleteScheduler will delete files immediately.
class DeleteScheduler {
 public:
  DeleteScheduler(Env* env, const std::string& trash_dir,
                  int64_t rate_bytes_per_sec, Logger* info_log,
                  SstFileManagerImpl* sst_file_manager);

  ~DeleteScheduler();

  // Return delete rate limit in bytes per second
  int64_t GetRateBytesPerSecond() { return rate_bytes_per_sec_.load(); }

  // Set delete rate limit in bytes per second
  void SetRateBytesPerSecond(int64_t bytes_per_sec) {
    return rate_bytes_per_sec_.store(bytes_per_sec);
  }

  // Move file to trash directory and schedule it's deletion
  Status DeleteFile(const std::string& fname);

  // Wait for all files being deleteing in the background to finish or for
  // destructor to be called.
  void WaitForEmptyTrash();

  // Return a map containing errors that happened in BackgroundEmptyTrash
  // file_path => error status
  std::map<std::string, Status> GetBackgroundErrors();

 private:
  Status MoveToTrash(const std::string& file_path, std::string* path_in_trash);

  Status DeleteTrashFile(const std::string& path_in_trash,
                         uint64_t* deleted_bytes);

  void BackgroundEmptyTrash();

  Env* env_;
  // Path to the trash directory
  std::string trash_dir_;
  // Maximum number of bytes that should be deleted per second
  std::atomic<int64_t> rate_bytes_per_sec_;
  // Mutex to protect queue_, pending_files_, bg_errors_, closing_
  InstrumentedMutex mu_;
  // Queue of files in trash that need to be deleted
  std::queue<std::string> queue_;
  // Number of files in trash that are waiting to be deleted
  int32_t pending_files_;
  // Errors that happened in BackgroundEmptyTrash (file_path => error)
  std::map<std::string, Status> bg_errors_;
  // Set to true in ~DeleteScheduler() to force BackgroundEmptyTrash to stop
  bool closing_;
  // Condition variable signaled in these conditions
  //    - pending_files_ value change from 0 => 1
  //    - pending_files_ value change from 1 => 0
  //    - closing_ value is set to true
  InstrumentedCondVar cv_;
  // Background thread running BackgroundEmptyTrash
  std::unique_ptr<port::Thread> bg_thread_;
  // Mutex to protect threads from file name conflicts
  InstrumentedMutex file_move_mu_;
  Logger* info_log_;
  SstFileManagerImpl* sst_file_manager_;
  static const uint64_t kMicrosInSecond = 1000 * 1000LL;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
