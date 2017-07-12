//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>

#include "port/port.h"

#include "rocksdb/sst_file_manager.h"
#include "util/delete_scheduler.h"

namespace rocksdb {

class Env;
class Logger;

// SstFileManager is used to track SST files in the DB and control there
// deletion rate.
// All SstFileManager public functions are thread-safe.
class SstFileManagerImpl : public SstFileManager {
 public:
  explicit SstFileManagerImpl(Env* env, std::shared_ptr<Logger> logger,
                              const std::string& trash_dir,
                              int64_t rate_bytes_per_sec);

  ~SstFileManagerImpl();

  // DB will call OnAddFile whenever a new sst file is added.
  Status OnAddFile(const std::string& file_path);

  // DB will call OnDeleteFile whenever an sst file is deleted.
  Status OnDeleteFile(const std::string& file_path);

  // DB will call OnMoveFile whenever an sst file is move to a new path.
  Status OnMoveFile(const std::string& old_path, const std::string& new_path,
                    uint64_t* file_size = nullptr);

  // Update the maximum allowed space that should be used by RocksDB, if
  // the total size of the SST files exceeds max_allowed_space, writes to
  // RocksDB will fail.
  //
  // Setting max_allowed_space to 0 will disable this feature, maximum allowed
  // space will be infinite (Default value).
  //
  // thread-safe.
  void SetMaxAllowedSpaceUsage(uint64_t max_allowed_space) override;

  // Return true if the total size of SST files exceeded the maximum allowed
  // space usage.
  //
  // thread-safe.
  bool IsMaxAllowedSpaceReached() override;

  // Return the total size of all tracked files.
  uint64_t GetTotalSize() override;

  // Return a map containing all tracked files and there corresponding sizes.
  std::unordered_map<std::string, uint64_t> GetTrackedFiles() override;

  // Return delete rate limit in bytes per second.
  virtual int64_t GetDeleteRateBytesPerSecond() override;

  // Update the delete rate limit in bytes per second.
  virtual void SetDeleteRateBytesPerSecond(int64_t delete_rate) override;

  // Move file to trash directory and schedule it's deletion.
  virtual Status ScheduleFileDeletion(const std::string& file_path);

  // Wait for all files being deleteing in the background to finish or for
  // destructor to be called.
  virtual void WaitForEmptyTrash();

  DeleteScheduler* delete_scheduler() { return &delete_scheduler_; }

 private:
  // REQUIRES: mutex locked
  void OnAddFileImpl(const std::string& file_path, uint64_t file_size);
  // REQUIRES: mutex locked
  void OnDeleteFileImpl(const std::string& file_path);

  Env* env_;
  std::shared_ptr<Logger> logger_;
  // Mutex to protect tracked_files_, total_files_size_
  port::Mutex mu_;
  // The summation of the sizes of all files in tracked_files_ map
  uint64_t total_files_size_;
  // A map containing all tracked files and there sizes
  //  file_path => file_size
  std::unordered_map<std::string, uint64_t> tracked_files_;
  // The maximum allowed space (in bytes) for sst files.
  uint64_t max_allowed_space_;
  // DeleteScheduler used to throttle file deletition, if SstFileManagerImpl was
  // created with rate_bytes_per_sec == 0 or trash_dir == "", delete_scheduler_
  // rate limiting will be disabled and will simply delete the files.
  DeleteScheduler delete_scheduler_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
