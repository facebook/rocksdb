//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#pragma once
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {
class CloudScheduler;

// schedule/unschedule file deletion jobs
class CloudFileDeletionScheduler
    : public std::enable_shared_from_this<CloudFileDeletionScheduler> {
  struct PrivateTag {};

 public:
  static std::shared_ptr<CloudFileDeletionScheduler> Create(
      const std::shared_ptr<CloudScheduler>& scheduler,
      std::chrono::seconds file_deletion_delay);

  explicit CloudFileDeletionScheduler(
      PrivateTag, const std::shared_ptr<CloudScheduler>& scheduler,
      std::chrono::seconds file_deletion_delay)
      : scheduler_(scheduler), file_deletion_delay_(file_deletion_delay) {}

  ~CloudFileDeletionScheduler();

  void UnscheduleFileDeletion(const std::string& filename);
  using FileDeletionRunnable = std::function<void()>;
  // Schedule the file deletion runnable(which actually delets the file from
  // cloud) to be executed in the future (specified by `file_deletion_delay_`).
  rocksdb::IOStatus ScheduleFileDeletion(const std::string& filename,
                                         FileDeletionRunnable runnable);

#ifndef NDEBUG
  size_t TEST_NumScheduledJobs() const;

  // Return all the files that are scheduled to be deleted(but not deleted yet)
  std::vector<std::string> TEST_FilesToDelete() const {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    std::vector<std::string> files;
    for (auto& [file, handle] : files_to_delete_) {
      files.push_back(file);
      (void)handle;
    }
    return files;
  }
#endif

 private:
  // execute the `FileDeletionRunnable`
  void DoDeleteFile(const std::string& fname, FileDeletionRunnable cb);
  std::shared_ptr<CloudScheduler> scheduler_;

  mutable std::mutex files_to_delete_mutex_;
  std::unordered_map<std::string, int> files_to_delete_;
  std::chrono::seconds file_deletion_delay_;
};

}  // namespace ROCKSDB_NAMESPACE
