//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.

#include "rocksdb/cloud/cloud_file_deletion_scheduler.h"

#include "cloud/cloud_scheduler.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<CloudFileDeletionScheduler> CloudFileDeletionScheduler::Create(
     const std::shared_ptr<CloudScheduler>& scheduler,
     std::chrono::seconds file_deletion_delay) {
  return std::make_shared<CloudFileDeletionScheduler>(PrivateTag(), scheduler,
                                                      file_deletion_delay);
}

CloudFileDeletionScheduler::~CloudFileDeletionScheduler() {
  TEST_SYNC_POINT(
      "CloudFileDeletionScheduler::~CloudFileDeletionScheduler:"
      "BeforeCancelJobs");
  // NOTE: no need to cancel jobs here. These jobs won't be executed
  // as longs as `CloudFileDeletionScheduler` is destructed. Also,
  // `LocalCloudScheduler` will remove the jobs in the queue when destructed
}

void CloudFileDeletionScheduler::UnscheduleFileDeletion(const std::string& filename) {
  std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
  auto itr = files_to_delete_.find(filename);
  if (itr != files_to_delete_.end()) {
    scheduler_->CancelJob(itr->second);
    files_to_delete_.erase(itr);
  }
}

rocksdb::IOStatus CloudFileDeletionScheduler::ScheduleFileDeletion(
    const std::string& fname, FileDeletionRunnable runnable) {
  auto wp = this->weak_from_this();
  auto doDeleteFile = [wp = std::move(wp), fname, runnable = std::move(runnable)](void*) {
    TEST_SYNC_POINT(
        "CloudFileDeletionScheduler::ScheduleFileDeletion:BeforeFileDeletion");
    auto sp = wp.lock();
    bool file_deleted = false;
    if (sp) {
      file_deleted = true;
      sp->DoDeleteFile(std::move(fname), std::move(runnable));
    }
    TEST_SYNC_POINT_CALLBACK(
        "CloudFileDeletionScheduler::ScheduleFileDeletion:AfterFileDeletion",
        &file_deleted);
    (void) file_deleted;
  };

  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    if (files_to_delete_.find(fname) != files_to_delete_.end()) {
      // already in the queue
      return IOStatus::OK();
    }

    auto handle = scheduler_->ScheduleJob(file_deletion_delay_,
                                          std::move(doDeleteFile), nullptr);
    files_to_delete_.emplace(fname, std::move(handle));
  }
  return IOStatus::OK();
}

void CloudFileDeletionScheduler::DoDeleteFile(const std::string& fname,
                                              FileDeletionRunnable runnable) {
  {
    std::lock_guard<std::mutex> lk(files_to_delete_mutex_);
    auto itr = files_to_delete_.find(fname);
    if (itr == files_to_delete_.end()) {
      // File was removed from files_to_delete_, do not delete!
      return;
    }
    files_to_delete_.erase(itr);
  }

  runnable();
}

#ifndef NDEBUG
size_t CloudFileDeletionScheduler::TEST_NumScheduledJobs() const {
  return scheduler_->TEST_NumScheduledJobs();
}
#endif

}
