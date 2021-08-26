//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "file/sst_file_manager_impl.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileCompletionCallback {
 public:
#ifdef ROCKSDB_LITE
  BlobFileCompletionCallback(SstFileManager* /*sst_file_manager*/,
                             InstrumentedMutex* /*mutex*/,
                             ErrorHandler* /*error_handler*/) {}
  Status OnBlobFileCompleted(
      const std::vector<std::shared_ptr<EventListener> >& /*listeners*/,
      const std::string& /*dbname*/, const std::string& /*file_name*/,
      const std::string& /*column_family_name*/, int /*job_id*/,
      BlobFileCreationReason /*creation_reason*/, Status /*report_status*/,
      std::string /*checksum_value*/, std::string /*checksum_method*/,
      uint64_t /*blob_count*/, uint64_t /*blob_bytes*/) {
    return Status::OK();
  }

  void OnBlobFileCreationStarted(
      const std::vector<std::shared_ptr<EventListener> >& /*listeners*/,
      const std::string& /*dbname*/, const std::string& /*file_name*/,
      const std::string& /*column_family_name*/, int /*job_id*/,
      BlobFileCreationReason /*creation_reason*/) {}
#else
  BlobFileCompletionCallback(SstFileManager* sst_file_manager,
                             InstrumentedMutex* mutex,
                             ErrorHandler* error_handler)
      : sst_file_manager_(sst_file_manager),
        mutex_(mutex),
        error_handler_(error_handler) {}

  Status OnBlobFileCompleted(
      const std::vector<std::shared_ptr<EventListener> >& listeners,
      const std::string& dbname, const std::string& file_name,
      const std::string& column_family_name, int job_id,
      BlobFileCreationReason creation_reason, Status report_status,
      std::string checksum_value, std::string checksum_method,
      uint64_t blob_count, uint64_t blob_bytes) {
    Status s;
    auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager_);
    if (sfm) {
      // Report new blob files to SstFileManagerImpl
      s = sfm->OnAddFile(file_name);
      if (sfm->IsMaxAllowedSpaceReached()) {
        s = Status::SpaceLimit("Max allowed space was reached");
        TEST_SYNC_POINT(
            "BlobFileCompletionCallback::CallBack::MaxAllowedSpaceReached");
        InstrumentedMutexLock l(mutex_);
        error_handler_->SetBGError(s, BackgroundErrorReason::kFlush);
      }
    }
    // Notify the listeners.
    EventHelpers::NotifyBlobFileCreationFinished(
        listeners, dbname, column_family_name, file_name, job_id,
        creation_reason, report_status, checksum_value, checksum_method,
        blob_count, blob_bytes);
    return s;
  }

  void OnBlobFileCreationStarted(
      const std::vector<std::shared_ptr<EventListener> >& listeners,
      const std::string& dbname, const std::string& file_name,
      const std::string& column_family_name, int job_id,
      BlobFileCreationReason creation_reason) {
    // Notify the listeners.
    EventHelpers::NotifyBlobFileCreationStarted(listeners, dbname,
                                                column_family_name, file_name,
                                                job_id, creation_reason);
  }

 private:
  SstFileManager* sst_file_manager_;
  InstrumentedMutex* mutex_;
  ErrorHandler* error_handler_;
#endif  // ROCKSDB_LITE
};
}  // namespace ROCKSDB_NAMESPACE
