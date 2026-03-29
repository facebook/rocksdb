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
  BlobFileCompletionCallback(
      SstFileManager* sst_file_manager, InstrumentedMutex* mutex,
      ErrorHandler* error_handler, EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& dbname)
      : event_logger_(event_logger), listeners_(listeners), dbname_(dbname) {
    sst_file_manager_ = sst_file_manager;
    mutex_ = mutex;
    error_handler_ = error_handler;
  }

  void OnBlobFileCreationStarted(const std::string& file_name,
                                 const std::string& column_family_name,
                                 int job_id,
                                 BlobFileCreationReason creation_reason);

  Status OnBlobFileCompleted(const std::string& file_name,
                             const std::string& column_family_name, int job_id,
                             uint64_t file_number,
                             BlobFileCreationReason creation_reason,
                             const Status& report_status,
                             const std::string& checksum_value,
                             const std::string& checksum_method,
                             uint64_t blob_count, uint64_t blob_bytes);

 private:
  SstFileManager* sst_file_manager_;
  InstrumentedMutex* mutex_;
  ErrorHandler* error_handler_;
  EventLogger* event_logger_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::string dbname_;
};
}  // namespace ROCKSDB_NAMESPACE
