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
#include "file/sst_file_manager_impl.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileCompletionCallback {
 public:
  BlobFileCompletionCallback(SstFileManager* sst_file_manager,
                             InstrumentedMutex* mutex,
                             ErrorHandler* error_handler)
      : sst_file_manager_(sst_file_manager),
        mutex_(mutex),
        error_handler_(error_handler) {}

  Status OnBlobFileCompleted(const std::string& file_name) {
    Status s;

#ifndef ROCKSDB_LITE
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
#else
    (void)file_name;
#endif  // ROCKSDB_LITE
    return s;
  }

 private:
  SstFileManager* sst_file_manager_;
  InstrumentedMutex* mutex_;
  ErrorHandler* error_handler_;
};
}  // namespace ROCKSDB_NAMESPACE
