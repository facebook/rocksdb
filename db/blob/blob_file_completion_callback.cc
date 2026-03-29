//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_completion_callback.h"

namespace ROCKSDB_NAMESPACE {

void BlobFileCompletionCallback::OnBlobFileCreationStarted(
    const std::string& file_name, const std::string& column_family_name,
    int job_id, BlobFileCreationReason creation_reason) {
  // Notify the listeners.
  EventHelpers::NotifyBlobFileCreationStarted(listeners_, dbname_,
                                              column_family_name, file_name,
                                              job_id, creation_reason);
}

Status BlobFileCompletionCallback::OnBlobFileCompleted(
    const std::string& file_name, const std::string& column_family_name,
    int job_id, uint64_t file_number, BlobFileCreationReason creation_reason,
    const Status& report_status, const std::string& checksum_value,
    const std::string& checksum_method, uint64_t blob_count,
    uint64_t blob_bytes) {
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
  EventHelpers::LogAndNotifyBlobFileCreationFinished(
      event_logger_, listeners_, dbname_, column_family_name, file_name, job_id,
      file_number, creation_reason, (!report_status.ok() ? report_status : s),
      (checksum_value.empty() ? kUnknownFileChecksum : checksum_value),
      (checksum_method.empty() ? kUnknownFileChecksumFuncName
                               : checksum_method),
      blob_count, blob_bytes);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
