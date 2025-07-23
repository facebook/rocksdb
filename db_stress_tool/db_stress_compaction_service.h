//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db_stress_shared_state.h"
#include "db_stress_tool/db_stress_common.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// Service to simulate Remote Compaction in Stress Test
class DbStressCompactionService : public CompactionService {
 public:
  explicit DbStressCompactionService(const std::string& db_path,
                                     SharedState* shared)
      : db_path_(db_path), shared_(shared) {}

  static const char* kClassName() { return "DbStressCompactionService"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override {
    std::string job_id = info.db_id + "_" + info.db_session_id + "_" +
                         std::to_string(info.job_id);
    shared_->EnqueueRemoteCompaction(job_id, info, compaction_service_input);
    CompactionServiceScheduleResponse response(
        job_id, CompactionServiceJobStatus::kSuccess);
    return response;
  }

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    auto start = Env::Default()->NowMicros();
    while (Env::Default()->NowMicros() - start <
           FLAGS_remote_compaction_wait_timeout) {
      if (shared_->GetRemoteCompactionResult(scheduled_job_id, result).ok()) {
        return CompactionServiceJobStatus::kSuccess;
      }
      Env::Default()->SleepForMicroseconds(
          FLAGS_remote_compaction_wait_interval);
    }
    return CompactionServiceJobStatus::kFailure;
  }

  void OnInstallation(const std::string& scheduled_job_id,
                      CompactionServiceJobStatus /*status*/) override {
    // Clean up tmp directory
    std::string serialized;
    CompactionServiceResult result;
    if (shared_->GetRemoteCompactionResult(scheduled_job_id, &serialized)
            .ok()) {
      if (CompactionServiceResult::Read(serialized, &result).ok()) {
        std::vector<std::string> filenames;
        Status s = Env::Default()->GetChildren(result.output_path, &filenames);
        for (size_t i = 0; s.ok() && i < filenames.size(); ++i) {
          s = Env::Default()->DeleteFile(result.output_path + "/" +
                                         filenames[i]);
          if (!s.ok()) {
            // TODO - Handle clean up failure?
            break;
          }
        }
        if (s.ok()) {
          Env::Default()->DeleteDir(result.output_path).PermitUncheckedError();
        }
      }
      shared_->RemoveRemoteCompactionResult(scheduled_job_id);
    }
  }

  // TODO - Implement
  void CancelAwaitingJobs() override {}

 private:
  std::string db_path_;
  SharedState* shared_;
};

}  // namespace ROCKSDB_NAMESPACE
