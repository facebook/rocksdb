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
  explicit DbStressCompactionService(SharedState* shared,
                                     bool failure_should_fall_back_to_local)
      : shared_(shared),
        aborted_(false),
        failure_should_fall_back_to_local_(failure_should_fall_back_to_local) {}

  static const char* kClassName() { return "DbStressCompactionService"; }

  const char* Name() const override { return kClassName(); }

  static constexpr uint64_t kWaitIntervalInMicros = 10 * 1000;  // 10ms
  static constexpr uint64_t kWaitTimeoutInMicros =
      30 * 1000 * 1000;  // 30 seconds

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override {
    std::string job_id = info.db_id + "_" + info.db_session_id + "_" +
                         std::to_string(info.job_id);
    if (aborted_.load()) {
      return CompactionServiceScheduleResponse(
          job_id, CompactionServiceJobStatus::kUseLocal);
    }
    shared_->EnqueueRemoteCompaction(job_id, info, compaction_service_input);
    CompactionServiceScheduleResponse response(
        job_id, CompactionServiceJobStatus::kSuccess);
    return response;
  }

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    auto start = Env::Default()->NowMicros();
    while (Env::Default()->NowMicros() - start < kWaitTimeoutInMicros) {
      if (aborted_.load()) {
        return CompactionServiceJobStatus::kUseLocal;
      }
      if (shared_->GetRemoteCompactionResult(scheduled_job_id, result).ok()) {
        if (result && result->empty()) {
          // Race: Remote worker aborted before client sets aborted_ = true
          return CompactionServiceJobStatus::kUseLocal;
        }
        return CompactionServiceJobStatus::kSuccess;
      }
      Env::Default()->SleepForMicroseconds(kWaitIntervalInMicros);
    }
    if (failure_should_fall_back_to_local_) {
      fprintf(stdout,
              "Remote Compaction failed - fall back to local compaction!\n");
      return CompactionServiceJobStatus::kUseLocal;
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

  void CancelAwaitingJobs() override { aborted_.store(true); }

 private:
  SharedState* shared_;
  std::atomic_bool aborted_{false};
  bool failure_should_fall_back_to_local_;
};

}  // namespace ROCKSDB_NAMESPACE
