//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// Service to simulate Remote Compaction in Stress Test
class DbStressCompactionService : public CompactionService {
 public:
  explicit DbStressCompactionService() {}

  static const char* kClassName() { return "DbStressCompactionService"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& /*compaction_service_input*/) override {
    CompactionServiceScheduleResponse response(
        "Implement Me", CompactionServiceJobStatus::kUseLocal);
    return response;
  }

  CompactionServiceJobStatus Wait(const std::string& /*scheduled_job_id*/,
                                  std::string* /*result*/) override {
    // TODO - Implement
    return CompactionServiceJobStatus::kUseLocal;
  }

  // TODO - Implement
  void CancelAwaitingJobs() override {}
};

}  // namespace ROCKSDB_NAMESPACE
