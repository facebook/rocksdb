//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#include "db_stress_tool/db_stress_compaction_service.h"

#include <string>

#include "db_stress_tool/db_stress_test_base.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

CompactionServiceJobStatus DbStressCompactionService::Wait(
    const std::string& scheduled_job_id, std::string* result) {
  while (true) {
    if (aborted_.load()) {
      return CompactionServiceJobStatus::kAborted;
    }
    const auto& maybeResultStatus =
        shared_->GetRemoteCompactionResult(scheduled_job_id, result);
    if (maybeResultStatus.has_value()) {
      auto s = maybeResultStatus.value();
      if (s.ok()) {
        assert(result);
        assert(!result->empty());
        return CompactionServiceJobStatus::kSuccess;
      } else {
        // Remote Compaction failed
        if (failure_should_fall_back_to_local_) {
          return CompactionServiceJobStatus::kUseLocal;
        }
        if (StressTest::IsErrorInjectedAndRetryable(s)) {
          return CompactionServiceJobStatus::kUseLocal;
        }
        if (result && result->empty()) {
          // If result is empty, set the compaction status in the result so
          // that it can be bubbled up to main thread
          CompactionServiceResult compaction_result;
          compaction_result.status = s;
          if (compaction_result.Write(result).ok()) {
            assert(result);
            assert(!result->empty());
          }
        }
        return CompactionServiceJobStatus::kFailure;
      }
    } else {
      // Remote Compaction is still running
      Env::Default()->SleepForMicroseconds(kWaitIntervalInMicros);
    }
  }
  return CompactionServiceJobStatus::kFailure;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
