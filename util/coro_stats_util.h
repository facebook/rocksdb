//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if defined(USE_COROUTINES)
#include <memory>

#include "rocksdb/perf_level.h"
#include "rocksdb/rocksdb_namespace.h"

namespace folly {
class RequestData;
struct ShallowCopyRequestContextScopeGuard;
}  // namespace folly

namespace ROCKSDB_NAMESPACE {

class Env;

struct CoroutineStatsConfig {
  PerfLevel perf_level = PerfLevel::kEnableCount;
  bool per_level_perf_context_enabled = false;
  bool iostats_disabled = false;
};

CoroutineStatsConfig CaptureCoroutineStatsConfig();

// Installs request-scoped perf and IO stats while a coroutine is active. Folly
// request context is used to save the request stats on suspension and reload
// them with the captured configuration on resumption, so multiple coroutines
// can share a single executor thread, but each own separate stats contexts.
// Call Finalize() once operation has completed, to ensure stats are flushed.
class CoroutineStatsContextScope {
 public:
  explicit CoroutineStatsContextScope(CoroutineStatsConfig stats_config,
                                      Env* env);
  ~CoroutineStatsContextScope();

  CoroutineStatsContextScope(const CoroutineStatsContextScope&) = delete;
  CoroutineStatsContextScope& operator=(const CoroutineStatsContextScope&) =
      delete;
  CoroutineStatsContextScope(CoroutineStatsContextScope&&) = delete;
  CoroutineStatsContextScope& operator=(CoroutineStatsContextScope&&) = delete;

  void Finalize() const;

 private:
  folly::RequestData* request_data_;
  std::unique_ptr<folly::ShallowCopyRequestContextScopeGuard> guard_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_COROUTINES
