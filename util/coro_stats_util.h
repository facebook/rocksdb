//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if defined(USE_COROUTINES)
#include <cassert>
#include <memory>
#include <optional>

#ifndef NDEBUG
#include "folly/coro/CurrentExecutor.h"
#include "folly/executors/IOExecutor.h"
#include "folly/io/async/EventBase.h"
#endif

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
bool IsCoroutineStatsEnabled();

// Installs request-scoped perf and IO stats while a coroutine is active. Folly
// request context is used to save the request stats on suspension and reload
// them with the captured configuration on resumption, so multiple coroutines
// can share a single executor thread, but each own separate stats contexts.
// Collected stats are published to thread-local storage on destruction.
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

 private:
  folly::RequestData* request_data_ = nullptr;
  std::unique_ptr<folly::ShallowCopyRequestContextScopeGuard> guard_;
};

#ifndef NDEBUG
#define INSTALL_COROUTINE_STATS_CONTEXT_SCOPE(read_executor_arg, env_arg) \
  auto* const coroutine_stats_read_executor = (read_executor_arg);        \
  if (coroutine_stats_read_executor != nullptr) {                         \
    assert(co_await folly::coro::co_current_executor ==                   \
           coroutine_stats_read_executor->getEventBase());                \
  }                                                                       \
  std::optional<CoroutineStatsContextScope> stats_scope;                  \
  if (IsCoroutineStatsEnabled()) {                                        \
    stats_scope.emplace(CaptureCoroutineStatsConfig(), (env_arg));        \
  }
#else
#define INSTALL_COROUTINE_STATS_CONTEXT_SCOPE(read_executor_arg, env_arg) \
  std::optional<CoroutineStatsContextScope> stats_scope;                  \
  if (IsCoroutineStatsEnabled()) {                                        \
    stats_scope.emplace(CaptureCoroutineStatsConfig(), (env_arg));        \
  }
#endif

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_COROUTINES
