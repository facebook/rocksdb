//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_stats_util.h"

#if defined(USE_COROUTINES)

#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>

#include "folly/io/async/Request.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
namespace {

struct CoroutineStatsRequestDataTraits {
  static const folly::RequestToken kToken;
};

const folly::RequestToken CoroutineStatsRequestDataTraits::kToken(
    "rocksdb_coroutine_stats_context");

class CoroutineStatsRequestData;

#ifndef NDEBUG
CoroutineStatsRequestData* GetCoroutineStatsData();
#endif

#ifndef NPERF_CONTEXT
struct CoroutineStatsThreadLocalState {
  CoroutineStatsRequestData* data = nullptr;
  uint64_t get_cpu_nanos_start = 0;
};

thread_local CoroutineStatsThreadLocalState coroutine_stats_thread_local_state;
#endif

class CoroutineStatsRequestData : public folly::RequestData {
 public:
  explicit CoroutineStatsRequestData(CoroutineStatsConfig stats_config,
                                     Env* env)
      : env_(env), perf_level_(stats_config.perf_level) {
    (void)env_;
    assert(env_ != nullptr);
#ifndef NDEBUG
    owner_thread_id_ = env_->GetThreadID();
#endif
    assert(CapturedPerfLevel() > PerfLevel::kUninitialized);
    assert(CapturedPerfLevel() < PerfLevel::kOutOfBounds);
#ifndef NPERF_CONTEXT
    if (stats_config.per_level_perf_context_enabled) {
      perf_context_.EnablePerLevelPerfContext();
    }
#endif
#ifndef NIOSTATS_CONTEXT
    iostats_context_.Reset();
    iostats_context_.disable_iostats = stats_config.iostats_disabled;
#endif
  }

  bool hasCallback() override { return true; }

  void onSet() override {
    AssertSameThread();
    SetPerfLevel(CapturedPerfLevel());
    LoadThreadLocalStats();
    StartGetCpuTimer();
  }

  void onUnset() override {
    StopGetCpuTimer();
    SaveThreadLocalStats();
  }

  bool PerfLevelAtLeast(PerfLevel perf_level) const {
    return CapturedPerfLevel() >= perf_level;
  }

  PerfLevel CapturedPerfLevel() const { return perf_level_; }

  void Finalize() {
    assert(GetCoroutineStatsData() == this);
    assert(!finalized_);
    finalized_ = true;
    StopGetCpuTimer();
  }

 private:
  void LoadThreadLocalStats() {
#ifndef NPERF_CONTEXT
    *get_perf_context() = std::move(perf_context_);
#endif
#ifndef NIOSTATS_CONTEXT
    *get_iostats_context() = std::move(iostats_context_);
#endif
  }

  void SaveThreadLocalStats() {
#ifndef NPERF_CONTEXT
    perf_context_ = std::move(*get_perf_context());
#endif
#ifndef NIOSTATS_CONTEXT
    iostats_context_ = std::move(*get_iostats_context());
#endif
  }

  // Folly event bases resume requests on the thread where they suspended.
  void AssertSameThread() {
#ifndef NDEBUG
    const uint64_t current_thread_id = env_->GetThreadID();
    assert(owner_thread_id_ == current_thread_id);
#endif
  }

  void StartGetCpuTimer() {
#ifndef NPERF_CONTEXT
    if (finalized_) {
      return;
    }
    if (!PerfLevelAtLeast(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex)) {
      return;
    }
    coroutine_stats_thread_local_state.data = this;
    coroutine_stats_thread_local_state.get_cpu_nanos_start =
        env_->GetSystemClock()->CPUNanos();
#endif
  }

  void StopGetCpuTimer() {
#ifndef NPERF_CONTEXT
    if (coroutine_stats_thread_local_state.data != this) {
      return;
    }
    const uint64_t start =
        coroutine_stats_thread_local_state.get_cpu_nanos_start;
    coroutine_stats_thread_local_state = {};
    const uint64_t now = env_->GetSystemClock()->CPUNanos();
    if (now >= start) {
      get_perf_context()->get_cpu_nanos += now - start;
    }
#endif
  }

  Env* const env_;
  PerfLevel perf_level_;
#ifndef NPERF_CONTEXT
  PerfContext perf_context_;
#endif
#ifndef NIOSTATS_CONTEXT
  IOStatsContext iostats_context_;
#endif
  bool finalized_ = false;
#ifndef NDEBUG
  uint64_t owner_thread_id_ = 0;
#endif
};

#ifndef NDEBUG
CoroutineStatsRequestData* GetCoroutineStatsData() {
  auto* context = folly::RequestContext::try_get();
  if (context == nullptr) {
    return nullptr;
  }
  return static_cast<CoroutineStatsRequestData*>(
      context->getThreadCachedContextData<CoroutineStatsRequestDataTraits>());
}
#endif

}  // namespace

CoroutineStatsConfig CaptureCoroutineStatsConfig() {
  CoroutineStatsConfig stats_config;
  stats_config.perf_level = GetPerfLevel();
#ifndef NPERF_CONTEXT
  stats_config.per_level_perf_context_enabled =
      get_perf_context()->per_level_perf_context_enabled;
#endif
#ifndef NIOSTATS_CONTEXT
  stats_config.iostats_disabled = get_iostats_context()->disable_iostats;
#endif
  return stats_config;
}

CoroutineStatsContextScope::CoroutineStatsContextScope(
    CoroutineStatsConfig stats_config, Env* env)
    : request_data_(nullptr) {
  assert(GetCoroutineStatsData() == nullptr);  // NO nesting allowed
  auto data =
      std::make_unique<CoroutineStatsRequestData>(std::move(stats_config), env);
  request_data_ = data.get();
  guard_ = std::make_unique<folly::ShallowCopyRequestContextScopeGuard>(
      CoroutineStatsRequestDataTraits::kToken, std::move(data));
}

CoroutineStatsContextScope::~CoroutineStatsContextScope() = default;

void CoroutineStatsContextScope::Finalize() const {
  auto* data = static_cast<CoroutineStatsRequestData*>(request_data_);
  assert(data != nullptr);
  data->Finalize();
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // defined(USE_COROUTINES)
