//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_level_imp.h"
#include "monitoring/statistics.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class PerfStepTimer {
 public:
  explicit PerfStepTimer(
      uint64_t* metric, SystemClock* clock = nullptr, bool use_cpu_time = false,
      PerfLevel enable_level = PerfLevel::kEnableTimeExceptForMutex,
      Statistics* statistics = nullptr, uint32_t ticker_type = 0)
      : perf_counter_enabled_(perf_level >= enable_level),
        use_cpu_time_(use_cpu_time),
        ticker_type_(ticker_type),
        clock_((perf_counter_enabled_ || statistics != nullptr)
                   ? (clock ? clock : SystemClock::Default().get())
                   : nullptr),
        start_(0),
        metric_(metric),
        statistics_(statistics) {}

  ~PerfStepTimer() {
    Stop();
  }

  void Start() {
    if (perf_counter_enabled_ || statistics_ != nullptr) {
      start_ = time_now();
    }
  }

  void Measure() {
    if (start_) {
      uint64_t now = time_now();
      *metric_ += now - start_;
      start_ = now;
    }
  }

  void Stop() {
    if (start_) {
      uint64_t duration = time_now() - start_;
      if (perf_counter_enabled_) {
        *metric_ += duration;
      }

      if (statistics_ != nullptr) {
        RecordTick(statistics_, ticker_type_, duration);
      }
      start_ = 0;
    }
  }

 private:
  uint64_t time_now() {
    if (!use_cpu_time_) {
      return clock_->NowNanos();
    } else {
      return clock_->CPUNanos();
    }
  }

  const bool perf_counter_enabled_;
  const bool use_cpu_time_;
  uint32_t ticker_type_;
  SystemClock* const clock_;
  uint64_t start_;
  uint64_t* metric_;
  Statistics* statistics_;
};

}  // namespace ROCKSDB_NAMESPACE
