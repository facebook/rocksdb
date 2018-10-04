//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_level_imp.h"
#include "rocksdb/env.h"
#include "util/stop_watch.h"

namespace rocksdb {

class PerfStepTimer {
 public:
  explicit PerfStepTimer(uint64_t* metric, bool for_mutex = false,
                         Statistics* statistics = nullptr,
                         uint32_t ticker_type = 0)
      : perf_counter_enabled_(
            perf_level >= PerfLevel::kEnableTime ||
            (!for_mutex && perf_level >= kEnableTimeExceptForMutex)),
        env_((perf_counter_enabled_ || statistics != nullptr) ? Env::Default()
                                                              : nullptr),
        start_(0),
        metric_(metric),
        statistics_(statistics),
        ticker_type_(ticker_type) {}

  ~PerfStepTimer() {
    Stop();
  }

  void Start() {
    if (perf_counter_enabled_ || statistics_ != nullptr) {
      start_ = env_->NowNanos();
    }
  }

  void Measure() {
    if (start_) {
      uint64_t now = env_->NowNanos();
      *metric_ += now - start_;
      start_ = now;
    }
  }

  void Stop() {
    if (start_) {
      uint64_t duration = env_->NowNanos() - start_;
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
  const bool perf_counter_enabled_;
  Env* const env_;
  uint64_t start_;
  uint64_t* metric_;
  Statistics* statistics_;
  uint32_t ticker_type_;
};

}  // namespace rocksdb
