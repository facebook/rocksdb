//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "rocksdb/env.h"
#include "util/statistics.h"

namespace rocksdb {
// Auto-scoped.
// Records the statistic into the corresponding histogram.
class StopWatch {
 public:
  StopWatch(Env * const env, Statistics* statistics,
            const uint32_t hist_type, bool force_enable = false)
    : env_(env),
      statistics_(statistics),
      hist_type_(hist_type),
      enabled_(statistics && statistics->HistEnabledForType(hist_type)),
      start_time_(enabled_ || force_enable ? env->NowMicros() : 0) {
  }

  uint64_t ElapsedMicros() const {
    return env_->NowMicros() - start_time_;
  }

  ~StopWatch() {
    if (enabled_) {
      statistics_->measureTime(hist_type_, ElapsedMicros());
    }
  }

 private:
  Env* const env_;
  Statistics* statistics_;
  const uint32_t hist_type_;
  bool enabled_;
  const uint64_t start_time_;
};

// a nano second precision stopwatch
class StopWatchNano {
 public:
  explicit StopWatchNano(Env* const env, bool auto_start = false)
      : env_(env), start_(0) {
    if (auto_start) {
      Start();
    }
  }

  void Start() { start_ = env_->NowNanos(); }

  uint64_t ElapsedNanos(bool reset = false) {
    auto now = env_->NowNanos();
    auto elapsed = now - start_;
    if (reset) {
      start_ = now;
    }
    return elapsed;
  }

 private:
  Env* const env_;
  uint64_t start_;
};

} // namespace rocksdb
