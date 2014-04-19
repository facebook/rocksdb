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
  explicit StopWatch(
    Env * const env,
    Statistics* statistics = nullptr,
    const Histograms histogram_name = DB_GET,
    bool auto_start = true) :
      env_(env),
      start_time_((!auto_start && !statistics) ? 0 : env->NowMicros()),
      statistics_(statistics),
      histogram_name_(histogram_name) {}



  uint64_t ElapsedMicros() {
    return env_->NowMicros() - start_time_;
  }

  ~StopWatch() { MeasureTime(statistics_, histogram_name_, ElapsedMicros()); }

 private:
  Env* const env_;
  const uint64_t start_time_;
  Statistics* statistics_;
  const Histograms histogram_name_;

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
