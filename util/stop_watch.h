//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
#pragma once
#include "monitoring/statistics.h"
#include "rocksdb/env.h"

namespace rocksdb {
// Auto-scoped.
// Records the measure time into the corresponding histogram if statistics
// is not nullptr. It is also saved into *elapsed if the pointer is not nullptr
// and overwrite is true, it will be added to *elapsed if overwrite is false.
class StopWatch {
 public:
  StopWatch(Env* const env, Statistics* statistics, const uint32_t hist_type,
            uint64_t* elapsed = nullptr, bool overwrite = true)
      : env_(env),
        statistics_(statistics),
        hist_type_(hist_type),
        elapsed_(elapsed),
        overwrite_(overwrite),
        stats_enabled_(statistics && statistics->HistEnabledForType(hist_type)),
        start_time_((stats_enabled_ || elapsed != nullptr) ? env->NowMicros()
                                                           : 0) {}

  ~StopWatch() {
    if (elapsed_) {
      if (overwrite_) {
        *elapsed_ = env_->NowMicros() - start_time_;
      } else {
        *elapsed_ += env_->NowMicros() - start_time_;
      }
    }
    if (stats_enabled_) {
      statistics_->measureTime(hist_type_,
          (elapsed_ != nullptr) ? *elapsed_ :
                                  (env_->NowMicros() - start_time_));
    }
  }

  uint64_t start_time() const { return start_time_; }

 private:
  Env* const env_;
  Statistics* statistics_;
  const uint32_t hist_type_;
  uint64_t* elapsed_;
  bool overwrite_;
  bool stats_enabled_;
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

  uint64_t ElapsedNanosSafe(bool reset = false) {
    return (env_ != nullptr) ? ElapsedNanos(reset) : 0U;
  }

 private:
  Env* const env_;
  uint64_t start_;
};

} // namespace rocksdb
