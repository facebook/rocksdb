//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <chrono>
#include <memory>

#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace rocksdb {

// An AutoTuner changes dynamic options at regular intervals based on statistics
// data. They can be configured in DBOptions::auto_tuners.
class AutoTuner {
 public:
  virtual ~AutoTuner() {}

  // Invoked approximately every GetInterval() milliseconds to examine
  // statistics and adjust dynamic options accordingly.
  virtual Status Tune(std::chrono::milliseconds now) = 0;

  // Desired interval between calls to Tune().
  virtual std::chrono::milliseconds GetInterval() = 0;

  // These are called during DB::Open with the final statistics and info logger
  // objects.
  void SetStatistics(Statistics* stats) { stats_ = stats; }
  void SetLogger(Logger* logger) { logger_ = logger; }

 protected:
  Statistics* stats_ = nullptr;
  Logger* logger_ = nullptr;
};

// This rate limiter dynamically adjusts I/O rate limit to try to keep the
// percentage of drained intervals in the watermarked range.
//
// This auto-tuner assumes stats and rate_limiter objects are used for a single
// database only.
//
// @param rate_limiter Rate limiter that we'll be adjusting
// @param rate_limiter_interval Duration of rate limiter interval (ms)
// @param low_watermark_pct Below this percentage of drained intervals, decrease
//    rate limit by adjust_factor_pct
// @param high_watermark_pct Above this percentage of drained intervals,
//    increase rate limit by adjust_factor_pct
// @param adjust_factor_pct Percentage by which we increase/decrease rate limit
// @param min_bytes_per_sec Rate limit will not be adjusted below this rate
// @param max_bytes_per_sec Rate limit will not be adjusted above this rate
AutoTuner* NewRateLimiterAutoTuner(
    std::shared_ptr<RateLimiter> rate_limiter,
    std::chrono::milliseconds rate_limiter_interval, int low_watermark_pct,
    int high_watermark_pct, int adjust_factor_pct, int64_t min_bytes_per_sec,
    int64_t max_bytes_per_sec);

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
