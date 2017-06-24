//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once
#ifndef ROCKSDB_LITE

#include <chrono>
#include <memory>

#include "rocksdb/db.h"
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

  // Initializes state that isn't available to at construction time because
  // DB::Open(), which creates the DB object and sanitizes options, hasn't been
  // called yet.
  //
  // AutoTuner implementors may wish to override this function to access other
  // DBOptions members. If so, AutoTuner::Init() must still be called by the
  // overriding function.
  virtual void Init(DB* db, const DBOptions& init_db_options) {
    assert(!init_);
    db_ = db;
    stats_ = init_db_options.statistics.get();
    logger_ = init_db_options.info_log.get();
    init_ = true;
  }

 protected:
  DB* GetDB() {
    assert(init_);
    return db_;
  }
  Statistics* GetStatistics() {
    assert(init_);
    return stats_;
  }
  Logger* GetLogger() {
    assert(init_);
    return logger_;
  }

 private:
  DB* db_ = nullptr;
  Statistics* stats_ = nullptr;
  Logger* logger_ = nullptr;
  bool init_ = false;
};

// This rate limiter dynamically adjusts I/O rate limit to try to keep the
// percentage of drained intervals in the watermarked range.
//
// This auto-tuner assumes DBOptions::rate_limiter objects are used for a single
// database only.
//
// @param rate_limiter_interval Duration of rate limiter interval (ms)
// @param low_watermark_pct Below this percentage of drained intervals, decrease
//    rate limit by adjust_factor_pct
// @param high_watermark_pct Above this percentage of drained intervals,
//    increase rate limit by adjust_factor_pct
// @param adjust_factor_pct Percentage by which we increase/decrease rate limit
// @param min_bytes_per_sec Rate limit will not be adjusted below this rate
// @param max_bytes_per_sec Rate limit will not be adjusted above this rate
AutoTuner* NewRateLimiterAutoTuner(
    std::chrono::milliseconds rate_limiter_interval, int low_watermark_pct,
    int high_watermark_pct, int adjust_factor_pct, int64_t min_bytes_per_sec,
    int64_t max_bytes_per_sec);

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
