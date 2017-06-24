//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#ifndef ROCKSDB_LITE

#include <inttypes.h>
#include <algorithm>

#include "rocksdb/auto_tuner.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"
#include "util/logging.h"

namespace rocksdb {

class RateLimiterAutoTuner : public AutoTuner {
 public:
  RateLimiterAutoTuner(std::chrono::milliseconds rate_limiter_interval,
                       int low_watermark_pct, int high_watermark_pct,
                       int adjust_factor_pct, int64_t min_bytes_per_sec,
                       int64_t max_bytes_per_sec)
      : tuned_time_(0),
        rate_limiter_interval_(rate_limiter_interval),
        low_watermark_pct_(low_watermark_pct),
        high_watermark_pct_(high_watermark_pct),
        adjust_factor_pct_(adjust_factor_pct),
        rate_limiter_drains_(0),
        min_bytes_per_sec_(min_bytes_per_sec),
        max_bytes_per_sec_(max_bytes_per_sec) {}

  virtual Status Tune(std::chrono::milliseconds now) override;
  virtual std::chrono::milliseconds GetInterval() override;

  virtual void Init(DB* db, const DBOptions& init_db_options) override {
    assert(init_db_options.rate_limiter != nullptr);
    rate_limiter_ = init_db_options.rate_limiter.get();
    AutoTuner::Init(db, init_db_options);
  }

 private:
  RateLimiter* rate_limiter_;
  std::chrono::milliseconds tuned_time_;
  std::chrono::milliseconds rate_limiter_interval_;
  int low_watermark_pct_;
  int high_watermark_pct_;
  int adjust_factor_pct_;
  int64_t rate_limiter_drains_;
  int64_t min_bytes_per_sec_;
  int64_t max_bytes_per_sec_;
};

Status RateLimiterAutoTuner::Tune(std::chrono::milliseconds now) {
  assert(rate_limiter_ != nullptr);
  std::chrono::milliseconds prev_tuned_time = tuned_time_;
  tuned_time_ = now;
  int64_t prev_rate_limiter_drains = rate_limiter_drains_;
  rate_limiter_drains_ =
      static_cast<int64_t>(GetStatistics()->getTickerCount(NUMBER_RATE_LIMITER_DRAINS));

  if (prev_tuned_time == std::chrono::milliseconds(0)) {
    // do nothing when no history window
    return Status::OK();
  }

  int64_t elapsed_intervals =
      (tuned_time_ - prev_tuned_time + rate_limiter_interval_ -
       std::chrono::milliseconds(1)) /
      rate_limiter_interval_;
  int64_t drained_pct = (rate_limiter_drains_ - prev_rate_limiter_drains) *
                        100 / elapsed_intervals;
  int64_t prev_bytes_per_sec = rate_limiter_->GetBytesPerSecond();
  int64_t new_bytes_per_sec;
  if (drained_pct < low_watermark_pct_) {
    new_bytes_per_sec =
        std::max(min_bytes_per_sec_,
                 prev_bytes_per_sec * 100 / (100 + adjust_factor_pct_));
  } else if (drained_pct > high_watermark_pct_) {
    new_bytes_per_sec =
        std::min(max_bytes_per_sec_,
                 prev_bytes_per_sec * (100 + adjust_factor_pct_) / 100);
  } else {
    new_bytes_per_sec = prev_bytes_per_sec;
  }
  if (new_bytes_per_sec != prev_bytes_per_sec) {
    rate_limiter_->SetBytesPerSecond(new_bytes_per_sec);
    ROCKS_LOG_INFO(GetLogger(), "adjusted rate limit to %" PRId64,
                   new_bytes_per_sec);
  }
  return Status::OK();
}

std::chrono::milliseconds RateLimiterAutoTuner::GetInterval() {
  return 100 * rate_limiter_interval_;
}

AutoTuner* NewRateLimiterAutoTuner(
    std::chrono::milliseconds rate_limiter_interval, int low_watermark_pct,
    int high_watermark_pct, int adjust_factor_pct, int64_t min_bytes_per_sec,
    int64_t max_bytes_per_sec) {
  return new RateLimiterAutoTuner(rate_limiter_interval, low_watermark_pct,
                                  high_watermark_pct, adjust_factor_pct,
                                  min_bytes_per_sec, max_bytes_per_sec);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
