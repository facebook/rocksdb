//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "rocksdb/auto_tuner.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"

namespace rocksdb {

class RateLimiterAutoTuner : public AutoTuner {
 public:
  RateLimiterAutoTuner(const std::shared_ptr<Statistics>& stats,
                       RateLimiter* rate_limiter,
                       std::chrono::milliseconds rate_limiter_interval,
                       int low_watermark_pct, int high_watermark_pct,
                       int adjust_factor_pct, int64_t init_bytes_per_sec,
                       int64_t min_bytes_per_sec, int64_t max_bytes_per_sec)
      : stats_(stats),
        rate_limiter_(rate_limiter),
        tuned_time_(0),
        rate_limiter_interval_(rate_limiter_interval),
        low_watermark_pct_(low_watermark_pct),
        high_watermark_pct_(high_watermark_pct),
        adjust_factor_pct_(adjust_factor_pct),
        rate_limiter_drains_(0),
        bytes_per_sec_(init_bytes_per_sec),
        min_bytes_per_sec_(min_bytes_per_sec),
        max_bytes_per_sec_(max_bytes_per_sec) {
    assert(stats_ != nullptr);
  }
  virtual Status Tune(std::chrono::milliseconds now) override;
  virtual std::chrono::milliseconds GetInterval() override;

 private:
  std::shared_ptr<Statistics> stats_;
  RateLimiter* rate_limiter_;
  std::chrono::milliseconds tuned_time_;
  std::chrono::milliseconds rate_limiter_interval_;
  int low_watermark_pct_;
  int high_watermark_pct_;
  int adjust_factor_pct_;
  int64_t rate_limiter_drains_;
  int64_t bytes_per_sec_;
  int64_t min_bytes_per_sec_;
  int64_t max_bytes_per_sec_;
};

Status RateLimiterAutoTuner::Tune(std::chrono::milliseconds now) {
  std::chrono::milliseconds prev_tuned_time = tuned_time_;
  tuned_time_ = now;
  int64_t prev_rate_limiter_drains = rate_limiter_drains_;
  rate_limiter_drains_ =
      static_cast<int64_t>(stats_->getTickerCount(NUMBER_RATE_LIMITER_DRAINS));

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
  int64_t prev_bytes_per_sec = bytes_per_sec_;
  if (drained_pct < low_watermark_pct_) {
    bytes_per_sec_ =
        std::max(min_bytes_per_sec_,
                 prev_bytes_per_sec * 100 / (100 + adjust_factor_pct_));
  } else if (drained_pct > high_watermark_pct_) {
    bytes_per_sec_ =
        std::min(max_bytes_per_sec_,
                 prev_bytes_per_sec * (100 + adjust_factor_pct_) / 100);
  }
  if (bytes_per_sec_ != prev_bytes_per_sec) {
    rate_limiter_->SetBytesPerSecond(bytes_per_sec_);
  }
  return Status::OK();
}

std::chrono::milliseconds RateLimiterAutoTuner::GetInterval() {
  return 100 * rate_limiter_interval_;
}

AutoTuner* NewRateLimiterAutoTuner(
    const std::shared_ptr<Statistics>& stats, RateLimiter* rate_limiter,
    std::chrono::milliseconds rate_limiter_interval, int low_watermark_pct,
    int high_watermark_pct, int adjust_factor_pct, int64_t init_bytes_per_sec,
    int64_t min_bytes_per_sec, int64_t max_bytes_per_sec) {
  return new RateLimiterAutoTuner(stats, rate_limiter, rate_limiter_interval,
                                  low_watermark_pct, high_watermark_pct,
                                  adjust_factor_pct, init_bytes_per_sec,
                                  min_bytes_per_sec, max_bytes_per_sec);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
