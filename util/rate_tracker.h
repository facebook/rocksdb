//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#if defined(_WIN32)
#else
// Unix/Linux-specific headers
#include <sys/resource.h>
#endif

#include <atomic>

#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

// RateTracker is a template class that tracks the rate of change of
// values over time. It records data points with microsecond timestamps
// and calculates the rate of change between consecutive recordings.
// The GetRate() method returns the rate in units per second (unit/s).
// Template parameter T must be a numeric type supporting arithmetic
// operations (addition, subtraction, division).
template <typename T>
class RateTracker {
 public:
  explicit RateTracker(const std::shared_ptr<SystemClock>& clock)
      : clock_(clock ? clock : SystemClock::Default()),
        has_previous_data_(false),
        previous_value_(T{}),
        rate_(0.0),
        previous_timestamp_us_(0) {}

  virtual ~RateTracker() = default;

  RateTracker(const RateTracker&) = delete;
  RateTracker& operator=(const RateTracker&) = delete;

  RateTracker(RateTracker&& other) noexcept
      : clock_(std::move(other.clock_)),
        has_previous_data_(other.has_previous_data_),
        previous_value_(other.previous_value_),
        rate_(other.rate_),
        previous_timestamp_us_(other.previous_timestamp_us_) {}

  RateTracker& operator=(RateTracker&& other) noexcept {
    if (this != &other) {
      clock_ = std::move(other.clock_);
      has_previous_data_ = std::move(other.has_previous_data_);
      previous_value_ = std::move(other.previous_value_);
      rate_ = std::move(other.rate_);
      previous_timestamp_us_ = std::move(other.previous_timestamp_us_);
    }
    return *this;
  }

  double Record(T value) {
    uint64_t current_timestamp_us = GetCurrentTimeMicros();
    if (!has_previous_data_) {
      previous_value_ = value;
      previous_timestamp_us_ = current_timestamp_us;
      has_previous_data_ = true;
      return 0.0;
    }

    // Calculate time delta
    uint64_t time_delta_us = current_timestamp_us - previous_timestamp_us_;
    if (time_delta_us == 0) {
      // No time has passed, return rate_ to avoid division by zero
      return rate_;
    }
    double time_delta_seconds = static_cast<double>(time_delta_us) / 1000000.0;
    T value_delta = value - previous_value_;
    rate_ = static_cast<double>(value_delta) / time_delta_seconds;
    previous_value_ = value;
    previous_timestamp_us_ = current_timestamp_us;
    return rate_;
  }

  inline double GetRate() const { return rate_; }

 private:
  uint64_t GetCurrentTimeMicros() { return clock_->NowMicros(); }

  std::shared_ptr<SystemClock> clock_;
  bool has_previous_data_;
  T previous_value_;
  double rate_;
  uint64_t previous_timestamp_us_;
};
// Class to track CPU and IO utilization
// Track process cpu usage using sysresource.h on linux
// Windows is not supported yet
// Track IO utilization using IO through rate limiter
class CPUIOUtilizationTracker {
 public:
  explicit CPUIOUtilizationTracker(
      const std::shared_ptr<RateLimiter>& rate_limiter,
      const std::shared_ptr<SystemClock>& clock = nullptr);

  void Record();
  double GetCpuUtilization();
  double GetIoUtilization();
  std::pair<double, double> GetUtilization();

 private:
  void RecordCPUUsage();
  void RecordIOUtilization();
  std::mutex mutex_;
  std::shared_ptr<RateLimiter> rate_limiter_;
  RateTracker<size_t> rate_limiter_bytes_rate_;
  RateTracker<double> cpu_usage_rate_;
};

}  // namespace ROCKSDB_NAMESPACE
