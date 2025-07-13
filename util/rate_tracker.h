//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <stdio.h>
#include <stdlib.h>

#if defined(_WIN32)
#else
// Unix/Linux-specific headers
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <atomic>
#include <memory>

#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/system_clock.h"
#include "util/auto_refill_budget.h"

namespace ROCKSDB_NAMESPACE {

// AtomicRateTracker is a template class that tracks the rate of change of
// values over time. It records data points with timestamps and calculates the
// rate of change between consecutive recordings.
//
// Template parameter T should be a numeric type that supports arithmetic
// operations (addition, subtraction, division).
template <typename T>
class AtomicRateTracker {
 public:
  explicit AtomicRateTracker(
      const std::shared_ptr<SystemClock>& clock = nullptr)
      : clock_(clock ? clock : SystemClock::Default()),
        has_previous_data_(false),
        previous_value_(T{}),
        rate_(0.0),
        previous_timestamp_us_(0) {}

  virtual ~AtomicRateTracker() = default;

  AtomicRateTracker(const AtomicRateTracker&) = delete;
  AtomicRateTracker& operator=(const AtomicRateTracker&) = delete;

  AtomicRateTracker(AtomicRateTracker&& other) noexcept
      : clock_(std::move(other.clock_)),
        has_previous_data_(other.has_previous_data_.load()),
        previous_value_(other.previous_value_.load()),
        rate_(other.rate_.load()),
        previous_timestamp_us_(other.previous_timestamp_us_.load()) {}

  AtomicRateTracker& operator=(AtomicRateTracker&& other) noexcept {
    if (this != &other) {
      clock_ = std::move(other.clock_);
      has_previous_data_.store(other.has_previous_data_.load());
      previous_value_.store(other.previous_value_.load());
      rate_.store(other.rate_.load());
      previous_timestamp_us_.store(other.previous_timestamp_us_.load());
    }
    return *this;
  }

  double Record(T value) {
    uint64_t current_timestamp_us = GetCurrentTimeMicros();
    bool had_previous_data = has_previous_data_.load(std::memory_order_acquire);

    if (!had_previous_data) {
      T expected_value = T{};
      uint64_t expected_timestamp = 0;

      if (previous_value_.compare_exchange_strong(expected_value, value,
                                                  std::memory_order_acq_rel) &&
          previous_timestamp_us_.compare_exchange_strong(
              expected_timestamp, current_timestamp_us,
              std::memory_order_acq_rel)) {
        has_previous_data_.store(true, std::memory_order_release);
        return 0.0;
      }
      // If CAS failed, another thread set the initial values, continue with
      // rate calculation
      return 0.0;
    }

    // Load previous values atomically
    T prev_value = previous_value_.load(std::memory_order_acquire);
    uint64_t prev_timestamp =
        previous_timestamp_us_.load(std::memory_order_acquire);

    // Calculate time delta
    uint64_t time_delta_us = current_timestamp_us - prev_timestamp;
    if (time_delta_us == 0) {
      // No time has passed, return 0 rate to avoid division by zero
      fprintf(stderr, "time_delta_us is 0, return previous rate\n");
      return rate_;
    }

    double time_delta_seconds = static_cast<double>(time_delta_us) / 1000000.0;
    T value_delta = value - prev_value;
    rate_ = static_cast<double>(value_delta) / time_delta_seconds;

    // Update stored values atomically
    previous_value_.store(value, std::memory_order_release);
    previous_timestamp_us_.store(current_timestamp_us,
                                 std::memory_order_release);
    has_previous_data_.store(true, std::memory_order_release);
    return rate_;
  }

  T GetLastValue() const {
    return previous_value_.load(std::memory_order_acquire);
  }

  uint64_t GetLastTimestampUs() const {
    return previous_timestamp_us_.load(std::memory_order_acquire);
  }

  bool HasData() const {
    return has_previous_data_.load(std::memory_order_acquire);
  }

  void Reset() {
    has_previous_data_.store(false, std::memory_order_release);
    previous_value_.store(T{}, std::memory_order_release);
    previous_timestamp_us_.store(0, std::memory_order_release);
  }

  void TEST_SetClock(std::shared_ptr<SystemClock> clock) {
    clock_ = std::move(clock);
  }

  double GetRate() const { return rate_.load(std::memory_order_acquire); }

 private:
  uint64_t GetCurrentTimeMicros() { return clock_->NowMicros(); }

  std::shared_ptr<SystemClock> clock_;
  std::atomic<bool> has_previous_data_;
  std::atomic<T> previous_value_;
  std::atomic<double> rate_;
  std::atomic<uint64_t> previous_timestamp_us_;
};
// Class to track CPU and IO utilization
class CPUIOUtilizationTracker {
 public:
  explicit CPUIOUtilizationTracker(
      const std::shared_ptr<RateLimiter>& rate_limiter,
      const std::shared_ptr<SystemClock>& clock = nullptr);

  bool Record();
  double GetCpuUtilization();
  double GetIoUtilization();

 private:
  void RecordCPUUsage();
  void RecordIOUtilization();
  uint64_t GetCurrentTimeMicros();

  std::shared_ptr<RateLimiter> rate_limiter_;
  std::shared_ptr<SystemClock> clock_;
  double io_utilization_;
  double cpu_usage_;
  AtomicRateTracker<size_t> rate_limiter_bytes_rate_;
  AtomicRateTracker<double> cpu_usage_rate_;
};

class RequestRateLimiter {
 private:
  // Use AutoRefillBudget to track requests per second
  std::unique_ptr<AutoRefillBudget<size_t>> request_budget_;
  static constexpr uint64_t kMicrosInSecond = 1000000;

 public:
  explicit RequestRateLimiter(size_t requests_per_second);
  bool TryProcessRequest(size_t request_cost = 1);
  size_t GetAvailableRequests();
  void UpdateRateLimit(size_t new_requests_per_second);
  void Reset();
};
}  // namespace ROCKSDB_NAMESPACE
