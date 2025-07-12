//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
#include <chrono>
#include <memory>

#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
  static constexpr uint64_t kMicrosInSecond = 1000000;
// Structure to hold CPU statistics
typedef struct {
  unsigned long user;
  unsigned long nice;
  unsigned long system;
  unsigned long idle;
  unsigned long iowait;
  unsigned long irq;
  unsigned long softirq;
} proc_cpu_stats;

// Function to read CPU statistics from /proc/stat
// Implementation moved to rate_tracker.cc to avoid multiple definition errors
void read_cpu_stats(proc_cpu_stats* stats);

// AtomicRateTracker is a template class that tracks the rate of change of values
// over time. It records data points with timestamps and calculates the rate
// of change between consecutive recordings.
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

// RateTrackerWithY is a template class that tracks the rate of change of x with respect to y
// it stores the last x and y values and calculates the rate of change between consecutive recordings.
template <typename T>
class AtomicRateTrackerWithY {
 public:
  explicit AtomicRateTrackerWithY()
      : has_previous_data_(false), previous_x_(0), previous_y_(0), rate_(0.0) {}
  virtual ~AtomicRateTrackerWithY() = default;
  // Explicitly delete copy constructor and copy assignment operator
  // since atomic members make the class non-copyable
  AtomicRateTrackerWithY(const AtomicRateTrackerWithY&) = delete;
  AtomicRateTrackerWithY& operator=(const AtomicRateTrackerWithY&) = delete;

  // Move constructor and move assignment operator
  AtomicRateTrackerWithY(AtomicRateTrackerWithY&& other) noexcept
      : has_previous_data_(other.has_previous_data_.load()),
        previous_x_(other.previous_x_.load()),
        previous_y_(other.previous_x_.load()),
        rate_(other.rate_.load()) {}

  AtomicRateTrackerWithY& operator=(AtomicRateTrackerWithY&& other) noexcept {
    if (this != &other) {
      has_previous_data_.store(other.has_previous_data_.load());
      previous_x_.store(other.previous_x_.load());
      previous_y_.store(other.previous_y_.load());
      rate_.store(other.rate_.load());
    }
    return *this;
  }

  // Thread-safe version of Record
  double Record(T xvalue, T yvalue) {
    // Use atomic operations to ensure thread safety
    bool had_previous_data = has_previous_data_.load(std::memory_order_acquire);
    if (!had_previous_data) {
      // First recording - try to set the initial values atomically
      T expected_value = T{};

      if (previous_x_.compare_exchange_strong(expected_value, xvalue,
                                              std::memory_order_acq_rel) &&
          previous_y_.compare_exchange_strong(expected_value, yvalue,
                                              std::memory_order_acq_rel)) {
        has_previous_data_.store(true, std::memory_order_release);
        return 0.0;
      }
      // If CAS failed, another thread set the initial values, continue with
      // rate calculation
      return 0.0;
    }

    // Load previous values atomically
    T prev_x = previous_x_.load(std::memory_order_acquire);
    T prev_y = previous_y_.load(std::memory_order_acquire);

    // Calculate time delta
    uint64_t y_delta = yvalue - prev_y;
    if (y_delta == 0) {
      // No time has passed, return 0 rate to avoid division by zero
      printf("y_delta_us is 0, return previous rate\n");
      return rate_;
    }

    T x_delta = xvalue - prev_x;
    rate_ = static_cast<double>(x_delta) / y_delta;

    // Update stored values atomically
    previous_x_.store(xvalue, std::memory_order_release);
    previous_y_.store(yvalue, std::memory_order_release);
    has_previous_data_.store(true, std::memory_order_release);
    return rate_;
  }

  std::pair<T, T> GetLastValue() const {
    return {previous_x_.load(std::memory_order_acquire),
            previous_y_.load(std::memory_order_acquire)};
  }

  bool HasData() const {
    return has_previous_data_.load(std::memory_order_acquire);
  }

  void Reset() {
    has_previous_data_.store(false, std::memory_order_release);
    previous_x_.store(T{}, std::memory_order_release);
    previous_y_.store(0, std::memory_order_release);
  }

  double GetRate() const { return rate_.load(std::memory_order_acquire); }

 private:
  std::atomic<bool> has_previous_data_;
  std::atomic<T> previous_x_, previous_y_;
  std::atomic<double> rate_;
};
// Class to track CPU and IO utilization using /proc/stat
class CPUIOUtilizationTracker {
 public:
  explicit CPUIOUtilizationTracker(const std::shared_ptr<SystemClock>& clock,
                                   size_t min_wait_us, DBOptions opt);

  bool Record();
  float GetCpuUtilization();
  float GetIoUtilization();

 private:
  void RecordCPUUsage();
  void RecordIOUtilization();
  uint64_t GetCurrentTimeMicros();

  std::shared_ptr<SystemClock> clock_;
  size_t min_wait_us_;
  AtomicRateTracker<size_t> rate_limiter_bytes_rate_;
  AtomicRateTracker<size_t> cpu_usage_rate_;
  float cpu_usage_;
  float io_utilization_;
  size_t next_record_time_us_;
  DBOptions opt_;
};

// Class to track CPU utilization using /proc/stat
class ProcSysCPUUtilizationTracker {
 public:
  explicit ProcSysCPUUtilizationTracker();
  bool Record();
  double GetCpuUtilization();
 private:
  AtomicRateTrackerWithY<size_t> cpu_usage_rate_;
};
}  // namespace ROCKSDB_NAMESPACE
