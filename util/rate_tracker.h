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

// RateTracker is a template class that tracks the rate of change of values
// over time. It records data points with timestamps and calculates the rate
// of change between consecutive recordings.
//
// Template parameter T should be a numeric type that supports arithmetic
// operations (addition, subtraction, division).
//
// Example usage:
//   RateTracker<int64_t> bytes_tracker;
//   bytes_tracker.Record(1000);  // First recording, no rate calculated
//   std::this_thread::sleep_for(std::chrono::seconds(1));
//   auto rate = bytes_tracker.Record(2000);  // Returns rate: 1000 bytes/second
template <typename T>
class RateTracker {
 public:
  // Constructor
  // clock: System clock for timing (optional, uses default if nullptr)
  explicit RateTracker(const std::shared_ptr<SystemClock>& clock = nullptr)
      : clock_(clock ? clock : SystemClock::Default()),
        has_previous_data_(false),
        previous_value_(T{}),
        rate_(-1),
        previous_timestamp_us_(0) {}

  virtual ~RateTracker() = default;

  // Record a new value and return the rate of change since the last recording.
  // For the first recording, returns 0 since there's no previous data.
  //
  // value: The new value to record
  // Returns: Rate of change (value_delta / time_delta_seconds)
  //          Returns 0 for first recording or if time delta is zero
  double Record(T value) {
    uint64_t current_timestamp_us = GetCurrentTimeMicros();

    if (!has_previous_data_) {
      // First recording - store the data but return 0 rate
      previous_value_ = value;
      previous_timestamp_us_ = current_timestamp_us;
      has_previous_data_ = true;
      return 0.0;
    }

    // Calculate time delta in seconds
    uint64_t time_delta_us = current_timestamp_us - previous_timestamp_us_;
    if (time_delta_us == 0) {
      // No time has passed, return 0 rate to avoid division by zero
      fprintf(stderr, "time_delta_us is 0, return 0.0\n");
      return 0.0;
    }

    double time_delta_seconds = static_cast<double>(time_delta_us) / 1000000.0;

    // Calculate value delta
    T value_delta = value - previous_value_;

    // Calculate rate (value change per second)
    rate_ = static_cast<double>(value_delta) / time_delta_seconds;

    // Update stored values for next calculation
    previous_value_ = value;
    previous_timestamp_us_ = current_timestamp_us;

    return rate_;
  }
  double GetRate() { return rate_; }

  // Get the last recorded value
  // Returns the default value of T if no data has been recorded
  T GetLastValue() const { return has_previous_data_ ? previous_value_ : T{}; }

  // Get the timestamp of the last recorded value in microseconds
  // Returns 0 if no data has been recorded
  uint64_t GetLastTimestampUs() const {
    return has_previous_data_ ? previous_timestamp_us_ : 0;
  }

  // Check if any data has been recorded
  bool HasData() const { return has_previous_data_; }

  // Reset the tracker, clearing all recorded data
  void Reset() {
    has_previous_data_ = false;
    previous_value_ = T{};
    previous_timestamp_us_ = 0;
  }

  // For testing purposes - allows injection of custom clock
  void TEST_SetClock(std::shared_ptr<SystemClock> clock) {
    clock_ = std::move(clock);
  }

 private:
  uint64_t GetCurrentTimeMicros() { return clock_->NowMicros(); }

  // System clock for timing
  std::shared_ptr<SystemClock> clock_;

  // Flag indicating if we have previous data to calculate rate
  bool has_previous_data_;

  // Previous recorded value
  T previous_value_;
  double rate_;

  // Timestamp of previous recording in microseconds
  uint64_t previous_timestamp_us_;
};

// Specialized rate tracker for common use cases
using BytesRateTracker = RateTracker<int64_t>;
using CountRateTracker = RateTracker<int64_t>;
using SizeRateTracker = RateTracker<size_t>;

// Thread-safe version of RateTracker using atomic operations
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

  // Explicitly delete copy constructor and copy assignment operator
  // since atomic members make the class non-copyable
  AtomicRateTracker(const AtomicRateTracker&) = delete;
  AtomicRateTracker& operator=(const AtomicRateTracker&) = delete;

  // Move constructor and move assignment operator
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

  // Thread-safe version of Record
  double Record(T value) {
    uint64_t current_timestamp_us = GetCurrentTimeMicros();

    // Use atomic operations to ensure thread safety
    bool had_previous_data = has_previous_data_.load(std::memory_order_acquire);

    if (!had_previous_data) {
      // First recording - try to set the initial values atomically
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
      printf("time_delta_us is 0, return previous rate\n");
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
    // fprintf(stderr, "current_value: %lu rate_ = %f\n", value, rate_.load());
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
// Implementation moved to rate_tracker.cc
class CPUIOUtilizationTracker {
 public:
  explicit CPUIOUtilizationTracker(const std::shared_ptr<SystemClock>& clock,
                                   size_t min_wait_us, DBOptions opt);

  // True -> Recorded Prediction changed
  // False -> wait time is not long enough
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
  double cpu_usage_;
  double io_utilization_;
  size_t next_record_time_us_;
  DBOptions opt_;
};
// Thread-safe version of RateTracker using atomic operations
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

// Class to track CPU utilization using /proc/stat
// Implementation moved to rate_tracker.cc
class ProcSysCPUUtilizationTracker {
 public:
  explicit ProcSysCPUUtilizationTracker();
  bool Record();
  float GetCpuUtilization();

 private:
  AtomicRateTrackerWithY<size_t> cpu_usage_rate_;
};
}  // namespace ROCKSDB_NAMESPACE
