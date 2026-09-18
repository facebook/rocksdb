//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;

struct OffpeakTimeInfo {
  bool is_now_offpeak = false;
  int seconds_till_next_offpeak_start = 0;
};

struct OffpeakTimeOption {
  static constexpr int kSecondsPerDay = 86400;
  static constexpr int kSecondsPerHour = 3600;
  static constexpr int kSecondsPerMinute = 60;

  OffpeakTimeOption();
  explicit OffpeakTimeOption(const std::string& offpeak_time_string);
  std::string daily_offpeak_time_utc = "";
  int daily_offpeak_start_time_utc = 0;
  int daily_offpeak_end_time_utc = 0;

  void SetFromOffpeakTimeString(const std::string& offpeak_time_string);

  OffpeakTimeInfo GetOffpeakTimeInfo(const int64_t& current_time) const;
};

struct DynamicOffpeakPrediction {
  bool available = false;
  uint32_t bucket_start_utc_minutes = 0;
  double bytes_per_second = 0;
  double operations_per_second = 0;
};

struct DynamicOffpeakObservation {
  bool available = false;
  uint64_t start_time = 0;
  uint64_t end_time = 0;
  double bytes_per_second = 0;
  double operations_per_second = 0;
};

class DynamicOffpeakModel {
 public:
  static constexpr uint32_t kBucketMinutes = 15;
  static constexpr uint32_t kSmoothingMinutes = 60;
  static constexpr uint32_t kBucketsPerDay = 24 * 60 / kBucketMinutes;
  static constexpr uint64_t kBucketSeconds = kBucketMinutes * 60;
  static constexpr uint64_t kSecondsPerDay = 24 * 60 * 60;
  static constexpr uint64_t kMaxModelAgeSeconds = 7 * kSecondsPerDay;

  // Returns true when a completed UTC day updates the learned model.
  bool AddSample(uint64_t start_time, uint64_t end_time,
                 uint64_t foreground_bytes, uint64_t foreground_operations,
                 uint32_t percentile);

  bool IsTrained() const { return trained_days_ > 0; }
  bool IsFresh(uint64_t now) const;
  uint32_t TrainedDays() const { return trained_days_; }
  uint32_t LastDayCoveragePercent() const { return last_day_coverage_percent_; }
  uint64_t LastUpdateTime() const { return last_update_time_; }
  const std::string& LearnedWindow() const { return learned_window_; }
  DynamicOffpeakPrediction GetPrediction(uint64_t current_time) const;
  DynamicOffpeakObservation GetLatestObservation() const {
    return latest_observation_;
  }

  void RecomputeWindow(uint32_t percentile);
  std::string Encode() const;
  Status Decode(const Slice& encoded, uint32_t percentile);

 private:
  bool FinalizeDay(uint32_t percentile);
  void ResetDay(uint64_t day_start);
  void AddSegment(uint64_t start_time, uint64_t end_time, double byte_rate,
                  double operation_rate);

  std::array<double, kBucketsPerDay> byte_rate_ewma_{};
  std::array<double, kBucketsPerDay> operation_rate_ewma_{};
  std::array<bool, kBucketsPerDay> known_buckets_{};

  std::array<double, kBucketsPerDay> day_bytes_{};
  std::array<double, kBucketsPerDay> day_operations_{};
  std::array<uint32_t, kBucketsPerDay> day_coverage_seconds_{};
  uint64_t current_day_start_ = 0;
  bool has_current_day_ = false;

  uint32_t trained_days_ = 0;
  uint32_t last_day_coverage_percent_ = 0;
  uint64_t last_update_time_ = 0;
  std::string learned_window_;
  DynamicOffpeakObservation latest_observation_;
};

}  // namespace ROCKSDB_NAMESPACE
