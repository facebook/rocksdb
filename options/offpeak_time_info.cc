//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/offpeak_time_info.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <vector>

#include "rocksdb/system_clock.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
OffpeakTimeOption::OffpeakTimeOption() : OffpeakTimeOption("") {}
OffpeakTimeOption::OffpeakTimeOption(const std::string& offpeak_time_string) {
  SetFromOffpeakTimeString(offpeak_time_string);
}

void OffpeakTimeOption::SetFromOffpeakTimeString(
    const std::string& offpeak_time_string) {
  const int old_start_time = daily_offpeak_start_time_utc;
  const int old_end_time = daily_offpeak_end_time_utc;
  if (TryParseTimeRangeString(offpeak_time_string, daily_offpeak_start_time_utc,
                              daily_offpeak_end_time_utc)) {
    daily_offpeak_time_utc = offpeak_time_string;
  } else {
    daily_offpeak_start_time_utc = old_start_time;
    daily_offpeak_end_time_utc = old_end_time;
  }
}

OffpeakTimeInfo OffpeakTimeOption::GetOffpeakTimeInfo(
    const int64_t& current_time) const {
  OffpeakTimeInfo offpeak_time_info;
  if (daily_offpeak_start_time_utc == daily_offpeak_end_time_utc) {
    return offpeak_time_info;
  }
  int seconds_since_midnight = static_cast<int>(current_time % kSecondsPerDay);
  int seconds_since_midnight_to_nearest_minute =
      (seconds_since_midnight / kSecondsPerMinute) * kSecondsPerMinute;
  // if the offpeak duration spans overnight (i.e. 23:30 - 4:30 next day)
  if (daily_offpeak_start_time_utc > daily_offpeak_end_time_utc) {
    offpeak_time_info.is_now_offpeak =
        daily_offpeak_start_time_utc <=
            seconds_since_midnight_to_nearest_minute ||
        seconds_since_midnight_to_nearest_minute <= daily_offpeak_end_time_utc;
  } else {
    offpeak_time_info.is_now_offpeak =
        daily_offpeak_start_time_utc <=
            seconds_since_midnight_to_nearest_minute &&
        seconds_since_midnight_to_nearest_minute <= daily_offpeak_end_time_utc;
  }
  offpeak_time_info.seconds_till_next_offpeak_start =
      seconds_since_midnight < daily_offpeak_start_time_utc
          ? daily_offpeak_start_time_utc - seconds_since_midnight
          : ((daily_offpeak_start_time_utc + kSecondsPerDay) -
             seconds_since_midnight);
  return offpeak_time_info;
}

namespace {

constexpr double kDailyEwmaWeight = 0.25;
constexpr uint32_t kModelEncodingVersion = 1;

uint64_t EncodeDouble(double value) {
  uint64_t result;
  static_assert(sizeof(result) == sizeof(value), "unexpected double size");
  std::memcpy(&result, &value, sizeof(result));
  return result;
}

double DecodeDouble(uint64_t value) {
  double result;
  std::memcpy(&result, &value, sizeof(result));
  return result;
}

std::string FormatWindow(uint32_t start_bucket, uint32_t bucket_count) {
  const uint32_t start_minutes =
      (start_bucket % DynamicOffpeakModel::kBucketsPerDay) *
      DynamicOffpeakModel::kBucketMinutes;
  const uint32_t end_minutes =
      (start_minutes + bucket_count * DynamicOffpeakModel::kBucketMinutes - 1) %
      (24 * 60);
  char result[12];
  std::snprintf(result, sizeof(result), "%02u:%02u-%02u:%02u",
                start_minutes / 60, start_minutes % 60, end_minutes / 60,
                end_minutes % 60);
  return result;
}

}  // namespace

bool DynamicOffpeakModel::AddSample(uint64_t start_time, uint64_t end_time,
                                    uint64_t foreground_bytes,
                                    uint64_t foreground_operations,
                                    uint32_t percentile) {
  if (end_time <= start_time) {
    return false;
  }

  const double duration = static_cast<double>(end_time - start_time);
  const double byte_rate = foreground_bytes / duration;
  const double operation_rate = foreground_operations / duration;
  latest_observation_ = {true, start_time, end_time, byte_rate, operation_rate};
  bool model_updated = false;
  uint64_t cursor = start_time;
  while (cursor < end_time) {
    const uint64_t day_start = cursor - cursor % kSecondsPerDay;
    if (!has_current_day_) {
      ResetDay(day_start);
    } else if (day_start != current_day_start_) {
      model_updated = FinalizeDay(percentile) || model_updated;
      ResetDay(day_start);
    }
    const uint64_t segment_end = std::min(end_time, day_start + kSecondsPerDay);
    AddSegment(cursor, segment_end, byte_rate, operation_rate);
    cursor = segment_end;
  }
  return model_updated;
}

void DynamicOffpeakModel::AddSegment(uint64_t start_time, uint64_t end_time,
                                     double byte_rate, double operation_rate) {
  uint64_t cursor = start_time;
  while (cursor < end_time) {
    const uint32_t bucket =
        static_cast<uint32_t>((cursor - current_day_start_) / kBucketSeconds);
    const uint64_t bucket_end =
        current_day_start_ + (bucket + 1) * kBucketSeconds;
    const uint64_t segment_end = std::min(end_time, bucket_end);
    const uint32_t covered = static_cast<uint32_t>(segment_end - cursor);
    day_bytes_[bucket] += byte_rate * covered;
    day_operations_[bucket] += operation_rate * covered;
    day_coverage_seconds_[bucket] += covered;
    cursor = segment_end;
  }
}

void DynamicOffpeakModel::ResetDay(uint64_t day_start) {
  day_bytes_.fill(0);
  day_operations_.fill(0);
  day_coverage_seconds_.fill(0);
  current_day_start_ = day_start;
  has_current_day_ = true;
}

bool DynamicOffpeakModel::FinalizeDay(uint32_t percentile) {
  uint64_t coverage = 0;
  for (uint32_t bucket_coverage : day_coverage_seconds_) {
    coverage += bucket_coverage;
  }
  last_day_coverage_percent_ =
      static_cast<uint32_t>(coverage * 100 / kSecondsPerDay);
  if (last_day_coverage_percent_ < 90) {
    return false;
  }

  for (uint32_t i = 0; i < kBucketsPerDay; ++i) {
    if (day_coverage_seconds_[i] == 0) {
      continue;
    }
    const double byte_rate = day_bytes_[i] / day_coverage_seconds_[i];
    const double operation_rate = day_operations_[i] / day_coverage_seconds_[i];
    if (known_buckets_[i]) {
      byte_rate_ewma_[i] = kDailyEwmaWeight * byte_rate +
                           (1.0 - kDailyEwmaWeight) * byte_rate_ewma_[i];
      operation_rate_ewma_[i] =
          kDailyEwmaWeight * operation_rate +
          (1.0 - kDailyEwmaWeight) * operation_rate_ewma_[i];
    } else {
      byte_rate_ewma_[i] = byte_rate;
      operation_rate_ewma_[i] = operation_rate;
      known_buckets_[i] = true;
    }
  }
  ++trained_days_;
  last_update_time_ = current_day_start_ + kSecondsPerDay;
  RecomputeWindow(percentile);
  return true;
}

void DynamicOffpeakModel::RecomputeWindow(uint32_t percentile) {
  std::array<double, kBucketsPerDay> byte_smoothed{};
  std::array<double, kBucketsPerDay> operation_smoothed{};
  std::vector<uint32_t> known;
  constexpr uint32_t kSmoothingBuckets = kSmoothingMinutes / kBucketMinutes;
  for (uint32_t i = 0; i < kBucketsPerDay; ++i) {
    if (!known_buckets_[i]) {
      continue;
    }
    uint32_t count = 0;
    for (uint32_t offset = 0; offset < kSmoothingBuckets; ++offset) {
      const uint32_t bucket = (i + kBucketsPerDay - offset) % kBucketsPerDay;
      if (known_buckets_[bucket]) {
        byte_smoothed[i] += byte_rate_ewma_[bucket];
        operation_smoothed[i] += operation_rate_ewma_[bucket];
        ++count;
      }
    }
    byte_smoothed[i] /= count;
    operation_smoothed[i] /= count;
    known.push_back(i);
  }
  if (known.empty()) {
    learned_window_.clear();
    return;
  }

  std::array<double, kBucketsPerDay> scores{};
  for (uint32_t bucket : known) {
    uint32_t lower_bytes = 0;
    uint32_t lower_operations = 0;
    for (uint32_t other : known) {
      lower_bytes += byte_smoothed[other] < byte_smoothed[bucket];
      lower_operations +=
          operation_smoothed[other] < operation_smoothed[bucket];
    }
    const double denominator =
        known.size() > 1 ? static_cast<double>(known.size() - 1) : 1.0;
    scores[bucket] =
        (lower_bytes / denominator + lower_operations / denominator) / 2.0;
  }

  std::vector<double> sorted_scores;
  sorted_scores.reserve(known.size());
  for (uint32_t bucket : known) {
    sorted_scores.push_back(scores[bucket]);
  }
  std::sort(sorted_scores.begin(), sorted_scores.end());
  const size_t threshold_index =
      std::max<size_t>(1, (sorted_scores.size() * percentile + 99) / 100) - 1;
  const double threshold = sorted_scores[threshold_index];

  std::array<bool, kBucketsPerDay> eligible{};
  for (uint32_t bucket : known) {
    eligible[bucket] = scores[bucket] <= threshold;
  }

  uint32_t best_start = 0;
  uint32_t best_length = 0;
  double best_average = std::numeric_limits<double>::infinity();
  for (uint32_t start = 0; start < kBucketsPerDay; ++start) {
    if (!eligible[start] ||
        eligible[(start + kBucketsPerDay - 1) % kBucketsPerDay]) {
      continue;
    }
    uint32_t length = 0;
    double total = 0;
    while (length < kBucketsPerDay &&
           eligible[(start + length) % kBucketsPerDay]) {
      total += scores[(start + length) % kBucketsPerDay];
      ++length;
    }
    const double average = total / length;
    if (length > best_length ||
        (length == best_length && average < best_average) ||
        (length == best_length && average == best_average &&
         start < best_start)) {
      best_start = start;
      best_length = length;
      best_average = average;
    }
  }
  if (best_length == 0 && std::all_of(eligible.begin(), eligible.end(),
                                      [](bool value) { return value; })) {
    best_length = kBucketsPerDay;
  }
  learned_window_ =
      best_length == 0 ? "" : FormatWindow(best_start, best_length);
}

bool DynamicOffpeakModel::IsFresh(uint64_t now) const {
  return IsTrained() && now >= last_update_time_ &&
         now - last_update_time_ <= kMaxModelAgeSeconds;
}

DynamicOffpeakPrediction DynamicOffpeakModel::GetPrediction(
    uint64_t current_time) const {
  const uint32_t bucket =
      static_cast<uint32_t>(current_time % kSecondsPerDay / kBucketSeconds);
  if (!known_buckets_[bucket]) {
    return {};
  }
  return {true, bucket * kBucketMinutes, byte_rate_ewma_[bucket],
          operation_rate_ewma_[bucket]};
}

std::string DynamicOffpeakModel::Encode() const {
  std::string encoded;
  PutVarint32(&encoded, kModelEncodingVersion);
  PutVarint32(&encoded, trained_days_);
  PutVarint32(&encoded, last_day_coverage_percent_);
  PutVarint64(&encoded, last_update_time_);
  PutLengthPrefixedSlice(&encoded, learned_window_);
  for (uint32_t i = 0; i < kBucketsPerDay; ++i) {
    encoded.push_back(known_buckets_[i] ? 1 : 0);
    PutFixed64(&encoded, EncodeDouble(byte_rate_ewma_[i]));
    PutFixed64(&encoded, EncodeDouble(operation_rate_ewma_[i]));
  }
  return encoded;
}

Status DynamicOffpeakModel::Decode(const Slice& encoded, uint32_t percentile) {
  Slice input = encoded;
  uint32_t version = 0;
  uint32_t trained_days = 0;
  uint32_t coverage = 0;
  uint64_t last_update = 0;
  Slice learned_window;
  if (!GetVarint32(&input, &version) || version != kModelEncodingVersion ||
      !GetVarint32(&input, &trained_days) || !GetVarint32(&input, &coverage) ||
      !GetVarint64(&input, &last_update) ||
      !GetLengthPrefixedSlice(&input, &learned_window)) {
    return Status::Corruption("dynamic offpeak model header");
  }
  std::array<bool, kBucketsPerDay> known{};
  std::array<double, kBucketsPerDay> byte_rates{};
  std::array<double, kBucketsPerDay> operation_rates{};
  for (uint32_t i = 0; i < kBucketsPerDay; ++i) {
    uint64_t bytes = 0;
    uint64_t operations = 0;
    if (input.empty()) {
      return Status::Corruption("dynamic offpeak model buckets");
    }
    known[i] = input[0] != 0;
    input.remove_prefix(1);
    if (!GetFixed64(&input, &bytes) || !GetFixed64(&input, &operations)) {
      return Status::Corruption("dynamic offpeak model rates");
    }
    byte_rates[i] = DecodeDouble(bytes);
    operation_rates[i] = DecodeDouble(operations);
    if (!std::isfinite(byte_rates[i]) || !std::isfinite(operation_rates[i])) {
      return Status::Corruption("dynamic offpeak model non-finite rate");
    }
  }
  if (!input.empty() || coverage > 100) {
    return Status::Corruption("dynamic offpeak model trailing data");
  }
  known_buckets_ = known;
  byte_rate_ewma_ = byte_rates;
  operation_rate_ewma_ = operation_rates;
  trained_days_ = trained_days;
  last_day_coverage_percent_ = coverage;
  last_update_time_ = last_update;
  learned_window_ = learned_window.ToString();
  if (percentile > 0) {
    RecomputeWindow(percentile);
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
