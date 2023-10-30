//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;

struct OffpeakTimeInfo {
  bool is_now_offpeak = false;
  int seconds_till_next_offpeak_start = 0;
};

struct OffpeakTimeOption {
  static constexpr int kSecondsPerDay = 86400;
  static constexpr int kSecondsPerMinute = 60;

  OffpeakTimeOption();
  explicit OffpeakTimeOption(const std::string& offpeak_time);
  std::string daily_offpeak_time_utc;

  OffpeakTimeInfo GetOffpeakTimeInfo(SystemClock* clock) const;
};

}  // namespace ROCKSDB_NAMESPACE
