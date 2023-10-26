//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "options/db_options.h"

namespace ROCKSDB_NAMESPACE {

class SystemClock;

struct OffpeakTimeInfo {
  OffpeakTimeInfo();
  explicit OffpeakTimeInfo(const std::string& offpeak_time);
  std::string daily_offpeak_time_utc;
  bool IsNowOffpeak(SystemClock* clock) const;
  void SetOffpeakTime(const std::string& offpeak_time) {
    daily_offpeak_time_utc = offpeak_time;
  }
};

}  // namespace ROCKSDB_NAMESPACE
