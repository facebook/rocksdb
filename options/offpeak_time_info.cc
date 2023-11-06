//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/offpeak_time_info.h"

#include "rocksdb/system_clock.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
OffpeakTimeInfo::OffpeakTimeInfo() : daily_offpeak_time_utc("") {}
OffpeakTimeInfo::OffpeakTimeInfo(const std::string& offpeak_time)
    : daily_offpeak_time_utc(offpeak_time) {}

bool OffpeakTimeInfo::IsNowOffpeak(SystemClock* clock) const {
  if (daily_offpeak_time_utc.empty()) {
    return false;
  }
  int64_t now;
  if (clock->GetCurrentTime(&now).ok()) {
    constexpr int kSecondsPerDay = 86400;
    constexpr int kSecondsPerMinute = 60;
    int seconds_since_midnight_to_nearest_minute =
        (static_cast<int>(now % kSecondsPerDay) / kSecondsPerMinute) *
        kSecondsPerMinute;
    int start_time = 0, end_time = 0;
    bool success =
        TryParseTimeRangeString(daily_offpeak_time_utc, start_time, end_time);
    assert(success);
    assert(start_time != end_time);
    if (!success) {
      // If the validation was done properly, we should never reach here
      return false;
    }
    // if the offpeak duration spans overnight (i.e. 23:30 - 4:30 next day)
    if (start_time > end_time) {
      return start_time <= seconds_since_midnight_to_nearest_minute ||
             seconds_since_midnight_to_nearest_minute <= end_time;
    } else {
      return start_time <= seconds_since_midnight_to_nearest_minute &&
             seconds_since_midnight_to_nearest_minute <= end_time;
    }
  }
  return false;
}

}  // namespace ROCKSDB_NAMESPACE
