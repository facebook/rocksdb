//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/offpeak_time_info.h"

#include "rocksdb/system_clock.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
OffpeakTimeOption::OffpeakTimeOption() : daily_offpeak_time_utc("") {}
OffpeakTimeOption::OffpeakTimeOption(const std::string& offpeak_time)
    : daily_offpeak_time_utc(offpeak_time) {}

OffpeakTimeInfo OffpeakTimeOption::GetOffpeakTimeInfo(
    SystemClock* clock) const {
  OffpeakTimeInfo offpeak_time_info;
  if (daily_offpeak_time_utc.empty()) {
    return offpeak_time_info;
  }
  int64_t now;
  if (clock->GetCurrentTime(&now).ok()) {
    int seconds_since_midnight = static_cast<int>(now % kSecondsPerDay);
    int seconds_since_midnight_to_nearest_minute =
        (seconds_since_midnight / kSecondsPerMinute) * kSecondsPerMinute;
    int start_time = 0, end_time = 0;
    bool is_valid =
        TryParseTimeRangeString(daily_offpeak_time_utc, start_time, end_time) &&
        start_time != end_time;
    assert(is_valid);
    if (!is_valid) {
      // If the validation was done properly, we should never reach here
      return offpeak_time_info;
    }
    // if the offpeak duration spans overnight (i.e. 23:30 - 4:30 next day)
    if (start_time > end_time) {
      offpeak_time_info.is_now_offpeak =
          start_time <= seconds_since_midnight_to_nearest_minute ||
          seconds_since_midnight_to_nearest_minute <= end_time;
    } else {
      offpeak_time_info.is_now_offpeak =
          start_time <= seconds_since_midnight_to_nearest_minute &&
          seconds_since_midnight_to_nearest_minute <= end_time;
    }
    offpeak_time_info.seconds_till_next_offpeak_start =
        seconds_since_midnight < start_time
            ? start_time - seconds_since_midnight
            : ((start_time + kSecondsPerDay) - seconds_since_midnight);
  }
  return offpeak_time_info;
}

}  // namespace ROCKSDB_NAMESPACE
