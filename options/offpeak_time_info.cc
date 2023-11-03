//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/offpeak_time_info.h"

#include "rocksdb/system_clock.h"
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

}  // namespace ROCKSDB_NAMESPACE
