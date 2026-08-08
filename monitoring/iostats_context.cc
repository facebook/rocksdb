// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <sstream>

#include "monitoring/iostats_context_imp.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

#ifdef NIOSTATS_CONTEXT
// Should not be used because the counters are not thread-safe.
// Put here just to make get_iostats_context() simple without ifdef.
static IOStatsContext iostats_context;
#else
thread_local IOStatsContext iostats_context;
#endif

IOStatsContext* get_iostats_context() { return &iostats_context; }

void FileIOByTemperature::Merge(const FileIOByTemperature& other) {
#ifndef NIOSTATS_CONTEXT
  hot_file_bytes_read += other.hot_file_bytes_read;
  warm_file_bytes_read += other.warm_file_bytes_read;
  cool_file_bytes_read += other.cool_file_bytes_read;
  cold_file_bytes_read += other.cold_file_bytes_read;
  ice_file_bytes_read += other.ice_file_bytes_read;
  unknown_non_last_level_bytes_read += other.unknown_non_last_level_bytes_read;
  unknown_last_level_bytes_read += other.unknown_last_level_bytes_read;
  hot_file_read_count += other.hot_file_read_count;
  warm_file_read_count += other.warm_file_read_count;
  cool_file_read_count += other.cool_file_read_count;
  cold_file_read_count += other.cold_file_read_count;
  ice_file_read_count += other.ice_file_read_count;
  unknown_non_last_level_read_count += other.unknown_non_last_level_read_count;
  unknown_last_level_read_count += other.unknown_last_level_read_count;
#else
  (void)other;
#endif
}

void IOStatsContext::Reset() {
#ifndef NIOSTATS_CONTEXT
  thread_pool_id = Env::Priority::TOTAL;
  bytes_read = 0;
  bytes_written = 0;
  open_nanos = 0;
  allocate_nanos = 0;
  write_nanos = 0;
  read_nanos = 0;
  range_sync_nanos = 0;
  prepare_write_nanos = 0;
  fsync_nanos = 0;
  logger_nanos = 0;
  cpu_write_nanos = 0;
  cpu_read_nanos = 0;
  file_io_stats_by_temperature.Reset();
#endif  //! NIOSTATS_CONTEXT
}

void IOStatsContext::Merge(const IOStatsContext& other) {
#ifndef NIOSTATS_CONTEXT
  bytes_read += other.bytes_read;
  bytes_written += other.bytes_written;
  open_nanos += other.open_nanos;
  allocate_nanos += other.allocate_nanos;
  write_nanos += other.write_nanos;
  read_nanos += other.read_nanos;
  range_sync_nanos += other.range_sync_nanos;
  prepare_write_nanos += other.prepare_write_nanos;
  fsync_nanos += other.fsync_nanos;
  logger_nanos += other.logger_nanos;
  cpu_write_nanos += other.cpu_write_nanos;
  cpu_read_nanos += other.cpu_read_nanos;
  file_io_stats_by_temperature.Merge(other.file_io_stats_by_temperature);
#else
  (void)other;
#endif
}

#define IOSTATS_CONTEXT_OUTPUT(counter)         \
  if (!exclude_zero_counters || counter > 0) {  \
    ss << #counter << " = " << counter << ", "; \
  }

std::string IOStatsContext::ToString(bool exclude_zero_counters) const {
#ifdef NIOSTATS_CONTEXT
  (void)exclude_zero_counters;
  return "";
#else
  std::ostringstream ss;
  IOSTATS_CONTEXT_OUTPUT(thread_pool_id);
  IOSTATS_CONTEXT_OUTPUT(bytes_read);
  IOSTATS_CONTEXT_OUTPUT(bytes_written);
  IOSTATS_CONTEXT_OUTPUT(open_nanos);
  IOSTATS_CONTEXT_OUTPUT(allocate_nanos);
  IOSTATS_CONTEXT_OUTPUT(write_nanos);
  IOSTATS_CONTEXT_OUTPUT(read_nanos);
  IOSTATS_CONTEXT_OUTPUT(range_sync_nanos);
  IOSTATS_CONTEXT_OUTPUT(fsync_nanos);
  IOSTATS_CONTEXT_OUTPUT(prepare_write_nanos);
  IOSTATS_CONTEXT_OUTPUT(logger_nanos);
  IOSTATS_CONTEXT_OUTPUT(cpu_write_nanos);
  IOSTATS_CONTEXT_OUTPUT(cpu_read_nanos);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.hot_file_bytes_read);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.warm_file_bytes_read);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.cool_file_bytes_read);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.cold_file_bytes_read);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.hot_file_read_count);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.warm_file_read_count);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.cool_file_read_count);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.cold_file_read_count);
  std::string str = ss.str();
  str.erase(str.find_last_not_of(", ") + 1);
  return str;
#endif  //! NIOSTATS_CONTEXT
}

}  // namespace ROCKSDB_NAMESPACE
