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
#elif defined(ROCKSDB_SUPPORT_THREAD_LOCAL)
__thread IOStatsContext iostats_context;
#else
#error \
    "No thread-local support. Disable iostats context with -DNIOSTATS_CONTEXT."
#endif

IOStatsContext* get_iostats_context() {
  return &iostats_context;
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
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.cold_file_bytes_read);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.hot_file_read_count);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.warm_file_read_count);
  IOSTATS_CONTEXT_OUTPUT(file_io_stats_by_temperature.cold_file_read_count);
  std::string str = ss.str();
  str.erase(str.find_last_not_of(", ") + 1);
  return str;
#endif  //! NIOSTATS_CONTEXT
}

}  // namespace ROCKSDB_NAMESPACE
