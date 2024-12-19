// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <stdint.h>

#include <string>

#include "rocksdb/perf_level.h"

/*
 * NOTE:
 * If you plan to add new metrics, please read documentation in perf_level.h and
 * try to come up with a metric name that follows the naming conventions
 * mentioned there. It helps to indicate the metric's starting enabling P
 * erfLevel. Document this starting PerfLevel if the metric name cannot meet the
 * naming conventions.
 */

// A thread local context for gathering io-stats efficiently and transparently.
// Use SetPerfLevel(PerfLevel::kEnableTime) to enable time stats.
//

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL: the IO statistics for tiered storage. It matches with each
// item in Temperature class.
struct FileIOByTemperature {
  // the number of bytes read to Temperature::kHot file
  uint64_t hot_file_bytes_read;
  // the number of bytes read to Temperature::kWarm file
  uint64_t warm_file_bytes_read;
  // the number of bytes read to Temperature::kCold file
  uint64_t cold_file_bytes_read;
  // total number of reads to Temperature::kHot file
  uint64_t hot_file_read_count;
  // total number of reads to Temperature::kWarm file
  uint64_t warm_file_read_count;
  // total number of reads to Temperature::kCold file
  uint64_t cold_file_read_count;
  // reset all the statistics to 0.
  void Reset() {
    hot_file_bytes_read = 0;
    warm_file_bytes_read = 0;
    cold_file_bytes_read = 0;
    hot_file_read_count = 0;
    warm_file_read_count = 0;
    cold_file_read_count = 0;
  }
};

struct IOStatsContext {
  // reset all io-stats counter to zero
  void Reset();

  std::string ToString(bool exclude_zero_counters = false) const;

  // the thread pool id
  uint64_t thread_pool_id;

  // number of bytes that has been written.
  uint64_t bytes_written;
  // number of bytes that has been read.
  uint64_t bytes_read;

  // time spent in open() and fopen().
  uint64_t open_nanos;
  // time spent in fallocate().
  uint64_t allocate_nanos;
  // time spent in write() and pwrite().
  uint64_t write_nanos;
  // time spent in read() and pread()
  uint64_t read_nanos;
  // time spent in sync_file_range().
  uint64_t range_sync_nanos;
  // time spent in fsync
  uint64_t fsync_nanos;
  // time spent in preparing write (fallocate etc).
  uint64_t prepare_write_nanos;
  // time spent in Logger::Logv().
  uint64_t logger_nanos;
  // CPU time spent in write() and pwrite()
  uint64_t cpu_write_nanos;
  // CPU time spent in read() and pread()
  uint64_t cpu_read_nanos;

  FileIOByTemperature file_io_stats_by_temperature;

  // It is not consistent that whether iostats follows PerfLevel.Timer counters
  // follows it but BackupEngine relies on counter metrics to always be there.
  // Here we create a backdoor option to disable some counters, so that some
  // existing stats are not polluted by file operations, such as logging, by
  // turning this off.
  bool disable_iostats = false;
};

// If RocksDB is compiled with -DNIOSTATS_CONTEXT, then a pointer to a global,
// non-thread-local IOStatsContext object will be returned. Attempts to update
// this object will be ignored, and reading from it will also be no-op.
// Otherwise, a pointer to a thread-local IOStatsContext object will be
// returned.
//
// This function never returns nullptr.
IOStatsContext* get_iostats_context();

}  // namespace ROCKSDB_NAMESPACE
