//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/rate_tracker.h"

#include <stdio.h>
#include <stdlib.h>

namespace ROCKSDB_NAMESPACE {

// Implementation of read_cpu_stats moved from header to source file
void read_cpu_stats(proc_cpu_stats* stats) {
  FILE* file = fopen("/proc/stat", "r");
  if (!file) {
    perror("Error opening /proc/stat");
    exit(1);
  }

  char line[256];
  fgets(line, sizeof(line), file);

  // Parse the first line, which contains overall CPU statistics
  sscanf(line, "cpu %lu %lu %lu %lu %lu %lu %lu", &stats->user, &stats->nice,
         &stats->system, &stats->idle, &stats->iowait, &stats->irq,
         &stats->softirq);

  fclose(file);
}

// Implementation of CPUIOUtilizationTracker methods

CPUIOUtilizationTracker::CPUIOUtilizationTracker(
    const std::shared_ptr<SystemClock>& clock, size_t min_wait_us,
    DBOptions opt)
    : clock_(clock ? clock : SystemClock::Default()),
      min_wait_us_(min_wait_us),
      cpu_usage_(0.0),
      io_utilization_(0.0),
      next_record_time_us_(0),
      opt_(opt) {
  RecordCPUUsage();
  RecordIOUtilization();
}

bool CPUIOUtilizationTracker::Record() {
  uint64_t current_timestamp_us = GetCurrentTimeMicros();
  if (current_timestamp_us < next_record_time_us_) {
    return false;
  }
  next_record_time_us_ = current_timestamp_us + min_wait_us_;
  // Calculate time delta
  RecordCPUUsage();
  RecordIOUtilization();
  return true;
}

float CPUIOUtilizationTracker::GetCpuUtilization() { return cpu_usage_; }

float CPUIOUtilizationTracker::GetIoUtilization() { return io_utilization_; }

void CPUIOUtilizationTracker::RecordCPUUsage() {
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  double cpu_time_used =
      (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
      (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
  cpu_usage_ = cpu_usage_rate_.Record(cpu_time_used);
}

void CPUIOUtilizationTracker::RecordIOUtilization() {
  io_utilization_ = rate_limiter_bytes_rate_.Record(
      opt_.rate_limiter->GetTotalBytesThrough());
}

uint64_t CPUIOUtilizationTracker::GetCurrentTimeMicros() {
  return clock_->NowMicros();
}

// Implementation of ProcSysCPUUtilizationTracker methods

ProcSysCPUUtilizationTracker::ProcSysCPUUtilizationTracker() {}

bool ProcSysCPUUtilizationTracker::Record() {
  proc_cpu_stats current_stats;
  read_cpu_stats(&current_stats);
  auto total_time = current_stats.user + current_stats.nice +
                    current_stats.system + current_stats.idle +
                    current_stats.iowait + current_stats.irq +
                    current_stats.softirq;
  auto cpu_time = total_time - current_stats.idle;
  cpu_usage_rate_.Record(cpu_time, total_time);
  return true;
}

float ProcSysCPUUtilizationTracker::GetCpuUtilization() {
  return cpu_usage_rate_.GetRate();
}

}  // namespace ROCKSDB_NAMESPACE
