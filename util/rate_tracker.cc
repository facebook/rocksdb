//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/rate_tracker.h"

#include <stdio.h>
#include <stdlib.h>

namespace ROCKSDB_NAMESPACE {

void read_cpu_stats(proc_cpu_stats* stats) {
#if defined(_WIN32)
  fprintf(stderr, "read_cpu_stats not implemented on Windows\n");
  exit(1);
#else
  // Unix/Linux implementation - read from /proc/stat
  FILE* file = fopen("/proc/stat", "r");
  if (!file) {
    perror("Error opening /proc/stat");
    exit(1);
  }

  char line[256];

  if (fgets(line, sizeof(line), file) == nullptr) {
    perror("Error reading /proc/stat");
    exit(1);
  }

  // Parse the first line, which contains overall CPU statistics
  sscanf(line, "cpu %lu %lu %lu %lu %lu %lu %lu", &stats->user, &stats->nice,
         &stats->system, &stats->idle, &stats->iowait, &stats->irq,
         &stats->softirq);
  fclose(file);
#endif
}

CPUIOUtilizationTracker::CPUIOUtilizationTracker(
    const std::shared_ptr<RateLimiter>& rate_limiter,
    const std::shared_ptr<SystemClock>& clock)
    : rate_limiter_(rate_limiter),
      clock_(clock ? clock : SystemClock::Default()),
      io_utilization_(0.0),
      cpu_usage_(0.0) {
  RecordCPUUsage();
  RecordIOUtilization();
}

bool CPUIOUtilizationTracker::Record() {
  RecordCPUUsage();
  RecordIOUtilization();
  return true;
}

float CPUIOUtilizationTracker::GetCpuUtilization() { return cpu_usage_; }

float CPUIOUtilizationTracker::GetIoUtilization() { return io_utilization_; }

void CPUIOUtilizationTracker::RecordCPUUsage() {
#if defined(_WIN32)
  // Windows implementation: not implemented
  fprintf(stderr, "RecordCPUUsage not implemented on Windows\n");
  exit(1);
#else
  // Unix/Linux implementation - use getrusage
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  double cpu_time_used =
      (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
      (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
  cpu_usage_ = cpu_usage_rate_.Record(cpu_time_used);
#endif
}

void CPUIOUtilizationTracker::RecordIOUtilization() {
  if (rate_limiter_ != nullptr) {
    io_utilization_ =
        rate_limiter_bytes_rate_.Record(rate_limiter_->GetTotalBytesThrough());
  }
}

uint64_t CPUIOUtilizationTracker::GetCurrentTimeMicros() {
  return clock_->NowMicros();
}

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

double ProcSysCPUUtilizationTracker::GetCpuUtilization() {
  return cpu_usage_rate_.GetRate();
}

}  // namespace ROCKSDB_NAMESPACE
