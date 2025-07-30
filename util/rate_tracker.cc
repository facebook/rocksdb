//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "util/rate_tracker.h"

namespace ROCKSDB_NAMESPACE {

CPUIOUtilizationTracker::CPUIOUtilizationTracker(
    const std::shared_ptr<RateLimiter>& rate_limiter,
    const std::shared_ptr<SystemClock>& clock)
    : rate_limiter_(rate_limiter),
      rate_limiter_bytes_rate_(clock),
      cpu_usage_rate_(clock) {
  Record();
}

void CPUIOUtilizationTracker::Record() {
  std::lock_guard<std::mutex> lock(mutex_);
  RecordCPUUsage();
  RecordIOUtilization();
}

double CPUIOUtilizationTracker::GetCpuUtilization() {
  return cpu_usage_rate_.GetRate();
}

double CPUIOUtilizationTracker::GetIoUtilization() {
  return rate_limiter_bytes_rate_.GetRate();
}
std::pair<double, double> CPUIOUtilizationTracker::GetUtilization() {
  return {GetIoUtilization(), GetCpuUtilization()};
}

void CPUIOUtilizationTracker::RecordCPUUsage() {
#if defined(_WIN32)
  // Windows implementation: not implemented
  return;
#else
  // Unix/Linux implementation - use getrusage.
  struct rusage usage {};
  getrusage(RUSAGE_SELF, &usage);
  double cpu_time_used =
      (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
      (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
  cpu_usage_rate_.Record(cpu_time_used);
#endif
}

void CPUIOUtilizationTracker::RecordIOUtilization() {
  if (rate_limiter_ != nullptr) {
    rate_limiter_bytes_rate_.Record(rate_limiter_->GetTotalBytesThrough());
  }
}

}  // namespace ROCKSDB_NAMESPACE
