//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "util/rate_tracker.h"

#include <stdio.h>
#include <stdlib.h>

namespace ROCKSDB_NAMESPACE {

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

double CPUIOUtilizationTracker::GetCpuUtilization() { return cpu_usage_; }

double CPUIOUtilizationTracker::GetIoUtilization() { return io_utilization_; }

void CPUIOUtilizationTracker::RecordCPUUsage() {
#if defined(_WIN32)
  // Windows implementation: not implemented
  fprintf(stderr, "RecordCPUUsage not implemented on Windows\n");
  return;
#else
  // Unix/Linux implementation - use getrusage.
  struct rusage usage {};
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

RequestRateLimiter::RequestRateLimiter(size_t requests_per_second) {
  request_budget_ = std::make_unique<AutoRefillBudget<size_t>>(
      requests_per_second, kMicrosInSecond);
}

bool RequestRateLimiter::TryProcessRequest(size_t request_cost) {
  return request_budget_->TryConsume(request_cost);
}

size_t RequestRateLimiter::GetAvailableRequests() {
  return request_budget_->GetAvailableBudget();
}

void RequestRateLimiter::UpdateRateLimit(size_t new_requests_per_second) {
  request_budget_->SetRefillParameters(new_requests_per_second, 1000000);
}

void RequestRateLimiter::Reset() { request_budget_->Reset(); }

}  // namespace ROCKSDB_NAMESPACE
