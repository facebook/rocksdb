//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class GenericRateLimiter : public RateLimiter {
 public:
  GenericRateLimiter(int64_t refill_bytes, int64_t refill_period_us,
                     int32_t fairness, RateLimiter::Mode mode,
                     const std::shared_ptr<SystemClock>& clock,
                     bool auto_tuned);

  virtual ~GenericRateLimiter();

  // This API allows user to dynamically change rate limiter's bytes per second.
  virtual void SetBytesPerSecond(int64_t bytes_per_second) override;

  // Request for token to write bytes. If this request can not be satisfied,
  // the call is blocked. Caller is responsible to make sure
  // bytes <= GetSingleBurstBytes() and bytes >= 0. Negative bytes
  // passed in will be rounded up to 0.
  using RateLimiter::Request;
  virtual void Request(const int64_t bytes, const Env::IOPriority pri,
                       Statistics* stats) override;

  virtual int64_t GetSingleBurstBytes() const override {
    return refill_bytes_per_period_.load(std::memory_order_relaxed);
  }

  virtual int64_t GetTotalBytesThrough(
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    MutexLock g(&request_mutex_);
    if (pri == Env::IO_TOTAL) {
      int64_t total_bytes_through_sum = 0;
      for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
        total_bytes_through_sum += total_bytes_through_[i];
      }
      return total_bytes_through_sum;
    }
    return total_bytes_through_[pri];
  }

  virtual int64_t GetTotalRequests(
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    MutexLock g(&request_mutex_);
    if (pri == Env::IO_TOTAL) {
      int64_t total_requests_sum = 0;
      for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
        total_requests_sum += total_requests_[i];
      }
      return total_requests_sum;
    }
    return total_requests_[pri];
  }

  virtual Status GetTotalPendingRequests(
      int64_t* total_pending_requests,
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    assert(total_pending_requests != nullptr);
    MutexLock g(&request_mutex_);
    if (pri == Env::IO_TOTAL) {
      int64_t total_pending_requests_sum = 0;
      for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
        total_pending_requests_sum += static_cast<int64_t>(queue_[i].size());
      }
      *total_pending_requests = total_pending_requests_sum;
    } else {
      *total_pending_requests = static_cast<int64_t>(queue_[pri].size());
    }
    return Status::OK();
  }

  virtual int64_t GetBytesPerSecond() const override {
    return rate_bytes_per_sec_.load(std::memory_order_relaxed);
  }

  virtual void TEST_SetClock(std::shared_ptr<SystemClock> clock) {
    MutexLock g(&request_mutex_);
    clock_ = std::move(clock);
    next_refill_us_ = NowMicrosMonotonicLocked();
  }

 private:
  void RefillBytesAndGrantRequestsLocked();
  std::vector<Env::IOPriority> GeneratePriorityIterationOrderLocked();
  int64_t CalculateRefillBytesPerPeriodLocked(int64_t rate_bytes_per_sec);
  Status TuneLocked();
  void SetBytesPerSecondLocked(int64_t bytes_per_second);

  uint64_t NowMicrosMonotonicLocked() {
    return clock_->NowNanos() / std::milli::den;
  }

  // This mutex guard all internal states
  mutable port::Mutex request_mutex_;

  const int64_t refill_period_us_;

  std::atomic<int64_t> rate_bytes_per_sec_;
  std::atomic<int64_t> refill_bytes_per_period_;
  std::shared_ptr<SystemClock> clock_;

  bool stop_;
  port::CondVar exit_cv_;
  int32_t requests_to_wait_;

  int64_t total_requests_[Env::IO_TOTAL];
  int64_t total_bytes_through_[Env::IO_TOTAL];
  int64_t available_bytes_;
  int64_t next_refill_us_;

  int32_t fairness_;
  Random rnd_;

  struct Req;
  std::deque<Req*> queue_[Env::IO_TOTAL];
  bool wait_until_refill_pending_;

  bool auto_tuned_;
  int64_t num_drains_;
  const int64_t max_bytes_per_sec_;
  std::chrono::microseconds tuned_time_;
};

}  // namespace ROCKSDB_NAMESPACE
