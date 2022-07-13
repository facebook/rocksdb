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
  struct GenericRateLimiterOptions {
    static const char* kName() { return "GenericRateLimiterOptions"; }
    GenericRateLimiterOptions(int64_t _rate_bytes_per_sec,
                              int64_t _refill_period_us, int32_t _fairness,
                              const std::shared_ptr<SystemClock>& _clock,
                              bool _auto_tuned)
        : max_bytes_per_sec(_rate_bytes_per_sec),
          refill_period_us(_refill_period_us),
          clock(_clock),
          fairness(_fairness > 100 ? 100 : _fairness),
          auto_tuned(_auto_tuned) {}
    int64_t max_bytes_per_sec;
    int64_t refill_period_us;
    std::shared_ptr<SystemClock> clock;
    int32_t fairness;
    bool auto_tuned;
  };

 public:
  explicit GenericRateLimiter(
      int64_t refill_bytes, int64_t refill_period_us = 100 * 1000,
      int32_t fairness = 10,
      RateLimiter::Mode mode = RateLimiter::Mode::kWritesOnly,
      const std::shared_ptr<SystemClock>& clock = nullptr,
      bool auto_tuned = false);

  virtual ~GenericRateLimiter();

  static const char* kClassName() { return "GenericRateLimiter"; }
  const char* Name() const override { return kClassName(); }
  Status PrepareOptions(const ConfigOptions& options) override;

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
    return rate_bytes_per_sec_;
  }

 private:
  void Initialize();
  void RefillBytesAndGrantRequests();
  std::vector<Env::IOPriority> GeneratePriorityIterationOrder();
  int64_t CalculateRefillBytesPerPeriod(int64_t rate_bytes_per_sec);
  Status Tune();

  uint64_t NowMicrosMonotonic() {
    return options_.clock->NowNanos() / std::milli::den;
  }

  // This mutex guard all internal states
  mutable port::Mutex request_mutex_;

  GenericRateLimiterOptions options_;

  int64_t rate_bytes_per_sec_;
  // This variable can be changed dynamically.
  std::atomic<int64_t> refill_bytes_per_period_;

  bool stop_;
  port::CondVar exit_cv_;
  int32_t requests_to_wait_;

  int64_t total_requests_[Env::IO_TOTAL];
  int64_t total_bytes_through_[Env::IO_TOTAL];
  int64_t available_bytes_;
  int64_t next_refill_us_;

  Random rnd_;

  struct Req;
  std::deque<Req*> queue_[Env::IO_TOTAL];
  bool wait_until_refill_pending_;

  int64_t num_drains_;
  std::chrono::microseconds tuned_time_;
};

}  // namespace ROCKSDB_NAMESPACE
