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

class MultiTenantRateLimiter : public RateLimiter {
 public:
  MultiTenantRateLimiter(
    int num_clients, std::vector<int64_t> writes_bytes_per_sec, 
    std::vector<int64_t> read_bytes_per_sec, int64_t refill_period_us,
    int32_t fairness, RateLimiter::Mode mode, const std::shared_ptr<SystemClock>& clock, 
    int64_t single_burst_bytes);

  virtual ~MultiTenantRateLimiter();

  // Permits dynamically change rate limiter's bytes per second.
  void SetBytesPerSecond(int64_t bytes_per_second) override;
  void SetBytesPerSecond(int client_id, int64_t bytes_per_second);

  Status SetSingleBurstBytes(int64_t single_burst_bytes) override;

  // Request for token to write bytes. If this request can not be satisfied,
  // the call is blocked. Caller is responsible to make sure 
  // bytes <= GetSingleBurstBytes(). Negative bytes passed in will be
  // rounded up to 0.
  using RateLimiter::Request;
  void Request(const int64_t bytes, const Env::IOPriority pri,
               Statistics* stats) override;
  void Request(const int64_t bytes, const Env::IOPriority pri,
                       Statistics* stats, OpType op_type) override;
              
  Status GetTotalPendingRequests(
      int64_t* total_pending_requests,
      const Env::IOPriority pri = Env::IO_TOTAL) const override;

  // TODO(tgriggs): make this per-tenant
  int64_t GetSingleBurstBytes() const override {
    int client_id = 1;
    return GetSingleBurstBytes(client_id);
  }

  int64_t GetSingleBurstBytes(int client_id) const; 

  int64_t GetTotalBytesThrough(
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

  int64_t GetTotalRequests(
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

  // TODO(tgriggs): update this based on column family IDs
  int ClientId2ClientIdx(int client_id) const {
    if (client_id > 0) {
      return client_id - 1;
    }
    return client_id;
  }

  int ClientIdx2ClientId(int client_idx) const {
    if (client_idx >= 0) {
      return client_idx + 1;
    }
    return client_idx;
  }

  // TODO(tgriggs): Make this per-client? Maybe thread-level storage?
  // Only used in [rocksdb/db/db_impl/db_impl_open.cc] to sanitize options
  int64_t GetBytesPerSecond() const override {
    int client_id = 0;
    return GetBytesPerSecond(client_id);
  }

  int64_t GetBytesPerSecond(int client_id) const {
    int client_idx = ClientId2ClientIdx(client_id);
    return rate_bytes_per_sec_[client_idx].load(std::memory_order_relaxed);
  }

  RateLimiter* GetReadRateLimiter() {
    return read_rate_limiter_;
  }

  virtual void TEST_SetClock(std::shared_ptr<SystemClock> clock) {
    MutexLock g(&request_mutex_);
    clock_ = std::move(clock);
    next_refill_us_ = NowMicrosMonotonicLocked();
  }

 private:
  static constexpr int kMicrosecondsPerSecond = 1000000;
  void RefillBytesAndGrantRequestsLocked();
  std::vector<Env::IOPriority> GeneratePriorityIterationOrderLocked();
  int64_t CalculateRefillBytesPerPeriodLocked(int64_t rate_bytes_per_sec);
  Status TuneLocked();
  void SetBytesPerSecondLocked(int64_t bytes_per_second);
  void SetBytesPerSecondLocked(int client_id, int64_t bytes_per_second);

  void TGprintStackTrace();

  uint64_t NowMicrosMonotonicLocked() {
    return clock_->NowNanos() / std::milli::den;
  }

  // This mutex guard all internal states
  mutable port::Mutex request_mutex_;

  const int64_t refill_period_us_;

  // This value is validated but unsanitized (may be zero).
  std::atomic<int64_t> raw_single_burst_bytes_;
  std::shared_ptr<SystemClock> clock_;

  bool stop_;
  port::CondVar exit_cv_;
  int32_t requests_to_wait_;

  int64_t total_requests_[Env::IO_TOTAL];
  int64_t total_bytes_through_[Env::IO_TOTAL];
  // int64_t available_bytes_;
  int64_t next_refill_us_;

  int32_t fairness_;
  Random rnd_;

  struct Req;
  struct ReqKey;

  bool wait_until_refill_pending_;

  // Multi-tenant extensions
  int num_clients_;
  std::vector<std::atomic<int64_t>> rate_bytes_per_sec_;
  std::vector<std::atomic<int64_t>> refill_bytes_per_period_;
  std::vector<int64_t> available_bytes_;
  std::map<ReqKey, std::deque<Req*>> request_queue_map_;
  std::vector<int64_t> calls_per_client_;
  int total_calls_;
  RateLimiter* read_rate_limiter_ = nullptr;
  int64_t read_rate_bytes_per_sec_;
};

}  // namespace ROCKSDB_NAMESPACE
