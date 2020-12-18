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
#include "util/mutexlock.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class WriteAmpBasedRateLimiter : public RateLimiter {
 public:
  WriteAmpBasedRateLimiter(int64_t refill_bytes, int64_t refill_period_us,
                           int32_t fairness, RateLimiter::Mode mode, Env* env,
                           bool auto_tuned);

  virtual ~WriteAmpBasedRateLimiter();

  // This API allows user to dynamically change rate limiter's bytes per second.
  // When auto-tuned is on, this sets rate limit's upper bound instead.
  virtual void SetBytesPerSecond(int64_t bytes_per_second) override;

  // Dynamically change rate limiter's auto_tuned mode.
  virtual void SetAutoTuned(bool auto_tuned) override;

  // Request for token to write bytes. If this request can not be satisfied,
  // the call is blocked. Caller is responsible to make sure
  // bytes <= GetSingleBurstBytes()
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
      return total_bytes_through_[Env::IO_LOW] +
             total_bytes_through_[Env::IO_HIGH];
    }
    return total_bytes_through_[pri];
  }

  virtual int64_t GetTotalRequests(
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    MutexLock g(&request_mutex_);
    if (pri == Env::IO_TOTAL) {
      return total_requests_[Env::IO_LOW] + total_requests_[Env::IO_HIGH];
    }
    return total_requests_[pri];
  }

  virtual int64_t GetBytesPerSecond() const override {
    return rate_bytes_per_sec_;
  }

  virtual bool GetAutoTuned() const override {
    return auto_tuned_.load(std::memory_order_acquire);
  }

  virtual void PaceUp(bool critical) override;

 private:
  void Refill();
  int64_t CalculateRefillBytesPerPeriod(int64_t rate_bytes_per_sec);
  void SetActualBytesPerSecond(int64_t bytes_per_second);
  Status Tune();

  uint64_t NowMicrosMonotonic(Env* env) {
    return env->NowNanos() / std::milli::den;
  }

  // This mutex guard all internal states
  mutable port::Mutex request_mutex_;

  const int64_t kMinRefillBytesPerPeriod = 100;

  const int64_t refill_period_us_;

  int64_t rate_bytes_per_sec_;
  // This variable can be changed dynamically.
  std::atomic<int64_t> refill_bytes_per_period_;
  Env* const env_;

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
  Req* leader_;
  std::deque<Req*> queue_[Env::IO_TOTAL];

  // only used to synchronize auto_tuned setters
  port::Mutex auto_tuned_mutex_;

  std::atomic<bool> auto_tuned_;
  std::atomic<int64_t> max_bytes_per_sec_;
  std::chrono::microseconds tuned_time_;
  int64_t duration_highpri_bytes_through_;
  int64_t duration_bytes_through_;

  template <size_t kWindowSize, size_t kRecentWindowSize = 1>
  class WindowSmoother {
   public:
    WindowSmoother() {
      static_assert(kWindowSize >= kRecentWindowSize,
                    "Expect recent window no larger than full window");
      static_assert(kRecentWindowSize >= 1, "Expect window size larger than 0");
      memset(data_, 0, sizeof(int64_t) * kWindowSize);
    }
    void AddSample(int64_t v) {
      auto recent_cursor =
          (cursor_ + 1 + kWindowSize - kRecentWindowSize) % kWindowSize;
      cursor_ = (cursor_ + 1) % kWindowSize;
      full_sum_ += v - data_[cursor_];
      recent_sum_ += v - data_[recent_cursor];
      data_[cursor_] = v;
    }
    int64_t GetFullValue() { return full_sum_ / kWindowSize; }
    int64_t GetRecentValue() { return recent_sum_ / kRecentWindowSize; }
    bool AtTimePoint() const { return cursor_ == 0; }

   private:
    uint32_t cursor_{0};  // point to the most recent sample
    int64_t data_[kWindowSize];
    int64_t full_sum_{0};
    int64_t recent_sum_{0};
  };

  static constexpr size_t kSmoothWindowSize = 300;       // 300 * 1s = 5m
  static constexpr size_t kRecentSmoothWindowSize = 30;  // 30 * 1s = 30s

  WindowSmoother<kSmoothWindowSize, kRecentSmoothWindowSize> bytes_sampler_;
  WindowSmoother<kSmoothWindowSize, kRecentSmoothWindowSize>
      highpri_bytes_sampler_;
  WindowSmoother<kRecentSmoothWindowSize, kRecentSmoothWindowSize>
      limit_bytes_sampler_;
  std::atomic<bool> critical_pace_up_;
  std::atomic<bool> normal_pace_up_;
  uint32_t percent_delta_;
};

}  // namespace ROCKSDB_NAMESPACE
