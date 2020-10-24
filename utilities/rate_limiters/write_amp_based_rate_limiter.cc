//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "utilities/rate_limiters/write_amp_based_rate_limiter.h"

#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

// Pending request
struct WriteAmpBasedRateLimiter::Req {
  explicit Req(int64_t _bytes, port::Mutex* _mu)
      : request_bytes(_bytes), bytes(_bytes), cv(_mu), granted(false) {}
  int64_t request_bytes;
  int64_t bytes;
  port::CondVar cv;
  bool granted;
};

WriteAmpBasedRateLimiter::WriteAmpBasedRateLimiter(int64_t rate_bytes_per_sec,
                                                   int64_t refill_period_us,
                                                   int32_t fairness,
                                                   RateLimiter::Mode mode,
                                                   Env* env, bool auto_tuned)
    : RateLimiter(mode),
      refill_period_us_(refill_period_us),
      rate_bytes_per_sec_(auto_tuned ? rate_bytes_per_sec / 2
                                     : rate_bytes_per_sec),
      refill_bytes_per_period_(
          CalculateRefillBytesPerPeriod(rate_bytes_per_sec_)),
      env_(env),
      stop_(false),
      exit_cv_(&request_mutex_),
      requests_to_wait_(0),
      available_bytes_(0),
      next_refill_us_(NowMicrosMonotonic(env_)),
      fairness_(fairness > 100 ? 100 : fairness),
      rnd_((uint32_t)time(nullptr)),
      leader_(nullptr),
      auto_tuned_(auto_tuned),
      max_bytes_per_sec_(rate_bytes_per_sec),
      tuned_time_(NowMicrosMonotonic(env_)),
      duration_highpri_bytes_through_(0),
      duration_bytes_through_(0),
      ratio_delta_(0) {
  total_requests_[0] = 0;
  total_requests_[1] = 0;
  total_bytes_through_[0] = 0;
  total_bytes_through_[1] = 0;
}

WriteAmpBasedRateLimiter::~WriteAmpBasedRateLimiter() {
  MutexLock g(&request_mutex_);
  stop_ = true;
  requests_to_wait_ = static_cast<int32_t>(queue_[Env::IO_LOW].size() +
                                           queue_[Env::IO_HIGH].size());
  for (auto& r : queue_[Env::IO_HIGH]) {
    r->cv.Signal();
  }
  for (auto& r : queue_[Env::IO_LOW]) {
    r->cv.Signal();
  }
  while (requests_to_wait_ > 0) {
    exit_cv_.Wait();
  }
}

void WriteAmpBasedRateLimiter::SetBytesPerSecond(int64_t bytes_per_second) {
  assert(bytes_per_second > 0);
  if (auto_tuned_) {
    max_bytes_per_sec_.store(bytes_per_second, std::memory_order_relaxed);
  } else {
    SetActualBytesPerSecond(bytes_per_second);
  }
}

void WriteAmpBasedRateLimiter::SetActualBytesPerSecond(
    int64_t bytes_per_second) {
  rate_bytes_per_sec_ = bytes_per_second;
  refill_bytes_per_period_.store(
      CalculateRefillBytesPerPeriod(bytes_per_second),
      std::memory_order_relaxed);
}

void WriteAmpBasedRateLimiter::Request(int64_t bytes, const Env::IOPriority pri,
                                       Statistics* stats) {
  static constexpr int kSecondsPerTune = 1;
  static constexpr int kMicrosPerTune = 1000 * 1000 * kSecondsPerTune;
  TEST_SYNC_POINT("WriteAmpBasedRateLimiter::Request");
  TEST_SYNC_POINT_CALLBACK("WriteAmpBasedRateLimiter::Request:1",
                           &rate_bytes_per_sec_);
  if (auto_tuned_ && pri == Env::IO_HIGH &&
      duration_highpri_bytes_through_ + duration_bytes_through_ + bytes <=
          max_bytes_per_sec_.load(std::memory_order_relaxed) *
              kSecondsPerTune) {
    total_bytes_through_[Env::IO_HIGH] += bytes;
    ++total_requests_[Env::IO_HIGH];
    duration_highpri_bytes_through_ += bytes;
    return;
  }
  assert(bytes <= refill_bytes_per_period_.load(std::memory_order_relaxed));
  MutexLock g(&request_mutex_);

  if (auto_tuned_) {
    std::chrono::microseconds now(NowMicrosMonotonic(env_));
    if (now - tuned_time_ >= std::chrono::microseconds(kMicrosPerTune)) {
      Tune();
    }
  }

  if (stop_) {
    return;
  }

  ++total_requests_[pri];

  if (available_bytes_ >= bytes) {
    // Refill thread assigns quota and notifies requests waiting on
    // the queue under mutex. So if we get here, that means nobody
    // is waiting?
    available_bytes_ -= bytes;
    total_bytes_through_[pri] += bytes;
    duration_bytes_through_ += bytes;
    return;
  }

  // Request cannot be satisfied at this moment, enqueue
  Req r(bytes, &request_mutex_);
  queue_[pri].push_back(&r);

  do {
    bool timedout = false;
    // Leader election, candidates can be:
    // (1) a new incoming request,
    // (2) a previous leader, whose quota has not been not assigned yet due
    //     to lower priority
    // (3) a previous waiter at the front of queue, who got notified by
    //     previous leader
    if (leader_ == nullptr &&
        ((!queue_[Env::IO_HIGH].empty() &&
          &r == queue_[Env::IO_HIGH].front()) ||
         (!queue_[Env::IO_LOW].empty() && &r == queue_[Env::IO_LOW].front()))) {
      leader_ = &r;
      int64_t delta = next_refill_us_ - NowMicrosMonotonic(env_);
      delta = delta > 0 ? delta : 0;
      if (delta == 0) {
        timedout = true;
      } else {
        int64_t wait_until = env_->NowMicros() + delta;
        RecordTick(stats, NUMBER_RATE_LIMITER_DRAINS);
        timedout = r.cv.TimedWait(wait_until);
      }
    } else {
      // Not at the front of queue or an leader has already been elected
      r.cv.Wait();
    }

    // request_mutex_ is held from now on
    if (stop_) {
      --requests_to_wait_;
      exit_cv_.Signal();
      return;
    }

    // Make sure the waken up request is always the header of its queue
    assert(
        r.granted ||
        (!queue_[Env::IO_HIGH].empty() && &r == queue_[Env::IO_HIGH].front()) ||
        (!queue_[Env::IO_LOW].empty() && &r == queue_[Env::IO_LOW].front()));
    assert(leader_ == nullptr ||
           (!queue_[Env::IO_HIGH].empty() &&
            leader_ == queue_[Env::IO_HIGH].front()) ||
           (!queue_[Env::IO_LOW].empty() &&
            leader_ == queue_[Env::IO_LOW].front()));

    if (leader_ == &r) {
      // Waken up from TimedWait()
      if (timedout) {
        // Time to do refill!
        Refill();

        // Re-elect a new leader regardless. This is to simplify the
        // election handling.
        leader_ = nullptr;

        // Notify the header of queue if current leader is going away
        if (r.granted) {
          // Current leader already got granted with quota. Notify header
          // of waiting queue to participate next round of election.
          assert((queue_[Env::IO_HIGH].empty() ||
                  &r != queue_[Env::IO_HIGH].front()) &&
                 (queue_[Env::IO_LOW].empty() ||
                  &r != queue_[Env::IO_LOW].front()));
          if (!queue_[Env::IO_HIGH].empty()) {
            queue_[Env::IO_HIGH].front()->cv.Signal();
          } else if (!queue_[Env::IO_LOW].empty()) {
            queue_[Env::IO_LOW].front()->cv.Signal();
          }
          // Done
          break;
        }
      } else {
        // Spontaneous wake up, need to continue to wait
        assert(!r.granted);
        leader_ = nullptr;
      }
    } else {
      // Waken up by previous leader:
      // (1) if requested quota is granted, it is done.
      // (2) if requested quota is not granted, this means current thread
      // was picked as a new leader candidate (previous leader got quota).
      // It needs to participate leader election because a new request may
      // come in before this thread gets waken up. So it may actually need
      // to do Wait() again.
      assert(!timedout);
    }
  } while (!r.granted);
}

void WriteAmpBasedRateLimiter::Refill() {
  TEST_SYNC_POINT("WriteAmpBasedRateLimiter::Refill");
  next_refill_us_ = NowMicrosMonotonic(env_) + refill_period_us_;
  // Carry over the left over quota from the last period
  auto refill_bytes_per_period =
      refill_bytes_per_period_.load(std::memory_order_relaxed);
  available_bytes_ = refill_bytes_per_period;

  int use_low_pri_first = rnd_.OneIn(fairness_) ? 0 : 1;
  for (int q = 0; q < 2; ++q) {
    auto use_pri = (use_low_pri_first == q) ? Env::IO_LOW : Env::IO_HIGH;
    auto* queue = &queue_[use_pri];
    while (!queue->empty()) {
      auto* next_req = queue->front();
      if (available_bytes_ < next_req->request_bytes) {
        // avoid starvation
        next_req->request_bytes -= available_bytes_;
        available_bytes_ = 0;
        break;
      }
      available_bytes_ -= next_req->request_bytes;
      next_req->request_bytes = 0;
      total_bytes_through_[use_pri] += next_req->bytes;
      duration_bytes_through_ += next_req->bytes;
      queue->pop_front();

      next_req->granted = true;
      if (next_req != leader_) {
        // Quota granted, signal the thread
        next_req->cv.Signal();
      }
    }
  }
}

int64_t WriteAmpBasedRateLimiter::CalculateRefillBytesPerPeriod(
    int64_t rate_bytes_per_sec) {
  if (port::kMaxInt64 / rate_bytes_per_sec < refill_period_us_) {
    // Avoid unexpected result in the overflow case. The result now is still
    // inaccurate but is a number that is large enough.
    return port::kMaxInt64 / 1000000;
  } else {
    return std::max(kMinRefillBytesPerPeriod,
                    rate_bytes_per_sec * refill_period_us_ / 1000000);
  }
}

Status WriteAmpBasedRateLimiter::Tune() {
  // computed rate limit will be larger than `kMinBytesPerSec`
  const int64_t kMinBytesPerSec = 10 * 1024 * 1024;
  // high-priority bytes are padded to 20MB
  const int64_t kHighBytesLower = 20 * 1024 * 1024;
  // lower bound for write amplification estimation
  const int kRatioLower = 12;
  // Two reasons for using a ratio larger than estimation:
  // 1. compaction cannot fully utilize the IO quota we set.
  // 2. make it faster to digest unexpected burst of pending compaction bytes,
  // generally this will help flatten IO waves.
  const int kRatioPaddingPercent = 18;

  std::chrono::microseconds prev_tuned_time = tuned_time_;
  tuned_time_ = std::chrono::microseconds(NowMicrosMonotonic(env_));
  auto duration = tuned_time_ - prev_tuned_time;
  auto duration_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

  int64_t prev_bytes_per_sec = GetBytesPerSecond();

  bytes_sampler_.AddSample(duration_bytes_through_ * 1000 / duration_ms);
  highpri_bytes_sampler_.AddSample(duration_highpri_bytes_through_ * 1000 /
                                   duration_ms);
  if (bytes_sampler_.AtTimePoint()) {
    long_term_bytes_sampler_.AddSample(bytes_sampler_.GetFullValue());
    long_term_highpri_bytes_sampler_.AddSample(
        highpri_bytes_sampler_.GetFullValue());
  }
  limit_bytes_sampler_.AddSample(prev_bytes_per_sec);
  // As LSM grows higher, it tends to generate compaction tasks in waves
  // (cascaded). We use extra long-term window to help reduce this fluctuation.
  int32_t ratio = std::max(
      kRatioLower, static_cast<int32_t>(
                       long_term_bytes_sampler_.GetFullValue() * 10 /
                       std::max(long_term_highpri_bytes_sampler_.GetFullValue(),
                                kHighBytesLower)));
  ratio = std::max(ratio, static_cast<int32_t>(
                              bytes_sampler_.GetFullValue() * 10 /
                              std::max(highpri_bytes_sampler_.GetFullValue(),
                                       kHighBytesLower)));
  int32_t ratio_padding = ratio * kRatioPaddingPercent / 100;

  // in case there are compaction bursts even when online writes are stable
  auto util = bytes_sampler_.GetRecentValue() * 100 /
              limit_bytes_sampler_.GetRecentValue();
  if (util > 98) {
    ratio_delta_ += 1;
  } else if (util < 95 && ratio_delta_ > 0) {
    ratio_delta_ -= 1;
  }

  int64_t new_bytes_per_sec =
      (ratio + ratio_padding + ratio_delta_) *
      std::max(highpri_bytes_sampler_.GetRecentValue(), kHighBytesLower) / 10;
  new_bytes_per_sec =
      std::max(kMinBytesPerSec,
               std::min(new_bytes_per_sec,
                        max_bytes_per_sec_.load(std::memory_order_relaxed) -
                            highpri_bytes_sampler_.GetRecentValue()));
  if (new_bytes_per_sec != prev_bytes_per_sec) {
    SetActualBytesPerSecond(new_bytes_per_sec);
  }

  duration_bytes_through_ = 0;
  duration_highpri_bytes_through_ = 0;
  return Status::OK();
}

RateLimiter* NewWriteAmpBasedRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us /* = 100 * 1000 */,
    int32_t fairness /* = 10 */,
    RateLimiter::Mode mode /* = RateLimiter::Mode::kWritesOnly */,
    bool auto_tuned /* = false */) {
  assert(rate_bytes_per_sec > 0);
  assert(refill_period_us > 0);
  assert(fairness > 0);
  return new WriteAmpBasedRateLimiter(rate_bytes_per_sec, refill_period_us,
                                      fairness, mode, Env::Default(),
                                      auto_tuned);
}

}  // namespace ROCKSDB_NAMESPACE
