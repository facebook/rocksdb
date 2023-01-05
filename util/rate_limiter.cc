//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/rate_limiter.h"

#include <algorithm>

#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {
size_t RateLimiter::RequestToken(size_t bytes, size_t alignment,
                                 Env::IOPriority io_priority, Statistics* stats,
                                 RateLimiter::OpType op_type) {
  if (io_priority < Env::IO_TOTAL && IsRateLimited(op_type)) {
    bytes = std::min(bytes, static_cast<size_t>(GetSingleBurstBytes()));

    if (alignment > 0) {
      // Here we may actually require more than burst and block
      // as we can not write/read less than one page at a time on direct I/O
      // thus we do not want to be strictly constrained by burst
      bytes = std::max(alignment, TruncateToPageBoundary(alignment, bytes));
    }
    Request(bytes, io_priority, stats, op_type);
  }
  return bytes;
}

// Pending request
struct GenericRateLimiter::Req {
  explicit Req(int64_t _bytes, port::Mutex* _mu)
      : request_bytes(_bytes), bytes(_bytes), cv(_mu), granted(false) {}
  int64_t request_bytes;
  int64_t bytes;
  port::CondVar cv;
  bool granted;
};

GenericRateLimiter::GenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness,
    RateLimiter::Mode mode, const std::shared_ptr<SystemClock>& clock,
    bool auto_tuned)
    : RateLimiter(mode),
      refill_period_us_(refill_period_us),
      rate_bytes_per_sec_(auto_tuned ? rate_bytes_per_sec / 2
                                     : rate_bytes_per_sec),
      refill_bytes_per_period_(
          CalculateRefillBytesPerPeriodLocked(rate_bytes_per_sec_)),
      clock_(clock),
      stop_(false),
      exit_cv_(&request_mutex_),
      requests_to_wait_(0),
      available_bytes_(0),
      next_refill_us_(NowMicrosMonotonicLocked()),
      fairness_(fairness > 100 ? 100 : fairness),
      rnd_((uint32_t)time(nullptr)),
      wait_until_refill_pending_(false),
      auto_tuned_(auto_tuned),
      num_drains_(0),
      max_bytes_per_sec_(rate_bytes_per_sec),
      tuned_time_(NowMicrosMonotonicLocked()) {
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    total_requests_[i] = 0;
    total_bytes_through_[i] = 0;
  }
}

GenericRateLimiter::~GenericRateLimiter() {
  MutexLock g(&request_mutex_);
  stop_ = true;
  std::deque<Req*>::size_type queues_size_sum = 0;
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    queues_size_sum += queue_[i].size();
  }
  requests_to_wait_ = static_cast<int32_t>(queues_size_sum);

  for (int i = Env::IO_TOTAL - 1; i >= Env::IO_LOW; --i) {
    std::deque<Req*> queue = queue_[i];
    for (auto& r : queue) {
      r->cv.Signal();
    }
  }

  while (requests_to_wait_ > 0) {
    exit_cv_.Wait();
  }
}

// This API allows user to dynamically change rate limiter's bytes per second.
void GenericRateLimiter::SetBytesPerSecond(int64_t bytes_per_second) {
  MutexLock g(&request_mutex_);
  SetBytesPerSecondLocked(bytes_per_second);
}

void GenericRateLimiter::SetBytesPerSecondLocked(int64_t bytes_per_second) {
  assert(bytes_per_second > 0);
  rate_bytes_per_sec_.store(bytes_per_second, std::memory_order_relaxed);
  refill_bytes_per_period_.store(
      CalculateRefillBytesPerPeriodLocked(bytes_per_second),
      std::memory_order_relaxed);
}

void GenericRateLimiter::Request(int64_t bytes, const Env::IOPriority pri,
                                 Statistics* stats) {
  assert(bytes <= refill_bytes_per_period_.load(std::memory_order_relaxed));
  bytes = std::max(static_cast<int64_t>(0), bytes);
  TEST_SYNC_POINT("GenericRateLimiter::Request");
  TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:1",
                           &rate_bytes_per_sec_);
  MutexLock g(&request_mutex_);

  if (auto_tuned_) {
    static const int kRefillsPerTune = 100;
    std::chrono::microseconds now(NowMicrosMonotonicLocked());
    if (now - tuned_time_ >=
        kRefillsPerTune * std::chrono::microseconds(refill_period_us_)) {
      Status s = TuneLocked();
      s.PermitUncheckedError();  //**TODO: What to do on error?
    }
  }

  if (stop_) {
    // It is now in the clean-up of ~GenericRateLimiter().
    // Therefore any new incoming request will exit from here
    // and not get satiesfied.
    return;
  }

  ++total_requests_[pri];

  if (available_bytes_ >= bytes) {
    // Refill thread assigns quota and notifies requests waiting on
    // the queue under mutex. So if we get here, that means nobody
    // is waiting?
    available_bytes_ -= bytes;
    total_bytes_through_[pri] += bytes;
    return;
  }

  // Request cannot be satisfied at this moment, enqueue
  Req r(bytes, &request_mutex_);
  queue_[pri].push_back(&r);
  TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:PostEnqueueRequest",
                           &request_mutex_);
  // A thread representing a queued request coordinates with other such threads.
  // There are two main duties.
  //
  // (1) Waiting for the next refill time.
  // (2) Refilling the bytes and granting requests.
  do {
    int64_t time_until_refill_us = next_refill_us_ - NowMicrosMonotonicLocked();
    if (time_until_refill_us > 0) {
      if (wait_until_refill_pending_) {
        // Somebody is performing (1). Trust we'll be woken up when our request
        // is granted or we are needed for future duties.
        r.cv.Wait();
      } else {
        // Whichever thread reaches here first performs duty (1) as described
        // above.
        int64_t wait_until = clock_->NowMicros() + time_until_refill_us;
        RecordTick(stats, NUMBER_RATE_LIMITER_DRAINS);
        ++num_drains_;
        wait_until_refill_pending_ = true;
        r.cv.TimedWait(wait_until);
        TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:PostTimedWait",
                                 &time_until_refill_us);
        wait_until_refill_pending_ = false;
      }
    } else {
      // Whichever thread reaches here first performs duty (2) as described
      // above.
      RefillBytesAndGrantRequestsLocked();
      if (r.granted) {
        // If there is any remaining requests, make sure there exists at least
        // one candidate is awake for future duties by signaling a front request
        // of a queue.
        for (int i = Env::IO_TOTAL - 1; i >= Env::IO_LOW; --i) {
          std::deque<Req*> queue = queue_[i];
          if (!queue.empty()) {
            queue.front()->cv.Signal();
            break;
          }
        }
      }
    }
    // Invariant: non-granted request is always in one queue, and granted
    // request is always in zero queues.
#ifndef NDEBUG
    int num_found = 0;
    for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
      if (std::find(queue_[i].begin(), queue_[i].end(), &r) !=
          queue_[i].end()) {
        ++num_found;
      }
    }
    if (r.granted) {
      assert(num_found == 0);
    } else {
      assert(num_found == 1);
    }
#endif  // NDEBUG
  } while (!stop_ && !r.granted);

  if (stop_) {
    // It is now in the clean-up of ~GenericRateLimiter().
    // Therefore any woken-up request will have come out of the loop and then
    // exit here. It might or might not have been satisfied.
    --requests_to_wait_;
    exit_cv_.Signal();
  }
}

std::vector<Env::IOPriority>
GenericRateLimiter::GeneratePriorityIterationOrderLocked() {
  std::vector<Env::IOPriority> pri_iteration_order(Env::IO_TOTAL /* 4 */);
  // We make Env::IO_USER a superior priority by always iterating its queue
  // first
  pri_iteration_order[0] = Env::IO_USER;

  bool high_pri_iterated_after_mid_low_pri = rnd_.OneIn(fairness_);
  TEST_SYNC_POINT_CALLBACK(
      "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
      "PostRandomOneInFairnessForHighPri",
      &high_pri_iterated_after_mid_low_pri);
  bool mid_pri_itereated_after_low_pri = rnd_.OneIn(fairness_);
  TEST_SYNC_POINT_CALLBACK(
      "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
      "PostRandomOneInFairnessForMidPri",
      &mid_pri_itereated_after_low_pri);

  if (high_pri_iterated_after_mid_low_pri) {
    pri_iteration_order[3] = Env::IO_HIGH;
    pri_iteration_order[2] =
        mid_pri_itereated_after_low_pri ? Env::IO_MID : Env::IO_LOW;
    pri_iteration_order[1] =
        (pri_iteration_order[2] == Env::IO_MID) ? Env::IO_LOW : Env::IO_MID;
  } else {
    pri_iteration_order[1] = Env::IO_HIGH;
    pri_iteration_order[3] =
        mid_pri_itereated_after_low_pri ? Env::IO_MID : Env::IO_LOW;
    pri_iteration_order[2] =
        (pri_iteration_order[3] == Env::IO_MID) ? Env::IO_LOW : Env::IO_MID;
  }

  TEST_SYNC_POINT_CALLBACK(
      "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
      "PreReturnPriIterationOrder",
      &pri_iteration_order);
  return pri_iteration_order;
}

void GenericRateLimiter::RefillBytesAndGrantRequestsLocked() {
  TEST_SYNC_POINT_CALLBACK(
      "GenericRateLimiter::RefillBytesAndGrantRequestsLocked", &request_mutex_);
  next_refill_us_ = NowMicrosMonotonicLocked() + refill_period_us_;
  // Carry over the left over quota from the last period
  auto refill_bytes_per_period =
      refill_bytes_per_period_.load(std::memory_order_relaxed);
  if (available_bytes_ < refill_bytes_per_period) {
    available_bytes_ += refill_bytes_per_period;
  }

  std::vector<Env::IOPriority> pri_iteration_order =
      GeneratePriorityIterationOrderLocked();

  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    assert(!pri_iteration_order.empty());
    Env::IOPriority current_pri = pri_iteration_order[i];
    auto* queue = &queue_[current_pri];
    while (!queue->empty()) {
      auto* next_req = queue->front();
      if (available_bytes_ < next_req->request_bytes) {
        // Grant partial request_bytes to avoid starvation of requests
        // that become asking for more bytes than available_bytes_
        // due to dynamically reduced rate limiter's bytes_per_second that
        // leads to reduced refill_bytes_per_period hence available_bytes_
        next_req->request_bytes -= available_bytes_;
        available_bytes_ = 0;
        break;
      }
      available_bytes_ -= next_req->request_bytes;
      next_req->request_bytes = 0;
      total_bytes_through_[current_pri] += next_req->bytes;
      queue->pop_front();

      next_req->granted = true;
      // Quota granted, signal the thread to exit
      next_req->cv.Signal();
    }
  }
}

int64_t GenericRateLimiter::CalculateRefillBytesPerPeriodLocked(
    int64_t rate_bytes_per_sec) {
  if (std::numeric_limits<int64_t>::max() / rate_bytes_per_sec <
      refill_period_us_) {
    // Avoid unexpected result in the overflow case. The result now is still
    // inaccurate but is a number that is large enough.
    return std::numeric_limits<int64_t>::max() / 1000000;
  } else {
    return rate_bytes_per_sec * refill_period_us_ / 1000000;
  }
}

Status GenericRateLimiter::TuneLocked() {
  const int kLowWatermarkPct = 50;
  const int kHighWatermarkPct = 90;
  const int kAdjustFactorPct = 5;
  // computed rate limit will be in
  // `[max_bytes_per_sec_ / kAllowedRangeFactor, max_bytes_per_sec_]`.
  const int kAllowedRangeFactor = 20;

  std::chrono::microseconds prev_tuned_time = tuned_time_;
  tuned_time_ = std::chrono::microseconds(NowMicrosMonotonicLocked());

  int64_t elapsed_intervals = (tuned_time_ - prev_tuned_time +
                               std::chrono::microseconds(refill_period_us_) -
                               std::chrono::microseconds(1)) /
                              std::chrono::microseconds(refill_period_us_);
  // We tune every kRefillsPerTune intervals, so the overflow and division-by-
  // zero conditions should never happen.
  assert(num_drains_ <= std::numeric_limits<int64_t>::max() / 100);
  assert(elapsed_intervals > 0);
  int64_t drained_pct = num_drains_ * 100 / elapsed_intervals;

  int64_t prev_bytes_per_sec = GetBytesPerSecond();
  int64_t new_bytes_per_sec;
  if (drained_pct == 0) {
    new_bytes_per_sec = max_bytes_per_sec_ / kAllowedRangeFactor;
  } else if (drained_pct < kLowWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec =
        std::min(prev_bytes_per_sec, std::numeric_limits<int64_t>::max() / 100);
    new_bytes_per_sec =
        std::max(max_bytes_per_sec_ / kAllowedRangeFactor,
                 sanitized_prev_bytes_per_sec * 100 / (100 + kAdjustFactorPct));
  } else if (drained_pct > kHighWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec =
        std::min(prev_bytes_per_sec, std::numeric_limits<int64_t>::max() /
                                         (100 + kAdjustFactorPct));
    new_bytes_per_sec =
        std::min(max_bytes_per_sec_,
                 sanitized_prev_bytes_per_sec * (100 + kAdjustFactorPct) / 100);
  } else {
    new_bytes_per_sec = prev_bytes_per_sec;
  }
  if (new_bytes_per_sec != prev_bytes_per_sec) {
    SetBytesPerSecondLocked(new_bytes_per_sec);
  }
  num_drains_ = 0;
  return Status::OK();
}

RateLimiter* NewGenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us /* = 100 * 1000 */,
    int32_t fairness /* = 10 */,
    RateLimiter::Mode mode /* = RateLimiter::Mode::kWritesOnly */,
    bool auto_tuned /* = false */) {
  assert(rate_bytes_per_sec > 0);
  assert(refill_period_us > 0);
  assert(fairness > 0);
  std::unique_ptr<RateLimiter> limiter(
      new GenericRateLimiter(rate_bytes_per_sec, refill_period_us, fairness,
                             mode, SystemClock::Default(), auto_tuned));
  return limiter.release();
}

}  // namespace ROCKSDB_NAMESPACE
