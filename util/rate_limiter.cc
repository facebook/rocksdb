//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/rate_limiter.h"

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
      // but we can not write less than one page at a time on direct I/O
      // thus we may want not to use ratelimiter
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
          CalculateRefillBytesPerPeriod(rate_bytes_per_sec_)),
      clock_(clock),
      stop_(false),
      exit_cv_(&request_mutex_),
      requests_to_wait_(0),
      available_bytes_(0),
      next_refill_us_(NowMicrosMonotonic()),
      fairness_(fairness > 100 ? 100 : fairness),
      rnd_((uint32_t)time(nullptr)),
      leader_(nullptr),
      auto_tuned_(auto_tuned),
      num_drains_(0),
      prev_num_drains_(0),
      max_bytes_per_sec_(rate_bytes_per_sec),
      tuned_time_(NowMicrosMonotonic()) {
  total_requests_[0] = 0;
  total_requests_[1] = 0;
  total_bytes_through_[0] = 0;
  total_bytes_through_[1] = 0;
}

GenericRateLimiter::~GenericRateLimiter() {
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

// This API allows user to dynamically change rate limiter's bytes per second.
void GenericRateLimiter::SetBytesPerSecond(int64_t bytes_per_second) {
  assert(bytes_per_second > 0);
  rate_bytes_per_sec_ = bytes_per_second;
  refill_bytes_per_period_.store(
      CalculateRefillBytesPerPeriod(bytes_per_second),
      std::memory_order_relaxed);
}

void GenericRateLimiter::Request(int64_t bytes, const Env::IOPriority pri,
                                 Statistics* stats) {
  assert(bytes <= refill_bytes_per_period_.load(std::memory_order_relaxed));
  TEST_SYNC_POINT("GenericRateLimiter::Request");
  TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:1",
                           &rate_bytes_per_sec_);
  MutexLock g(&request_mutex_);

  if (auto_tuned_) {
    static const int kRefillsPerTune = 100;
    std::chrono::microseconds now(NowMicrosMonotonic());
    if (now - tuned_time_ >=
        kRefillsPerTune * std::chrono::microseconds(refill_period_us_)) {
      Status s = Tune();
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

  do {
    bool timedout = false;

    // Leader election:
    //  Leader request's duty:
    //  (1) Waiting for the next refill time;
    //  (2) Refilling the bytes and granting requests.
    //
    //  If the following three conditions are all true for a request,
    //  then the request is selected as a leader:
    //  (1) The request thread acquired the request_mutex_ and is running;
    //  (2) There is currently no leader;
    //  (3) The request sits at the front of a queue.
    //
    //  If not selected as a leader, the request thread will wait
    //  for one of the following signals to wake up and
    //  compete for the request_mutex_:
    //  (1) Signal from the previous leader to exit since its requested bytes
    //      are fully granted;
    //  (2) Signal from the previous leader to particpate in next-round
    //      leader election;
    //  (3) Signal from rate limiter's destructor as part of the clean-up.
    //
    //  Therefore, a leader request can only be one of the following types:
    //  (1) a new incoming request placed at the front of a queue;
    //  (2) a previous leader request whose quota has not been not fully
    //      granted yet due to its lower priority, hence still at
    //      the front of a queue;
    //  (3) a waiting request at the front of a queue, which got
    //      signaled by the previous leader to participate in leader election.
    if (leader_ == nullptr &&
        ((!queue_[Env::IO_HIGH].empty() &&
            &r == queue_[Env::IO_HIGH].front()) ||
         (!queue_[Env::IO_LOW].empty() &&
            &r == queue_[Env::IO_LOW].front()))) {
      leader_ = &r;

      int64_t delta = next_refill_us_ - NowMicrosMonotonic();
      delta = delta > 0 ? delta : 0;
      if (delta == 0) {
        timedout = true;
      } else {
        // The leader request thread waits till next_refill_us_
        int64_t wait_until = clock_->NowMicros() + delta;
        RecordTick(stats, NUMBER_RATE_LIMITER_DRAINS);
        ++num_drains_;
        timedout = r.cv.TimedWait(wait_until);
      }
    } else {
      r.cv.Wait();
    }

    if (stop_) {
      // It is now in the clean-up of ~GenericRateLimiter().
      // Therefore any woken-up request will exit here,
      // might or might not has been satiesfied.
      --requests_to_wait_;
      exit_cv_.Signal();
      return;
    }

    // Assertion: request thread running through this point is one of the
    // following in terms of the request type and quota granting situation:
    // (1) a leader request that is not fully granted with quota and about
    //     to carry out its leader's work;
    // (2) a non-leader request that got fully granted with quota and is
    //     running to exit;
    // (3) a non-leader request that is not fully granted with quota and
    //     is running to particpate in next-round leader election.
    assert((&r == leader_ && !r.granted) || (&r != leader_ && r.granted) ||
           (&r != leader_ && !r.granted));

    // Assertion: request thread running through this point is one of the
    // following in terms of its position in queue:
    // (1) a request got popped off the queue because it is fully granted
    //     with bytes;
    // (2) a request sits at the front of its queue.
    assert(r.granted ||
           (!queue_[Env::IO_HIGH].empty() &&
            &r == queue_[Env::IO_HIGH].front()) ||
           (!queue_[Env::IO_LOW].empty() &&
            &r == queue_[Env::IO_LOW].front()));

    if (leader_ == &r) {
      // The leader request thread is now running.
      // It might or might not has been TimedWait().
      if (timedout) {
        // Time for the leader to do refill and grant bytes to requests
        RefillBytesAndGrantRequests();

        // The leader request retires after refilling and granting bytes
        // regardless. This is to simplify the election handling.
        leader_ = nullptr;

        if (r.granted) {
          // The leader request (that was just retired)
          // already got fully granted with quota and will soon exit

          // Assertion: the fully granted leader request is popped off its queue
          assert((queue_[Env::IO_HIGH].empty() ||
                    &r != queue_[Env::IO_HIGH].front()) &&
                 (queue_[Env::IO_LOW].empty() ||
                    &r != queue_[Env::IO_LOW].front()));

          // If there is any remaining requests, the leader request (that was
          // just retired) makes sure there exists at least one leader candidate
          // by signaling a front request of a queue to particpate in
          // next-round leader election
          if (!queue_[Env::IO_HIGH].empty()) {
            queue_[Env::IO_HIGH].front()->cv.Signal();
          } else if (!queue_[Env::IO_LOW].empty()) {
            queue_[Env::IO_LOW].front()->cv.Signal();
          }

          // The leader request (that was just retired) exits
          break;
        } else {
          // The leader request (that was just retired) is not fully granted
          // with quota. It will particpate in leader election and claim back
          // the leader position immediately.
          assert(!r.granted);
        }
      } else {
        // Spontaneous wake up, need to continue to wait
        assert(!r.granted);
        leader_ = nullptr;
      }
    } else {
      // The non-leader request thread is running.
      // It is one of the following request types:
      // (1) The request got fully granted with quota and signaled to run to
      //     exit by the previous leader;
      // (2) The request is not fully granted with quota and signaled to run to
      //     particpate in next-round leader election by the previous leader.
      //     It might or might not become the next-round leader because a new
      //     request may come in and acquire the request_mutex_ before this
      //     request thread does after it was signaled. The new request might
      //     sit at front of a queue and hence become the next-round leader
      //     instead.
      assert(&r != leader_);
    }
  } while (!r.granted);
}

void GenericRateLimiter::RefillBytesAndGrantRequests() {
  TEST_SYNC_POINT("GenericRateLimiter::RefillBytesAndGrantRequests");
  next_refill_us_ = NowMicrosMonotonic() + refill_period_us_;
  // Carry over the left over quota from the last period
  auto refill_bytes_per_period =
      refill_bytes_per_period_.load(std::memory_order_relaxed);
  if (available_bytes_ < refill_bytes_per_period) {
    available_bytes_ += refill_bytes_per_period;
  }

  int use_low_pri_first = rnd_.OneIn(fairness_) ? 0 : 1;
  for (int q = 0; q < 2; ++q) {
    auto use_pri = (use_low_pri_first == q) ? Env::IO_LOW : Env::IO_HIGH;
    auto* queue = &queue_[use_pri];
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
      total_bytes_through_[use_pri] += next_req->bytes;
      queue->pop_front();

      next_req->granted = true;
      if (next_req != leader_) {
        // Quota granted, signal the thread to exit
        next_req->cv.Signal();
      }
    }
  }
}

int64_t GenericRateLimiter::CalculateRefillBytesPerPeriod(
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

Status GenericRateLimiter::Tune() {
  const int kLowWatermarkPct = 50;
  const int kHighWatermarkPct = 90;
  const int kAdjustFactorPct = 5;
  // computed rate limit will be in
  // `[max_bytes_per_sec_ / kAllowedRangeFactor, max_bytes_per_sec_]`.
  const int kAllowedRangeFactor = 20;

  std::chrono::microseconds prev_tuned_time = tuned_time_;
  tuned_time_ = std::chrono::microseconds(NowMicrosMonotonic());

  int64_t elapsed_intervals = (tuned_time_ - prev_tuned_time +
                               std::chrono::microseconds(refill_period_us_) -
                               std::chrono::microseconds(1)) /
                              std::chrono::microseconds(refill_period_us_);
  // We tune every kRefillsPerTune intervals, so the overflow and division-by-
  // zero conditions should never happen.
  assert(num_drains_ - prev_num_drains_ <= port::kMaxInt64 / 100);
  assert(elapsed_intervals > 0);
  int64_t drained_pct =
      (num_drains_ - prev_num_drains_) * 100 / elapsed_intervals;

  int64_t prev_bytes_per_sec = GetBytesPerSecond();
  int64_t new_bytes_per_sec;
  if (drained_pct == 0) {
    new_bytes_per_sec = max_bytes_per_sec_ / kAllowedRangeFactor;
  } else if (drained_pct < kLowWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec =
        std::min(prev_bytes_per_sec, port::kMaxInt64 / 100);
    new_bytes_per_sec =
        std::max(max_bytes_per_sec_ / kAllowedRangeFactor,
                 sanitized_prev_bytes_per_sec * 100 / (100 + kAdjustFactorPct));
  } else if (drained_pct > kHighWatermarkPct) {
    // sanitize to prevent overflow
    int64_t sanitized_prev_bytes_per_sec = std::min(
        prev_bytes_per_sec, port::kMaxInt64 / (100 + kAdjustFactorPct));
    new_bytes_per_sec =
        std::min(max_bytes_per_sec_,
                 sanitized_prev_bytes_per_sec * (100 + kAdjustFactorPct) / 100);
  } else {
    new_bytes_per_sec = prev_bytes_per_sec;
  }
  if (new_bytes_per_sec != prev_bytes_per_sec) {
    SetBytesPerSecond(new_bytes_per_sec);
  }
  num_drains_ = prev_num_drains_;
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
  return new GenericRateLimiter(rate_bytes_per_sec, refill_period_us, fairness,
                                mode, SystemClock::Default(), auto_tuned);
}

}  // namespace ROCKSDB_NAMESPACE
