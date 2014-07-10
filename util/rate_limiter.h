//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <thread>
#include <deque>

#include "port/port_posix.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "rocksdb/env.h"

namespace rocksdb {

class RateLimiter {
 public:
  RateLimiter(int64_t refill_bytes, int64_t refill_period_us, int32_t fairness);

  ~RateLimiter();

  // Request for token to write bytes. If this request can not be satisfied,
  // the call is blocked. Caller is responsible to make sure
  // bytes < GetSingleBurstBytes()
  void Request(const int64_t bytes, const Env::IOPriority pri);

  int64_t GetTotalBytesThrough(
      const Env::IOPriority pri = Env::IO_TOTAL) const {
    MutexLock g(&request_mutex_);
    if (pri == Env::IO_TOTAL) {
      return total_bytes_through_[Env::IO_LOW] +
             total_bytes_through_[Env::IO_HIGH];
    }
    return total_bytes_through_[pri];
  }

  int64_t GetTotalRequests(const Env::IOPriority pri = Env::IO_TOTAL) const {
    MutexLock g(&request_mutex_);
    if (pri == Env::IO_TOTAL) {
      return total_requests_[Env::IO_LOW] + total_requests_[Env::IO_HIGH];
    }
    return total_requests_[pri];
  }

  int64_t GetSingleBurstBytes() const {
    // const var
    return refill_bytes_per_period_;
  }

  int64_t GetAvailableBytes() const {
    MutexLock g(&request_mutex_);
    return available_bytes_;
  }

 private:
  void Refill();

  // This mutex guard all internal states
  mutable port::Mutex request_mutex_;

  const int64_t refill_period_us_;
  const int64_t refill_bytes_per_period_;
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
};

}  // namespace rocksdb
