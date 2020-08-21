// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// NOTE: SpecialEnv offers most of this functionality, along with hooks
// for safe DB behavior under a mock time environment, so should be used
// instead of MockTimeEnv for DB tests.
class MockTimeEnv : public EnvWrapper {
 public:
  explicit MockTimeEnv(Env* base) : EnvWrapper(base) {}

  virtual Status GetCurrentTime(int64_t* time_sec) override {
    assert(time_sec != nullptr);
    *time_sec = static_cast<int64_t>(current_time_us_ / kMicrosInSecond);
    return Status::OK();
  }

  virtual uint64_t NowSeconds() { return current_time_us_ / kMicrosInSecond; }

  virtual uint64_t NowMicros() override { return current_time_us_; }

  virtual uint64_t NowNanos() override {
    assert(current_time_us_ <= std::numeric_limits<uint64_t>::max() / 1000);
    return current_time_us_ * 1000;
  }

  uint64_t RealNowMicros() { return target()->NowMicros(); }

  void set_current_time(uint64_t time_sec) {
    assert(time_sec < std::numeric_limits<uint64_t>::max() / kMicrosInSecond);
    assert(time_sec * kMicrosInSecond >= current_time_us_);
    current_time_us_ = time_sec * kMicrosInSecond;
  }

  // It's a fake sleep that just updates the Env current time, which is similar
  // to `NoSleepEnv.SleepForMicroseconds()` and
  // `SpecialEnv.MockSleepForMicroseconds()`.
  // It's also similar to `set_current_time()`, which takes an absolute time in
  // seconds, vs. this one takes the sleep in microseconds.
  // Note: Not thread safe.
  void MockSleepForMicroseconds(int micros) {
    assert(micros >= 0);
    assert(current_time_us_ + static_cast<uint64_t>(micros) >=
           current_time_us_);
    current_time_us_.fetch_add(micros);
  }

  void MockSleepForSeconds(int seconds) {
    assert(seconds >= 0);
    uint64_t micros = static_cast<uint64_t>(seconds) * kMicrosInSecond;
    assert(current_time_us_ + micros >= current_time_us_);
    current_time_us_.fetch_add(micros);
  }

  // TODO: this is a workaround for the different behavior on different platform
  // for timedwait timeout. Ideally timedwait API should be moved to env.
  // details: PR #7101.
  void InstallTimedWaitFixCallback() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
#if defined(OS_MACOSX) && !defined(NDEBUG)
    // This is an alternate way (vs. SpecialEnv) of dealing with the fact
    // that on some platforms, pthread_cond_timedwait does not appear to
    // release the lock for other threads to operate if the deadline time
    // is already passed. (TimedWait calls are currently a bad abstraction
    // because the deadline parameter is usually computed from Env time,
    // but is interpreted in real clock time.)
    SyncPoint::GetInstance()->SetCallBack(
        "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
          uint64_t time_us = *reinterpret_cast<uint64_t*>(arg);
          if (time_us < this->RealNowMicros()) {
            *reinterpret_cast<uint64_t*>(arg) = this->RealNowMicros() + 1000;
          }
        });
#endif  // OS_MACOSX && !NDEBUG
    SyncPoint::GetInstance()->EnableProcessing();
  }

 private:
  std::atomic<uint64_t> current_time_us_{0};
};

}  // namespace ROCKSDB_NAMESPACE
