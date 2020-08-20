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

  virtual Status GetCurrentTime(int64_t* time) override {
    assert(time != nullptr);
    assert(current_time_ <=
           static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    *time = static_cast<int64_t>(current_time_);
    return Status::OK();
  }

  virtual uint64_t NowMicros() override {
    assert(current_time_ <= std::numeric_limits<uint64_t>::max() / 1000000);
    return current_time_ * 1000000;
  }

  virtual uint64_t NowNanos() override {
    assert(current_time_ <= std::numeric_limits<uint64_t>::max() / 1000000000);
    return current_time_ * 1000000000;
  }

  uint64_t RealNowMicros() { return target()->NowMicros(); }

  void set_current_time(uint64_t time) {
    assert(time >= current_time_);
    current_time_ = time;
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
  std::atomic<uint64_t> current_time_{0};
};

}  // namespace ROCKSDB_NAMESPACE
