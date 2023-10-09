// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_util/mock_time_env.h"

#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

// TODO: this is a workaround for the different behavior on different platform
// for timedwait timeout. Ideally timedwait API should be moved to env.
// details: PR #7101.
void MockSystemClock::InstallTimedWaitFixCallback() {
#ifndef NDEBUG
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
#ifdef OS_MACOSX
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
#endif  // OS_MACOSX
  SyncPoint::GetInstance()->EnableProcessing();
#endif  // !NDEBUG
}

}  // namespace ROCKSDB_NAMESPACE
