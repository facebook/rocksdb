//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
// A SystemClock that fakes sleeps.
class FakeSleepSystemClock : public SystemClockWrapper {
 public:
  explicit FakeSleepSystemClock(const std::shared_ptr<SystemClock>& c)
      : SystemClockWrapper(c), fake_sleep_micros_(0) {}

  void SleepForMicroseconds(int micros) override {
    fake_sleep_micros_.fetch_add(static_cast<uint64_t>(micros));
  }

  static const char* kClassName() { return "FakeSleepSystemClock"; }
  const char* Name() const override { return kClassName(); }

  Status GetCurrentTime(int64_t* unix_time) override {
    auto s = SystemClockWrapper::GetCurrentTime(unix_time);
    if (s.ok()) {
      auto fake_time = fake_sleep_micros_.load() / (1000 * 1000);
      *unix_time += fake_time;
    }
    return s;
  }

  uint64_t NowMicros() override {
    return SystemClockWrapper::NowMicros() + fake_sleep_micros_.load();
  }

  uint64_t NowNanos() override {
    return SystemClockWrapper::NowNanos() + fake_sleep_micros_.load() * 1000;
  }

 private:
  std::atomic<int64_t> fake_sleep_micros_;
};

class MockEnv : public CompositeEnvWrapper {
 public:
  static MockEnv* Create(Env* base);

  Status CorruptBuffer(const std::string& fname);
 private:
  MockEnv(Env* env, const std::shared_ptr<FileSystem>& fs,
          const std::shared_ptr<SystemClock>& clock);
};

}  // namespace ROCKSDB_NAMESPACE
