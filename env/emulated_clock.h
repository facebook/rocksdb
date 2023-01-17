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
#include <string>

#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
// A SystemClock that can "mock" sleep and counts its operations.
class EmulatedSystemClock : public SystemClockWrapper {
 private:
  // Something to return when mocking current time
  const int64_t maybe_starting_time_;
  std::atomic<int> sleep_counter_{0};
  std::atomic<int> cpu_counter_{0};
  std::atomic<int64_t> addon_microseconds_{0};
  // Do not modify in the env of a running DB (could cause deadlock)
  std::atomic<bool> time_elapse_only_sleep_;
  bool no_slowdown_;

 public:
  explicit EmulatedSystemClock(const std::shared_ptr<SystemClock>& base,
                               bool time_elapse_only_sleep = false);

  static const char* kClassName() { return "TimeEmulatedSystemClock"; }
  const char* Name() const override { return kClassName(); }

  virtual void SleepForMicroseconds(int micros) override {
    sleep_counter_++;
    if (no_slowdown_ || time_elapse_only_sleep_) {
      addon_microseconds_.fetch_add(micros);
    }
    if (!no_slowdown_) {
      SystemClockWrapper::SleepForMicroseconds(micros);
    }
  }

  void MockSleepForMicroseconds(int64_t micros) {
    sleep_counter_++;
    assert(no_slowdown_);
    addon_microseconds_.fetch_add(micros);
  }

  void MockSleepForSeconds(int64_t seconds) {
    sleep_counter_++;
    assert(no_slowdown_);
    addon_microseconds_.fetch_add(seconds * 1000000);
  }

  void SetTimeElapseOnlySleep(bool enabled) {
    // We cannot set these before destroying the last DB because they might
    // cause a deadlock or similar without the appropriate options set in
    // the DB.
    time_elapse_only_sleep_ = enabled;
    no_slowdown_ = enabled;
  }

  bool IsTimeElapseOnlySleep() const { return time_elapse_only_sleep_.load(); }
  void SetMockSleep(bool enabled = true) { no_slowdown_ = enabled; }
  bool IsMockSleepEnabled() const { return no_slowdown_; }

  int GetSleepCounter() const { return sleep_counter_.load(); }

  virtual Status GetCurrentTime(int64_t* unix_time) override {
    Status s;
    if (time_elapse_only_sleep_) {
      *unix_time = maybe_starting_time_;
    } else {
      s = SystemClockWrapper::GetCurrentTime(unix_time);
    }
    if (s.ok()) {
      // mock microseconds elapsed to seconds of time
      *unix_time += addon_microseconds_.load() / 1000000;
    }
    return s;
  }

  virtual uint64_t CPUNanos() override {
    cpu_counter_++;
    return SystemClockWrapper::CPUNanos();
  }

  virtual uint64_t CPUMicros() override {
    cpu_counter_++;
    return SystemClockWrapper::CPUMicros();
  }

  virtual uint64_t NowNanos() override {
    return (time_elapse_only_sleep_ ? 0 : SystemClockWrapper::NowNanos()) +
           addon_microseconds_.load() * 1000;
  }

  virtual uint64_t NowMicros() override {
    return (time_elapse_only_sleep_ ? 0 : SystemClockWrapper::NowMicros()) +
           addon_microseconds_.load();
  }

  int GetCpuCounter() const { return cpu_counter_.load(); }

  void ResetCounters() {
    cpu_counter_.store(0);
    sleep_counter_.store(0);
  }
};
}  // namespace ROCKSDB_NAMESPACE
