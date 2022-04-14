//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <chrono>
#include <thread>

#include <folly/portability/Asm.h>
#include <folly/synchronization/WaitOptions.h>

namespace folly {
namespace detail {

enum class spin_result {
  success, // condition passed
  timeout, // exceeded deadline
  advance, // exceeded current wait-options component timeout
};

template <typename Clock, typename Duration, typename F>
spin_result spin_pause_until(
    std::chrono::time_point<Clock, Duration> const& deadline,
    WaitOptions const& opt,
    F f) {
  if (opt.spin_max() <= opt.spin_max().zero()) {
    return spin_result::advance;
  }

  auto tbegin = Clock::now();
  while (true) {
    if (f()) {
      return spin_result::success;
    }

    auto const tnow = Clock::now();
    if (tnow >= deadline) {
      return spin_result::timeout;
    }

    //  Backward time discontinuity in Clock? revise pre_block starting point
    tbegin = std::min(tbegin, tnow);
    if (tnow >= tbegin + opt.spin_max()) {
      return spin_result::advance;
    }

    //  The pause instruction is the polite way to spin, but it doesn't
    //  actually affect correctness to omit it if we don't have it. Pausing
    //  donates the full capabilities of the current core to its other
    //  hyperthreads for a dozen cycles or so.
    asm_volatile_pause();
  }
}

template <typename Clock, typename Duration, typename F>
spin_result spin_yield_until(
    std::chrono::time_point<Clock, Duration> const& deadline,
    F f) {
  while (true) {
    if (f()) {
      return spin_result::success;
    }

    auto const max = std::chrono::time_point<Clock, Duration>::max();
    if (deadline != max && Clock::now() >= deadline) {
      return spin_result::timeout;
    }

    std::this_thread::yield();
  }
}

} // namespace detail
} // namespace folly
