//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <chrono>

namespace folly {

/// WaitOptions
///
/// Various synchronization primitives as well as various concurrent data
/// structures built using them have operations which might wait. This type
/// represents a set of options for controlling such waiting.
class WaitOptions {
 public:
  struct Defaults {
    /// spin_max
    ///
    /// If multiple threads are actively using a synchronization primitive,
    /// whether indirectly via a higher-level concurrent data structure or
    /// directly, where the synchronization primitive has an operation which
    /// waits and another operation which wakes the waiter, it is common for
    /// wait and wake events to happen almost at the same time. In this state,
    /// we lose big 50% of the time if the wait blocks immediately.
    ///
    /// We can improve our chances of being waked immediately, before blocking,
    /// by spinning for a short duration, although we have to balance this
    /// against the extra cpu utilization, latency reduction, power consumption,
    /// and priority inversion effect if we end up blocking anyway.
    ///
    /// We use a default maximum of 2 usec of spinning. As partial consolation,
    /// since spinning as implemented in folly uses the pause instruction where
    /// available, we give a small speed boost to the colocated hyperthread.
    ///
    /// On circa-2013 devbox hardware, it costs about 7 usec to FUTEX_WAIT and
    /// then be awoken. Spins on this hw take about 7 nsec, where all but 0.5
    /// nsec is the pause instruction.
    static constexpr std::chrono::nanoseconds spin_max =
        std::chrono::microseconds(2);
  };

  std::chrono::nanoseconds spin_max() const {
    return spin_max_;
  }
  WaitOptions& spin_max(std::chrono::nanoseconds dur) {
    spin_max_ = dur;
    return *this;
  }

 private:
  std::chrono::nanoseconds spin_max_ = Defaults::spin_max;
};

} // namespace folly
