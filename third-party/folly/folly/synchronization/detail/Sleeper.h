//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

/*
 * @author Keith Adams <kma@fb.com>
 * @author Jordan DeLong <delong.j@fb.com>
 */

#include <cstdint>
#include <thread>

#include <folly/portability/Asm.h>

namespace folly {

//////////////////////////////////////////////////////////////////////

namespace detail {

/*
 * A helper object for the contended case. Starts off with eager
 * spinning, and falls back to sleeping for small quantums.
 */
class Sleeper {
  static const uint32_t kMaxActiveSpin = 4000;

  uint32_t spinCount;

 public:
  Sleeper() noexcept : spinCount(0) {}

  static void sleep() noexcept {
    /*
     * Always sleep 0.5ms, assuming this will make the kernel put
     * us down for whatever its minimum timer resolution is (in
     * linux this varies by kernel version from 1ms to 10ms).
     */
    std::this_thread::sleep_for(std::chrono::microseconds{500});
  }

  void wait() noexcept {
    if (spinCount < kMaxActiveSpin) {
      ++spinCount;
      asm_volatile_pause();
    } else {
      sleep();
    }
  }
};

} // namespace detail
} // namespace folly

