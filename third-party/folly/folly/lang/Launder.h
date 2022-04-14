//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <new>

#include <folly/Portability.h>

/***
 *  include or backport:
 *  * std::launder
 */

namespace folly {

/**
 * Approximate backport from C++17 of std::launder. It should be `constexpr`
 * but that can't be done without specific support from the compiler.
 */
template <typename T>
FOLLY_NODISCARD inline T* launder(T* in) noexcept {
#if FOLLY_HAS_BUILTIN(__builtin_launder) || __GNUC__ >= 7
  // The builtin has no unwanted side-effects.
  return __builtin_launder(in);
#elif __GNUC__
  // This inline assembler block declares that `in` is an input and an output,
  // so the compiler has to assume that it has been changed inside the block.
  __asm__("" : "+r"(in));
  return in;
#elif defined(_WIN32)
  // MSVC does not currently have optimizations around const members of structs.
  // _ReadWriteBarrier() will prevent compiler reordering memory accesses.
  _ReadWriteBarrier();
  return in;
#else
  static_assert(
      false, "folly::launder is not implemented for this environment");
#endif
}

/* The standard explicitly forbids laundering these */
void launder(void*) = delete;
void launder(void const*) = delete;
void launder(void volatile*) = delete;
void launder(void const volatile*) = delete;
template <typename T, typename... Args>
void launder(T (*)(Args...)) = delete;
} // namespace folly
