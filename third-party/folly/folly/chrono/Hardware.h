//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <folly/Portability.h>

#include <chrono>
#include <cstdint>

#if _MSC_VER && (defined(_M_IX86) || defined(_M_X64))
extern "C" std::uint64_t __rdtsc();
#pragma intrinsic(__rdtsc)
#endif

namespace folly {

inline std::uint64_t hardware_timestamp() {
#if _MSC_VER && (defined(_M_IX86) || defined(_M_X64))
  return __rdtsc();
#elif __GNUC__ && (__i386__ || FOLLY_X64)
  return __builtin_ia32_rdtsc();
#else
  // use steady_clock::now() as an approximation for the timestamp counter on
  // non-x86 systems
  return std::chrono::steady_clock::now().time_since_epoch().count();
#endif
}

} // namespace folly

