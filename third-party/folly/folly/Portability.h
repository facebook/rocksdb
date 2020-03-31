//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <folly/CPortability.h>

#if defined(__arm__)
#define FOLLY_ARM 1
#else
#define FOLLY_ARM 0
#endif

#if defined(__x86_64__) || defined(_M_X64)
#define FOLLY_X64 1
#else
#define FOLLY_X64 0
#endif

#if defined(__aarch64__)
#define FOLLY_AARCH64 1
#else
#define FOLLY_AARCH64 0
#endif

#if defined(__powerpc64__)
#define FOLLY_PPC64 1
#else
#define FOLLY_PPC64 0
#endif

#if defined(__has_builtin)
#define FOLLY_HAS_BUILTIN(...) __has_builtin(__VA_ARGS__)
#else
#define FOLLY_HAS_BUILTIN(...) 0
#endif

#if defined(__has_cpp_attribute)
#if __has_cpp_attribute(nodiscard)
#define FOLLY_NODISCARD [[nodiscard]]
#endif
#endif
#if !defined FOLLY_NODISCARD
#if defined(_MSC_VER) && (_MSC_VER >= 1700)
#define FOLLY_NODISCARD _Check_return_
#elif defined(__GNUC__)
#define FOLLY_NODISCARD __attribute__((__warn_unused_result__))
#else
#define FOLLY_NODISCARD
#endif
#endif

namespace folly {
constexpr bool kIsArchArm = FOLLY_ARM == 1;
constexpr bool kIsArchAmd64 = FOLLY_X64 == 1;
constexpr bool kIsArchAArch64 = FOLLY_AARCH64 == 1;
constexpr bool kIsArchPPC64 = FOLLY_PPC64 == 1;
} // namespace folly

namespace folly {
#ifdef NDEBUG
constexpr auto kIsDebug = false;
#else
constexpr auto kIsDebug = true;
#endif
} // namespace folly

namespace folly {
#if defined(_MSC_VER)
constexpr bool kIsMsvc = true;
#else
constexpr bool kIsMsvc = false;
#endif
} // namespace folly

namespace folly {
#if FOLLY_SANITIZE_THREAD
constexpr bool kIsSanitizeThread = true;
#else
constexpr bool kIsSanitizeThread = false;
#endif
} // namespace folly
