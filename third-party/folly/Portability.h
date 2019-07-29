/*
 * Copyright 2011-present Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

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
