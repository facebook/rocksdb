//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <folly/Traits.h>

#include <utility>
#include <type_traits>

namespace folly {
namespace scope_guard_detail {
template <typename F>
class ScopeGuardImpl {
 public:
  explicit ScopeGuardImpl(F&& f) : f_{std::forward<F>(f)} {}
  ~ScopeGuardImpl() {
    f_();
  }

 private:
  F f_;
};

enum class ScopeGuardEnum {};
template <typename Func, typename DecayedFunc = _t<std::decay<Func>>>
ScopeGuardImpl<DecayedFunc> operator+(ScopeGuardEnum, Func&& func) {
  return ScopeGuardImpl<DecayedFunc>{std::forward<Func>(func)};
}
} // namespace scope_guard_detail
} // namespace folly

/**
 * FB_ANONYMOUS_VARIABLE(str) introduces an identifier starting with
 * str and ending with a number that varies with the line.
 */
#ifndef FB_ANONYMOUS_VARIABLE
#define FB_CONCATENATE_IMPL(s1, s2) s1##s2
#define FB_CONCATENATE(s1, s2) FB_CONCATENATE_IMPL(s1, s2)
#ifdef __COUNTER__
#define FB_ANONYMOUS_VARIABLE(str) \
  FB_CONCATENATE(FB_CONCATENATE(FB_CONCATENATE(str, __COUNTER__), _), __LINE__)
#else
#define FB_ANONYMOUS_VARIABLE(str) FB_CONCATENATE(str, __LINE__)
#endif
#endif

#ifndef SCOPE_EXIT
#define SCOPE_EXIT                                    \
    auto FB_ANONYMOUS_VARIABLE(SCOPE_EXIT_STATE) =    \
        ::folly::scope_guard_detail::ScopeGuardEnum{} + [&]() noexcept
#endif
