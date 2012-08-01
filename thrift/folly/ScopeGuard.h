/*
 * Copyright 2012 Facebook, Inc.
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

#ifndef FOLLY_SCOPEGUARD_H_
#define FOLLY_SCOPEGUARD_H_

#include <cstddef>
#include <iostream>
#include <functional>
#include <new>
//#include <glog/logging.h>

#include "folly/Preprocessor.h"

namespace folly {

/**
 * ScopeGuard is a general implementation of the "Initilization is
 * Resource Acquisition" idiom.  Basically, it guarantees that a function
 * is executed upon leaving the currrent scope unless otherwise told.
 *
 * The makeGuard() function is used to create a new ScopeGuard object.
 * It can be instantiated with a lambda function, a std::function<void()>,
 * a functor, or a void(*)() function pointer.
 *
 *
 * Usage example: Add a friend to memory iff it is also added to the db.
 *
 * void User::addFriend(User& newFriend) {
 *   // add the friend to memory
 *   friends_.push_back(&newFriend);
 *
 *   // If the db insertion that follows fails, we should
 *   // remove it from memory.
 *   // (You could also declare this as "auto guard = makeGuard(...)")
 *   ScopeGuard guard = makeGuard([&] { friends_.pop_back(); });
 *
 *   // this will throw an exception upon error, which
 *   // makes the ScopeGuard execute UserCont::pop_back()
 *   // once the Guard's destructor is called.
 *   db_->addFriend(GetName(), newFriend.GetName());
 *
 *   // an exception was not thrown, so don't execute
 *   // the Guard.
 *   guard.dismiss();
 * }
 *
 * Examine ScopeGuardTest.cpp for some more sample usage.
 *
 * Stolen from:
 *   Andrei's and Petru Marginean's CUJ article:
 *     http://drdobbs.com/184403758
 *   and the loki library:
 *     http://loki-lib.sourceforge.net/index.php?n=Idioms.ScopeGuardPointer
 *   and triendl.kj article:
 *     http://www.codeproject.com/KB/cpp/scope_guard.aspx
 */
class ScopeGuardImplBase {
 public:
  void dismiss() noexcept {
    dismissed_ = true;
  }

 protected:
  ScopeGuardImplBase()
    : dismissed_(false) {}

  ScopeGuardImplBase(ScopeGuardImplBase&& other)
    : dismissed_(other.dismissed_) {
    other.dismissed_ = true;
  }

  bool dismissed_;
};

template<typename FunctionType>
class ScopeGuardImpl : public ScopeGuardImplBase {
 public:
  explicit ScopeGuardImpl(const FunctionType& fn)
    : function_(fn) {}

  explicit ScopeGuardImpl(FunctionType&& fn)
    : function_(std::move(fn)) {}

  ScopeGuardImpl(ScopeGuardImpl&& other)
    : ScopeGuardImplBase(std::move(other)),
      function_(std::move(other.function_)) {
  }

  ~ScopeGuardImpl() noexcept {
    if (!dismissed_) {
      execute();
    }
  }

private:
  void* operator new(size_t) = delete;

  void execute() noexcept {
    try {
      function_();
    } catch (const std::exception& ex) {
      std::cout << "ScopeGuard cleanup function threw a " <<
        typeid(ex).name() << "exception: " << ex.what();
    } catch (...) {
      std::cout << "ScopeGuard cleanup function threw a non-exception object";
    }
  }

  FunctionType function_;
};

template<typename FunctionType>
ScopeGuardImpl<typename std::decay<FunctionType>::type>
makeGuard(FunctionType&& fn) {
  return ScopeGuardImpl<typename std::decay<FunctionType>::type>(
      std::forward<FunctionType>(fn));
}

/**
 * This is largely unneeded if you just use auto for your guards.
 */
typedef ScopeGuardImplBase&& ScopeGuard;

namespace detail {
/**
 * Internal use for the macro SCOPE_EXIT below
 */
enum class ScopeGuardOnExit {};

template <typename FunctionType>
ScopeGuardImpl<typename std::decay<FunctionType>::type>
operator+(detail::ScopeGuardOnExit, FunctionType&& fn) {
  return ScopeGuardImpl<typename std::decay<FunctionType>::type>(
      std::forward<FunctionType>(fn));
}
} // namespace detail

} // folly

#define SCOPE_EXIT \
  auto FB_ANONYMOUS_VARIABLE(SCOPE_EXIT_STATE) \
  = ::folly::detail::ScopeGuardOnExit() + [&]

#endif // FOLLY_SCOPEGUARD_H_
