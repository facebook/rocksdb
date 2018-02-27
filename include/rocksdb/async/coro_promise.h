// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Experimental coroutines support to explore async implementations.
//
// For detailed information see: 
// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2017/n4649.pdf
//
// Quick cookbooks
// https://youtu.be/ZTqHjjm86Bw
// https://youtu.be/8C8NnE1Dg4A?t=2414
// 

#pragma once

#ifndef STORAGE_ROCKSDB_INCLUDE_CORO_PROMISE_H_
#define STORAGE_ROCKSDB_INCLUDE_CORO_PROMISE_H_

#if !(defined ROCKSDB_COROUTINES)
#error "Only for Coroutines support"
#endif

#if !(defined _MSC_VER)
// also LLVM/Clang supports this but I have not tested
#error "Currently support for MS compiler only"
#endif

#include <experimental/resumable>

namespace rocksdb {
// This promise enables few things:
// - capture the callers coroutine handle so it can be resumed(called back)
// - preserve the existing functions signature that usually (but not always)
//      return Status
// - deliver the actual return value via result_ (most of the time Status)
// This lacks get_return_object that is to be implemented in the return type
// specific manner
template<typename Result>
struct PromiseType
{
  using coroutine_handle = std::experimental::coroutine_handle<>;
  // Caller coroutine handle. essentially a callback
  coroutine_handle   cb_;
  // Result delivered here
  Result             result_;

  PromiseType() : cb_(nullptr) {}

  // Always suspend, return to the caller
  // and capture the callers handle and then immediately resume
  // AwaitableStatus in status.h where co_await is overloaded on Status
  std::experimental::suspend_always initial_suspend() {
    return{};
  }
  // Return the awaiter so we can resume the calling coroutine
  // essentially invoking the callback that points the next statement
  // of the caller
  auto final_suspend() {
    struct CallbackAwaiterInvoker {
      PromiseType* me_;
      explicit CallbackAwaiterInvoker(PromiseType* p) : me_(p) {}
      bool await_ready() { return false; }
      void await_suspend(coroutine_handle) {
        me_->cb_.resume();
      }
      void await_resume() {}
    };
    return CallbackAwaiterInvoker{ this };
  }
  // Called automatically by co_return statement
  template<typename U>
  void return_value(U&& v) {
    result_ = std::forward<U&&>(v);
  }
};
} // rocksdb

#endif // STORAGE_ROCKSDB_INCLUDE_CORO_PROMISE_H_

