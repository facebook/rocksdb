// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <coroutine>
#include <iostream>
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

template <typename T> 
struct async_result {

  struct promise_type {

    async_result get_return_object() {
      auto h = std::coroutine_handle<promise_type>::from_promise(*this);
      return async_result(h);
    }

    auto initial_suspend() { return std::suspend_never{};}

    auto final_suspend() noexcept {
      if (prev_ != nullptr) {
        auto h = std::coroutine_handle<promise_type>::from_promise(*prev_);
        h.resume();
      }

      return std::suspend_never{};
    }

    void unhandled_exception() { std::exit(1); }

    void return_value(T result) { 
      result_ = result; 
      result_set_ = true;
    }

    promise_type* prev_ = nullptr;
    T result_;
    bool result_set_ = false;
  };

  async_result(bool async = false) : async_(async) {}

  async_result(std::coroutine_handle<promise_type> h) : h_{h} {}

  constexpr bool await_ready() const noexcept { 
    //std::cout<<"h_.done():"<<h_.done()<<"\n";
    //std::cout<<"result_set_:"<<h_.promise().result_set_<<"\n";
    return h_.promise().result_set_;
  }

  void await_suspend(std::coroutine_handle<promise_type> h) {
    if (!async_) 
      h_.promise().prev_ = &h.promise();

    if (async_) {
      // TODO: really go off async
    }
  }

  void await_resume() const noexcept {}

  T result() { return h_.promise().result_; }

  std::coroutine_handle<promise_type> h_;
  bool async_ = false;
};
}// namespace ROCKSDB_NAMESPACE



