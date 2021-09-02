// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <coroutine>
#include <iostream>
#include "rocksdb/status.h"
#include "io_status.h"

namespace ROCKSDB_NAMESPACE {

struct async_wal_result {
  struct promise_type {
    async_wal_result get_return_object() {
      auto h = std::coroutine_handle<promise_type>::from_promise(*this);
      return async_wal_result(h);
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

    void return_value(Status result) {
      result_ = result;
      result_set_ = true;
    }

    void return_value(IOStatus io_result) {
      io_result_ = io_result;
      result_set_ = true;
    }

    void return_value(bool posix_write_result) {
      posix_write_result_ = posix_write_result;
      result_set_ = true;
    }

    promise_type* prev_ = nullptr;
    Status result_;
    IOStatus io_result_;
    bool posix_write_result_;
    bool result_set_ = false;
  };

  async_wal_result() : async_(false) {}

  async_wal_result(bool async) : async_(async) {}

  async_wal_result(std::coroutine_handle<promise_type> h) : h_{h} {}

  bool await_ready() const noexcept {
    if (async_) {
      return false;
    } else {
      std::cout<<"h_.done():"<<h_.done()<<"\n";
      std::cout<<"result_set_:"<<h_.promise().result_set_<<"\n";
      return h_.promise().result_set_;
    }
  }

  void await_suspend(std::coroutine_handle<promise_type> h);

  /*
  void await_suspend(std::coroutine_handle<promise_type> h) {
    if (!async_)
      h_.promise().prev_ = &h.promise();
    else
      context_->promise = &h.promise();
  }*/

  void await_resume() const noexcept {}

  Status result() { return h_.promise().result_; }

  IOStatus io_result() { return h_.promise().io_result_; }

  bool posix_result() { return h_.promise().posix_write_result_; }

  std::coroutine_handle<promise_type> h_;
  bool async_ = false;
};

}// namespace ROCKSDB_NAMESPACE



