// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <liburing.h>
#include <sys/uio.h>
#include <coroutine>
#include <iostream>
#include "rocksdb/status.h"
#include "io_status.h"

namespace ROCKSDB_NAMESPACE {

struct file_page;

// used to store co_return value
struct ret_back {
  // whether the result has be co_returned
  bool result_set_ = false;
  // different return type by coroutine
  Status result_;
  IOStatus io_result_;
  bool posix_write_result_;
};

struct async_result {
  struct promise_type {
    async_result get_return_object() {
      auto h = std::coroutine_handle<promise_type>::from_promise(*this);
      ret_back_promise = new ret_back{};
      return async_result(h, *ret_back_promise);
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
      ret_back_promise->result_ = result;
      ret_back_promise->result_set_ = true;
    }

    void return_value(IOStatus io_result) {
      ret_back_promise->io_result_ = io_result;
      ret_back_promise->result_set_ = true;
    }

    void return_value(bool posix_write_result) {
      ret_back_promise->posix_write_result_ = posix_write_result;
      ret_back_promise->result_set_ = true;
    }

    promise_type* prev_ = nullptr;
    ret_back *ret_back_promise;
  };

  async_result() : async_(false) {}

  async_result(bool async, struct file_page* context) : async_(async), context_(context) {}

  async_result(std::coroutine_handle<promise_type> h, ret_back& ret_back) : h_{h} {
    ret_back_ = &ret_back;
  }

  bool await_ready() const noexcept {
    if (async_) 
      return false;
    else 
      return ret_back_->result_set_;
  }

  void await_suspend(std::coroutine_handle<promise_type> h);

  void await_resume() const noexcept {}

  Status result() { return ret_back_->result_; }

  IOStatus io_result() { return ret_back_->io_result_; }

  bool posix_result() { return ret_back_->posix_write_result_; }

  std::coroutine_handle<promise_type> h_;
  ret_back *ret_back_;
  bool async_ = false;
  struct file_page* context_;
};

// used for liburing read or write
struct file_page {
  file_page(int pages) {
    iov = (iovec*)calloc(pages, sizeof(struct iovec));
  }

  async_result::promise_type* promise;
  struct iovec *iov;
};

}// namespace ROCKSDB_NAMESPACE



