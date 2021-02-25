//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

// This file implements a thread local object allocation pool for
// async IO. The pool caches objects for reuse in order to minimize the
// memory allocation overhead. Usage is as follows -
// 1. Define a sub-class of struct Context
//    struct MyContext : public Context {
//    };
// 2. Define a template specialization  of ThreadLocalContextPool. For
//    example -
//    static thread_local ThreadLocalContextPool<MyContext> pool;
// 3. Call Allocate() and Free()
namespace ROCKSDB_NAMESPACE {
struct Context {
  virtual ~Context() {}
  Context() : next_(nullptr) {}
  Context(Context&&) = delete;
  Context(const Context&) = delete;
  void operator=(const Context&) = delete;

  void SetNext(Context* ctx) { next_ = ctx; }

  Context* Next() { return next_; }

  Context* next_;
};

class ContextPool {
 public:
  virtual ~ContextPool() {}
  virtual void Clear() = 0;
};

template <typename T>
class ThreadLocalContextPool : public ContextPool {
 public:
  ThreadLocalContextPool() : count_(0), cache_limit_(8), head_(nullptr) {}
  ~ThreadLocalContextPool() {
    cache_limit_ = 0;
    Clear();
  }

  template <typename... Args>
  T* Allocate(Args&&... args) {
    T* ctx;

    if (head_) {
      ctx = static_cast<T*>(head_);
      head_ = ctx->Next();
      ctx->SetNext(nullptr);
      count_--;
      ctx = new (ctx) T(std::forward<Args>(args)...);
    } else {
      ctx = new T(std::forward<Args>(args)...);
    }

    return ctx;
  }

  void Free(T* t) {
    // t->Reset();
    t->~T();
    assert(!t->next_);
    if (head_) {
      t->SetNext(head_);
    }
    head_ = t;
    count_++;
  }

  void Clear() {
    while (count_-- > cache_limit_) {
      Context* next = head_;
      head_ = next->Next();
      operator delete(next);
    }
  }

 private:
  size_t count_;
  size_t cache_limit_;
  Context* head_;
};
}  // namespace ROCKSDB_NAMESPACE
