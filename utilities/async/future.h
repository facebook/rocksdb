//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "util/autovector.h"

// An implementation of primitives to track and wait for async calls. This
// is somewhat like std::future/std::promise and folly::Future/folly::Promise.
// Unlike folly, there is no built-in support for continuations. Compared to
// std::future, this implementation provides an efficient way to wait for many
// completions with low context switching overhead, much like folly::Collect.
//
// Usage is as follows -
// 1. Asynchronous processing
//    Future<Value> foo_async() {
//      Promise<Value> p = Promise<Value>::make();
//      /* Dispatch the request and save p on heap */
//      return p.GetFuture();
//    }
//
//    void foo_completion(Value val) {
//      ...
//      p.SetValue(val);
//    }
//
//    void foo_caller() {
//      Future<Value> f = foo_async();
//      f.Wait();
//      assert(f.IsReady());
//
//      Value val = f.Value();
//    }
//
// 2. Sycnchronous processing
//    Future<Value> foo_async() {
//      return Future<Value>(Value());
//    }
//
namespace ROCKSDB_NAMESPACE {

// Shared state between a Promise and Future
template <class T>
struct SharedFutureState {
  // State of callback
  typedef enum {
    kUninit,      // Uninitialized (no value or callback registered)
    kNotReady,    // Callback registered, but no value yet
    kNoCallback,  // Value is ready, but no callback registered
    kDone,        // Value is set and callback has been invoked
  } State;
  std::atomic<State> state_;
  std::function<void()> signal_fn_;  // Callback to be invoked when value is
                                     // ready
  union {
    T value_;  // Will be explicitly constructed when the value is ready
  };

  SharedFutureState() : state_(State::kUninit) {}
  ~SharedFutureState() {
    assert(state_.load() > State::kUninit);
    value_.~T();
  }
};

template <class T>
class Future;
template <class T>
class Promise;
template <class T>
Future<std::vector<T>> CollectAll(std::vector<Future<T>>&);

template <class T>
class Future {
 public:
  explicit Future(std::shared_ptr<SharedFutureState<T>> shared)
      : shared_(shared) {}
  Future() {}
  // Construct by value (no shared state)
  Future(T&& val) : val_(std::move(val)) {}

  ~Future() {}

  bool IsReady() {
    return !shared_ ||
           shared_->state_.load() > SharedFutureState<T>::State::kNotReady;
  }

  T& Value() {
    assert(IsReady());
    if (shared_) {
      return shared_->value_;
    } else {
      return val_;
    }
  }

  // Can be called only once. Behavior is undefined otherwise
  // The callback can be called in another thread, or immediately if the
  // value is ready
  void SetCallback(std::function<void()> fn) {
    auto state = shared_->state_.load();
    shared_->signal_fn_ = fn;
    if (state == SharedFutureState<T>::State::kUninit) {
      if (shared_->state_.compare_exchange_strong(
              state, SharedFutureState<T>::State::kNotReady)) {
        return;
      }
      state = shared_->state_.load();
    }
    if (state == SharedFutureState<T>::State::kNoCallback) {
      shared_->state_.store(SharedFutureState<T>::State::kDone);
      (shared_->signal_fn_)();
      return;
    }
    assert(false);
  }

  void Wait() {
    struct WaitContext {
      port::Mutex mu;
      port::CondVar cv;

      WaitContext() : cv(&mu) {}
    };
    WaitContext ctx;

    SetCallback([&ctx]() {
      ctx.mu.Lock();
      ctx.cv.SignalAll();
      ctx.mu.Unlock();
    });
    ctx.mu.Lock();
    if (!IsReady()) {
      ctx.cv.Wait();
    }
    ctx.mu.Unlock();
  }

 private:
  friend Future<std::vector<T>> CollectAll<T>(std::vector<Future<T>>&);

  T val_;
  std::shared_ptr<SharedFutureState<T>> shared_;
};

template <class T>
class Promise {
 public:
  static Promise<T> make() { return Promise<T>(new SharedFutureState<T>()); }
  Promise() {}
  Promise(SharedFutureState<T>* shared) : shared_(shared) {}
  ~Promise() {}

  // Set the value and call the callback, if one is registered
  void SetValue(T&& value) {
    assert(shared_ != nullptr);
    new (&shared_->value_) T(std::move(value));

    auto state = shared_->state_.load();
    if (state == SharedFutureState<T>::State::kUninit) {
      if (shared_->state_.compare_exchange_strong(
              state, SharedFutureState<T>::State::kNoCallback)) {
        return;
      }
      state = shared_->state_.load();
    }
    if (state == SharedFutureState<T>::State::kNotReady) {
      shared_->state_.store(SharedFutureState<T>::State::kDone);
      (shared_->signal_fn_)();
      return;
    }
    assert(false);
  }

  Future<T> GetFuture() {
    assert(shared_ != nullptr);
    return Future<T>(shared_);
  }

 private:
  std::shared_ptr<SharedFutureState<T>> shared_;
};

// Return a single Future with std::vector<T> as the value. The Future will
// be ready when all the input futures are ready
template <class T>
Future<std::vector<T>> CollectAll(std::vector<Future<T>>& futures) {
  struct CollectContext {
    Promise<std::vector<T>> p;
    std::atomic<size_t> num_events;
    std::vector<T> results;

    explicit CollectContext(size_t n)
        : p(Promise<std::vector<T>>::make()), num_events(n), results(n) {}
  };
  size_t num_events = futures.size();
  size_t i = 0;

  // Fast path
  std::vector<T> results(num_events);
  for (auto& future : futures) {
    if (!future.shared_) {
      results[i] = future.Value();
      num_events--;
    }
    i++;
  }
  if (!num_events) {
    return Future<std::vector<T>>(std::move(results));
  }

  auto ctx = std::make_shared<CollectContext>(num_events);
  ctx->results = std::move(results);
  i = 0;
  for (Future<T>& future : futures) {
    if (future.shared_) {
      future.SetCallback([&future, ctx, i]() {
        T result = future.Value();
        ctx->results[i] = result;
        size_t val = ctx->num_events.fetch_sub(1);
        if (val == 1) {
          ctx->p.SetValue(std::move(ctx->results));
        }
      });
    }
    i++;
  }

  return ctx->p.GetFuture();
}
}  // namespace ROCKSDB_NAMESPACE
