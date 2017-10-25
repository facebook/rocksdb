//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include <assert.h>
#include <string.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <mutex>
#include <typeinfo>
#include <type_traits>

namespace rocksdb {
namespace async {

// This class facilitates pooling of the RequestsContexts
// It has a limit to the number of the contexts that can be
// stored and re-used. This is done to avoid memory allocation
// and the contention that is associated with it
// The pool returns a nullptr if there are no contexts available
// In that case the client needs to create one anew
//
// On context release if there is not enough space the pool destroys
// the context. Contexts needs to support LinkAhead() and Unlink() methods
// that allow them to be linked in memory quickly.
// Released and requested entries are always linked/removed to/from the head
// so the most recently used entry is re-used.
//
// The request instances need to support re-used. That is the entries that are placed
// in the pool needs to be reset from any residual of the most recent execution
// so when they are re-used, they are indistinguishable from instances that are 
// just created anew. For that reason, requesting the entry from the pool will also
// involve its re-initialization for the current request in progress.

// This class facilitates memory chunks caching
// and re-using for a specific class type
// It does not pool memory although this can be done

template<class T>
class ContextPool;

namespace context_pool_detail {
template<class T>
struct pooled_deletor {
  ContextPool<T>* p_;
  explicit pooled_deletor(ContextPool<T>* p) :
    p_(p) {
  }
  void operator()(T* t) const {
    if (t) p_->Release(t);
  }
};
}

template<class T>
class ContextPool {

  struct CachedChunck {
    CachedChunck* next_;
    explicit CachedChunck(CachedChunck* next) :
      next_(next) {
    }
  };

public:
  ContextPool(const ContextPool&) = delete;
  ContextPool& operator=(const ContextPool&) = delete;

  ContextPool() {}

  ~ContextPool() {
    ReportStats();
    Clear();
  }

  void Release(T* v) {
    assert(v != nullptr);
    v->~T();
    Link(v);
  }

  using
  Deletor = context_pool_detail::pooled_deletor<T>;

  using
  Ptr = std::unique_ptr<T, Deletor>;

  template<class... Args>
  Ptr Get(Args... args) {

    using
    AllocType = typename std::aligned_union<sizeof(T), T, CachedChunck>::type;

    void* p = Unlink();
    if (p == nullptr) {
      p = new AllocType;
    }
    // This is not exception safe
    // but we do not throw
    T* result = new (p) T(std::forward<Args>(args)...);
    Ptr ptr(result, Deletor(this));
    return ptr;
  }

private:

  void Link(void* v) {
    std::lock_guard<std::mutex> l(m_);
    auto to_link = new(v) CachedChunck(head_);
    head_ = to_link;
    ++count_;
    max_items_ = std::max(count_, max_items_);
  }

  CachedChunck* Unlink() {
    CachedChunck* result = nullptr;
    std::lock_guard<std::mutex> l(m_);
    if (head_) {
      result = head_;
      head_ = head_->next_;
      --count_;
    }
    return result;
  }

  void Clear() {

    using
    AllocType = typename std::aligned_union<sizeof(T), T, CachedChunck>::type;

    while (head_ != nullptr) {
      auto h = head_;
      head_ = head_->next_;
      delete reinterpret_cast<AllocType*>(h);
    }
  }

  void ReportStats() {
    std::cout << "ctx_pool: " << typeid(T).name() <<
      " max_items: " << max_items_ << " max_charge: " <<
      max_items_ * sizeof(T) << std::endl;
  }

  std::mutex     m_;
  CachedChunck*  head_ = nullptr;
  size_t         count_ = 0;
  size_t         max_items_ = 0;
};

}
}
