//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <type_traits>

#include "port/likely.h"
#include "rocksdb/cache.h"
#include "rocksdb/cleanable.h"

namespace ROCKSDB_NAMESPACE {

// CachableEntry is a handle to an object that may or may not be in the block
// cache. It is used in a variety of ways:
//
// 1) It may refer to an object in the block cache. In this case, cache_ and
// cache_handle_ are not nullptr, and the cache handle has to be released when
// the CachableEntry is destroyed (the lifecycle of the cached object, on the
// other hand, is managed by the cache itself).
// 2) It may uniquely own the (non-cached) object it refers to (examples include
// a block read directly from file, or uncompressed blocks when there is a
// compressed block cache but no uncompressed block cache). In such cases, the
// object has to be destroyed when the CachableEntry is destroyed.
// 3) It may point to an object (cached or not) without owning it. In this case,
// no action is needed when the CachableEntry is destroyed.
// 4) Sometimes, management of a cached or owned object (see #1 and #2 above)
// is transferred to some other object. This is used for instance with iterators
// (where cleanup is performed using a chain of cleanup functions,
// see Cleanable).
//
// Because of #1 and #2 above, copying a CachableEntry is not safe (and thus not
// allowed); hence, this is a move-only type, where a move transfers the
// management responsibilities, and leaves the source object in an empty state.

template <class T>
class CachableEntry {
 public:
  CachableEntry() = default;

  CachableEntry(T* value, Cache* cache, Cache::Handle* cache_handle,
                bool own_value)
      : value_(value),
        cache_(cache),
        cache_handle_(cache_handle),
        own_value_(own_value) {
    assert(value_ != nullptr ||
           (cache_ == nullptr && cache_handle_ == nullptr && !own_value_));
    assert(!!cache_ == !!cache_handle_);
    assert(!cache_handle_ || !own_value_);
  }

  CachableEntry(const CachableEntry&) = delete;
  CachableEntry& operator=(const CachableEntry&) = delete;

  CachableEntry(CachableEntry&& rhs) noexcept
      : value_(rhs.value_),
        cache_(rhs.cache_),
        cache_handle_(rhs.cache_handle_),
        own_value_(rhs.own_value_) {
    assert(value_ != nullptr ||
           (cache_ == nullptr && cache_handle_ == nullptr && !own_value_));
    assert(!!cache_ == !!cache_handle_);
    assert(!cache_handle_ || !own_value_);

    rhs.ResetFields();
  }

  CachableEntry& operator=(CachableEntry&& rhs) noexcept {
    if (UNLIKELY(this == &rhs)) {
      return *this;
    }

    ReleaseResource();

    value_ = rhs.value_;
    cache_ = rhs.cache_;
    cache_handle_ = rhs.cache_handle_;
    own_value_ = rhs.own_value_;

    assert(value_ != nullptr ||
           (cache_ == nullptr && cache_handle_ == nullptr && !own_value_));
    assert(!!cache_ == !!cache_handle_);
    assert(!cache_handle_ || !own_value_);

    rhs.ResetFields();

    return *this;
  }

  ~CachableEntry() { ReleaseResource(); }

  bool IsEmpty() const {
    return value_ == nullptr && cache_ == nullptr && cache_handle_ == nullptr &&
           !own_value_;
  }

  bool IsCached() const {
    assert(!!cache_ == !!cache_handle_);

    return cache_handle_ != nullptr;
  }

  T* GetValue() const { return value_; }
  Cache* GetCache() const { return cache_; }
  Cache::Handle* GetCacheHandle() const { return cache_handle_; }
  bool GetOwnValue() const { return own_value_; }

  void Reset() {
    ReleaseResource();
    ResetFields();
  }

  void TransferTo(Cleanable* cleanable) {
    if (cleanable) {
      if (cache_handle_ != nullptr) {
        assert(cache_ != nullptr);
        cleanable->RegisterCleanup(&ReleaseCacheHandle, cache_, cache_handle_);
      } else if (own_value_) {
        cleanable->RegisterCleanup(&DeleteValue, value_, nullptr);
      }
    }

    ResetFields();
  }

  void SetOwnedValue(std::unique_ptr<T>&& value) {
    assert(value.get() != nullptr);

    if (UNLIKELY(value_ == value.get() && own_value_)) {
      assert(cache_ == nullptr && cache_handle_ == nullptr);
      return;
    }

    Reset();

    value_ = value.release();
    own_value_ = true;
  }

  void SetUnownedValue(T* value) {
    assert(value != nullptr);

    if (UNLIKELY(value_ == value && cache_ == nullptr &&
                 cache_handle_ == nullptr && !own_value_)) {
      return;
    }

    Reset();

    value_ = value;
    assert(!own_value_);
  }

  void SetCachedValue(T* value, Cache* cache, Cache::Handle* cache_handle) {
    assert(cache != nullptr);
    assert(cache_handle != nullptr);

    if (UNLIKELY(value_ == value && cache_ == cache &&
                 cache_handle_ == cache_handle && !own_value_)) {
      return;
    }

    Reset();

    value_ = value;
    cache_ = cache;
    cache_handle_ = cache_handle;
    assert(!own_value_);
  }

  void UpdateCachedValue() {
    assert(cache_ != nullptr);
    assert(cache_handle_ != nullptr);

    value_ = static_cast<T*>(cache_->Value(cache_handle_));
  }

  bool IsReady() {
    if (!own_value_) {
      assert(cache_ != nullptr);
      assert(cache_handle_ != nullptr);
      return cache_->IsReady(cache_handle_);
    }
    return true;
  }

  // Since this class is essentially an elaborate pointer, it's sometimes
  // useful to be able to upcast or downcast the base type of the pointer,
  // especially when interacting with typed_cache.h.
  template <class TWrapper>
  std::enable_if_t<sizeof(TWrapper) == sizeof(T) &&
                       (std::is_base_of_v<TWrapper, T> ||
                        std::is_base_of_v<T, TWrapper>),
                   /* Actual return type */
                   CachableEntry<TWrapper>&>
  As() {
    CachableEntry<TWrapper>* result_ptr =
        reinterpret_cast<CachableEntry<TWrapper>*>(this);
    // Ensure no weirdness in template instantiations
    assert(static_cast<void*>(&this->value_) ==
           static_cast<void*>(&result_ptr->value_));
    assert(&this->cache_handle_ == &result_ptr->cache_handle_);
    // This function depends on no arithmetic involved in the pointer
    // conversion, which is not statically checkable.
    assert(static_cast<void*>(this->value_) ==
           static_cast<void*>(result_ptr->value_));
    return *result_ptr;
  }

 private:
  void ReleaseResource() noexcept {
    if (LIKELY(cache_handle_ != nullptr)) {
      assert(cache_ != nullptr);
      cache_->Release(cache_handle_);
    } else if (own_value_) {
      delete value_;
    }
  }

  void ResetFields() noexcept {
    value_ = nullptr;
    cache_ = nullptr;
    cache_handle_ = nullptr;
    own_value_ = false;
  }

  static void ReleaseCacheHandle(void* arg1, void* arg2) {
    Cache* const cache = static_cast<Cache*>(arg1);
    assert(cache);

    Cache::Handle* const cache_handle = static_cast<Cache::Handle*>(arg2);
    assert(cache_handle);

    cache->Release(cache_handle);
  }

  static void DeleteValue(void* arg1, void* /* arg2 */) {
    delete static_cast<T*>(arg1);
  }

 private:
  // Have to be your own best friend
  template <class TT>
  friend class CachableEntry;

  T* value_ = nullptr;
  Cache* cache_ = nullptr;
  Cache::Handle* cache_handle_ = nullptr;
  bool own_value_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
