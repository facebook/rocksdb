//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

template <typename T>
T* GetFromHandle(Cache* cache, Cache::Handle* handle) {
  assert(cache);
  assert(handle);

  return static_cast<T*>(cache->Value(handle));
}

template <typename T>
void DeleteEntry(const Slice& /* key */, void* value) {
  delete static_cast<T*>(value);
}

template <typename T>
Slice GetSlice(const T* t) {
  return Slice(reinterpret_cast<const char*>(t), sizeof(T));
}

template <typename T>
class CacheHandleGuard {
 public:
  CacheHandleGuard() = default;

  CacheHandleGuard(Cache* cache, Cache::Handle* handle)
      : cache_(cache),
        handle_(handle),
        value_(GetFromHandle<T>(cache, handle)) {
    assert(cache_ && handle_ && value_);
  }

  CacheHandleGuard(const CacheHandleGuard&) = delete;
  CacheHandleGuard& operator=(const CacheHandleGuard&) = delete;

  CacheHandleGuard(CacheHandleGuard&& rhs) noexcept
      : cache_(rhs.cache_), handle_(rhs.handle_), value_(rhs.value_) {
    assert((!cache_ && !handle_ && !value_) || (cache_ && handle_ && value_));

    rhs.ResetFields();
  }

  CacheHandleGuard& operator=(CacheHandleGuard&& rhs) noexcept {
    if (this == &rhs) {
      return *this;
    }

    ReleaseHandle();

    cache_ = rhs.cache_;
    handle_ = rhs.handle_;
    value_ = rhs.value_;

    assert((!cache_ && !handle_ && !value_) || (cache_ && handle_ && value_));

    rhs.ResetFields();

    return *this;
  }

  ~CacheHandleGuard() { ReleaseHandle(); }

  bool IsEmpty() const { return !handle_; }

  Cache* GetCache() const { return cache_; }
  Cache::Handle* GetCacheHandle() const { return handle_; }
  T* GetValue() const { return value_; }

  void Reset() {
    ReleaseHandle();
    ResetFields();
  }

 private:
  void ReleaseHandle() {
    if (IsEmpty()) {
      return;
    }

    assert(cache_);
    cache_->Release(handle_);
  }

  void ResetFields() {
    cache_ = nullptr;
    handle_ = nullptr;
    value_ = nullptr;
  }

 private:
  Cache* cache_ = nullptr;
  Cache::Handle* handle_ = nullptr;
  T* value_ = nullptr;
};

}  // namespace ROCKSDB_NAMESPACE
