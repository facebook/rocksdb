//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Returns the cached value given a cache handle.
template <typename T>
T* GetFromCacheHandle(Cache* cache, Cache::Handle* handle) {
  assert(cache);
  assert(handle);

  return static_cast<T*>(cache->Value(handle));
}

// Simple generic deleter for Cache (to be used with Cache::Insert).
template <typename T>
void DeleteCacheEntry(const Slice& /* key */, void* value) {
  delete static_cast<T*>(value);
}

// Turns a T* into a Slice so it can be used as a key with Cache.
template <typename T>
Slice GetSlice(const T* t) {
  return Slice(reinterpret_cast<const char*>(t), sizeof(T));
}

// Generic resource management object for cache handles that releases the handle
// when destroyed. Has unique ownership of the handle, so copying it is not
// allowed, while moving it transfers ownership.
template <typename T>
class CacheHandleGuard {
 public:
  CacheHandleGuard() = default;

  CacheHandleGuard(Cache* cache, Cache::Handle* handle)
      : cache_(cache),
        handle_(handle),
        value_(GetFromCacheHandle<T>(cache, handle)) {
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

// Build an aliasing shared_ptr that keeps `handle` in cache while there
// are references, but the pointer is to the value for that cache entry,
// which must be of type T. This is copyable, unlike CacheHandleGuard, but
// does not provide access to caching details.
template <typename T>
std::shared_ptr<T> MakeSharedCacheHandleGuard(Cache* cache,
                                              Cache::Handle* handle) {
  auto wrapper = std::make_shared<CacheHandleGuard<T>>(cache, handle);
  return std::shared_ptr<T>(wrapper, static_cast<T*>(cache->Value(handle)));
}

}  // namespace ROCKSDB_NAMESPACE
