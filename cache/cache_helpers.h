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
void DeleteEntry(const Slice& /*key*/, void* value) {
  delete static_cast<T*>(value);
}

template <typename T>
Slice GetSlice(const T* t) {
  return Slice(reinterpret_cast<const char*>(t), sizeof(T));
}

}  // namespace ROCKSDB_NAMESPACE
