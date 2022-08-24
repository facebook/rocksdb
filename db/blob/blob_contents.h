//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <memory>

#include "cache/cache_helpers.h"
#include "memory/memory_allocator.h"
#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A class representing a single uncompressed value read from a blob file.
class BlobContents {
 public:
  static std::unique_ptr<BlobContents> Create(CacheAllocationPtr&& allocation,
                                              size_t size);

  BlobContents(const BlobContents&) = delete;
  BlobContents& operator=(const BlobContents&) = delete;

  const Slice& data() const { return data_; }
  size_t size() const { return data_.size(); }

  // Callbacks for secondary cache
  static size_t SizeCallback(void* obj);

  static Status SaveToCallback(void* from_obj, size_t from_offset,
                               size_t length, void* out);

  static void DeleteCallback(const Slice& key, void* value);

  static Cache::CacheItemHelper* GetCacheItemHelper();

  static Status CreateCallback(const void* buf, size_t size, void** out_obj,
                               size_t* charge);

 private:
  BlobContents(CacheAllocationPtr&& allocation, size_t size)
      : allocation_(std::move(allocation)), data_(allocation_.get(), size) {}

  CacheAllocationPtr allocation_;
  Slice data_;
};

inline std::unique_ptr<BlobContents> BlobContents::Create(
    CacheAllocationPtr&& allocation, size_t size) {
  return std::unique_ptr<BlobContents>(
      new BlobContents(std::move(allocation), size));
}

inline size_t BlobContents::SizeCallback(void* obj) {
  assert(obj);

  return static_cast<const BlobContents*>(obj)->size();
}

inline Status BlobContents::SaveToCallback(void* from_obj, size_t from_offset,
                                           size_t length, void* out) {
  assert(from_obj);

  const BlobContents* buf = static_cast<const BlobContents*>(from_obj);
  assert(buf->size() >= from_offset + length);

  memcpy(out, buf->data().data() + from_offset, length);

  return Status::OK();
}

inline void BlobContents::DeleteCallback(const Slice& key, void* value) {
  DeleteCacheEntry<BlobContents>(key, value);
}

inline Cache::CacheItemHelper* BlobContents::GetCacheItemHelper() {
  static Cache::CacheItemHelper cache_helper(&SizeCallback, &SaveToCallback,
                                             &DeleteCallback);

  return &cache_helper;
}

inline Status BlobContents::CreateCallback(const void* buf, size_t size,
                                           void** out_obj, size_t* charge) {
  CacheAllocationPtr allocation(new char[size]);
  memcpy(allocation.get(), buf, size);

  std::unique_ptr<BlobContents> obj = Create(std::move(allocation), size);
  BlobContents* const contents = obj.release();

  *out_obj = contents;
  *charge = contents->size();

  return Status::OK();
};

}  // namespace ROCKSDB_NAMESPACE
