//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

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

  BlobContents(BlobContents&&) = default;
  BlobContents& operator=(BlobContents&&) = default;

  ~BlobContents() = default;

  const Slice& data() const { return data_; }
  size_t size() const { return data_.size(); }

  size_t ApproximateMemoryUsage() const;

  // Callbacks for secondary cache
  static size_t SizeCallback(void* obj);

  static Status SaveToCallback(void* from_obj, size_t from_offset,
                               size_t length, void* out);

  static Cache::CacheItemHelper* GetCacheItemHelper();

  static Status CreateCallback(CacheAllocationPtr&& allocation, const void* buf,
                               size_t size, void** out_obj, size_t* charge);

 private:
  BlobContents(CacheAllocationPtr&& allocation, size_t size)
      : allocation_(std::move(allocation)), data_(allocation_.get(), size) {}

  CacheAllocationPtr allocation_;
  Slice data_;
};

}  // namespace ROCKSDB_NAMESPACE
