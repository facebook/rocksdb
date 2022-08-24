//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "memory/memory_allocator.h"
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
