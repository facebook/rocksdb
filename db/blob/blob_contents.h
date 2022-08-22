//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "memory/memory_allocator.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// A class representing a single value read from a blob file.
class BlobContents {
 public:
  static std::unique_ptr<BlobContents> Create(CacheAllocationPtr&& allocation,
                                              size_t size) {
    return std::unique_ptr<BlobContents>(
        new BlobContents(std::move(allocation), size));
  }

  BlobContents(const BlobContents&) = delete;
  BlobContents& operator=(const BlobContents&) = delete;

  const Slice& data() const { return data_; }
  size_t size() const { return data_.size(); }

 private:
  BlobContents(CacheAllocationPtr&& allocation, size_t size)
      : allocation_(std::move(allocation)), data_(allocation_.get(), size) {}

  CacheAllocationPtr allocation_;
  Slice data_;
};

}  // namespace ROCKSDB_NAMESPACE
