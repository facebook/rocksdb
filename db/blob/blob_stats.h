//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines a collection of statistics collectors.
#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class BlobStats {
 public:
  void AddBlob(uint64_t bytes) {
    ++count_;
    bytes_ += bytes;
  }
  void AddBlobs(uint64_t count, uint64_t bytes) {
    count_ += count;
    bytes_ += bytes;
  }

  uint64_t GetCount() const { return count_; }
  uint64_t GetBytes() const { return bytes_; }

 private:
  uint64_t count_ = 0;
  uint64_t bytes_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
