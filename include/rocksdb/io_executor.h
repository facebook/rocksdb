//  Copyright (c) Meta Platforms, Inc. and affiliates.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// IOExecutor controls the execution of async IO operations. It is used to
// control the amount specific operation (i.e., scans) prefetch.
#pragma once

#include <cstddef>

namespace ROCKSDB_NAMESPACE {

class IOExecutor {
 public:
  IOExecutor(size_t max_inflight_bytes, size_t max_inflight_ops)
      : max_inflight_ops_(max_inflight_ops),
        max_bytes_inflight_(max_inflight_bytes) {}

 private:
  size_t max_inflight_ops_;
  size_t max_bytes_inflight_;
};

}  // namespace ROCKSDB_NAMESPACE
