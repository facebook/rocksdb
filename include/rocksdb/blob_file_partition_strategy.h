// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

// Public interface for customizing blob direct write partition assignment.
// Implementations may keep internal state, but SelectPartition() may be called
// concurrently from multiple writer threads, so any internal mutation must be
// thread-safe.
class BlobFilePartitionStrategy {
 public:
  virtual ~BlobFilePartitionStrategy() = default;

  // Returns a name that identifies this strategy for debugging and logging.
  virtual const char* Name() const = 0;

  // Select a partition for the given blob direct write.
  //
  // `value` is the original uncompressed user value, even when blob
  // compression is enabled for the target blob file. The return value can be
  // any uint32_t; the caller applies modulo num_partitions internally.
  virtual uint32_t SelectPartition(uint32_t num_partitions,
                                   uint32_t column_family_id, const Slice& key,
                                   const Slice& value) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
