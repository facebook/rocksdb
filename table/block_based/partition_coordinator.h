// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Interface for coordinating partition boundaries between the partitioned
// index builder and the partitioned filter builder.
//
// The partitioned filter builder needs to know:
// 1. When to cut a filter partition (aligned with index partitions)
// 2. What key to use as the partition boundary
// 3. Whether separators include sequence numbers (for choosing the
//    correct top-level index block builder)
//
// This interface decouples the filter builder from the concrete
// PartitionedIndexBuilder type, allowing the index to be a pluggable
// abstraction without leaking its concrete type to the filter.
class PartitionCoordinator {
 public:
  virtual ~PartitionCoordinator() = default;

  // Request that the index builder cut a partition at the next opportunity.
  // Called by the filter builder when it has accumulated enough keys.
  // The actual cut happens asynchronously — the filter must poll
  // ShouldCutFilterBlock() to check.
  virtual void RequestPartitionCut() = 0;

  // Returns true if the index builder has cut a partition since the last
  // call. The filter builder should cut its own partition in response.
  virtual bool ShouldCutFilterBlock() = 0;

  // Returns the partition boundary key from the index builder.
  // Used as the separator key in the filter's top-level index.
  virtual const std::string& GetPartitionKey() = 0;

  // Returns whether index separators include sequence numbers.
  // Controls which top-level index block builder the filter uses:
  // - true: separators are full internal keys (user_key + seq + type)
  // - false: separators are user keys only
  virtual bool separator_is_key_plus_seq() = 0;
};

}  // namespace ROCKSDB_NAMESPACE
