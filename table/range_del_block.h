//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>

#include "rocksdb/slice.h"
#include "table/block_based/block_builder.h"

namespace ROCKSDB_NAMESPACE {

// Encodes range tombstones in RocksDB's canonical range deletion block format.
// The serialized block is an uncompressed payload without a block trailer.
class RangeDelBlockBuilder {
 public:
  RangeDelBlockBuilder(size_t timestamp_size,
                       bool persist_user_defined_timestamps);

  ~RangeDelBlockBuilder() = default;

  RangeDelBlockBuilder(const RangeDelBlockBuilder&) = delete;
  RangeDelBlockBuilder& operator=(const RangeDelBlockBuilder&) = delete;
  RangeDelBlockBuilder(RangeDelBlockBuilder&&) = delete;
  RangeDelBlockBuilder& operator=(RangeDelBlockBuilder&&) = delete;

  // `internal_start_key` has kTypeRangeDeletion and `end_user_key` is the
  // exclusive range end.
  void Add(const Slice& internal_start_key, const Slice& end_user_key);

  bool empty() const { return block_builder_.empty(); }

  size_t CurrentSizeEstimate() const {
    return block_builder_.CurrentSizeEstimate();
  }

  // The returned Slice is owned by this builder.
  Slice Finish() { return block_builder_.Finish(); }

 private:
  const size_t strip_timestamp_size_;
  BlockBuilder block_builder_;
};

}  // namespace ROCKSDB_NAMESPACE
