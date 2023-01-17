//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/block_based/data_block_hash_index.h"

namespace ROCKSDB_NAMESPACE {

class BlockBuilder {
 public:
  BlockBuilder(const BlockBuilder&) = delete;
  void operator=(const BlockBuilder&) = delete;

  explicit BlockBuilder(int block_restart_interval,
                        bool use_delta_encoding = true,
                        bool use_value_delta_encoding = false,
                        BlockBasedTableOptions::DataBlockIndexType index_type =
                            BlockBasedTableOptions::kDataBlockBinarySearch,
                        double data_block_hash_table_util_ratio = 0.75);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // Swap the contents in BlockBuilder with buffer, then reset the BlockBuilder.
  void SwapAndReset(std::string& buffer);

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // DO NOT mix with AddWithLastKey() between Resets. For efficiency, use
  // AddWithLastKey() in contexts where previous added key is already known
  // and delta encoding might be used.
  void Add(const Slice& key, const Slice& value,
           const Slice* const delta_value = nullptr);

  // A faster version of Add() if the previous key is already known for all
  // Add()s.
  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // REQUIRES: if AddWithLastKey has been called since last Reset(), last_key
  // is the key from most recent AddWithLastKey. (For convenience, last_key
  // is ignored on first call after creation or Reset().)
  // DO NOT mix with Add() between Resets.
  void AddWithLastKey(const Slice& key, const Slice& value,
                      const Slice& last_key,
                      const Slice* const delta_value = nullptr);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  inline size_t CurrentSizeEstimate() const {
    return estimate_ + (data_block_hash_index_builder_.Valid()
                            ? data_block_hash_index_builder_.EstimateSize()
                            : 0);
  }

  // Returns an estimated block size after appending key and value.
  size_t EstimateSizeAfterKV(const Slice& key, const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  inline void AddWithLastKeyImpl(const Slice& key, const Slice& value,
                                 const Slice& last_key,
                                 const Slice* const delta_value,
                                 size_t buffer_size);

  const int block_restart_interval_;
  // TODO(myabandeh): put it into a separate IndexBlockBuilder
  const bool use_delta_encoding_;
  // Refer to BlockIter::DecodeCurrentValue for format of delta encoded values
  const bool use_value_delta_encoding_;

  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  size_t estimate_;
  int counter_;    // Number of entries emitted since restart
  bool finished_;  // Has Finish() been called?
  std::string last_key_;
  DataBlockHashIndexBuilder data_block_hash_index_builder_;
#ifndef NDEBUG
  bool add_with_last_key_called_ = false;
#endif
};

}  // namespace ROCKSDB_NAMESPACE
