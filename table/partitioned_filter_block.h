//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <list>
#include <string>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "util/hash.h"

#include "table/index_builder.h"

namespace rocksdb {

/**
 * IndexBuilder for two-level indexing. Internally it creates a new index for
 * each partition and Finish then in order when Finish is called on it
 * continiously until Status::OK() is returned.
 *
 * The format on the disk would be I I I I I I IP where I is block containing a
 * partition of indexes built using ShortenedIndexBuilder and IP is a block
 * containing a secondary index on the partitions, built using
 * ShortenedIndexBuilder.
 */
class PartitionIndexBuilder : public IndexBuilder {
 public:
  explicit PartitionIndexBuilder(const InternalKeyComparator* comparator,
                                 const SliceTransform* prefix_extractor,
                                 const uint64_t index_per_partition,
                                 int index_block_restart_interval);

  virtual ~PartitionIndexBuilder();

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle);

  virtual Status Finish(IndexBlocks* index_blocks,
                        const BlockHandle& last_partition_block_handle);

  virtual size_t EstimatedSize() const;

 private:
  static const BlockBasedTableOptions::IndexType sub_type_ =
      BlockBasedTableOptions::kBinarySearch;
  struct Entry {
    std::string key;
    std::unique_ptr<IndexBuilder> value;
  };
  std::list<Entry> entries_;  // list of partitioned indexes and their keys
  const SliceTransform* prefix_extractor_;
  BlockBuilder index_block_builder_;  // top-level index builder
  IndexBuilder* sub_index_builder_;   // the active partition index builder
  uint64_t index_per_partition_;
  int index_block_restart_interval_;
  uint64_t num_indexes = 0;
  bool finishing =
      false;  // true if Finish is called once but not complete yet.
};

}  // namespace rocksdb
