//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "db/dbformat.h"
#include "util/hash.h"

#include "table/filter_block.h"
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
class PartitionIndexBuilder : public IndexBuilder, public FullFilterBlockBuilder {
 public:
  explicit PartitionIndexBuilder(const InternalKeyComparator* comparator,
                                 const SliceTransform* prefix_extractor,
                                 const uint64_t index_per_partition,
                                 int index_block_restart_interval,
                                 bool whole_key_filtering,
                                 FilterBitsBuilder* filter_bits_builder,
                                 const BlockBasedTableOptions& table_opt);

  virtual ~PartitionIndexBuilder();

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t EstimatedSize() const override;

  void AddKey(const Slice& key) override;

  virtual Slice Finish(
      const BlockHandle& last_partition_block_handle, Status* status) override;

 private:
  static const BlockBasedTableOptions::IndexType sub_type_ = BlockBasedTableOptions::kBinarySearch;
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
  bool finishing_indexes =
      false;  // true if Finish is called once but not complete yet.
  // Filter data
  BlockBuilder index_on_filter_block_builder_;  // top-level index builder
  struct FilterEntry {
    std::string key;
    Slice filter;
  };
  std::list<FilterEntry> filters;  // list of partitioned indexes and their keys
  std::unique_ptr<IndexBuilder> value;
  std::vector<std::unique_ptr<const char[]>> filter_gc;
  bool finishing_filters =
      false;  // true if Finish is called once but not complete yet.
  bool cut_filter_block =
      false;  // true if it should cut the next filter partition block
  const BlockBasedTableOptions& table_opt_;
  // FullFilterBlockBuilder
  // The policy of when cut a filter block and Finish it
  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }
  void CutAFilterBlock();
};

class PartitionedFilterBlockReader : public FullFilterBlockReader {
 public:
  explicit PartitionedFilterBlockReader(const SliceTransform* prefix_extractor,
                                        bool whole_key_filtering_2,
                                        BlockContents&& contents,
                                        FilterBitsReader* filter_bits_reader,
                                        Statistics* stats,
                                        const Comparator& comparator,
                                        const BlockBasedTable* table);

  bool KeyMayMatch(const Slice& key, uint64_t block_offset, const bool no_io);

  bool PrefixMayMatch(const Slice& prefix, uint64_t block_offset,
                      const bool no_io);

 private:
  std::unique_ptr<Block> idx_on_fltr_blk_;
  const Comparator& comparator_;
  const BlockBasedTable* table_;
  BlockBasedTable::CachableEntry<FilterBlockReader> GetFilterPartition(
      const Slice& entry, const bool no_io);
};

}  // namespace rocksdb
