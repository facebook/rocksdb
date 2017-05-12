//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#include <list>
#include <string>
#include <unordered_map>
#include "db/dbformat.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/full_filter_block.h"
#include "table/index_builder.h"
#include "util/autovector.h"

namespace rocksdb {

class PartitionedFilterBlockBuilder : public FullFilterBlockBuilder {
 public:
  explicit PartitionedFilterBlockBuilder(
      const SliceTransform* prefix_extractor, bool whole_key_filtering,
      FilterBitsBuilder* filter_bits_builder, int index_block_restart_interval,
      PartitionedIndexBuilder* const p_index_builder);

  virtual ~PartitionedFilterBlockBuilder();

  void AddKey(const Slice& key) override;

  virtual Slice Finish(const BlockHandle& last_partition_block_handle,
                       Status* status) override;

 private:
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
  // The policy of when cut a filter block and Finish it
  void MaybeCutAFilterBlock();
  PartitionedIndexBuilder* const p_index_builder_;
};

class PartitionedFilterBlockReader : public FilterBlockReader {
 public:
  explicit PartitionedFilterBlockReader(const SliceTransform* prefix_extractor,
                                        bool whole_key_filtering,
                                        BlockContents&& contents,
                                        FilterBitsReader* filter_bits_reader,
                                        Statistics* stats,
                                        const Comparator& comparator,
                                        const BlockBasedTable* table);
  virtual ~PartitionedFilterBlockReader();

  virtual bool IsBlockBased() override { return false; }
  virtual bool KeyMayMatch(
      const Slice& key, uint64_t block_offset = kNotValid,
      const bool no_io = false,
      const Slice* const const_ikey_ptr = nullptr) override;
  virtual bool PrefixMayMatch(
      const Slice& prefix, uint64_t block_offset = kNotValid,
      const bool no_io = false,
      const Slice* const const_ikey_ptr = nullptr) override;
  virtual size_t ApproximateMemoryUsage() const override;

 private:
  Slice GetFilterPartitionHandle(const Slice& entry);
  BlockBasedTable::CachableEntry<FilterBlockReader> GetFilterPartition(
      Slice* handle, const bool no_io, bool* cached);

  const SliceTransform* prefix_extractor_;
  std::unique_ptr<Block> idx_on_fltr_blk_;
  const Comparator& comparator_;
  const BlockBasedTable* table_;
  std::unordered_map<uint64_t, FilterBlockReader*> filter_cache_;
  autovector<Cache::Handle*> handle_list_;
  struct BlockHandleCmp {
    bool operator()(const BlockHandle& lhs, const BlockHandle& rhs) const {
      return lhs.offset() < rhs.offset();
    }
  };
  std::set<BlockHandle, BlockHandleCmp> filter_block_set_;
  port::RWMutex mu_;
};

}  // namespace rocksdb
