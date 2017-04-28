//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <list>
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

#include "table/full_filter_block.h"
#include "table/index_builder.h"

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

}  // namespace rocksdb
