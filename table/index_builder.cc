//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/index_builder.h"
#include <assert.h>
#include <inttypes.h>

#include <list>
#include <string>

#include "rocksdb/comparator.h"
#include "table/format.h"
#include "table/partitioned_filter_block.h"

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace rocksdb {
// using namespace rocksdb;
// Create a index builder based on its type.
IndexBuilder* IndexBuilder::CreateIndexBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const InternalKeyComparator* comparator,
    const InternalKeySliceTransform* int_key_slice_transform,
    const BlockBasedTableOptions& table_opt) {
  switch (index_type) {
    case BlockBasedTableOptions::kBinarySearch: {
      return new ShortenedIndexBuilder(comparator,
                                       table_opt.index_block_restart_interval);
    }
    case BlockBasedTableOptions::kHashSearch: {
      return new HashIndexBuilder(comparator, int_key_slice_transform,
                                  table_opt.index_block_restart_interval);
    }
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      return PartitionedIndexBuilder::CreateIndexBuilder(comparator, table_opt);
    }
    default: {
      assert(!"Do not recognize the index type ");
      return nullptr;
    }
  }
  // impossible.
  assert(false);
  return nullptr;
}

PartitionedIndexBuilder* PartitionedIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt) {
  return new PartitionedIndexBuilder(comparator, table_opt);
}

PartitionedIndexBuilder::PartitionedIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt)
    : IndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval),
      table_opt_(table_opt) {
  sub_index_builder_ = IndexBuilder::CreateIndexBuilder(sub_type_, comparator_,
                                                        nullptr, table_opt_);
}

PartitionedIndexBuilder::~PartitionedIndexBuilder() {
  delete sub_index_builder_;
}

void PartitionedIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  sub_index_builder_->AddIndexEntry(last_key_in_current_block,
                                    first_key_in_next_block, block_handle);
  num_indexes++;
  if (UNLIKELY(first_key_in_next_block == nullptr)) {  // no more keys
    entries_.push_back({std::string(*last_key_in_current_block),
                        std::unique_ptr<IndexBuilder>(sub_index_builder_)});
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else if (num_indexes % table_opt_.index_per_partition == 0) {
    entries_.push_back({std::string(*last_key_in_current_block),
                        std::unique_ptr<IndexBuilder>(sub_index_builder_)});
    sub_index_builder_ = IndexBuilder::CreateIndexBuilder(
        sub_type_, comparator_, nullptr, table_opt_);
    cut_filter_block = true;
  }
}

Status PartitionedIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {
  assert(!entries_.empty());
  // It must be set to null after last key is added
  assert(sub_index_builder_ == nullptr);
  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();
    std::string handle_encoding;
    last_partition_block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(last_entry.key, handle_encoding);
    entries_.pop_front();
  }
  // If there is no sub_index left, then return the 2nd level index.
  if (UNLIKELY(entries_.empty())) {
    index_blocks->index_block_contents = index_block_builder_.Finish();
    return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();
    auto s = entry.value->Finish(index_blocks);
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t PartitionedIndexBuilder::EstimatedSize() const {
  size_t total = 0;
  for (auto it = entries_.begin(); it != entries_.end(); ++it) {
    total += it->value->EstimatedSize();
  }
  total += index_block_builder_.CurrentSizeEstimate();
  total +=
      sub_index_builder_ == nullptr ? 0 : sub_index_builder_->EstimatedSize();
  return total;
}
}  // namespace rocksdb
