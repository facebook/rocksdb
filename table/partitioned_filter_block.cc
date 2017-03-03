//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/partitioned_filter_block.h"

#include "port/port.h"
#include "util/coding.h"

namespace rocksdb {
PartitionIndexBuilder::PartitionIndexBuilder(
    const InternalKeyComparator* comparator,
    const SliceTransform* prefix_extractor, const uint64_t index_per_partition,
    int index_block_restart_interval)
    : IndexBuilder(comparator),
      prefix_extractor_(prefix_extractor),
      index_block_builder_(index_block_restart_interval),
      index_per_partition_(index_per_partition),
      index_block_restart_interval_(index_block_restart_interval) {
  sub_index_builder_ =
      CreateIndexBuilder(sub_type_, comparator_, prefix_extractor_,
                         index_block_restart_interval_, index_per_partition_);
}

PartitionIndexBuilder::~PartitionIndexBuilder() { delete sub_index_builder_; }

void PartitionIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  sub_index_builder_->AddIndexEntry(last_key_in_current_block,
                                    first_key_in_next_block, block_handle);
  num_indexes++;
  if (UNLIKELY(first_key_in_next_block == nullptr)) {  // no more keys
    entries_.push_back({std::string(*last_key_in_current_block),
                        std::unique_ptr<IndexBuilder>(sub_index_builder_)});
    sub_index_builder_ = nullptr;
  } else if (num_indexes % index_per_partition_ == 0) {
    entries_.push_back({std::string(*last_key_in_current_block),
                        std::unique_ptr<IndexBuilder>(sub_index_builder_)});
    sub_index_builder_ =
        CreateIndexBuilder(sub_type_, comparator_, prefix_extractor_,
                           index_block_restart_interval_, index_per_partition_);
  }
}

Status PartitionIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {
  assert(!entries_.empty());
  // It must be set to null after last key is added
  assert(sub_index_builder_ == nullptr);
  if (finishing == true) {
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
    finishing = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t PartitionIndexBuilder::EstimatedSize() const {
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
