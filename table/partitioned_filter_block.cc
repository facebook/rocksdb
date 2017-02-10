//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/partitioned_filter_block.h"

#include "table/block_based_table_reader.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "util/coding.h"
#include "util/perf_context_imp.h"

namespace rocksdb {

PartitionIndexBuilder::PartitionIndexBuilder(
    const InternalKeyComparator* comparator,
    const SliceTransform* prefix_extractor, const uint64_t index_per_partition,
    int index_block_restart_interval, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder,
    const BlockBasedTableOptions& table_opt)
    : IndexBuilder(comparator),
      FullFilterBlockBuilder(prefix_extractor, whole_key_filtering,
                             filter_bits_builder),
      prefix_extractor_(prefix_extractor),
      index_block_builder_(index_block_restart_interval),
      index_per_partition_(index_per_partition),
      index_block_restart_interval_(index_block_restart_interval),
      index_on_filter_block_builder_(index_block_restart_interval),
      table_opt_(table_opt) {
  sub_index_builder_ = CreateIndexBuilder(
      sub_type_, comparator_, prefix_extractor_, index_block_restart_interval_,
      index_per_partition_, table_opt_);
}

PartitionIndexBuilder::~PartitionIndexBuilder() {
  delete sub_index_builder_;
}

void PartitionIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block,
    const BlockHandle& block_handle) {
  sub_index_builder_->AddIndexEntry(last_key_in_current_block,
                                    first_key_in_next_block, block_handle);
  num_indexes++;
  if (UNLIKELY(first_key_in_next_block == nullptr)) {  // no more keys
    entries_.push_back({std::string(*last_key_in_current_block),
                        std::unique_ptr<IndexBuilder>(sub_index_builder_)});
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else if (num_indexes % index_per_partition_ == 0) {
    entries_.push_back({std::string(*last_key_in_current_block),
                        std::unique_ptr<IndexBuilder>(sub_index_builder_)});
    sub_index_builder_ = CreateIndexBuilder(
        sub_type_, comparator_, prefix_extractor_,
        index_block_restart_interval_, index_per_partition_, table_opt_);
    cut_filter_block = true;
  }
}

Status PartitionIndexBuilder::Finish(
    IndexBlocks* index_blocks,
    const BlockHandle& last_partition_block_handle) {
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

// FullFilterBlockBuilder
void PartitionIndexBuilder::MayBeCutAFilterBlock() {
  if (!ShouldCutFilterBlock()) {
    return;
  }
  filter_gc.push_back(std::unique_ptr<const char[]>(nullptr));
  Slice filter = filter_bits_builder_->Finish(&filter_gc.back());
  std::string& index_key = entries_.back().key;
  filters.push_back({index_key, filter});
}

void PartitionIndexBuilder::AddKey(const Slice& key) {
  MayBeCutAFilterBlock();
  filter_bits_builder_->AddKey(key);
}

Slice PartitionIndexBuilder::Finish(
    const BlockHandle& last_partition_block_handle, Status* status) {
  if (finishing_filters == true) {
    FilterEntry& last_entry = filters.front();
    std::string handle_encoding;
    last_partition_block_handle.EncodeTo(&handle_encoding);
    // This is the full key vs. the user key in the filters. We assume that
    // user key <= full key
    index_on_filter_block_builder_.Add(last_entry.key, handle_encoding);
    filters.pop_front();
  }
  MayBeCutAFilterBlock();
  // If there is no sub_filter left, then return empty Slice
  if (UNLIKELY(filters.empty())) {
    *status = Status::OK();
    if (finishing_filters) {
      return index_on_filter_block_builder_.Finish();
    } else {
      return Slice();
    }
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    *status = Status::Incomplete();
    finishing_filters = true;
    return filters.front().filter;
  }
}

PartitionedFilterBlockReader::PartitionedFilterBlockReader(
    const SliceTransform* prefix_extractor, bool _whole_key_filtering,
    BlockContents&& contents, FilterBitsReader* filter_bits_reader,
    Statistics* stats, const Comparator& comparator,
    const BlockBasedTable* table)
    // : FullFilterBlockReader(prefix_extractor, whole_key_filtering,
    //                        contents.data, filter_bits_reader, stats),
    : FilterBlockReader(contents.data.size(), stats, _whole_key_filtering),
      prefix_extractor_(prefix_extractor),
      comparator_(comparator),
      table_(table) {
  idx_on_fltr_blk_.reset(new Block(std::move(contents),
                                   kDisableGlobalSequenceNumber,
                                   0 /* read_amp_bytes_per_bit */, stats));
  };

  bool PartitionedFilterBlockReader::KeyMayMatch(const Slice& key, uint64_t block_offset, const bool no_io) {
    assert(block_offset == kNotValid);
    if (!whole_key_filtering_) {
      return true;
    }
    if (UNLIKELY(idx_on_fltr_blk_->size() == 0)) {
      return true;
    }
    // This is the user key vs. the full key in the partition index. We assume
    // that user key <= full key
    auto filter_partition = GetFilterPartition(key, no_io);
    if (UNLIKELY(!filter_partition.value)) {
      return false;
    }
    auto res = filter_partition.value->KeyMayMatch(key, block_offset, no_io);
    filter_partition.Release(table_->rep_->table_options.block_cache.get());
    return res;
  }

  bool PartitionedFilterBlockReader::PrefixMayMatch(const Slice& prefix, uint64_t block_offset,
                      const bool no_io) {
    assert(block_offset == kNotValid);
    if (!prefix_extractor_) {
      return true;
    }
    if (UNLIKELY(idx_on_fltr_blk_->size() == 0)) {
      return true;
    }
    auto filter_partition = GetFilterPartition(prefix, no_io);
    if (UNLIKELY(!filter_partition.value)) {
      return false;
    }
    auto res = filter_partition.value->PrefixMayMatch(prefix, no_io);
    filter_partition.Release(table_->rep_->table_options.block_cache.get());
    return res;
  }

  BlockBasedTable::CachableEntry<FilterBlockReader> PartitionedFilterBlockReader::GetFilterPartition(
      const Slice& entry, const bool no_io) {
    BlockIter iter;
    idx_on_fltr_blk_->NewIterator(&comparator_, &iter, true);
    iter.Seek(entry.data());
    if (UNLIKELY(!iter.Valid())) {
      return BlockBasedTable::CachableEntry<FilterBlockReader>(nullptr, nullptr);
    }
    assert(iter.Valid());
    Slice handle_value = iter.value();
    BlockHandle fltr_blk_handle;
    auto s = fltr_blk_handle.DecodeFrom(&handle_value);
    assert(s.ok());
    const bool is_a_filter_partition = true;
    auto filter =
        table_->GetFilter(fltr_blk_handle, is_a_filter_partition, no_io);
    return filter;
  }

size_t PartitionedFilterBlockReader::ApproximateMemoryUsage() const {
  return idx_on_fltr_blk_->size();
}

}  // namespace rocksdb
