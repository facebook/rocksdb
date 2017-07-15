//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/partitioned_filter_block.h"

#include <utility>

#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "util/coding.h"

namespace rocksdb {

PartitionedFilterBlockBuilder::PartitionedFilterBlockBuilder(
    const SliceTransform* prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder, int index_block_restart_interval,
    PartitionedIndexBuilder* const p_index_builder)
    : FullFilterBlockBuilder(prefix_extractor, whole_key_filtering,
                             filter_bits_builder),
      index_on_filter_block_builder_(index_block_restart_interval),
      p_index_builder_(p_index_builder) {}

PartitionedFilterBlockBuilder::~PartitionedFilterBlockBuilder() {}

void PartitionedFilterBlockBuilder::MaybeCutAFilterBlock() {
  if (!p_index_builder_->ShouldCutFilterBlock()) {
    return;
  }
  filter_gc.push_back(std::unique_ptr<const char[]>(nullptr));
  Slice filter = filter_bits_builder_->Finish(&filter_gc.back());
  std::string& index_key = p_index_builder_->GetPartitionKey();
  filters.push_back({index_key, filter});
}

void PartitionedFilterBlockBuilder::AddKey(const Slice& key) {
  MaybeCutAFilterBlock();
  filter_bits_builder_->AddKey(key);
}

Slice PartitionedFilterBlockBuilder::Finish(
    const BlockHandle& last_partition_block_handle, Status* status) {
  if (finishing_filters == true) {
    // Record the handle of the last written filter block in the index
    FilterEntry& last_entry = filters.front();
    std::string handle_encoding;
    last_partition_block_handle.EncodeTo(&handle_encoding);
    index_on_filter_block_builder_.Add(last_entry.key, handle_encoding);
    filters.pop_front();
  } else {
    MaybeCutAFilterBlock();
  }
  // If there is no filter partition left, then return the index on filter
  // partitions
  if (UNLIKELY(filters.empty())) {
    *status = Status::OK();
    if (finishing_filters) {
      return index_on_filter_block_builder_.Finish();
    } else {
      // This is the rare case where no key was added to the filter
      return Slice();
    }
  } else {
    // Return the next filter partition in line and set Incomplete() status to
    // indicate we expect more calls to Finish
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
    : FilterBlockReader(contents.data.size(), stats, _whole_key_filtering),
      prefix_extractor_(prefix_extractor),
      comparator_(comparator),
      table_(table) {
  idx_on_fltr_blk_.reset(new Block(std::move(contents),
                                   kDisableGlobalSequenceNumber,
                                   0 /* read_amp_bytes_per_bit */, stats));
}

PartitionedFilterBlockReader::~PartitionedFilterBlockReader() {
  {
    ReadLock rl(&mu_);
    for (auto it = handle_list_.begin(); it != handle_list_.end(); ++it) {
      table_->rep_->table_options.block_cache.get()->Release(*it);
    }
  }
  char cache_key[BlockBasedTable::kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  for (auto it = filter_block_set_.begin(); it != filter_block_set_.end();
       ++it) {
    auto key = BlockBasedTable::GetCacheKey(table_->rep_->cache_key_prefix,
                                            table_->rep_->cache_key_prefix_size,
                                            *it, cache_key);
    table_->rep_->table_options.block_cache.get()->Erase(key);
  }
}

bool PartitionedFilterBlockReader::KeyMayMatch(
    const Slice& key, uint64_t block_offset, const bool no_io,
    const Slice* const const_ikey_ptr) {
  assert(const_ikey_ptr != nullptr);
  assert(block_offset == kNotValid);
  if (!whole_key_filtering_) {
    return true;
  }
  if (UNLIKELY(idx_on_fltr_blk_->size() == 0)) {
    return true;
  }
  auto filter_handle = GetFilterPartitionHandle(*const_ikey_ptr);
  if (UNLIKELY(filter_handle.size() == 0)) {  // key is out of range
    return false;
  }
  bool cached = false;
  auto filter_partition = GetFilterPartition(&filter_handle, no_io, &cached);
  if (UNLIKELY(!filter_partition.value)) {
    return true;
  }
  auto res = filter_partition.value->KeyMayMatch(key, block_offset, no_io);
  if (cached) {
    return res;
  }
  if (LIKELY(filter_partition.IsSet())) {
    filter_partition.Release(table_->rep_->table_options.block_cache.get());
  } else {
    delete filter_partition.value;
  }
  return res;
}

bool PartitionedFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, uint64_t block_offset, const bool no_io,
    const Slice* const const_ikey_ptr) {
  assert(const_ikey_ptr != nullptr);
  assert(block_offset == kNotValid);
  if (!prefix_extractor_) {
    return true;
  }
  if (UNLIKELY(idx_on_fltr_blk_->size() == 0)) {
    return true;
  }
  auto filter_handle = GetFilterPartitionHandle(*const_ikey_ptr);
  if (UNLIKELY(filter_handle.size() == 0)) {  // prefix is out of range
    return false;
  }
  bool cached = false;
  auto filter_partition = GetFilterPartition(&filter_handle, no_io, &cached);
  if (UNLIKELY(!filter_partition.value)) {
    return true;
  }
  auto res = filter_partition.value->PrefixMayMatch(prefix, kNotValid, no_io);
  if (cached) {
    return res;
  }
  if (LIKELY(filter_partition.IsSet())) {
    filter_partition.Release(table_->rep_->table_options.block_cache.get());
  } else {
    delete filter_partition.value;
  }
  return res;
}

Slice PartitionedFilterBlockReader::GetFilterPartitionHandle(
    const Slice& entry) {
  BlockIter iter;
  idx_on_fltr_blk_->NewIterator(&comparator_, &iter, true);
  iter.Seek(entry);
  if (UNLIKELY(!iter.Valid())) {
    return Slice();
  }
  assert(iter.Valid());
  Slice handle_value = iter.value();
  return handle_value;
}

BlockBasedTable::CachableEntry<FilterBlockReader>
PartitionedFilterBlockReader::GetFilterPartition(Slice* handle_value,
                                                 const bool no_io,
                                                 bool* cached) {
  BlockHandle fltr_blk_handle;
  auto s = fltr_blk_handle.DecodeFrom(handle_value);
  assert(s.ok());
  const bool is_a_filter_partition = true;
  auto block_cache = table_->rep_->table_options.block_cache.get();
  if (LIKELY(block_cache != nullptr)) {
    bool pin_cached_filters =
        GetLevel() == 0 &&
        table_->rep_->table_options.pin_l0_filter_and_index_blocks_in_cache;
    if (pin_cached_filters) {
      ReadLock rl(&mu_);
      auto iter = filter_cache_.find(fltr_blk_handle.offset());
      if (iter != filter_cache_.end()) {
        RecordTick(statistics(), BLOCK_CACHE_FILTER_HIT);
        *cached = true;
        return {iter->second, nullptr};
      }
    }
    auto filter =
        table_->GetFilter(fltr_blk_handle, is_a_filter_partition, no_io);
    if (filter.IsSet()) {
      WriteLock wl(&mu_);
      filter_block_set_.insert(fltr_blk_handle);
      if (pin_cached_filters) {
        std::pair<uint64_t, FilterBlockReader*> pair(fltr_blk_handle.offset(),
                                                     filter.value);
        auto succ = filter_cache_.insert(pair).second;
        if (succ) {
          handle_list_.push_back(filter.cache_handle);
        }  // Otherwise it is already inserted by a concurrent thread
        *cached = true;
      }
    }
    return filter;
  } else {
    auto filter = table_->ReadFilter(fltr_blk_handle, is_a_filter_partition);
    return {filter, nullptr};
  }
}

size_t PartitionedFilterBlockReader::ApproximateMemoryUsage() const {
  return idx_on_fltr_blk_->size();
}

}  // namespace rocksdb
