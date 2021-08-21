//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/block_based/partitioned_filter_block.h"

#include <utility>

#include "file/random_access_file_reader.h"
#include "monitoring/perf_context_imp.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/filter_policy.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

PartitionedFilterBlockBuilder::PartitionedFilterBlockBuilder(
    const SliceTransform* _prefix_extractor, bool whole_key_filtering,
    FilterBitsBuilder* filter_bits_builder, int index_block_restart_interval,
    const bool use_value_delta_encoding,
    PartitionedIndexBuilder* const p_index_builder,
    const uint32_t partition_size)
    : FullFilterBlockBuilder(_prefix_extractor, whole_key_filtering,
                             filter_bits_builder),
      index_on_filter_block_builder_(index_block_restart_interval,
                                     true /*use_delta_encoding*/,
                                     use_value_delta_encoding),
      index_on_filter_block_builder_without_seq_(index_block_restart_interval,
                                                 true /*use_delta_encoding*/,
                                                 use_value_delta_encoding),
      p_index_builder_(p_index_builder),
      keys_added_to_partition_(0),
      total_added_in_built_(0) {
  keys_per_partition_ = static_cast<uint32_t>(
      filter_bits_builder_->ApproximateNumEntries(partition_size));
  if (keys_per_partition_ < 1) {
    // partition_size (minus buffer, ~10%) might be smaller than minimum
    // filter size, sometimes based on cache line size. Try to find that
    // minimum size without CalculateSpace (not necessarily available).
    uint32_t larger = std::max(partition_size + 4, uint32_t{16});
    for (;;) {
      keys_per_partition_ = static_cast<uint32_t>(
          filter_bits_builder_->ApproximateNumEntries(larger));
      if (keys_per_partition_ >= 1) {
        break;
      }
      larger += larger / 4;
      if (larger > 100000) {
        // might be a broken implementation. substitute something reasonable:
        // 1 key / byte.
        keys_per_partition_ = partition_size;
        break;
      }
    }
  }
}

PartitionedFilterBlockBuilder::~PartitionedFilterBlockBuilder() {}

void PartitionedFilterBlockBuilder::MaybeCutAFilterBlock(
    const Slice* next_key) {
  // Use == to send the request only once
  if (keys_added_to_partition_ == keys_per_partition_) {
    // Currently only index builder is in charge of cutting a partition. We keep
    // requesting until it is granted.
    p_index_builder_->RequestPartitionCut();
  }
  if (!p_index_builder_->ShouldCutFilterBlock()) {
    return;
  }
  filter_gc.push_back(std::unique_ptr<const char[]>(nullptr));

  // Add the prefix of the next key before finishing the partition without
  // updating last_prefix_str_. This hack, fixes a bug with format_verison=3
  // where seeking for the prefix would lead us to the previous partition.
  const bool maybe_add_prefix =
      next_key && prefix_extractor() && prefix_extractor()->InDomain(*next_key);
  if (maybe_add_prefix) {
    const Slice next_key_prefix = prefix_extractor()->Transform(*next_key);
    if (next_key_prefix.compare(last_prefix_str()) != 0) {
      AddKey(next_key_prefix);
    }
  }

  total_added_in_built_ += filter_bits_builder_->EstimateEntriesAdded();
  Slice filter = filter_bits_builder_->Finish(&filter_gc.back());
  std::string& index_key = p_index_builder_->GetPartitionKey();
  filters.push_back({index_key, filter});
  keys_added_to_partition_ = 0;
  Reset();
}

void PartitionedFilterBlockBuilder::Add(const Slice& key) {
  MaybeCutAFilterBlock(&key);
  FullFilterBlockBuilder::Add(key);
}

void PartitionedFilterBlockBuilder::AddKey(const Slice& key) {
  FullFilterBlockBuilder::AddKey(key);
  keys_added_to_partition_++;
}

size_t PartitionedFilterBlockBuilder::EstimateEntriesAdded() {
  return total_added_in_built_ + filter_bits_builder_->EstimateEntriesAdded();
}

Slice PartitionedFilterBlockBuilder::Finish(
    const BlockHandle& last_partition_block_handle, Status* status) {
  if (finishing_filters == true) {
    // Record the handle of the last written filter block in the index
    FilterEntry& last_entry = filters.front();
    std::string handle_encoding;
    last_partition_block_handle.EncodeTo(&handle_encoding);
    std::string handle_delta_encoding;
    PutVarsignedint64(
        &handle_delta_encoding,
        last_partition_block_handle.size() - last_encoded_handle_.size());
    last_encoded_handle_ = last_partition_block_handle;
    const Slice handle_delta_encoding_slice(handle_delta_encoding);
    index_on_filter_block_builder_.Add(last_entry.key, handle_encoding,
                                       &handle_delta_encoding_slice);
    if (!p_index_builder_->seperator_is_key_plus_seq()) {
      index_on_filter_block_builder_without_seq_.Add(
          ExtractUserKey(last_entry.key), handle_encoding,
          &handle_delta_encoding_slice);
    }
    filters.pop_front();
  } else {
    MaybeCutAFilterBlock(nullptr);
  }
  // If there is no filter partition left, then return the index on filter
  // partitions
  if (UNLIKELY(filters.empty())) {
    *status = Status::OK();
    if (finishing_filters) {
      // Simplest to just add them all at the end
      total_added_in_built_ = 0;
      if (p_index_builder_->seperator_is_key_plus_seq()) {
        return index_on_filter_block_builder_.Finish();
      } else {
        return index_on_filter_block_builder_without_seq_.Finish();
      }
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
    const BlockBasedTable* t, CachableEntry<Block>&& filter_block)
    : FilterBlockReaderCommon(t, std::move(filter_block)) {}

std::unique_ptr<FilterBlockReader> PartitionedFilterBlockReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context) {
  assert(table);
  assert(table->get_rep());
  assert(!pin || prefetch);

  CachableEntry<Block> filter_block;
  if (prefetch || !use_cache) {
    const Status s = ReadFilterBlock(table, prefetch_buffer, ro, use_cache,
                                     nullptr /* get_context */, lookup_context,
                                     &filter_block);
    if (!s.ok()) {
      IGNORE_STATUS_IF_ERROR(s);
      return std::unique_ptr<FilterBlockReader>();
    }

    if (use_cache && !pin) {
      filter_block.Reset();
    }
  }

  return std::unique_ptr<FilterBlockReader>(
      new PartitionedFilterBlockReader(table, std::move(filter_block)));
}

bool PartitionedFilterBlockReader::KeyMayMatch(
    const Slice& key, const SliceTransform* prefix_extractor,
    uint64_t block_offset, const bool no_io, const Slice* const const_ikey_ptr,
    GetContext* get_context, BlockCacheLookupContext* lookup_context) {
  assert(const_ikey_ptr != nullptr);
  assert(block_offset == kNotValid);
  if (!whole_key_filtering()) {
    return true;
  }

  return MayMatch(key, prefix_extractor, block_offset, no_io, const_ikey_ptr,
                  get_context, lookup_context,
                  &FullFilterBlockReader::KeyMayMatch);
}

void PartitionedFilterBlockReader::KeysMayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    uint64_t block_offset, const bool no_io,
    BlockCacheLookupContext* lookup_context) {
  assert(block_offset == kNotValid);
  if (!whole_key_filtering()) {
    return;  // Any/all may match
  }

  MayMatch(range, prefix_extractor, block_offset, no_io, lookup_context,
           &FullFilterBlockReader::KeysMayMatch);
}

bool PartitionedFilterBlockReader::PrefixMayMatch(
    const Slice& prefix, const SliceTransform* prefix_extractor,
    uint64_t block_offset, const bool no_io, const Slice* const const_ikey_ptr,
    GetContext* get_context, BlockCacheLookupContext* lookup_context) {
  assert(const_ikey_ptr != nullptr);
  assert(block_offset == kNotValid);
  if (!table_prefix_extractor() && !prefix_extractor) {
    return true;
  }

  return MayMatch(prefix, prefix_extractor, block_offset, no_io, const_ikey_ptr,
                  get_context, lookup_context,
                  &FullFilterBlockReader::PrefixMayMatch);
}

void PartitionedFilterBlockReader::PrefixesMayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    uint64_t block_offset, const bool no_io,
    BlockCacheLookupContext* lookup_context) {
  assert(block_offset == kNotValid);
  if (!table_prefix_extractor() && !prefix_extractor) {
    return;  // Any/all may match
  }

  MayMatch(range, prefix_extractor, block_offset, no_io, lookup_context,
           &FullFilterBlockReader::PrefixesMayMatch);
}

BlockHandle PartitionedFilterBlockReader::GetFilterPartitionHandle(
    const CachableEntry<Block>& filter_block, const Slice& entry) const {
  IndexBlockIter iter;
  const InternalKeyComparator* const comparator = internal_comparator();
  Statistics* kNullStats = nullptr;
  filter_block.GetValue()->NewIndexIterator(
      comparator->user_comparator(),
      table()->get_rep()->get_global_seqno(BlockType::kFilter), &iter,
      kNullStats, true /* total_order_seek */, false /* have_first_key */,
      index_key_includes_seq(), index_value_is_full());
  iter.Seek(entry);
  if (UNLIKELY(!iter.Valid())) {
    // entry is larger than all the keys. However its prefix might still be
    // present in the last partition. If this is called by PrefixMayMatch this
    // is necessary for correct behavior. Otherwise it is unnecessary but safe.
    // Assuming this is an unlikely case for full key search, the performance
    // overhead should be negligible.
    iter.SeekToLast();
  }
  assert(iter.Valid());
  BlockHandle fltr_blk_handle = iter.value().handle;
  return fltr_blk_handle;
}

Status PartitionedFilterBlockReader::GetFilterPartitionBlock(
    FilePrefetchBuffer* prefetch_buffer, const BlockHandle& fltr_blk_handle,
    bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<ParsedFullFilterBlock>* filter_block) const {
  assert(table());
  assert(filter_block);
  assert(filter_block->IsEmpty());

  if (!filter_map_.empty()) {
    auto iter = filter_map_.find(fltr_blk_handle.offset());
    // This is a possible scenario since block cache might not have had space
    // for the partition
    if (iter != filter_map_.end()) {
      filter_block->SetUnownedValue(iter->second.GetValue());
      return Status::OK();
    }
  }

  ReadOptions read_options;
  if (no_io) {
    read_options.read_tier = kBlockCacheTier;
  }

  const Status s =
      table()->RetrieveBlock(prefetch_buffer, read_options, fltr_blk_handle,
                             UncompressionDict::GetEmptyDict(), filter_block,
                             BlockType::kFilter, get_context, lookup_context,
                             /* for_compaction */ false, /* use_cache */ true,
                             /* wait_for_cache */ true);

  return s;
}

bool PartitionedFilterBlockReader::MayMatch(
    const Slice& slice, const SliceTransform* prefix_extractor,
    uint64_t block_offset, bool no_io, const Slice* const_ikey_ptr,
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    FilterFunction filter_function) const {
  CachableEntry<Block> filter_block;
  Status s =
      GetOrReadFilterBlock(no_io, get_context, lookup_context, &filter_block);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return true;
  }

  if (UNLIKELY(filter_block.GetValue()->size() == 0)) {
    return true;
  }

  auto filter_handle = GetFilterPartitionHandle(filter_block, *const_ikey_ptr);
  if (UNLIKELY(filter_handle.size() == 0)) {  // key is out of range
    return false;
  }

  CachableEntry<ParsedFullFilterBlock> filter_partition_block;
  s = GetFilterPartitionBlock(nullptr /* prefetch_buffer */, filter_handle,
                              no_io, get_context, lookup_context,
                              &filter_partition_block);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return true;
  }

  FullFilterBlockReader filter_partition(table(),
                                         std::move(filter_partition_block));
  return (filter_partition.*filter_function)(
      slice, prefix_extractor, block_offset, no_io, const_ikey_ptr, get_context,
      lookup_context);
}

void PartitionedFilterBlockReader::MayMatch(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    uint64_t block_offset, bool no_io, BlockCacheLookupContext* lookup_context,
    FilterManyFunction filter_function) const {
  CachableEntry<Block> filter_block;
  Status s = GetOrReadFilterBlock(no_io, range->begin()->get_context,
                                  lookup_context, &filter_block);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return;  // Any/all may match
  }

  if (UNLIKELY(filter_block.GetValue()->size() == 0)) {
    return;  // Any/all may match
  }

  auto start_iter_same_handle = range->begin();
  BlockHandle prev_filter_handle = BlockHandle::NullBlockHandle();

  // For all keys mapping to same partition (must be adjacent in sorted order)
  // share block cache lookup and use full filter multiget on the partition
  // filter.
  for (auto iter = start_iter_same_handle; iter != range->end(); ++iter) {
    // TODO: re-use one top-level index iterator
    BlockHandle this_filter_handle =
        GetFilterPartitionHandle(filter_block, iter->ikey);
    if (!prev_filter_handle.IsNull() &&
        this_filter_handle != prev_filter_handle) {
      MultiGetRange subrange(*range, start_iter_same_handle, iter);
      MayMatchPartition(&subrange, prefix_extractor, block_offset,
                        prev_filter_handle, no_io, lookup_context,
                        filter_function);
      range->AddSkipsFrom(subrange);
      start_iter_same_handle = iter;
    }
    if (UNLIKELY(this_filter_handle.size() == 0)) {  // key is out of range
      // Not reachable with current behavior of GetFilterPartitionHandle
      assert(false);
      range->SkipKey(iter);
      prev_filter_handle = BlockHandle::NullBlockHandle();
    } else {
      prev_filter_handle = this_filter_handle;
    }
  }
  if (!prev_filter_handle.IsNull()) {
    MultiGetRange subrange(*range, start_iter_same_handle, range->end());
    MayMatchPartition(&subrange, prefix_extractor, block_offset,
                      prev_filter_handle, no_io, lookup_context,
                      filter_function);
    range->AddSkipsFrom(subrange);
  }
}

void PartitionedFilterBlockReader::MayMatchPartition(
    MultiGetRange* range, const SliceTransform* prefix_extractor,
    uint64_t block_offset, BlockHandle filter_handle, bool no_io,
    BlockCacheLookupContext* lookup_context,
    FilterManyFunction filter_function) const {
  CachableEntry<ParsedFullFilterBlock> filter_partition_block;
  Status s = GetFilterPartitionBlock(
      nullptr /* prefetch_buffer */, filter_handle, no_io,
      range->begin()->get_context, lookup_context, &filter_partition_block);
  if (UNLIKELY(!s.ok())) {
    IGNORE_STATUS_IF_ERROR(s);
    return;  // Any/all may match
  }

  FullFilterBlockReader filter_partition(table(),
                                         std::move(filter_partition_block));
  (filter_partition.*filter_function)(range, prefix_extractor, block_offset,
                                      no_io, lookup_context);
}

size_t PartitionedFilterBlockReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateFilterBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<PartitionedFilterBlockReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  return usage;
  // TODO(myabandeh): better estimation for filter_map_ size
}

// TODO(myabandeh): merge this with the same function in IndexReader
Status PartitionedFilterBlockReader::CacheDependencies(const ReadOptions& ro,
                                                       bool pin) {
  assert(table());

  const BlockBasedTable::Rep* const rep = table()->get_rep();
  assert(rep);

  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};

  CachableEntry<Block> filter_block;

  Status s = GetOrReadFilterBlock(false /* no_io */, nullptr /* get_context */,
                                  &lookup_context, &filter_block);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(rep->ioptions.logger,
                    "Error retrieving top-level filter block while trying to "
                    "cache filter partitions: %s",
                    s.ToString().c_str());
    return s;
  }

  // Before read partitions, prefetch them to avoid lots of IOs
  assert(filter_block.GetValue());

  IndexBlockIter biter;
  const InternalKeyComparator* const comparator = internal_comparator();
  Statistics* kNullStats = nullptr;
  filter_block.GetValue()->NewIndexIterator(
      comparator->user_comparator(), rep->get_global_seqno(BlockType::kFilter),
      &biter, kNullStats, true /* total_order_seek */,
      false /* have_first_key */, index_key_includes_seq(),
      index_value_is_full());
  // Index partitions are assumed to be consecuitive. Prefetch them all.
  // Read the first block offset
  biter.SeekToFirst();
  BlockHandle handle = biter.value().handle;
  uint64_t prefetch_off = handle.offset();

  // Read the last block's offset
  biter.SeekToLast();
  handle = biter.value().handle;
  uint64_t last_off = handle.offset() + handle.size() + kBlockTrailerSize;
  uint64_t prefetch_len = last_off - prefetch_off;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;
  rep->CreateFilePrefetchBuffer(0, 0, &prefetch_buffer,
                                false /* Implicit autoreadahead */);

  IOOptions opts;
  s = rep->file->PrepareIOOptions(ro, opts);
  if (s.ok()) {
    s = prefetch_buffer->Prefetch(opts, rep->file.get(), prefetch_off,
                                  static_cast<size_t>(prefetch_len));
  }
  if (!s.ok()) {
    return s;
  }

  // After prefetch, read the partitions one by one
  for (biter.SeekToFirst(); biter.Valid(); biter.Next()) {
    handle = biter.value().handle;

    CachableEntry<ParsedFullFilterBlock> block;
    // TODO: Support counter batch update for partitioned index and
    // filter blocks
    s = table()->MaybeReadBlockAndLoadToCache(
        prefetch_buffer.get(), ro, handle, UncompressionDict::GetEmptyDict(),
        /* wait */ true, &block, BlockType::kFilter, nullptr /* get_context */,
        &lookup_context, nullptr /* contents */);
    if (!s.ok()) {
      return s;
    }
    assert(s.ok() || block.GetValue() == nullptr);

    if (block.GetValue() != nullptr) {
      if (block.IsCached()) {
        if (pin) {
          filter_map_[handle.offset()] = std::move(block);
        }
      }
    }
  }
  return biter.status();
}

const InternalKeyComparator* PartitionedFilterBlockReader::internal_comparator()
    const {
  assert(table());
  assert(table()->get_rep());

  return &table()->get_rep()->internal_comparator;
}

bool PartitionedFilterBlockReader::index_key_includes_seq() const {
  assert(table());
  assert(table()->get_rep());

  return table()->get_rep()->index_key_includes_seq;
}

bool PartitionedFilterBlockReader::index_value_is_full() const {
  assert(table());
  assert(table()->get_rep());

  return table()->get_rep()->index_value_is_full;
}

}  // namespace ROCKSDB_NAMESPACE
