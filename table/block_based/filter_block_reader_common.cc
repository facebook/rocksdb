//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "table/block_based/filter_block_reader_common.h"
#include "monitoring/perf_context_imp.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/parsed_full_filter_block.h"

namespace rocksdb {

template <typename TBlocklike>
Status FilterBlockReaderCommon<TBlocklike>::ReadFilterBlock(
    const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
    const ReadOptions& read_options, bool use_cache, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<TBlocklike>* filter_block) {
  PERF_TIMER_GUARD(read_filter_block_nanos);

  assert(table);
  assert(filter_block);
  assert(filter_block->IsEmpty());

  const BlockBasedTable::Rep* const rep = table->get_rep();
  assert(rep);

  const Status s =
      table->RetrieveBlock(prefetch_buffer, read_options, rep->filter_handle,
                           UncompressionDict::GetEmptyDict(), filter_block,
                           BlockType::kFilter, get_context, lookup_context,
                           /* for_compaction */ false, use_cache);

  return s;
}

template <typename TBlocklike>
const SliceTransform*
FilterBlockReaderCommon<TBlocklike>::table_prefix_extractor() const {
  assert(table_);

  const BlockBasedTable::Rep* const rep = table_->get_rep();
  assert(rep);

  return rep->prefix_filtering ? rep->table_prefix_extractor.get() : nullptr;
}

template <typename TBlocklike>
bool FilterBlockReaderCommon<TBlocklike>::whole_key_filtering() const {
  assert(table_);
  assert(table_->get_rep());

  return table_->get_rep()->whole_key_filtering;
}

template <typename TBlocklike>
bool FilterBlockReaderCommon<TBlocklike>::cache_filter_blocks() const {
  assert(table_);
  assert(table_->get_rep());

  return table_->get_rep()->table_options.cache_index_and_filter_blocks;
}

template <typename TBlocklike>
Status FilterBlockReaderCommon<TBlocklike>::GetOrReadFilterBlock(
    bool no_io, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<TBlocklike>* filter_block) const {
  assert(filter_block);

  if (!filter_block_.IsEmpty()) {
    filter_block->SetUnownedValue(filter_block_.GetValue());
    return Status::OK();
  }

  ReadOptions read_options;
  if (no_io) {
    read_options.read_tier = kBlockCacheTier;
  }

  return ReadFilterBlock(table_, nullptr /* prefetch_buffer */, read_options,
                         cache_filter_blocks(), get_context, lookup_context,
                         filter_block);
}

template <typename TBlocklike>
size_t FilterBlockReaderCommon<TBlocklike>::ApproximateFilterBlockMemoryUsage()
    const {
  assert(!filter_block_.GetOwnValue() || filter_block_.GetValue() != nullptr);
  return filter_block_.GetOwnValue()
             ? filter_block_.GetValue()->ApproximateMemoryUsage()
             : 0;
}

// Explicitly instantiate templates for both "blocklike" types we use.
// This makes it possible to keep the template definitions in the .cc file.
template class FilterBlockReaderCommon<BlockContents>;
template class FilterBlockReaderCommon<Block>;
template class FilterBlockReaderCommon<ParsedFullFilterBlock>;

}  // namespace rocksdb
