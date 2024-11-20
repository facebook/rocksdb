//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/index_reader_common.h"

#include "block_cache.h"

namespace ROCKSDB_NAMESPACE {
Status BlockBasedTable::IndexReaderCommon::ReadIndexBlock(
    const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
    const ReadOptions& read_options, bool use_cache, GetContext* get_context,
    BlockCacheLookupContext* lookup_context,
    CachableEntry<Block>* index_block) {
  PERF_TIMER_GUARD(read_index_block_nanos);

  assert(table != nullptr);
  assert(index_block != nullptr);
  assert(index_block->IsEmpty());

  const Rep* const rep = table->get_rep();
  assert(rep != nullptr);

  const Status s = table->RetrieveBlock(
      prefetch_buffer, read_options, rep->index_handle,
      UncompressionDict::GetEmptyDict(), &index_block->As<Block_kIndex>(),
      get_context, lookup_context, /* for_compaction */ false, use_cache,
      /* async_read */ false, /* use_block_cache_for_lookup */ true);

  return s;
}

Status BlockBasedTable::IndexReaderCommon::GetOrReadIndexBlock(
    GetContext* get_context, BlockCacheLookupContext* lookup_context,
    CachableEntry<Block>* index_block, const ReadOptions& ro) const {
  assert(index_block != nullptr);

  if (!index_block_.IsEmpty()) {
    index_block->SetUnownedValue(index_block_.GetValue());
    return Status::OK();
  }

  return ReadIndexBlock(table_, /*prefetch_buffer=*/nullptr, ro,
                        cache_index_blocks(), get_context, lookup_context,
                        index_block);
}

void BlockBasedTable::IndexReaderCommon::EraseFromCacheBeforeDestruction(
    uint32_t uncache_aggressiveness) {
  if (uncache_aggressiveness > 0) {
    if (index_block_.IsCached()) {
      index_block_.ResetEraseIfLastRef();
    } else {
      table()->EraseFromCache(table()->get_rep()->index_handle);
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
