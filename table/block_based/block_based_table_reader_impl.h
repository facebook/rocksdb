//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "table/block_based/block_based_table_reader.h"

#include "table/block_based/reader_common.h"

// The file contains some member functions of BlockBasedTable that
// cannot be implemented in block_based_table_reader.cc because
// it's called by other files (e.g. block_based_iterator.h) and
// are templates.

namespace ROCKSDB_NAMESPACE {
// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
template <typename TBlockIter>
TBlockIter* BlockBasedTable::NewDataBlockIterator(
    const ReadOptions& ro, const BlockHandle& handle, TBlockIter* input_iter,
    BlockType block_type, GetContext* get_context,
    BlockCacheLookupContext* lookup_context, Status s,
    FilePrefetchBuffer* prefetch_buffer, bool for_compaction) const {
  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  TBlockIter* iter = input_iter != nullptr ? input_iter : new TBlockIter;
  if (!s.ok()) {
    iter->Invalidate(s);
    return iter;
  }

  CachableEntry<UncompressionDict> uncompression_dict;
  if (rep_->uncompression_dict_reader) {
    const bool no_io = (ro.read_tier == kBlockCacheTier);
    s = rep_->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        prefetch_buffer, no_io, get_context, lookup_context,
        &uncompression_dict);
    if (!s.ok()) {
      iter->Invalidate(s);
      return iter;
    }
  }

  const UncompressionDict& dict = uncompression_dict.GetValue()
                                      ? *uncompression_dict.GetValue()
                                      : UncompressionDict::GetEmptyDict();

  CachableEntry<Block> block;
  s = RetrieveBlock(prefetch_buffer, ro, handle, dict, &block, block_type,
                    get_context, lookup_context, for_compaction,
                    /* use_cache */ true, /* wait_for_cache */ true);

  if (!s.ok()) {
    assert(block.IsEmpty());
    iter->Invalidate(s);
    return iter;
  }

  assert(block.GetValue() != nullptr);

  // Block contents are pinned and it is still pinned after the iterator
  // is destroyed as long as cleanup functions are moved to another object,
  // when:
  // 1. block cache handle is set to be released in cleanup function, or
  // 2. it's pointing to immortal source. If own_bytes is true then we are
  //    not reading data from the original source, whether immortal or not.
  //    Otherwise, the block is pinned iff the source is immortal.
  const bool block_contents_pinned =
      block.IsCached() ||
      (!block.GetValue()->own_bytes() && rep_->immortal_table);
  iter = InitBlockIterator<TBlockIter>(rep_, block.GetValue(), block_type, iter,
                                       block_contents_pinned);

  if (!block.IsCached()) {
    if (!ro.fill_cache) {
      Cache* const block_cache = rep_->table_options.block_cache.get();
      if (block_cache) {
        // insert a dummy record to block cache to track the memory usage
        Cache::Handle* cache_handle = nullptr;
        CacheKey key = CacheKey::CreateUniqueForCacheLifetime(block_cache);
        s = block_cache->Insert(key.AsSlice(), nullptr,
                                block.GetValue()->ApproximateMemoryUsage(),
                                nullptr, &cache_handle);

        if (s.ok()) {
          assert(cache_handle != nullptr);
          iter->RegisterCleanup(&ForceReleaseCachedEntry, block_cache,
                                cache_handle);
        }
      }
    }
  } else {
    iter->SetCacheHandle(block.GetCacheHandle());
  }

  block.TransferTo(iter);

  return iter;
}

// Convert an uncompressed data block (i.e CachableEntry<Block>)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
template <typename TBlockIter>
TBlockIter* BlockBasedTable::NewDataBlockIterator(const ReadOptions& ro,
                                                  CachableEntry<Block>& block,
                                                  TBlockIter* input_iter,
                                                  Status s) const {
  PERF_TIMER_GUARD(new_table_block_iter_nanos);

  TBlockIter* iter = input_iter != nullptr ? input_iter : new TBlockIter;
  if (!s.ok()) {
    iter->Invalidate(s);
    return iter;
  }

  assert(block.GetValue() != nullptr);
  // Block contents are pinned and it is still pinned after the iterator
  // is destroyed as long as cleanup functions are moved to another object,
  // when:
  // 1. block cache handle is set to be released in cleanup function, or
  // 2. it's pointing to immortal source. If own_bytes is true then we are
  //    not reading data from the original source, whether immortal or not.
  //    Otherwise, the block is pinned iff the source is immortal.
  const bool block_contents_pinned =
      block.IsCached() ||
      (!block.GetValue()->own_bytes() && rep_->immortal_table);
  iter = InitBlockIterator<TBlockIter>(rep_, block.GetValue(), BlockType::kData,
                                       iter, block_contents_pinned);

  if (!block.IsCached()) {
    if (!ro.fill_cache) {
      Cache* const block_cache = rep_->table_options.block_cache.get();
      if (block_cache) {
        // insert a dummy record to block cache to track the memory usage
        Cache::Handle* cache_handle = nullptr;
        CacheKey key = CacheKey::CreateUniqueForCacheLifetime(block_cache);
        s = block_cache->Insert(key.AsSlice(), nullptr,
                                block.GetValue()->ApproximateMemoryUsage(),
                                nullptr, &cache_handle);

        if (s.ok()) {
          assert(cache_handle != nullptr);
          iter->RegisterCleanup(&ForceReleaseCachedEntry, block_cache,
                                cache_handle);
        }
      }
    }
  } else {
    iter->SetCacheHandle(block.GetCacheHandle());
  }

  block.TransferTo(iter);
  return iter;
}
}  // namespace ROCKSDB_NAMESPACE
