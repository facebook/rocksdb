//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/persistent_cache_helper.h"

#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

const PersistentCacheOptions PersistentCacheOptions::kEmpty;

void PersistentCacheHelper::InsertSerialized(
    const PersistentCacheOptions& cache_options, const BlockHandle& handle,
    const char* data, const size_t size) {
  assert(cache_options.persistent_cache);
  assert(cache_options.persistent_cache->IsCompressed());

  CacheKey key =
      BlockBasedTable::GetCacheKey(cache_options.base_cache_key, handle);

  cache_options.persistent_cache->Insert(key.AsSlice(), data, size)
      .PermitUncheckedError();
}

void PersistentCacheHelper::InsertUncompressed(
    const PersistentCacheOptions& cache_options, const BlockHandle& handle,
    const BlockContents& contents) {
  assert(cache_options.persistent_cache);
  assert(!cache_options.persistent_cache->IsCompressed());
  // Precondition:
  // (1) content is cacheable
  // (2) content is not compressed

  CacheKey key =
      BlockBasedTable::GetCacheKey(cache_options.base_cache_key, handle);

  cache_options.persistent_cache
      ->Insert(key.AsSlice(), contents.data.data(), contents.data.size())
      .PermitUncheckedError();
  ;
}

Status PersistentCacheHelper::LookupSerialized(
    const PersistentCacheOptions& cache_options, const BlockHandle& handle,
    std::unique_ptr<char[]>* out_data, const size_t expected_data_size) {
#ifdef NDEBUG
  (void)expected_data_size;
#endif
  assert(cache_options.persistent_cache);
  assert(cache_options.persistent_cache->IsCompressed());

  CacheKey key =
      BlockBasedTable::GetCacheKey(cache_options.base_cache_key, handle);

  size_t size;
  Status s =
      cache_options.persistent_cache->Lookup(key.AsSlice(), out_data, &size);
  if (!s.ok()) {
    // cache miss
    RecordTick(cache_options.statistics, PERSISTENT_CACHE_MISS);
    return s;
  }

  // cache hit
  // Block-based table is assumed
  assert(expected_data_size ==
         handle.size() + BlockBasedTable::kBlockTrailerSize);
  assert(size == expected_data_size);
  RecordTick(cache_options.statistics, PERSISTENT_CACHE_HIT);
  return Status::OK();
}

Status PersistentCacheHelper::LookupUncompressed(
    const PersistentCacheOptions& cache_options, const BlockHandle& handle,
    BlockContents* contents) {
  assert(cache_options.persistent_cache);
  assert(!cache_options.persistent_cache->IsCompressed());
  if (!contents) {
    // We shouldn't lookup in the cache. Either
    // (1) Nowhere to store
    return Status::NotFound();
  }

  CacheKey key =
      BlockBasedTable::GetCacheKey(cache_options.base_cache_key, handle);

  std::unique_ptr<char[]> data;
  size_t size;
  Status s =
      cache_options.persistent_cache->Lookup(key.AsSlice(), &data, &size);
  if (!s.ok()) {
    // cache miss
    RecordTick(cache_options.statistics, PERSISTENT_CACHE_MISS);
    return s;
  }

  // please note we are potentially comparing compressed data size with
  // uncompressed data size
  assert(handle.size() <= size);

  // update stats
  RecordTick(cache_options.statistics, PERSISTENT_CACHE_HIT);
  // construct result and return
  *contents = BlockContents(std::move(data), size);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
