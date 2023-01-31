// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <cstddef>
#include <memory>

#include "cache/lru_cache.h"
#include "memory/memory_allocator.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/compression.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class CompressedSecondaryCacheResultHandle : public SecondaryCacheResultHandle {
 public:
  CompressedSecondaryCacheResultHandle(Cache::ObjectPtr value, size_t size)
      : value_(value), size_(size) {}
  ~CompressedSecondaryCacheResultHandle() override = default;

  CompressedSecondaryCacheResultHandle(
      const CompressedSecondaryCacheResultHandle&) = delete;
  CompressedSecondaryCacheResultHandle& operator=(
      const CompressedSecondaryCacheResultHandle&) = delete;

  bool IsReady() override { return true; }

  void Wait() override {}

  Cache::ObjectPtr Value() override { return value_; }

  size_t Size() override { return size_; }

 private:
  Cache::ObjectPtr value_;
  size_t size_;
};

// The CompressedSecondaryCache is a concrete implementation of
// rocksdb::SecondaryCache.
//
// When a block is found from CompressedSecondaryCache::Lookup, we check whether
// there is a dummy block with the same key in the primary cache.
// 1. If the dummy block exits, we erase the block from
//    CompressedSecondaryCache and insert it into the primary cache.
// 2. If not, we just insert a dummy block into the primary cache
//    (charging the actual size of the block) and don not erase the block from
//    CompressedSecondaryCache. A standalone handle is returned to the caller.
//
// When a block is evicted from the primary cache, we check whether
// there is a dummy block with the same key in CompressedSecondaryCache.
// 1. If the dummy block exits, the block is inserted into
//    CompressedSecondaryCache.
// 2. If not, we just insert a dummy block (size 0) in CompressedSecondaryCache.
//
// Users can also cast a pointer to CompressedSecondaryCache and call methods on
// it directly, especially custom methods that may be added
// in the future.  For example -
// std::unique_ptr<rocksdb::SecondaryCache> cache =
//      NewCompressedSecondaryCache(opts);
// static_cast<CompressedSecondaryCache*>(cache.get())->Erase(key);

class CompressedSecondaryCache : public SecondaryCache {
 public:
  CompressedSecondaryCache(
      size_t capacity, int num_shard_bits, bool strict_capacity_limit,
      double high_pri_pool_ratio, double low_pri_pool_ratio,
      std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
      bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
      CacheMetadataChargePolicy metadata_charge_policy =
          kDefaultCacheMetadataChargePolicy,
      CompressionType compression_type = CompressionType::kLZ4Compression,
      uint32_t compress_format_version = 2,
      bool enable_custom_split_merge = false);
  ~CompressedSecondaryCache() override;

  const char* Name() const override { return "CompressedSecondaryCache"; }

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper) override;

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool /*wait*/, bool advise_erase,
      bool& is_in_sec_cache) override;

  bool SupportForceErase() const override { return true; }

  void Erase(const Slice& key) override;

  void WaitAll(std::vector<SecondaryCacheResultHandle*> /*handles*/) override {}

  Status SetCapacity(size_t capacity) override;

  Status GetCapacity(size_t& capacity) override;

  std::string GetPrintableOptions() const override;

 private:
  friend class CompressedSecondaryCacheTest;
  static constexpr std::array<uint16_t, 8> malloc_bin_sizes_{
      128, 256, 512, 1024, 2048, 4096, 8192, 16384};

  struct CacheValueChunk {
    // TODO try "CacheAllocationPtr next;".
    CacheValueChunk* next;
    size_t size;
    // Beginning of the chunk data (MUST BE THE LAST FIELD IN THIS STRUCT!)
    char data[1];

    void Free() { delete[] reinterpret_cast<char*>(this); }
  };

  // Split value into chunks to better fit into jemalloc bins. The chunks
  // are stored in CacheValueChunk and extra charge is needed for each chunk,
  // so the cache charge is recalculated here.
  CacheValueChunk* SplitValueIntoChunks(const Slice& value,
                                        CompressionType compression_type,
                                        size_t& charge);

  // After merging chunks, the extra charge for each chunk is removed, so
  // the charge is recalculated.
  CacheAllocationPtr MergeChunksIntoValue(const void* chunks_head,
                                          size_t& charge);

  // TODO: clean up to use cleaner interfaces in typed_cache.h
  const Cache::CacheItemHelper* GetHelper(bool enable_custom_split_merge) const;
  std::shared_ptr<Cache> cache_;
  CompressedSecondaryCacheOptions cache_options_;
  mutable port::Mutex capacity_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
