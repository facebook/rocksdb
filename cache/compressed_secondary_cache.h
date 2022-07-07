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

namespace ROCKSDB_NAMESPACE {

class CompressedSecondaryCacheResultHandle : public SecondaryCacheResultHandle {
 public:
  CompressedSecondaryCacheResultHandle(void* value, size_t size)
      : value_(value), size_(size) {}
  virtual ~CompressedSecondaryCacheResultHandle() override = default;

  CompressedSecondaryCacheResultHandle(
      const CompressedSecondaryCacheResultHandle&) = delete;
  CompressedSecondaryCacheResultHandle& operator=(
      const CompressedSecondaryCacheResultHandle&) = delete;

  bool IsReady() override { return true; }

  void Wait() override {}

  void* Value() override { return value_; }

  size_t Size() override { return size_; }

 private:
  void* value_;
  size_t size_;
};

// The CompressedSecondaryCache is a concrete implementation of
// rocksdb::SecondaryCache.
//
// Users can also cast a pointer to it and call methods on
// it directly, especially custom methods that may be added
// in the future.  For example -
// std::unique_ptr<rocksdb::SecondaryCache> cache =
//      NewCompressedSecondaryCache(opts);
// static_cast<CompressedSecondaryCache*>(cache.get())->Erase(key);

class CompressedSecondaryCache : public SecondaryCache {
 public:
  CompressedSecondaryCache(
      size_t capacity, int num_shard_bits, bool strict_capacity_limit,
      double high_pri_pool_ratio = 0.0,
      std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
      bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
      CacheMetadataChargePolicy metadata_charge_policy =
          kDefaultCacheMetadataChargePolicy,
      CompressionType compression_type = CompressionType::kLZ4Compression,
      uint32_t compress_format_version = 2);
  virtual ~CompressedSecondaryCache() override;

  const char* Name() const override { return "CompressedSecondaryCache"; }

  Status Insert(const Slice& key, void* value,
                const Cache::CacheItemHelper* helper) override;

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CreateCallback& create_cb, bool /*wait*/,
      bool& is_in_sec_cache) override;

  void Erase(const Slice& key) override;

  void WaitAll(std::vector<SecondaryCacheResultHandle*> /*handles*/) override {}

  std::string GetPrintableOptions() const override;

 private:
  friend class CompressedSecondaryCacheTest;
  // TODO add CACHE_LINE_SIZE info.
  // TODO what should be min and max size here.
  static constexpr std::array<uint16_t, 19> malloc_bin_sizes_{
      64,  96,  128, 160, 192,  224,  256,  320,  384,  448,
      512, 640, 768, 896, 1024, 2048, 4096, 8192, 16384};
  struct CacheValueChunk {
    CacheAllocationPtr* chunk_ptr = nullptr;
    size_t charge = 0;
    CacheValueChunk* next = nullptr;
  };

  // Split value into chunks to better fit into jemalloc bins. The chunks
  // are stored in CacheValueChunk and extra charge is needed for each chunk,
  // so the cache charge is recalculated here.
  CacheValueChunk* SplitValueIntoChunks(const std::string& value,
                                        size_t& charge);

  // After merging chunks, the extra charge for each chunk is removed, so
  // the charge is recalculated.
  CacheAllocationPtr* MergeChunksIntoValue(const void* chunks_head,
                                           size_t& charge);
  std::shared_ptr<Cache> cache_;
  CompressedSecondaryCacheOptions cache_options_;
};

}  // namespace ROCKSDB_NAMESPACE
