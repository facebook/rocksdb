//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/compressed_secondary_cache.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "memory/memory_allocator.h"
#include "util/compression.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

CompressedSecondaryCache::CompressedSecondaryCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    CompressionType compression_type, uint32_t compress_format_version)
    : cache_options_(capacity, num_shard_bits, strict_capacity_limit,
                     high_pri_pool_ratio, memory_allocator, use_adaptive_mutex,
                     metadata_charge_policy, compression_type,
                     compress_format_version) {
  cache_ = NewLRUCache(capacity, num_shard_bits, strict_capacity_limit,
                       high_pri_pool_ratio, memory_allocator,
                       use_adaptive_mutex, metadata_charge_policy);
}

CompressedSecondaryCache::~CompressedSecondaryCache() { cache_.reset(); }

std::unique_ptr<SecondaryCacheResultHandle> CompressedSecondaryCache::Lookup(
    const Slice& key, const Cache::CreateCallback& create_cb, bool /*wait*/,
    bool& is_in_sec_cache) {
  std::unique_ptr<SecondaryCacheResultHandle> handle;
  is_in_sec_cache = false;
  Cache::Handle* lru_handle = cache_->Lookup(key);
  if (lru_handle == nullptr) {
    return handle;
  }

  std::unique_ptr<CacheValueChunk>* ptr =
      reinterpret_cast<std::unique_ptr<CacheValueChunk>*>(
          cache_->Value(lru_handle));
  size_t handle_value_charge = 0;
  CacheAllocationPtr handle_value =
      MergeChunksIntoValue(ptr->get(), handle_value_charge);

  Status s;
  void* value = nullptr;
  size_t charge = 0;
  if (cache_options_.compression_type == kNoCompression) {
    s = create_cb(handle_value.get(), handle_value_charge, &value, &charge);
  } else {
    UncompressionContext uncompression_context(cache_options_.compression_type);
    UncompressionInfo uncompression_info(uncompression_context,
                                         UncompressionDict::GetEmptyDict(),
                                         cache_options_.compression_type);

    size_t uncompressed_size = 0;
    CacheAllocationPtr uncompressed;
    uncompressed = UncompressData(uncompression_info, (char*)handle_value.get(),
                                  handle_value_charge, &uncompressed_size,
                                  cache_options_.compress_format_version,
                                  cache_options_.memory_allocator.get());

    if (!uncompressed) {
      cache_->Release(lru_handle, /* erase_if_last_ref */ true);
      return handle;
    }
    s = create_cb(uncompressed.get(), uncompressed_size, &value, &charge);
  }

  if (!s.ok()) {
    cache_->Release(lru_handle, /* erase_if_last_ref */ true);
    return handle;
  }

  cache_->Release(lru_handle, /* erase_if_last_ref */ true);
  handle.reset(new CompressedSecondaryCacheResultHandle(value, charge));

  return handle;
}

Status CompressedSecondaryCache::Insert(const Slice& key, void* value,
                                        const Cache::CacheItemHelper* helper) {
  size_t size = (*helper->size_cb)(value);
  CacheAllocationPtr ptr =
      AllocateBlock(size, cache_options_.memory_allocator.get());

  Status s = (*helper->saveto_cb)(value, 0, size, ptr.get());
  if (!s.ok()) {
    return s;
  }
  Slice val(ptr.get(), size);

  std::string compressed_val;
  if (cache_options_.compression_type != kNoCompression) {
    CompressionOptions compression_opts;
    CompressionContext compression_context(cache_options_.compression_type);
    uint64_t sample_for_compression = 0;
    CompressionInfo compression_info(
        compression_opts, compression_context, CompressionDict::GetEmptyDict(),
        cache_options_.compression_type, sample_for_compression);

    bool success =
        CompressData(val, compression_info,
                     cache_options_.compress_format_version, &compressed_val);

    if (!success) {
      return Status::Corruption("Error compressing value.");
    }

    val = Slice(compressed_val);
    size = compressed_val.size();
  }

  std::unique_ptr<CacheValueChunk> value_chunks_head =
      SplitValueIntoChunks(val, cache_options_.compression_type, size);
  std::unique_ptr<CacheValueChunk>* buf =
      new std::unique_ptr<CacheValueChunk>(std::move(value_chunks_head));
  return cache_->Insert(key, buf, size, DeletionCallback);
}

void CompressedSecondaryCache::Erase(const Slice& key) { cache_->Erase(key); }

std::string CompressedSecondaryCache::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  ret.append(cache_->GetPrintableOptions());
  snprintf(buffer, kBufferSize, "    compression_type : %s\n",
           CompressionTypeToString(cache_options_.compression_type).c_str());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    compress_format_version : %d\n",
           cache_options_.compress_format_version);
  ret.append(buffer);
  return ret;
}

std::unique_ptr<CompressedSecondaryCache::CacheValueChunk>
CompressedSecondaryCache::SplitValueIntoChunks(
    const Slice& value, const CompressionType compression_type,
    size_t& charge) {
  assert(!value.empty());
  // If charge > the max size
  const char* src_ptr = value.data();
  size_t src_size = charge;
  CacheAllocationPtr ptr;

  std::unique_ptr<CacheValueChunk> head = std::make_unique<CacheValueChunk>();
  CacheValueChunk* current_chunk = head.get();
  // Do not split when value size is large or there is no compression.
  if (src_size >= malloc_bin_sizes_.back() ||
      compression_type == kNoCompression) {
    charge += sizeof(*current_chunk);
    ptr = AllocateBlock(src_size, cache_options_.memory_allocator.get());
    memcpy(ptr.get(), src_ptr, src_size);
    current_chunk->chunk_ptr = std::move(ptr);
    current_chunk->charge = src_size;

  } else {
    while (src_size > 0) {
      charge += sizeof(*current_chunk);
      if (src_size <= malloc_bin_sizes_.front()) {
        ptr = AllocateBlock(src_size, cache_options_.memory_allocator.get());
        memcpy(ptr.get(), src_ptr, src_size);
        current_chunk->chunk_ptr = std::move(ptr);
        current_chunk->charge = src_size;
        src_size = 0;
      } else {
        auto upper = std::upper_bound(malloc_bin_sizes_.begin(),
                                      malloc_bin_sizes_.end(), src_size);
        current_chunk->charge = *(--upper);
        ptr = AllocateBlock(current_chunk->charge,
                            cache_options_.memory_allocator.get());
        memcpy(ptr.get(), src_ptr, current_chunk->charge);
        current_chunk->chunk_ptr = std::move(ptr);
        src_size -= current_chunk->charge;
        src_ptr += current_chunk->charge;
      }
      if (src_size > 0) {
        current_chunk->next = std::make_unique<CacheValueChunk>();
        current_chunk = current_chunk->next.get();
      }
    }
  }

  return head;
}

CacheAllocationPtr CompressedSecondaryCache::MergeChunksIntoValue(
    const void* chunks_head, size_t& charge) {
  const CacheValueChunk* head =
      reinterpret_cast<const CacheValueChunk*>(chunks_head);
  const CacheValueChunk* current_chunk = head;
  charge = 0;
  while (current_chunk != nullptr && current_chunk->charge > 0) {
    charge += current_chunk->charge;
    current_chunk = current_chunk->next.get();
  }

  CacheAllocationPtr ptr =
      AllocateBlock(charge, cache_options_.memory_allocator.get());
  current_chunk = head;
  size_t pos = 0;
  while (current_chunk != nullptr) {
    memcpy(ptr.get() + pos, current_chunk->chunk_ptr.get(),
           current_chunk->charge);
    pos += current_chunk->charge;
    current_chunk = current_chunk->next.get();
  }

  return ptr;
}

void CompressedSecondaryCache::DeletionCallback(const Slice& /*key*/,
                                                void* obj) {
  delete reinterpret_cast<std::unique_ptr<CacheValueChunk>*>(obj);
  obj = nullptr;
}

std::shared_ptr<SecondaryCache> NewCompressedSecondaryCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    CompressionType compression_type, uint32_t compress_format_version) {
  return std::make_shared<CompressedSecondaryCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      memory_allocator, use_adaptive_mutex, metadata_charge_policy,
      compression_type, compress_format_version);
}

std::shared_ptr<SecondaryCache> NewCompressedSecondaryCache(
    const CompressedSecondaryCacheOptions& opts) {
  // The secondary_cache is disabled for this LRUCache instance.
  assert(opts.secondary_cache == nullptr);
  return NewCompressedSecondaryCache(
      opts.capacity, opts.num_shard_bits, opts.strict_capacity_limit,
      opts.high_pri_pool_ratio, opts.memory_allocator, opts.use_adaptive_mutex,
      opts.metadata_charge_policy, opts.compression_type,
      opts.compress_format_version);
}

}  // namespace ROCKSDB_NAMESPACE
