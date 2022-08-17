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
    double high_pri_pool_ratio, double low_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    CompressionType compression_type, uint32_t compress_format_version)
    : cache_options_(capacity, num_shard_bits, strict_capacity_limit,
                     high_pri_pool_ratio, memory_allocator, use_adaptive_mutex,
                     metadata_charge_policy, compression_type,
                     compress_format_version, low_pri_pool_ratio) {
  cache_ =
      NewLRUCache(capacity, num_shard_bits, strict_capacity_limit,
                  high_pri_pool_ratio, memory_allocator, use_adaptive_mutex,
                  metadata_charge_policy, low_pri_pool_ratio);
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

  CacheValueChunk* handle_value =
      reinterpret_cast<CacheValueChunk*>(cache_->Value(lru_handle));
  size_t handle_value_charge{0};
  CacheAllocationPtr merged_value =
      MergeChunksIntoValue(handle_value, handle_value_charge);

  Status s;
  void* value{nullptr};
  size_t charge{0};
  if (cache_options_.compression_type == kNoCompression) {
    s = create_cb(merged_value.get(), handle_value_charge, &value, &charge);
  } else {
    UncompressionContext uncompression_context(cache_options_.compression_type);
    UncompressionInfo uncompression_info(uncompression_context,
                                         UncompressionDict::GetEmptyDict(),
                                         cache_options_.compression_type);

    size_t uncompressed_size{0};
    CacheAllocationPtr uncompressed;
    uncompressed = UncompressData(uncompression_info, (char*)merged_value.get(),
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
    uint64_t sample_for_compression{0};
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
  }

  size_t charge{0};
  CacheValueChunk* value_chunks_head =
      SplitValueIntoChunks(val, cache_options_.compression_type, charge);
  return cache_->Insert(key, value_chunks_head, charge, DeletionCallback);
}

void CompressedSecondaryCache::Erase(const Slice& key) { cache_->Erase(key); }

std::string CompressedSecondaryCache::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize{200};
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

CompressedSecondaryCache::CacheValueChunk*
CompressedSecondaryCache::SplitValueIntoChunks(
    const Slice& value, const CompressionType compression_type,
    size_t& charge) {
  assert(!value.empty());
  const char* src_ptr = value.data();
  size_t src_size{value.size()};

  CacheValueChunk dummy_head = CacheValueChunk();
  CacheValueChunk* current_chunk = &dummy_head;
  // Do not split when value size is large or there is no compression.
  size_t predicted_chunk_size{0};
  size_t actual_chunk_size{0};
  size_t tmp_size{0};
  while (src_size > 0) {
    predicted_chunk_size = sizeof(CacheValueChunk) - 1 + src_size;
    auto upper =
        std::upper_bound(malloc_bin_sizes_.begin(), malloc_bin_sizes_.end(),
                         predicted_chunk_size);
    // Do not split when value size is too small, too large, close to a bin
    // size, or there is no compression.
    if (upper == malloc_bin_sizes_.begin() ||
        upper == malloc_bin_sizes_.end() ||
        *upper - predicted_chunk_size < malloc_bin_sizes_.front() ||
        compression_type == kNoCompression) {
      tmp_size = predicted_chunk_size;
    } else {
      tmp_size = *(--upper);
    }

    CacheValueChunk* new_chunk =
        reinterpret_cast<CacheValueChunk*>(new char[tmp_size]);
    current_chunk->next = new_chunk;
    current_chunk = current_chunk->next;
    actual_chunk_size = tmp_size - sizeof(CacheValueChunk) + 1;
    memcpy(current_chunk->data, src_ptr, actual_chunk_size);
    current_chunk->size = actual_chunk_size;
    src_ptr += actual_chunk_size;
    src_size -= actual_chunk_size;
    charge += tmp_size;
  }
  current_chunk->next = nullptr;

  return dummy_head.next;
}

CacheAllocationPtr CompressedSecondaryCache::MergeChunksIntoValue(
    const void* chunks_head, size_t& charge) {
  const CacheValueChunk* head =
      reinterpret_cast<const CacheValueChunk*>(chunks_head);
  const CacheValueChunk* current_chunk = head;
  charge = 0;
  while (current_chunk != nullptr) {
    charge += current_chunk->size;
    current_chunk = current_chunk->next;
  }

  CacheAllocationPtr ptr =
      AllocateBlock(charge, cache_options_.memory_allocator.get());
  current_chunk = head;
  size_t pos{0};
  while (current_chunk != nullptr) {
    memcpy(ptr.get() + pos, current_chunk->data, current_chunk->size);
    pos += current_chunk->size;
    current_chunk = current_chunk->next;
  }

  return ptr;
}

void CompressedSecondaryCache::DeletionCallback(const Slice& /*key*/,
                                                void* obj) {
  CacheValueChunk* chunks_head = reinterpret_cast<CacheValueChunk*>(obj);
  while (chunks_head != nullptr) {
    CacheValueChunk* tmp_chunk = chunks_head;
    chunks_head = chunks_head->next;
    tmp_chunk->Free();
  }
  obj = nullptr;
}

std::shared_ptr<SecondaryCache> NewCompressedSecondaryCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    CompressionType compression_type, uint32_t compress_format_version,
    double low_pri_pool_ratio) {
  return std::make_shared<CompressedSecondaryCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      low_pri_pool_ratio, memory_allocator, use_adaptive_mutex,
      metadata_charge_policy, compression_type, compress_format_version);
}

std::shared_ptr<SecondaryCache> NewCompressedSecondaryCache(
    const CompressedSecondaryCacheOptions& opts) {
  // The secondary_cache is disabled for this LRUCache instance.
  assert(opts.secondary_cache == nullptr);
  return NewCompressedSecondaryCache(
      opts.capacity, opts.num_shard_bits, opts.strict_capacity_limit,
      opts.high_pri_pool_ratio, opts.memory_allocator, opts.use_adaptive_mutex,
      opts.metadata_charge_policy, opts.compression_type,
      opts.compress_format_version, opts.low_pri_pool_ratio);
}

}  // namespace ROCKSDB_NAMESPACE
