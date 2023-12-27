//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/compressed_secondary_cache.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "memory/memory_allocator_impl.h"
#include "monitoring/perf_context_imp.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

CompressedSecondaryCache::CompressedSecondaryCache(
    const CompressedSecondaryCacheOptions& opts)
    : cache_(opts.LRUCacheOptions::MakeSharedCache()),
      cache_options_(opts),
      cache_res_mgr_(std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
              cache_))),
      disable_cache_(opts.capacity == 0) {}

CompressedSecondaryCache::~CompressedSecondaryCache() {}

std::unique_ptr<SecondaryCacheResultHandle> CompressedSecondaryCache::Lookup(
    const Slice& key, const Cache::CacheItemHelper* helper,
    Cache::CreateContext* create_context, bool /*wait*/, bool advise_erase,
    Statistics* stats, bool& kept_in_sec_cache) {
  assert(helper);
  // This is a minor optimization. Its ok to skip it in TSAN in order to
  // avoid a false positive.
#ifndef __SANITIZE_THREAD__
  if (disable_cache_) {
    return nullptr;
  }
#endif

  std::unique_ptr<SecondaryCacheResultHandle> handle;
  kept_in_sec_cache = false;
  Cache::Handle* lru_handle = cache_->Lookup(key);
  if (lru_handle == nullptr) {
    return nullptr;
  }

  void* handle_value = cache_->Value(lru_handle);
  if (handle_value == nullptr) {
    cache_->Release(lru_handle, /*erase_if_last_ref=*/false);
    RecordTick(stats, COMPRESSED_SECONDARY_CACHE_DUMMY_HITS);
    return nullptr;
  }

  CacheAllocationPtr* ptr{nullptr};
  CacheAllocationPtr merged_value;
  size_t handle_value_charge{0};
  const char* data_ptr = nullptr;
  CacheTier source = CacheTier::kVolatileCompressedTier;
  CompressionType type = cache_options_.compression_type;
  if (cache_options_.enable_custom_split_merge) {
    CacheValueChunk* value_chunk_ptr =
        reinterpret_cast<CacheValueChunk*>(handle_value);
    merged_value = MergeChunksIntoValue(value_chunk_ptr, handle_value_charge);
    ptr = &merged_value;
    data_ptr = ptr->get();
  } else {
    uint32_t type_32 = static_cast<uint32_t>(type);
    uint32_t source_32 = static_cast<uint32_t>(source);
    ptr = reinterpret_cast<CacheAllocationPtr*>(handle_value);
    handle_value_charge = cache_->GetCharge(lru_handle);
    data_ptr = ptr->get();
    data_ptr = GetVarint32Ptr(data_ptr, data_ptr + 1,
                              static_cast<uint32_t*>(&type_32));
    type = static_cast<CompressionType>(type_32);
    data_ptr = GetVarint32Ptr(data_ptr, data_ptr + 1,
                              static_cast<uint32_t*>(&source_32));
    source = static_cast<CacheTier>(source_32);
    handle_value_charge -= (data_ptr - ptr->get());
  }
  MemoryAllocator* allocator = cache_options_.memory_allocator.get();

  Status s;
  Cache::ObjectPtr value{nullptr};
  size_t charge{0};
  if (source == CacheTier::kVolatileCompressedTier) {
    if (cache_options_.compression_type == kNoCompression ||
        cache_options_.do_not_compress_roles.Contains(helper->role)) {
      s = helper->create_cb(Slice(data_ptr, handle_value_charge),
                            kNoCompression, CacheTier::kVolatileTier,
                            create_context, allocator, &value, &charge);
    } else {
      UncompressionContext uncompression_context(
          cache_options_.compression_type);
      UncompressionInfo uncompression_info(uncompression_context,
                                           UncompressionDict::GetEmptyDict(),
                                           cache_options_.compression_type);

      size_t uncompressed_size{0};
      CacheAllocationPtr uncompressed =
          UncompressData(uncompression_info, (char*)data_ptr,
                         handle_value_charge, &uncompressed_size,
                         cache_options_.compress_format_version, allocator);

      if (!uncompressed) {
        cache_->Release(lru_handle, /*erase_if_last_ref=*/true);
        return nullptr;
      }
      s = helper->create_cb(Slice(uncompressed.get(), uncompressed_size),
                            kNoCompression, CacheTier::kVolatileTier,
                            create_context, allocator, &value, &charge);
    }
  } else {
    // The item was not compressed by us. Let the helper create_cb
    // uncompress it
    s = helper->create_cb(Slice(data_ptr, handle_value_charge), type, source,
                          create_context, allocator, &value, &charge);
  }

  if (!s.ok()) {
    cache_->Release(lru_handle, /*erase_if_last_ref=*/true);
    return nullptr;
  }

  if (advise_erase) {
    cache_->Release(lru_handle, /*erase_if_last_ref=*/true);
    // Insert a dummy handle.
    cache_
        ->Insert(key, /*obj=*/nullptr,
                 GetHelper(cache_options_.enable_custom_split_merge),
                 /*charge=*/0)
        .PermitUncheckedError();
  } else {
    kept_in_sec_cache = true;
    cache_->Release(lru_handle, /*erase_if_last_ref=*/false);
  }
  handle.reset(new CompressedSecondaryCacheResultHandle(value, charge));
  RecordTick(stats, COMPRESSED_SECONDARY_CACHE_HITS);
  return handle;
}

bool CompressedSecondaryCache::MaybeInsertDummy(const Slice& key) {
  auto internal_helper = GetHelper(cache_options_.enable_custom_split_merge);
  Cache::Handle* lru_handle = cache_->Lookup(key);
  if (lru_handle == nullptr) {
    PERF_COUNTER_ADD(compressed_sec_cache_insert_dummy_count, 1);
    // Insert a dummy handle if the handle is evicted for the first time.
    cache_->Insert(key, /*obj=*/nullptr, internal_helper, /*charge=*/0)
        .PermitUncheckedError();
    return true;
  } else {
    cache_->Release(lru_handle, /*erase_if_last_ref=*/false);
  }

  return false;
}

Status CompressedSecondaryCache::InsertInternal(
    const Slice& key, Cache::ObjectPtr value,
    const Cache::CacheItemHelper* helper, CompressionType type,
    CacheTier source) {
  if (source != CacheTier::kVolatileCompressedTier &&
      cache_options_.enable_custom_split_merge) {
    // We don't support custom split/merge for the tiered case
    return Status::OK();
  }

  auto internal_helper = GetHelper(cache_options_.enable_custom_split_merge);
  char header[10];
  char* payload = header;
  payload = EncodeVarint32(payload, static_cast<uint32_t>(type));
  payload = EncodeVarint32(payload, static_cast<uint32_t>(source));

  size_t header_size = payload - header;
  size_t data_size = (*helper->size_cb)(value);
  size_t total_size = data_size + header_size;
  CacheAllocationPtr ptr =
      AllocateBlock(total_size, cache_options_.memory_allocator.get());
  char* data_ptr = ptr.get() + header_size;

  Status s = (*helper->saveto_cb)(value, 0, data_size, data_ptr);
  if (!s.ok()) {
    return s;
  }
  Slice val(data_ptr, data_size);

  std::string compressed_val;
  if (cache_options_.compression_type != kNoCompression &&
      type == kNoCompression &&
      !cache_options_.do_not_compress_roles.Contains(helper->role)) {
    PERF_COUNTER_ADD(compressed_sec_cache_uncompressed_bytes, data_size);
    CompressionOptions compression_opts;
    CompressionContext compression_context(cache_options_.compression_type,
                                           compression_opts);
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
    data_size = compressed_val.size();
    total_size = header_size + data_size;
    PERF_COUNTER_ADD(compressed_sec_cache_compressed_bytes, data_size);

    if (!cache_options_.enable_custom_split_merge) {
      ptr = AllocateBlock(total_size, cache_options_.memory_allocator.get());
      data_ptr = ptr.get() + header_size;
      memcpy(data_ptr, compressed_val.data(), data_size);
    }
  }

  PERF_COUNTER_ADD(compressed_sec_cache_insert_real_count, 1);
  if (cache_options_.enable_custom_split_merge) {
    size_t charge{0};
    CacheValueChunk* value_chunks_head =
        SplitValueIntoChunks(val, cache_options_.compression_type, charge);
    return cache_->Insert(key, value_chunks_head, internal_helper, charge);
  } else {
    std::memcpy(ptr.get(), header, header_size);
    CacheAllocationPtr* buf = new CacheAllocationPtr(std::move(ptr));
    return cache_->Insert(key, buf, internal_helper, total_size);
  }
}

Status CompressedSecondaryCache::Insert(const Slice& key,
                                        Cache::ObjectPtr value,
                                        const Cache::CacheItemHelper* helper,
                                        bool force_insert) {
  if (value == nullptr) {
    return Status::InvalidArgument();
  }

  if (!force_insert && MaybeInsertDummy(key)) {
    return Status::OK();
  }

  return InsertInternal(key, value, helper, kNoCompression,
                        CacheTier::kVolatileCompressedTier);
}

Status CompressedSecondaryCache::InsertSaved(
    const Slice& key, const Slice& saved, CompressionType type = kNoCompression,
    CacheTier source = CacheTier::kVolatileTier) {
  if (type == kNoCompression) {
    return Status::OK();
  }

  auto slice_helper = &kSliceCacheItemHelper;
  if (MaybeInsertDummy(key)) {
    return Status::OK();
  }

  return InsertInternal(
      key, static_cast<Cache::ObjectPtr>(const_cast<Slice*>(&saved)),
      slice_helper, type, source);
}

void CompressedSecondaryCache::Erase(const Slice& key) { cache_->Erase(key); }

Status CompressedSecondaryCache::SetCapacity(size_t capacity) {
  MutexLock l(&capacity_mutex_);
  cache_options_.capacity = capacity;
  cache_->SetCapacity(capacity);
  disable_cache_ = capacity == 0;
  return Status::OK();
}

Status CompressedSecondaryCache::GetCapacity(size_t& capacity) {
  MutexLock l(&capacity_mutex_);
  capacity = cache_options_.capacity;
  return Status::OK();
}

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
CompressedSecondaryCache::SplitValueIntoChunks(const Slice& value,
                                               CompressionType compression_type,
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

const Cache::CacheItemHelper* CompressedSecondaryCache::GetHelper(
    bool enable_custom_split_merge) const {
  if (enable_custom_split_merge) {
    static const Cache::CacheItemHelper kHelper{
        CacheEntryRole::kMisc,
        [](Cache::ObjectPtr obj, MemoryAllocator* /*alloc*/) {
          CacheValueChunk* chunks_head = static_cast<CacheValueChunk*>(obj);
          while (chunks_head != nullptr) {
            CacheValueChunk* tmp_chunk = chunks_head;
            chunks_head = chunks_head->next;
            tmp_chunk->Free();
            obj = nullptr;
          };
        }};
    return &kHelper;
  } else {
    static const Cache::CacheItemHelper kHelper{
        CacheEntryRole::kMisc,
        [](Cache::ObjectPtr obj, MemoryAllocator* /*alloc*/) {
          delete static_cast<CacheAllocationPtr*>(obj);
          obj = nullptr;
        }};
    return &kHelper;
  }
}

std::shared_ptr<SecondaryCache>
CompressedSecondaryCacheOptions::MakeSharedSecondaryCache() const {
  return std::make_shared<CompressedSecondaryCache>(*this);
}

Status CompressedSecondaryCache::Deflate(size_t decrease) {
  return cache_res_mgr_->UpdateCacheReservation(decrease, /*increase=*/true);
}

Status CompressedSecondaryCache::Inflate(size_t increase) {
  return cache_res_mgr_->UpdateCacheReservation(increase, /*increase=*/false);
}

}  // namespace ROCKSDB_NAMESPACE
