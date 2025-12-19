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
namespace {
// Format of values in CompressedSecondaryCache:
// If enable_custom_split_merge:
//  * A chain of CacheValueChunk representing the sequence of bytes for a tagged
//    value. The overall length of the tagged value is determined by the chain
//    of CacheValueChunks.
// If !enable_custom_split_merge:
//  * A LengthPrefixedSlice (starts with varint64 size) of a tagged value.
//
// A tagged value has a 2-byte header before the "saved" or compressed block
// data:
//  * 1 byte for "source" CacheTier indicating which tier is responsible for
//    compression/decompression.
//  * 1 byte for compression type which is generated/used by
//    CompressedSecondaryCache iff source == CacheTier::kVolatileCompressedTier
//    (original entry passed in was uncompressed). Otherwise, the compression
//    type is preserved from the entry passed in.
constexpr uint32_t kTagSize = 2;

// Size of tag + varint size prefix when applicable
uint32_t GetHeaderSize(size_t data_size, bool enable_split_merge) {
  return (enable_split_merge ? 0 : VarintLength(kTagSize + data_size)) +
         kTagSize;
}
}  // namespace

CompressedSecondaryCache::CompressedSecondaryCache(
    const CompressedSecondaryCacheOptions& opts)
    : cache_(opts.LRUCacheOptions::MakeSharedCache()),
      cache_options_(opts),
      cache_res_mgr_(std::make_shared<ConcurrentCacheReservationManager>(
          std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
              cache_))),
      disable_cache_(opts.capacity == 0) {
  auto mgr =
      GetBuiltinCompressionManager(cache_options_.compress_format_version);
  compressor_ = mgr->GetCompressor(cache_options_.compression_opts,
                                   cache_options_.compression_type);
  decompressor_ =
      mgr->GetDecompressorOptimizeFor(cache_options_.compression_type);
}

CompressedSecondaryCache::~CompressedSecondaryCache() = default;

std::unique_ptr<SecondaryCacheResultHandle> CompressedSecondaryCache::Lookup(
    const Slice& key, const Cache::CacheItemHelper* helper,
    Cache::CreateContext* create_context, bool /*wait*/, bool advise_erase,
    Statistics* stats, bool& kept_in_sec_cache) {
  assert(helper);
  if (disable_cache_.LoadRelaxed()) {
    return nullptr;
  }

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

  std::string merged_value;
  Slice tagged_data;
  if (cache_options_.enable_custom_split_merge) {
    CacheValueChunk* value_chunk_ptr =
        static_cast<CacheValueChunk*>(handle_value);
    merged_value = MergeChunksIntoValue(value_chunk_ptr);
    tagged_data = Slice(merged_value);
  } else {
    tagged_data = GetLengthPrefixedSlice(static_cast<char*>(handle_value));
  }

  auto source = lossless_cast<CacheTier>(tagged_data[0]);
  auto type = lossless_cast<CompressionType>(tagged_data[1]);

  std::unique_ptr<char[]> uncompressed;
  Slice saved(tagged_data.data() + kTagSize, tagged_data.size() - kTagSize);
  if (source == CacheTier::kVolatileCompressedTier) {
    if (type != kNoCompression) {
      // TODO: can we do something to avoid yet another allocation?
      Decompressor::Args args;
      args.compressed_data = saved;
      args.compression_type = type;
      Status s = decompressor_->ExtractUncompressedSize(args);
      assert(s.ok());  // in-memory data
      if (s.ok()) {
        uncompressed = std::make_unique<char[]>(args.uncompressed_size);
        s = decompressor_->DecompressBlock(args, uncompressed.get());
        assert(s.ok());  // in-memory data
      }
      if (!s.ok()) {
        cache_->Release(lru_handle, /*erase_if_last_ref=*/true);
        return nullptr;
      }
      saved = Slice(uncompressed.get(), args.uncompressed_size);
      type = kNoCompression;
      // Free temporary compressed data as early as we can. This could matter
      // for unusually large blocks because we also have
      // * Another compressed copy above (from lru_cache).
      // * The uncompressed copy in `uncompressed`.
      // * Another uncompressed copy in `result_value` below.
      // Let's try to max out at 3 copies instead of 4.
      merged_value = std::string();
    }
    // Reduced as if it came from primary cache
    source = CacheTier::kVolatileTier;
  }

  Cache::ObjectPtr result_value = nullptr;
  size_t result_charge = 0;
  Status s = helper->create_cb(saved, type, source, create_context,
                               cache_options_.memory_allocator.get(),
                               &result_value, &result_charge);
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
  handle.reset(
      new CompressedSecondaryCacheResultHandle(result_value, result_charge));
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
    const Cache::CacheItemHelper* helper, CompressionType from_type,
    CacheTier source) {
  bool enable_split_merge = cache_options_.enable_custom_split_merge;
  const Cache::CacheItemHelper* internal_helper = GetHelper(enable_split_merge);

  // TODO: variant of size_cb that also returns a pointer to the data if
  // already available. Saves an allocation if we keep the compressed version.
  const size_t data_size_original = (*helper->size_cb)(value);

  // Allocate enough memory for header/tag + original data because (a) we might
  // not be attempting compression at all, and (b) we might keep the original if
  // compression is insufficient. But we don't need the length prefix with
  // enable_split_merge. TODO: be smarter with CacheValueChunk to save an
  // allocation in the enable_split_merge case.
  size_t header_size = GetHeaderSize(data_size_original, enable_split_merge);
  CacheAllocationPtr allocation = AllocateBlock(
      header_size + data_size_original, cache_options_.memory_allocator.get());
  char* data_ptr = allocation.get() + header_size;
  Slice tagged_data(data_ptr - kTagSize, data_size_original + kTagSize);
  assert(tagged_data.data() >= allocation.get());

  Status s = (*helper->saveto_cb)(value, 0, data_size_original, data_ptr);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<char[]> tagged_compressed_data;
  CompressionType to_type = kNoCompression;
  if (compressor_ && from_type == kNoCompression &&
      !cache_options_.do_not_compress_roles.Contains(helper->role)) {
    assert(source == CacheTier::kVolatileCompressedTier);

    // TODO: consider malloc sizes for max acceptable compressed size
    // Or maybe max_compressed_bytes_per_kb
    size_t data_size_compressed = data_size_original - 1;
    tagged_compressed_data =
        std::make_unique<char[]>(data_size_compressed + kTagSize);
    s = compressor_->CompressBlock(Slice(data_ptr, data_size_original),
                                   tagged_compressed_data.get() + kTagSize,
                                   &data_size_compressed, &to_type,
                                   nullptr /*working_area*/);
    if (!s.ok()) {
      return s;
    }
    PERF_COUNTER_ADD(compressed_sec_cache_uncompressed_bytes,
                     data_size_original);
    if (to_type == kNoCompression) {
      // Compression rejected or otherwise aborted/failed
      to_type = kNoCompression;
      tagged_compressed_data.reset();
      // TODO: consider separate counters for rejected compressions
      PERF_COUNTER_ADD(compressed_sec_cache_compressed_bytes,
                       data_size_original);
    } else {
      PERF_COUNTER_ADD(compressed_sec_cache_compressed_bytes,
                       data_size_compressed);
      if (enable_split_merge) {
        // Only need tagged_data for copying into CacheValueChunks.
        tagged_data = Slice(tagged_compressed_data.get(),
                            data_size_compressed + kTagSize);
        allocation.reset();
      } else {
        // Replace allocation with compressed version, copied from string
        header_size = GetHeaderSize(data_size_compressed, enable_split_merge);
        allocation = AllocateBlock(header_size + data_size_compressed,
                                   cache_options_.memory_allocator.get());
        data_ptr = allocation.get() + header_size;
        // Ignore unpopulated tag on tagged_compressed_data; will only be
        // populated on the new allocation.
        std::memcpy(data_ptr, tagged_compressed_data.get() + kTagSize,
                    data_size_compressed);
        tagged_data =
            Slice(data_ptr - kTagSize, data_size_compressed + kTagSize);
        assert(tagged_data.data() >= allocation.get());
      }
    }
  }

  PERF_COUNTER_ADD(compressed_sec_cache_insert_real_count, 1);

  // Save the tag fields
  const_cast<char*>(tagged_data.data())[0] = lossless_cast<char>(source);
  const_cast<char*>(tagged_data.data())[1] = lossless_cast<char>(
      source == CacheTier::kVolatileCompressedTier ? to_type : from_type);

  if (enable_split_merge) {
    size_t split_charge{0};
    CacheValueChunk* value_chunks_head =
        SplitValueIntoChunks(tagged_data, split_charge);
    s = cache_->Insert(key, value_chunks_head, internal_helper, split_charge);
    assert(s.ok());  // LRUCache::Insert() with handle==nullptr always OK
  } else {
    // Save the size prefix
    char* ptr = allocation.get();
    ptr = EncodeVarint64(ptr, tagged_data.size());
    assert(ptr == tagged_data.data());
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    size_t charge = malloc_usable_size(allocation.get());
#else
    size_t charge = tagged_data.size();
#endif
    s = cache_->Insert(key, allocation.release(), internal_helper, charge);
    assert(s.ok());  // LRUCache::Insert() with handle==nullptr always OK
  }
  return Status::OK();
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
  if (source == CacheTier::kVolatileCompressedTier) {
    // Unexpected, would violate InsertInternal preconditions
    assert(source != CacheTier::kVolatileCompressedTier);
    return Status::OK();
  }
  if (type == kNoCompression) {
    // Not currently supported (why?)
    return Status::OK();
  }
  if (cache_options_.enable_custom_split_merge) {
    // We don't support custom split/merge for the tiered case (why?)
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
  disable_cache_.StoreRelaxed(capacity == 0);
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
  snprintf(buffer, kBufferSize, "    compression_opts : %s\n",
           CompressionOptionsToString(
               const_cast<CompressionOptions&>(cache_options_.compression_opts))
               .c_str());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    compress_format_version : %d\n",
           cache_options_.compress_format_version);
  ret.append(buffer);
  return ret;
}

// FIXME: this could use a lot of attention, including:
// * Use allocator
// * We shouldn't be worse than non-split; be more pro-actively aware of
// internal fragmentation
// * Consider a unified object/chunk structure that may or may not split
// * Optimize size overhead of chunks
CompressedSecondaryCache::CacheValueChunk*
CompressedSecondaryCache::SplitValueIntoChunks(const Slice& value,
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
        *upper - predicted_chunk_size < malloc_bin_sizes_.front()) {
      tmp_size = predicted_chunk_size;
    } else {
      tmp_size = *(--upper);
    }

    CacheValueChunk* new_chunk =
        static_cast<CacheValueChunk*>(static_cast<void*>(new char[tmp_size]));
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

std::string CompressedSecondaryCache::MergeChunksIntoValue(
    const CacheValueChunk* head) {
  const CacheValueChunk* current_chunk = head;
  size_t total_size = 0;
  while (current_chunk != nullptr) {
    total_size += current_chunk->size;
    current_chunk = current_chunk->next;
  }

  std::string result;
  result.reserve(total_size);
  current_chunk = head;
  while (current_chunk != nullptr) {
    result.append(current_chunk->data, current_chunk->size);
    current_chunk = current_chunk->next;
  }
  assert(result.size() == total_size);
  return result;
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
          }
        }};
    return &kHelper;
  } else {
    static const Cache::CacheItemHelper kHelper{
        CacheEntryRole::kMisc,
        [](Cache::ObjectPtr obj, MemoryAllocator* alloc) {
          if (obj != nullptr) {
            CacheAllocationDeleter{alloc}(static_cast<char*>(obj));
          }
        }};
    return &kHelper;
  }
}

size_t CompressedSecondaryCache::TEST_GetCharge(const Slice& key) {
  Cache::Handle* lru_handle = cache_->Lookup(key);
  if (lru_handle == nullptr) {
    return 0;
  }
  size_t charge = cache_->GetCharge(lru_handle);
  cache_->Release(lru_handle, /*erase_if_last_ref=*/false);
  return charge;
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
