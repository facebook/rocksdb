//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <cassert>
#include <string>

#include "db/blob/blob_file_reader.h"
#include "file/file_prefetch_buffer.h"
#include "options/cf_options.h"

namespace ROCKSDB_NAMESPACE {

BlobSource::BlobSource(Cache* cache, const ImmutableOptions* immutable_options,
                       const FileOptions* file_options,
                       const std::string& db_id,
                       const std::string& db_session_id,
                       uint32_t column_family_id,
                       HistogramImpl* blob_file_read_hist,
                       const std::shared_ptr<IOTracer>& io_tracer)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options->statistics.get()),
      blob_file_cache_(new BlobFileCache(cache, immutable_options, file_options,
                                         column_family_id, blob_file_read_hist,
                                         io_tracer)),
      blob_cache_(immutable_options->blob_cache) {}

BlobSource::~BlobSource() = default;

Status BlobSource::MaybeReadBlobFromCache(
    FilePrefetchBuffer* prefetch_buffer, const ReadOptions& read_options,
    const CacheKey& cache_key, uint64_t offset,
    CachableEntry<Slice>* blob_entry) const {
  assert(blob_entry != nullptr);
  assert(blob_entry->IsEmpty());

  // First, try to get the blob from the cache
  //
  // If either blob cache is enabled, we'll try to read from it.
  Status s;

  if (blob_cache_ != nullptr) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, blob_cache_.get(), blob_entry);
    if (blob_entry->GetValue()) {
      if (prefetch_buffer) {
        // Update the blob details so that PrefetchBuffer can use the read
        // pattern to determine if reads are sequential or not for prefetching.
        // It should also take in account blob read from cache.
        prefetch_buffer->UpdateReadPattern(
            offset, blob_entry->GetValue()->size(),
            read_options.adaptive_readahead /* decrease_readahead_size */);
      }
    }
  }

  assert(s.ok() || blob_entry->GetValue() == nullptr);

  return s;
}

inline Cache::Handle* BlobSource::GetEntryFromCache(
    Cache* blob_cache, const Slice& key, Cache::Priority priority) const {
  (void)priority;
  assert(blob_cache);
  return blob_cache->Lookup(key, statistics_);
}

inline Status BlobSource::InsertEntryIntoCache(Cache* blob_cache,
                                               const Slice& key,
                                               const Slice* blob, size_t charge,
                                               Cache::Handle** cache_handle,
                                               Cache::Priority priority) const {
  const Status s =
      blob_cache->Insert(key, const_cast<void*>(static_cast<const void*>(blob)),
                         charge, nullptr /* deleter */, cache_handle, priority);
  return s;
}

Status BlobSource::GetBlobFromCache(const Slice& cache_key, Cache* blob_cache,
                                    CachableEntry<Slice>* blob) const {
  assert(blob);
  assert(blob->IsEmpty());
  const Cache::Priority priority = Cache::Priority::LOW;

  if (blob_cache != nullptr) {
    assert(!cache_key.empty());
    Cache::Handle* cache_handle = nullptr;
    cache_handle = GetEntryFromCache(blob_cache, cache_key, priority);
    if (cache_handle != nullptr) {
      blob->SetCachedValue(
          reinterpret_cast<Slice*>(blob_cache->Value(cache_handle)), blob_cache,
          cache_handle);
      return Status::OK();
    }
  }

  assert(blob->IsEmpty());

  return Status::NotFound("Blob record not found in cache");
}

Status BlobSource::PutBlobIntoCache(const Slice& cache_key, Cache* blob_cache,
                                    CachableEntry<Slice>* cached_blob,
                                    PinnableSlice* blob) const {
  assert(blob);
  assert(!cache_key.empty());

  const Cache::Priority priority = Cache::Priority::LOW;

  Status s;

  if (blob_cache != nullptr) {
    Cache::Handle* cache_handle = nullptr;
    s = InsertEntryIntoCache(blob_cache, cache_key, blob, blob->size(),
                             &cache_handle, priority);
    if (s.ok()) {
      assert(cache_handle != nullptr);
      cached_blob->SetCachedValue(blob, blob_cache, cache_handle);
    }
  }

  return s;
}

inline CacheKey BlobSource::GetCacheKey(uint64_t file_number,
                                        uint64_t file_size,
                                        uint64_t offset) const {
  OffsetableCacheKey base_cache_key =
      OffsetableCacheKey(db_id_, db_session_id_, file_number, file_size);
  return base_cache_key.WithOffset(offset);
}

Status BlobSource::GetBlob(const ReadOptions& read_options,
                           const Slice& user_key, uint64_t file_number,
                           uint64_t offset, uint64_t file_size,
                           uint64_t value_size,
                           CompressionType compression_type,
                           FilePrefetchBuffer* prefetch_buffer,
                           PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);

  const uint64_t key_size = user_key.size();

  if (!IsValidBlobOffset(offset, key_size, value_size, file_size)) {
    return Status::Corruption("Invalid blob offset");
  }

  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);

  CachableEntry<Slice> blob_entry;

  if (blob_cache_) {
    const Status s = MaybeReadBlobFromCache(prefetch_buffer, read_options,
                                            cache_key, offset, &blob_entry);

    if (s.ok() && blob_entry.GetValue() != nullptr) {
      assert(blob_entry.GetValue()->size() == value_size);
      if (bytes_read) {
        *bytes_read = value_size;
      }
      value->PinSelf(*blob_entry.GetValue());
      return s;
    }
  }

  assert(blob_entry.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    return Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
  }

  // Can't find the blob from the cache. Since I/O is allowed, read from the
  // file.
  CacheHandleGuard<BlobFileReader> blob_file_reader;
  blob_file_cache_->GetBlobFileReader(file_number, &blob_file_reader);

  assert(blob_file_reader.GetValue());

  if (compression_type != blob_file_reader.GetValue()->GetCompressionType()) {
    return Status::Corruption("Compression type mismatch when reading blob");
  }

  {
    const Status s = blob_file_reader.GetValue()->GetBlob(
        read_options, user_key, offset, value_size, compression_type,
        prefetch_buffer, value, bytes_read);
    if (!s.ok()) {
      return s;
    }
  }

  if (blob_cache_ && read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // blob to the cache.
    Slice key = cache_key.AsSlice();
    const Status s =
        PutBlobIntoCache(key, blob_cache_.get(), &blob_entry, value);
    if (!s.ok()) {
      return s;
    }
    value->PinSelf(*blob_entry.GetValue());
  }

  return Status::OK();
}

bool BlobSource::TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                                  uint64_t offset) const {
  assert(blob_cache_ != nullptr);

  Cache* const cache = blob_cache_.get();
  if (cache == nullptr) {
    return false;
  }

  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);
  const Slice key = cache_key.AsSlice();

  Cache::Handle* const cache_handle = cache->Lookup(key);
  if (cache_handle == nullptr) {
    return false;
  }

  cache->Release(cache_handle);

  return true;
}

}  // namespace ROCKSDB_NAMESPACE
