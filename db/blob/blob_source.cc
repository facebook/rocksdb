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

BlobSource::BlobSource(const ImmutableOptions* immutable_options,
                       const std::string& db_id,
                       const std::string& db_session_id,
                       BlobFileCache* blob_file_cache)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options->statistics.get()),
      blob_file_cache_(blob_file_cache),
      blob_cache_(immutable_options->blob_cache) {}

BlobSource::~BlobSource() = default;

Status BlobSource::GetBlobFromCache(const Slice& cache_key, Cache* blob_cache,
                                    CachableEntry<PinnableSlice>* blob) const {
  assert(blob);
  assert(blob->IsEmpty());

  if (blob_cache != nullptr) {
    assert(!cache_key.empty());
    Cache::Handle* cache_handle = nullptr;
    cache_handle = GetEntryFromCache(blob_cache, cache_key);
    if (cache_handle != nullptr) {
      blob->SetCachedValue(
          static_cast<PinnableSlice*>(blob_cache->Value(cache_handle)),
          blob_cache, cache_handle);
      return Status::OK();
    }
  }

  assert(blob->IsEmpty());

  return Status::NotFound("Blob record not found in cache");
}

Status BlobSource::PutBlobIntoCache(const Slice& cache_key, Cache* blob_cache,
                                    CachableEntry<PinnableSlice>* cached_blob,
                                    PinnableSlice* blob) const {
  assert(blob);
  assert(!cache_key.empty());

  const Cache::Priority priority = Cache::Priority::LOW;

  Status s;

  if (blob_cache != nullptr) {
    // Objects to be put into the cache have to be heap-allocated and
    // self-contained, i.e. own their contents. The Cache has to be able to take
    // unique ownership of them. Therefore, we copy the blob into a separate
    // heap-allocated PinnableSlice, and insert that into the cache.
    PinnableSlice* value = new PinnableSlice();
    value->PinSelf(*blob);

    Cache::Handle* cache_handle = nullptr;
    s = InsertEntryIntoCache(blob_cache, cache_key, value, value->size(),
                             &cache_handle, priority);
    if (s.ok()) {
      assert(cache_handle != nullptr);
      cached_blob->SetCachedValue(value, blob_cache, cache_handle);
    }
  }

  return s;
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

  CachableEntry<PinnableSlice> blob_entry;

  // First, try to get the blob from the cache
  //
  // If blob cache is enabled, we'll try to read from it.
  Status s;
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, blob_cache_.get(), &blob_entry);
    if (blob_entry.GetValue()) {
      if (prefetch_buffer) {
        // Update the blob details so that PrefetchBuffer can use the read
        // pattern to determine if reads are sequential or not for prefetching.
        // It should also take in account blob read from cache.
        prefetch_buffer->UpdateReadPattern(offset,
                                           blob_entry.GetValue()->size(),
                                           false /* decrease_readahead_size */);
      }
    }

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
  {
    CacheHandleGuard<BlobFileReader> blob_file_reader;
    s = blob_file_cache_->GetBlobFileReader(file_number, &blob_file_reader);
    if (!s.ok()) {
      return s;
    }

    assert(blob_file_reader.GetValue());

    if (compression_type != blob_file_reader.GetValue()->GetCompressionType()) {
      return Status::Corruption("Compression type mismatch when reading blob");
    }

    s = blob_file_reader.GetValue()->GetBlob(
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
    s = PutBlobIntoCache(key, blob_cache_.get(), &blob_entry, value);
    if (!s.ok()) {
      return s;
    }
  }

  assert(s.ok());
  return s;
}

bool BlobSource::TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                                  uint64_t offset) const {
  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);
  const Slice key = cache_key.AsSlice();

  CachableEntry<PinnableSlice> blob_entry;
  const Status s = GetBlobFromCache(key, blob_cache_.get(), &blob_entry);

  if (s.ok() && blob_entry.GetValue() != nullptr) {
    return true;
  }

  return false;
}

}  // namespace ROCKSDB_NAMESPACE
