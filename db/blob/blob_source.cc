//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <cassert>
#include <string>

#include "db/blob/blob_file_reader.h"
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

Status BlobSource::GetBlobFromCache(const Slice& cache_key,
                                    CachableEntry<std::string>* blob) const {
  assert(blob);
  assert(blob->IsEmpty());
  assert(blob_cache_);
  assert(!cache_key.empty());

  Cache::Handle* cache_handle = nullptr;
  cache_handle = GetEntryFromCache(cache_key);
  if (cache_handle != nullptr) {
    blob->SetCachedValue(
        static_cast<std::string*>(blob_cache_->Value(cache_handle)),
        blob_cache_.get(), cache_handle);
    return Status::OK();
  }

  assert(blob->IsEmpty());

  return Status::NotFound("Blob not found in cache");
}

Status BlobSource::PutBlobIntoCache(const Slice& cache_key,
                                    CachableEntry<std::string>* cached_blob,
                                    PinnableSlice* blob) const {
  assert(blob);
  assert(!cache_key.empty());
  assert(blob_cache_);

  Status s;
  const Cache::Priority priority = Cache::Priority::LOW;

  // Objects to be put into the cache have to be heap-allocated and
  // self-contained, i.e. own their contents. The Cache has to be able to take
  // unique ownership of them. Therefore, we copy the blob into a string
  // directly, and insert that into the cache.
  std::string* buf = new std::string();
  buf->assign(blob->data(), blob->size());

  // TODO: support custom allocators and provide a better estimated memory
  // usage using malloc_usable_size.
  Cache::Handle* cache_handle = nullptr;
  s = InsertEntryIntoCache(cache_key, buf, buf->size(), &cache_handle,
                           priority);
  if (s.ok()) {
    assert(cache_handle != nullptr);
    cached_blob->SetCachedValue(buf, blob_cache_.get(), cache_handle);
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

  Status s;

  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);

  CachableEntry<std::string> blob_entry;

  // First, try to get the blob from the cache
  //
  // If blob cache is enabled, we'll try to read from it.
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, &blob_entry);
    if (s.ok() && blob_entry.GetValue()) {
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
    s = PutBlobIntoCache(key, &blob_entry, value);
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

  CachableEntry<std::string> blob_entry;
  const Status s = GetBlobFromCache(key, &blob_entry);

  if (s.ok() && blob_entry.GetValue() != nullptr) {
    return true;
  }

  return false;
}

}  // namespace ROCKSDB_NAMESPACE
