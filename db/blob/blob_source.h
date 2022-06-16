//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_log_format.h"
#include "rocksdb/rocksdb_namespace.h"
#include "table/block_based/cachable_entry.h"

namespace ROCKSDB_NAMESPACE {

class Cache;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
class Status;
class BlobFileReader;
class FilePrefetchBuffer;
class Slice;
class IOTracer;

// BlobSource is a class that provides universal access to blobs, regardless of
// whether they are in the blob cache, secondary cache, or (remote) storage.
// Depending on user settings, it always fetch blobs from multi-tier cache and
// storage with minimal cost.
class BlobSource {
 public:
  // The Cache* parameter is used to store blob file readers. When it's passed
  // in, BlobSource will exclusively own cache afterwards. If it's a raw pointer
  // managed by another shared/unique pointer, the developer must release the
  // ownership.
  BlobSource(Cache* cache, const ImmutableOptions* immutable_options,
             const FileOptions* file_options, const std::string& db_id,
             const std::string& db_session_id, uint32_t column_family_id,
             HistogramImpl* blob_file_read_hist,
             const std::shared_ptr<IOTracer>& io_tracer);

  BlobSource(const BlobSource&) = delete;
  BlobSource& operator=(const BlobSource&) = delete;

  ~BlobSource();

  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t file_number, uint64_t offset, uint64_t file_size,
                 uint64_t value_size, CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read);

  bool TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                        uint64_t offset) const;

 private:
  Status GetBlobFromCache(const Slice& cache_key, Cache* blob_cache,
                          CachableEntry<PinnableSlice>* blob) const;

  Status PutBlobIntoCache(const Slice& cache_key, Cache* blob_cache,
                          CachableEntry<PinnableSlice>* cached_blob,
                          PinnableSlice* blob) const;

  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t file_size,
                              uint64_t offset) const {
    OffsetableCacheKey base_cache_key =
        OffsetableCacheKey(db_id_, db_session_id_, file_number, file_size);
    return base_cache_key.WithOffset(offset);
  }

  inline Cache::Handle* GetEntryFromCache(Cache* blob_cache,
                                          const Slice& key) const {
    assert(blob_cache);
    return blob_cache->Lookup(key, statistics_);
  }

  inline Status InsertEntryIntoCache(Cache* blob_cache, const Slice& key,
                                     const PinnableSlice* blob, size_t charge,
                                     Cache::Handle** cache_handle,
                                     Cache::Priority priority) const {
    return blob_cache->Insert(key,
                              const_cast<void*>(static_cast<const void*>(blob)),
                              charge, nullptr, cache_handle, priority);
  }

  const std::string db_id_;
  const std::string db_session_id_;

  Statistics* statistics_;

  // A cache to store blob file reader.
  std::unique_ptr<BlobFileCache> blob_file_cache_;

  // A cache to store uncompressed blobs.
  std::shared_ptr<Cache> blob_cache_;
};

}  // namespace ROCKSDB_NAMESPACE
