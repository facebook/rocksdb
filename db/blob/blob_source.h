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

class BlobSource {
 public:
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
  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t file_size,
                              uint64_t offset) const;

  Status MaybeReadBlobFromCache(FilePrefetchBuffer* prefetch_buffer,
                                const ReadOptions& read_options,
                                const CacheKey& cache_key, uint64_t offset,
                                CachableEntry<Slice>* blob_entry) const;

  inline Cache::Handle* GetEntryFromCache(Cache* blob_cache, const Slice& key,
                                          Cache::Priority priority) const;

  inline Status InsertEntryIntoCache(Cache* blob_cache, const Slice& key,
                                     const Slice* blob, size_t charge,
                                     Cache::Handle** cache_handle,
                                     Cache::Priority priority) const;

  Status GetBlobFromCache(const Slice& cache_key, Cache* blob_cache,
                          CachableEntry<Slice>* blob) const;

  Status PutBlobIntoCache(const Slice& cache_key, Cache* blob_cache,
                          CachableEntry<Slice>* cached_blob,
                          PinnableSlice* blob) const;

  const std::string& db_id_;
  const std::string& db_session_id_;

  Statistics* statistics_;

  // A cache to store blob file reader.
  std::unique_ptr<BlobFileCache> blob_file_cache_;

  // A cache to store uncompressed blobs.
  std::shared_ptr<Cache> blob_cache_;
};

}  // namespace ROCKSDB_NAMESPACE
