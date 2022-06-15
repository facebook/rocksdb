//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_meta.h"
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
  CacheKey GetCacheKey(uint64_t file_number, uint64_t file_size,
                       uint64_t offset) const;

  void SaveValue(const Slice& src, PinnableSlice* dst);

  Status MaybeReadBlobFromCache(FilePrefetchBuffer* prefetch_buffer,
                                const ReadOptions& read_options,
                                const CacheKey& cache_key, uint64_t offset,
                                const bool wait,
                                CachableEntry<Slice>* blob_entry) const;

  Cache::Handle* GetEntryFromCache(const CacheTier& cache_tier,
                                   Cache* blob_cache, const Slice& key,
                                   const bool wait,
                                   const Cache::CacheItemHelper* cache_helper,
                                   const Cache::CreateCallback& create_cb,
                                   Cache::Priority priority) const;

  Status InsertEntryToCache(const CacheTier& cache_tier, Cache* blob_cache,
                            const Slice& key,
                            const Cache::CacheItemHelper* cache_helper,
                            const Slice* blob, size_t charge,
                            Cache::Handle** cache_handle,
                            Cache::Priority priority) const;

  Status GetBlobFromCache(const Slice& cache_key, Cache* blob_cache,
                          CachableEntry<Slice>* blob, bool wait) const;

  Status PutBlobToCache(const Slice& cache_key, Cache* blob_cache,
                        CachableEntry<Slice>* cached_blob,
                        PinnableSlice* blob) const;

  static size_t SizeCallback(void* obj) {
    assert(obj != nullptr);

    const Slice header_slice(static_cast<const char*>(obj),
                             BlobLogRecord::kHeaderSize);

    BlobLogRecord record;
    const Status s = record.DecodeHeaderFrom(header_slice);
    assert(s == Status::OK());

    return record.record_size();
  }

  static Status SaveToCallback(void* from_obj, size_t from_offset,
                               size_t length, void* out) {
    assert(from_obj != nullptr);

    const char* buf = static_cast<const char*>(from_obj);
    const Slice header_slice(buf, BlobLogRecord::kHeaderSize);

    BlobLogRecord record;
    const Status s = record.DecodeHeaderFrom(header_slice);
    assert(s == Status::OK());

    assert(length == record.record_size());
    (void)from_offset;
    memcpy(out, buf, length);

    return Status::OK();
  }

  static Cache::CacheItemHelper* GetCacheItemHelper() {
    static Cache::CacheItemHelper cache_helper(
        SizeCallback, SaveToCallback,
        [](const Slice& /*key*/, void* /*val*/) -> void {});
    return &cache_helper;
  }

  const std::string& db_id_;
  const std::string& db_session_id_;

  Statistics* statistics_;

  // A cache to store blob file reader.
  BlobFileCache* blob_file_cache_;

  // A cache to store uncompressed blobs.
  std::shared_ptr<Cache> blob_cache_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block cache (volatile tier), secondary cache (non-volatile tier).
  const CacheTier lowest_used_cache_tier_;
};

}  // namespace ROCKSDB_NAMESPACE
