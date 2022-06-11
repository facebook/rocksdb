//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "cache/cache_entry_roles.h"
#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/blob/blob_log_format.h"
#include "rocksdb/rocksdb_namespace.h"
#include "table/block_based/cachable_entry.h"
#include "util/autovector.h"

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

class BlobSource : public Cleanable {
 public:
  static Status Create(const ImmutableOptions& immutable_options,
                       const FileOptions& file_options,
                       uint32_t column_family_id,
                       HistogramImpl* blob_file_read_hist,
                       uint64_t blob_file_number, const std::string& db_id,
                       const std::string& db_session_id,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       std::unique_ptr<BlobSource>* blob_source);

  BlobSource(const BlobSource&) = delete;
  BlobSource& operator=(const BlobSource&) = delete;

  ~BlobSource();

  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t offset, uint64_t value_size,
                 CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read);

  // offsets must be sorted in ascending order by caller.
  void MultiGetBlob(
      const ReadOptions& read_options,
      const autovector<std::reference_wrapper<const Slice>>& user_keys,
      const autovector<uint64_t>& offsets,
      const autovector<uint64_t>& value_sizes, autovector<Status*>& statuses,
      autovector<PinnableSlice*>& values, uint64_t* bytes_read);

  const BlobFileReader* GetBlobFileReader() const { return blob_file_reader_; }

  bool TEST_BlobInCache(uint64_t offset) const;

 private:
  BlobSource(const BlobFileReader* reader,
             const ImmutableOptions& immutable_options,
             const std::string& db_id, const std::string& db_session_id,
             uint64_t file_number);

  Status MaybeReadBlobFromCache(FilePrefetchBuffer* prefetch_buffer,
                                const ReadOptions& read_options,
                                uint64_t offset, const bool wait,
                                CachableEntry<PinnableSlice>* blob_entry) const;

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
                          CachableEntry<PinnableSlice>* blob, bool wait) const;

  Status PutBlobToCache(const Slice& cache_key, Cache* blob_cache,
                        CachableEntry<PinnableSlice>* cached_blob,
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
        GetCacheEntryDeleterForRole<char[], CacheEntryRole::kBlobLogRecord>());
    return &cache_helper;
  }

  const BlobFileReader* blob_file_reader_;

  Statistics* statistics_;

  // A blob cache to store uncompressed blobs.
  std::shared_ptr<Cache> blob_cache_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block cache (volatile tier), secondary cache (non-volatile tier).
  const CacheTier lowest_used_cache_tier_;

  // A file-specific generator of cache keys, sometimes referred to as the
  // "base" cache key for a file because all the cache keys for various offsets
  // within the file are computed using simple arithmetic.
  const OffsetableCacheKey base_cache_key_;
};

}  // namespace ROCKSDB_NAMESPACE
