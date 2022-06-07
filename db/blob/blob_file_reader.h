//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "db/blob/blob_log_format.h"
#include "file/random_access_file_reader.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Status;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
struct ReadOptions;
class Slice;
class FilePrefetchBuffer;
class PinnableSlice;
class Statistics;

class BlobFileReader {
 public:
  static Status Create(const ImmutableOptions& immutable_options,
                       const FileOptions& file_options,
                       uint32_t column_family_id,
                       HistogramImpl* blob_file_read_hist,
                       uint64_t blob_file_number,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       std::unique_ptr<BlobFileReader>* reader);

  BlobFileReader(const BlobFileReader&) = delete;
  BlobFileReader& operator=(const BlobFileReader&) = delete;

  ~BlobFileReader();

  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t offset, uint64_t value_size,
                 CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read) const;

  // offsets must be sorted in ascending order by caller.
  void MultiGetBlob(
      const ReadOptions& read_options,
      const autovector<std::reference_wrapper<const Slice>>& user_keys,
      const autovector<uint64_t>& offsets,
      const autovector<uint64_t>& value_sizes, autovector<Status*>& statuses,
      autovector<PinnableSlice*>& values, uint64_t* bytes_read) const;

  CompressionType GetCompressionType() const { return compression_type_; }

  uint64_t GetFileSize() const { return file_size_; }

  bool TEST_BlobInCache(uint64_t offset) const;

 private:
  BlobFileReader(std::unique_ptr<RandomAccessFileReader>&& file_reader,
                 uint64_t file_size, uint64_t file_number,
                 CompressionType compression_type,
                 const ImmutableOptions& immutable_options);

  static Status OpenFile(const ImmutableOptions& immutable_options,
                         const FileOptions& file_opts,
                         HistogramImpl* blob_file_read_hist,
                         uint64_t blob_file_number,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         uint64_t* file_size,
                         std::unique_ptr<RandomAccessFileReader>* file_reader);

  static Status ReadHeader(const RandomAccessFileReader* file_reader,
                           uint32_t column_family_id, Statistics* statistics,
                           CompressionType* compression_type);

  static Status ReadFooter(const RandomAccessFileReader* file_reader,
                           uint64_t file_size, Statistics* statistics);

  using Buffer = std::unique_ptr<char[]>;

  static Status ReadFromFile(const RandomAccessFileReader* file_reader,
                             uint64_t read_offset, size_t read_size,
                             Statistics* statistics, Slice* slice, Buffer* buf,
                             AlignedBuf* aligned_buf,
                             Env::IOPriority rate_limiter_priority);

  static Status VerifyBlob(const Slice& record_slice, const Slice& user_key,
                           uint64_t value_size);

  static Status UncompressBlobIfNeeded(const Slice& value_slice,
                                       CompressionType compression_type,
                                       SystemClock* clock,
                                       Statistics* statistics,
                                       PinnableSlice* value,
                                       MemoryAllocator* memory_allocator);

  static void SaveValue(const Slice& src, PinnableSlice* dst);

  Status MaybeReadBlobAndLoadToCache(FilePrefetchBuffer* prefetch_buffer,
                                     const ReadOptions& read_options,
                                     uint64_t offset, const bool wait,
                                     Slice* record_slice) const;

  Cache::Handle* GetEntryFromCache(const CacheTier& cache_tier,
                                   Cache* blob_cache, const Slice& key,
                                   const bool wait,
                                   const Cache::CacheItemHelper* cache_helper,
                                   const Cache::CreateCallback& create_cb,
                                   Cache::Priority priority) const;

  Status InsertEntryToCache(const CacheTier& cache_tier, Cache* blob_cache,
                            const Slice& key,
                            const Cache::CacheItemHelper* cache_helper,
                            const Slice* record_slice, size_t charge,
                            Cache::Handle** cache_handle,
                            Cache::Priority priority) const;

  Status GetDataBlobFromCache(const Slice& cache_key, Cache* blob_cache,
                              Cache* blob_cache_compressed,
                              const ReadOptions& read_options,
                              Slice* record_slice, bool wait) const;

  Status PutDataBlobToCache(const Slice& cache_key, Cache* blob_cache,
                            Cache* blob_cache_compressed,
                            const Slice* record_slice,
                            const Slice* record_slice_compressed) const;

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

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  uint64_t file_size_;
  CompressionType compression_type_;

  Statistics* statistics_;
  SystemClock* clock_;

  std::shared_ptr<Cache> blob_cache_;

  // A file-specific generator of cache keys, sometimes referred to as the
  // "base" cache key for a file because all the cache keys for various offsets
  // within the file are computed using simple arithmetic.
  OffsetableCacheKey base_cache_key_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block cache (volatile tier), secondary cache (non-volatile tier).
  CacheTier lowest_used_cache_tier_;
};

}  // namespace ROCKSDB_NAMESPACE
