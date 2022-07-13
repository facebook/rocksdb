//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_read_request.h"
#include "monitoring/statistics.h"
#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"
#include "table/block_based/cachable_entry.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

struct ImmutableOptions;
class Status;
class FilePrefetchBuffer;
class Slice;

// BlobSource is a class that provides universal access to blobs, regardless of
// whether they are in the blob cache, secondary cache, or (remote) storage.
// Depending on user settings, it always fetch blobs from multi-tier cache and
// storage with minimal cost.
class BlobSource {
 public:
  BlobSource(const ImmutableOptions* immutable_options,
             const std::string& db_id, const std::string& db_session_id,
             BlobFileCache* blob_file_cache);

  BlobSource(const BlobSource&) = delete;
  BlobSource& operator=(const BlobSource&) = delete;

  ~BlobSource();

  // Read a blob from the underlying cache or one blob file.
  //
  // If successful, returns ok and sets "*value" to the newly retrieved
  // uncompressed blob. If there was an error while fetching the blob, sets
  // "*value" to empty and returns a non-ok status.
  //
  // Note: For consistency, whether the blob is found in the cache or on disk,
  // sets "*bytes_read" to the size of on-disk (possibly compressed) blob
  // record.
  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t file_number, uint64_t offset, uint64_t value_size,
                 CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read);

  // Read multiple blobs from the underlying cache or blob file(s).
  //
  // If successful, returns ok and sets "result" in the elements of "blob_reqs"
  // to the newly retrieved uncompressed blobs. If there was an error while
  // fetching one of blobs, sets its "result" to empty and sets its
  // corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and MultiGetBlobFromOneFile is
  //    that this function can read multiple blobs from multiple blob files.
  //
  //  - For consistency, whether the blob is found in the cache or on disk, sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) blob
  //  records.
  void MultiGetBlob(const ReadOptions& read_options,
                    autovector<BlobFileReadRequests>& blob_reqs,
                    uint64_t* bytes_read);

  // Read multiple blobs from the underlying cache or one blob file.
  //
  // If successful, returns ok and sets "result" in the elements of "blob_reqs"
  // to the newly retrieved uncompressed blobs. If there was an error while
  // fetching one of blobs, sets its "result" to empty and sets its
  // corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and MultiGetBlob is that this
  //  function is only used for the case where the demanded blobs are stored in
  //  one blob file. MultiGetBlob will call this function multiple times if the
  //  demanded blobs are stored in multiple blob files.
  //
  //  - For consistency, whether the blob is found in the cache or on disk, sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) blob
  //  records.
  void MultiGetBlobFromOneFile(const ReadOptions& read_options,
                               uint64_t file_number,
                               autovector<BlobReadRequest>& blob_reqs,
                               uint64_t* bytes_read);

  inline Status GetBlobFileReader(
      uint64_t blob_file_number,
      CacheHandleGuard<BlobFileReader>* blob_file_reader) {
    return blob_file_cache_->GetBlobFileReader(blob_file_number,
                                               blob_file_reader);
  }

  bool TEST_BlobInCache(uint64_t file_number, uint64_t offset) const;

  static Status PutBlobIntoCache(Cache* blob_cache, const Slice& key,
                                 const Slice* blob,
                                 CacheHandleGuard<std::string>* cached_blob,
                                 Statistics* const statistics) {
    assert(blob);
    assert(blob_cache);
    assert(!key.empty());

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
    s = InsertEntryIntoCache(blob_cache, key, buf, buf->size(), &cache_handle,
                             priority, statistics);
    if (s.ok()) {
      assert(cache_handle != nullptr);
      *cached_blob = CacheHandleGuard<std::string>(blob_cache, cache_handle);
    }

    return s;
  }

 private:
  Status GetBlobFromCache(const Slice& cache_key,
                          CacheHandleGuard<std::string>* blob) const;

  Cache::Handle* GetEntryFromCache(const Slice& key) const;

  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t offset) const {
    // The cache key does not take the real file size into account. This is due
    // to the fact that we want to provide the capability of warming up or
    // pre-populating the blob cache during flush, but we are unsure of the
    // actual size of the blob file in the middle of the flush. Therefore, we
    // also set the file size here to the uint64 maximum value to ensure that
    // the cache entry can be found if it exists.
    OffsetableCacheKey base_cache_key(
        db_id_, db_session_id_, file_number,
        std::numeric_limits<uint64_t>::max() /* file size */);
    return base_cache_key.WithOffset(offset);
  }

  static Status InsertEntryIntoCache(Cache* blob_cache, const Slice& key,
                                     std::string* value, size_t charge,
                                     Cache::Handle** cache_handle,
                                     Cache::Priority priority,
                                     Statistics* const statistics) {
    const Status s =
        blob_cache->Insert(key, value, charge, &DeleteCacheEntry<std::string>,
                           cache_handle, priority);
    if (s.ok()) {
      assert(*cache_handle != nullptr);
      RecordTick(statistics, BLOB_DB_CACHE_ADD);
      RecordTick(statistics, BLOB_DB_CACHE_BYTES_WRITE,
                 blob_cache->GetUsage(*cache_handle));
    } else {
      RecordTick(statistics, BLOB_DB_CACHE_ADD_FAILURES);
    }
    return s;
  }

  const std::string& db_id_;
  const std::string& db_session_id_;

  Statistics* statistics_;

  // A cache to store blob file reader.
  BlobFileCache* blob_file_cache_;

  // A cache to store uncompressed blobs.
  std::shared_ptr<Cache> blob_cache_;
};

}  // namespace ROCKSDB_NAMESPACE
