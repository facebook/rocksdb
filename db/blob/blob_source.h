//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/blob/blob_file_cache.h"
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

  // Read a blob from the underlying cache or storage.
  //
  // If successful, returns ok and sets "*value" to the newly retrieved
  // uncompressed blob. If there was an error while fetching the blob, sets
  // "*value" to empty and returns a non-ok status.
  //
  // Note: For consistency, whether the blob is found in the cache or on disk,
  // sets "*bytes_read" to the size of on-disk (possibly compressed) blob
  // record.
  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 uint64_t file_number, uint64_t offset, uint64_t file_size,
                 uint64_t value_size, CompressionType compression_type,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read);

  // Read multiple blobs from the underlying cache or storage.
  //
  // If successful, returns ok and sets the elements of blobs to the newly
  // retrieved uncompressed blobs. If there was an error while fetching one of
  // blobs, sets its corresponding "blobs[i]" to empty and sets "statuses[i]" to
  // a non-ok status.
  //
  // Note:
  //  - Offsets must be sorted in ascending order by caller.
  //  - For consistency, whether the blob is found in the cache or on disk, sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) blob
  //  records.
  void MultiGetBlob(
      const ReadOptions& read_options,
      const autovector<std::reference_wrapper<const Slice>>& user_keys,
      uint64_t file_number, uint64_t file_size,
      const autovector<uint64_t>& offsets,
      const autovector<uint64_t>& value_sizes, autovector<Status*>& statuses,
      autovector<PinnableSlice*>& blobs, uint64_t* bytes_read);

  inline Status GetBlobFileReader(
      uint64_t blob_file_number,
      CacheHandleGuard<BlobFileReader>* blob_file_reader) {
    return blob_file_cache_->GetBlobFileReader(blob_file_number,
                                               blob_file_reader);
  }

  bool TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                        uint64_t offset) const;

 private:
  Status GetBlobFromCache(const Slice& cache_key,
                          CachableEntry<std::string>* blob) const;

  Status PutBlobIntoCache(const Slice& cache_key,
                          CachableEntry<std::string>* cached_blob,
                          PinnableSlice* blob) const;

  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t file_size,
                              uint64_t offset) const {
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number,
                                      file_size);
    return base_cache_key.WithOffset(offset);
  }

  inline Cache::Handle* GetEntryFromCache(const Slice& key) const {
    return blob_cache_->Lookup(key, statistics_);
  }

  inline Status InsertEntryIntoCache(const Slice& key, std::string* value,
                                     size_t charge,
                                     Cache::Handle** cache_handle,
                                     Cache::Priority priority) const {
    return blob_cache_->Insert(key, value, charge,
                               &DeleteCacheEntry<std::string>, cache_handle,
                               priority);
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
