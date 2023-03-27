//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "cache/cache_key.h"
#include "cache/typed_cache.h"
#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_read_request.h"
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
                 uint64_t file_number, uint64_t offset, uint64_t file_size,
                 uint64_t value_size, CompressionType compression_type,
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
                               uint64_t file_number, uint64_t file_size,
                               autovector<BlobReadRequest>& blob_reqs,
                               uint64_t* bytes_read);

  inline Status GetBlobFileReader(
      uint64_t blob_file_number,
      CacheHandleGuard<BlobFileReader>* blob_file_reader) {
    return blob_file_cache_->GetBlobFileReader(blob_file_number,
                                               blob_file_reader);
  }

  inline Cache* GetBlobCache() const { return blob_cache_.get(); }

  bool TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                        uint64_t offset, size_t* charge = nullptr) const;

  // For TypedSharedCacheInterface
  void Create(BlobContents** out, const char* buf, size_t size,
              MemoryAllocator* alloc);

  using SharedCacheInterface =
      FullTypedSharedCacheInterface<BlobContents, BlobContentsCreator>;
  using TypedHandle = SharedCacheInterface::TypedHandle;

 private:
  Status GetBlobFromCache(const Slice& cache_key,
                          CacheHandleGuard<BlobContents>* cached_blob) const;

  Status PutBlobIntoCache(const Slice& cache_key,
                          std::unique_ptr<BlobContents>* blob,
                          CacheHandleGuard<BlobContents>* cached_blob) const;

  static void PinCachedBlob(CacheHandleGuard<BlobContents>* cached_blob,
                            PinnableSlice* value);

  static void PinOwnedBlob(std::unique_ptr<BlobContents>* owned_blob,
                           PinnableSlice* value);

  TypedHandle* GetEntryFromCache(const Slice& key) const;

  Status InsertEntryIntoCache(const Slice& key, BlobContents* value,
                              TypedHandle** cache_handle,
                              Cache::Priority priority) const;

  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t /*file_size*/,
                              uint64_t offset) const {
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);
    return base_cache_key.WithOffset(offset);
  }

  const std::string& db_id_;
  const std::string& db_session_id_;

  Statistics* statistics_;

  // A cache to store blob file reader.
  BlobFileCache* blob_file_cache_;

  // A cache to store uncompressed blobs.
  mutable SharedCacheInterface blob_cache_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block/blob cache (volatile tier) and secondary cache (this tier
  // isn't strictly speaking a non-volatile tier since the compressed cache in
  // this tier is in volatile memory).
  const CacheTier lowest_used_cache_tier_;
};

}  // namespace ROCKSDB_NAMESPACE
