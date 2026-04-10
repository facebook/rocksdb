//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "cache/typed_cache.h"
#include "db/blob/blob_file_reader.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class Cache;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
class Status;
class Slice;
class IOTracer;

class BlobFileCache {
 public:
  BlobFileCache(Cache* cache, const ImmutableOptions* immutable_options,
                const FileOptions* file_options, uint32_t column_family_id,
                HistogramImpl* blob_file_read_hist,
                const std::shared_ptr<IOTracer>& io_tracer);

  BlobFileCache(const BlobFileCache&) = delete;
  BlobFileCache& operator=(const BlobFileCache&) = delete;

  // Returns a cached reader for `blob_file_number`, opening and caching it on
  // miss. If `allow_footer_skip_retry` is true, a footer-validation corruption
  // retries once without requiring a footer.
  Status GetBlobFileReader(const ReadOptions& read_options,
                           uint64_t blob_file_number,
                           CacheHandleGuard<BlobFileReader>* blob_file_reader,
                           bool allow_footer_skip_retry = false);

  // Opens a blob file reader without inserting it into the cache.
  Status OpenBlobFileReaderUncached(
      const ReadOptions& read_options, uint64_t blob_file_number,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      bool allow_footer_skip_retry = false);

  // Inserts a freshly opened uncached reader unless another thread already
  // cached the same blob file.
  Status InsertBlobFileReader(
      uint64_t blob_file_number,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      CacheHandleGuard<BlobFileReader>* cached_blob_file_reader);

  // Installs a refresh-opened reader into the cache. If another thread has
  // already cached a reader opened on at least as large a file size, keep that
  // reader instead so a racing refresh cannot reintroduce an older active-file
  // view after the blob file grows.
  Status RefreshBlobFileReader(
      uint64_t blob_file_number,
      std::unique_ptr<BlobFileReader>* blob_file_reader,
      CacheHandleGuard<BlobFileReader>* cached_blob_file_reader);

  // Called when a blob file is obsolete to ensure it is removed from the cache
  // to avoid effectively leaking the open file and assicated memory
  void Evict(uint64_t blob_file_number);

  // Used to identify cache entries for blob files (not normally useful)
  static const Cache::CacheItemHelper* GetHelper() {
    return CacheInterface::GetBasicHelper();
  }

 private:
  using CacheInterface =
      BasicTypedCacheInterface<BlobFileReader, CacheEntryRole::kMisc>;
  using TypedHandle = CacheInterface::TypedHandle;
  CacheInterface cache_;
  // Note: mutex_ below is used to guard against multiple threads racing to open
  // the same file.
  Striped<CacheAlignedWrapper<port::Mutex>> mutex_;
  const ImmutableOptions* immutable_options_;
  const FileOptions* file_options_;
  uint32_t column_family_id_;
  HistogramImpl* blob_file_read_hist_;
  std::shared_ptr<IOTracer> io_tracer_;

  static constexpr size_t kNumberOfMutexStripes = 1 << 7;
};

}  // namespace ROCKSDB_NAMESPACE
