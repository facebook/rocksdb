//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_cache.h"

#include <cassert>
#include <memory>

#include "db/blob/blob_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

BlobFileCache::BlobFileCache(Cache* cache,
                             const ImmutableCFOptions* immutable_cf_options,
                             const FileOptions* file_options,
                             uint32_t column_family_id,
                             HistogramImpl* blob_file_read_hist)
    : cache_(cache),
      mutex_(kNumberOfMutexStripes, GetSliceNPHash64),
      immutable_cf_options_(immutable_cf_options),
      file_options_(file_options),
      column_family_id_(column_family_id),
      blob_file_read_hist_(blob_file_read_hist) {
  assert(cache_);
  assert(immutable_cf_options_);
  assert(file_options_);
}

Status BlobFileCache::GetBlobFileReader(
    uint64_t blob_file_number,
    CacheHandleGuard<BlobFileReader>* blob_file_reader) {
  assert(blob_file_reader);
  assert(blob_file_reader->IsEmpty());

  const Slice key = GetSlice(&blob_file_number);

  assert(cache_);

  Cache::Handle* handle = cache_->Lookup(key);
  if (handle) {
    *blob_file_reader = CacheHandleGuard<BlobFileReader>(cache_, handle);
    return Status::OK();
  }

  TEST_SYNC_POINT("BlobFileCache::GetBlobFileReader:DoubleCheck");

  // Check again while holding mutex
  MutexLock lock(mutex_.get(key));

  handle = cache_->Lookup(key);
  if (handle) {
    *blob_file_reader = CacheHandleGuard<BlobFileReader>(cache_, handle);
    return Status::OK();
  }

  assert(immutable_cf_options_);
  Statistics* const statistics = immutable_cf_options_->statistics;

  RecordTick(statistics, NO_FILE_OPENS);

  std::unique_ptr<BlobFileReader> reader;

  {
    assert(file_options_);
    const Status s = BlobFileReader::Create(
        *immutable_cf_options_, *file_options_, column_family_id_,
        blob_file_read_hist_, blob_file_number, &reader);
    if (!s.ok()) {
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  {
    constexpr size_t charge = 1;

    const Status s = cache_->Insert(key, reader.get(), charge,
                                    &DeleteCacheEntry<BlobFileReader>, &handle);
    if (!s.ok()) {
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  reader.release();

  *blob_file_reader = CacheHandleGuard<BlobFileReader>(cache_, handle);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
