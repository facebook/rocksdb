//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_cache.h"

#include <cassert>
#include <memory>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_source.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

BlobFileCache::BlobFileCache(Cache* cache,
                             const ImmutableOptions* immutable_options,
                             const FileOptions* file_options,
                             uint32_t column_family_id,
                             const std::string& db_id,
                             const std::string& db_session_id,
                             HistogramImpl* blob_file_read_hist,
                             const std::shared_ptr<IOTracer>& io_tracer)
    : cache_(cache),
      mutex_(kNumberOfMutexStripes, kGetSliceNPHash64UnseededFnPtr),
      immutable_options_(immutable_options),
      file_options_(file_options),
      column_family_id_(column_family_id),
      db_id_(db_id),
      db_session_id_(db_session_id),
      blob_file_read_hist_(blob_file_read_hist),
      io_tracer_(io_tracer) {
  assert(cache_);
  assert(immutable_options_);
  assert(file_options_);
}

Status BlobFileCache::GetBlobSource(uint64_t blob_file_number,
                                    CacheHandleGuard<BlobSource>* blob_source) {
  assert(blob_source);

  const Slice key = GetSlice(&blob_file_number);

  assert(cache_);

  Cache::Handle* handle = cache_->Lookup(key);
  if (handle) {
    *blob_source = CacheHandleGuard<BlobSource>(cache_, handle);
    return Status::OK();
  }

  TEST_SYNC_POINT("BlobFileCache::GetBlobSource:DoubleCheck");

  // Check again while holding mutex
  MutexLock lock(mutex_.get(key));

  handle = cache_->Lookup(key);
  if (handle) {
    *blob_source = CacheHandleGuard<BlobSource>(cache_, handle);
    return Status::OK();
  }

  assert(immutable_options_);
  Statistics* const statistics = immutable_options_->stats;

  std::unique_ptr<BlobSource> source;

  {
    assert(file_options_);
    const Status s = BlobSource::Create(*immutable_options_, *file_options_,
                                        column_family_id_, blob_file_read_hist_,
                                        blob_file_number, db_id_,
                                        db_session_id_, io_tracer_, &source);
    if (!s.ok()) {
      return s;
    }
  }

  {
    constexpr size_t charge = 1;

    const Status s = cache_->Insert(key, source.get(), charge,
                                    &DeleteCacheEntry<BlobSource>, &handle);
    if (!s.ok()) {
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  source.release();

  *blob_source = CacheHandleGuard<BlobSource>(cache_, handle);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
