//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_cache.h"

#include <cassert>
#include <memory>

#include "cache/cache_helpers.h"
#include "db/blob/blob_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

BlobFileCache::BlobFileCache(Cache* cache,
                             const ImmutableCFOptions* immutable_cf_options,
                             const FileOptions* file_options,
                             uint32_t column_family_id,
                             HistogramImpl* blob_file_read_hist)
    : cache_(cache),
      mutex_(128, GetSliceNPHash64),
      immutable_cf_options_(immutable_cf_options),
      file_options_(file_options),
      column_family_id_(column_family_id),
      blob_file_read_hist_(blob_file_read_hist) {
  assert(cache_);
  assert(immutable_cf_options_);
  assert(file_options_);
  assert(blob_file_read_hist_);
}

Status BlobFileCache::GetBlobFileReader(uint64_t blob_file_number,
                                        BlobFileReader** blob_file_reader) {
  assert(blob_file_reader);

  const Slice key = GetSlice(&blob_file_number);

  assert(cache_);

  Cache::Handle* handle = cache_->Lookup(key);
  if (handle) {
    *blob_file_reader = GetFromHandle<BlobFileReader>(cache_, handle);
    return Status::OK();
  }

  // Check again while holding mutex
  MutexLock lock(mutex_.get(key));

  handle = cache_->Lookup(key);
  if (handle) {
    *blob_file_reader = GetFromHandle<BlobFileReader>(cache_, handle);
    return Status::OK();
  }

  assert(immutable_cf_options_);

  RecordTick(immutable_cf_options_->statistics, NO_FILE_OPENS);

  std::unique_ptr<BlobFileReader> reader;

  {
    assert(file_options_);
    const Status s = BlobFileReader::Create(
        *immutable_cf_options_, *file_options_, column_family_id_,
        blob_file_read_hist_, blob_file_number, &reader);
    if (!s.ok()) {
      RecordTick(immutable_cf_options_->statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  {
    const Status s = cache_->Insert(key, reader.get(), 1,
                                    &DeleteEntry<BlobFileReader>, &handle);
    if (!s.ok()) {
      return s;
    }
  }

  reader.release();

  *blob_file_reader = GetFromHandle<BlobFileReader>(cache_, handle);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
