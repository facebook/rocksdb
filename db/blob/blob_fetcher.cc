//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_fetcher.h"

#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFetcher::FetchBlob(const Slice& user_key,
                              const Slice& blob_index_slice,
                              FilePrefetchBuffer* prefetch_buffer,
                              PinnableSlice* blob_value,
                              uint64_t* bytes_read) const {
  BlobIndex blob_index;
  Status status = blob_index.DecodeFrom(blob_index_slice);
  if (status.ok()) {
    status = FetchBlob(user_key, blob_index, prefetch_buffer, blob_value,
                       bytes_read);
  }
  return status;
}

Status BlobFetcher::FetchBlob(const Slice& user_key,
                              const BlobIndex& blob_index,
                              FilePrefetchBuffer* prefetch_buffer,
                              PinnableSlice* blob_value,
                              uint64_t* bytes_read) const {
  if (!allow_write_path_fallback_) {
    assert(version_);

    return version_->GetBlob(read_options_, user_key, blob_index,
                             prefetch_buffer, blob_value, bytes_read);
  }

  return BlobFilePartitionManager::ResolveBlobDirectWriteIndex(
      read_options_, user_key, blob_index, version_, blob_file_cache_,
      prefetch_buffer, blob_value, bytes_read);
}

}  // namespace ROCKSDB_NAMESPACE
