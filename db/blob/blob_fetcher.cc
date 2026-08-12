//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_fetcher.h"

#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/blob/same_file_blob_reader.h"
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

Status VersionBlobFetcherBase::FetchBlob(const Slice& user_key,
                                         const BlobIndex& blob_index,
                                         FilePrefetchBuffer* prefetch_buffer,
                                         PinnableSlice* blob_value,
                                         uint64_t* bytes_read) const {
  const ReadOptions& read_options = this->read_options();
  if (!allow_write_path_fallback_) {
    assert(version_);

    return version_->GetBlob(read_options, user_key, blob_index,
                             prefetch_buffer, blob_value, bytes_read);
  }

  return BlobFilePartitionManager::ResolveBlobDirectWriteIndex(
      read_options, user_key, blob_index, version_, blob_file_cache_,
      prefetch_buffer, blob_value, bytes_read);
}

Status VersionBlobFetcherBase::FetchBlobRange(const Slice& user_key,
                                              const BlobIndex& blob_index,
                                              uint64_t range_offset,
                                              size_t range_length,
                                              PinnableSlice* blob_value,
                                              uint64_t* bytes_read) const {
  // Only ever called when SupportsRangeRead() holds (a Version to read through,
  // no direct-write fallback); the direct-write path resolves whole records and
  // has no range variant.
  assert(SupportsRangeRead());
  assert(version_);
  return version_->GetBlobRange(read_options(), user_key, blob_index,
                                range_offset, range_length, blob_value,
                                bytes_read);
}

Status VersionBlobFetcherBase::FetchBlobForceVerify(
    const Slice& user_key, const BlobIndex& blob_index,
    FilePrefetchBuffer* prefetch_buffer, PinnableSlice* blob_value,
    uint64_t* bytes_read) const {
  if (read_options().verify_checksums) {
    // Verification already happens on the normal path.
    return FetchBlob(user_key, blob_index, prefetch_buffer, blob_value,
                     bytes_read);
  }

  // Force verification via a transient fetcher over a verify-enabled copy of
  // the read options. `verify_read_options` outlives `verifying_fetcher` (both
  // are local to this call), so the borrowed-options VersionBlobFetcher is
  // safe.
  ReadOptions verify_read_options = read_options();
  verify_read_options.verify_checksums = true;
  VersionBlobFetcher verifying_fetcher(version_, verify_read_options,
                                       blob_file_cache_,
                                       allow_write_path_fallback_);
  return verifying_fetcher.FetchBlob(user_key, blob_index, prefetch_buffer,
                                     blob_value, bytes_read);
}

Status EmbeddedAwareBlobFetcher::FetchBlob(const Slice& user_key,
                                           const BlobIndex& blob_index,
                                           FilePrefetchBuffer* prefetch_buffer,
                                           PinnableSlice* blob_value,
                                           uint64_t* bytes_read) const {
  // Only an enabled decorator is ever routed to (see EffectiveFetcher()).
  assert(same_file_reader_ != nullptr);
  if (blob_index.IsSameFile()) {
    // Same-file ("embedded") blob records live in the current SST and are read
    // by the SameFileBlobReader (the BlockBasedTable). A separate blob-file
    // prefetch buffer and bytes_read counter do not apply here (embedded reads
    // account their own BLOB_DB_* stats via BlobSource).
    return same_file_reader_->GetSameFileBlob(read_options(), blob_index,
                                              blob_value);
  }

  if (base_ == nullptr) {
    return Status::Corruption(
        "Cannot resolve non-same-file blob reference without a base blob "
        "fetcher");
  }
  return base_->FetchBlob(user_key, blob_index, prefetch_buffer, blob_value,
                          bytes_read);
}

}  // namespace ROCKSDB_NAMESPACE
