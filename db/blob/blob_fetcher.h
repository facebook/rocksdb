//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileCache;
class Version;
class Slice;
class FilePrefetchBuffer;
class PinnableSlice;
class BlobIndex;

// A thin wrapper around blob retrieval. By default it reads through Version,
// and it can optionally fall back to direct-write blob files that are not yet
// manifest-visible.
class BlobFetcher {
 public:
  BlobFetcher(const Version* version, const ReadOptions& read_options,
              BlobFileCache* blob_file_cache = nullptr,
              bool allow_write_path_fallback = false)
      : version_(version),
        read_options_(read_options),
        blob_file_cache_(blob_file_cache),
        allow_write_path_fallback_(allow_write_path_fallback) {}

  Status FetchBlob(const Slice& user_key, const Slice& blob_index_slice,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value, uint64_t* bytes_read) const;

  Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value, uint64_t* bytes_read) const;

 private:
  const Version* version_;
  ReadOptions read_options_;
  BlobFileCache* blob_file_cache_;
  bool allow_write_path_fallback_;
};
}  // namespace ROCKSDB_NAMESPACE
