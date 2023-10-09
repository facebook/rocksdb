//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Version;
class Slice;
class FilePrefetchBuffer;
class PinnableSlice;
class BlobIndex;

// A thin wrapper around the blob retrieval functionality of Version.
class BlobFetcher {
 public:
  BlobFetcher(const Version* version, const ReadOptions& read_options)
      : version_(version), read_options_(read_options) {}

  Status FetchBlob(const Slice& user_key, const Slice& blob_index_slice,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value, uint64_t* bytes_read) const;

  Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value, uint64_t* bytes_read) const;

 private:
  const Version* version_;
  ReadOptions read_options_;
};
}  // namespace ROCKSDB_NAMESPACE
