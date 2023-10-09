//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// A read Blob request structure for use in BlobSource::MultiGetBlob and
// BlobFileReader::MultiGetBlob.
struct BlobReadRequest {
  // User key to lookup the paired blob
  const Slice* user_key = nullptr;

  // File offset in bytes
  uint64_t offset = 0;

  // Length to read in bytes
  size_t len = 0;

  // Blob compression type
  CompressionType compression = kNoCompression;

  // Output parameter set by MultiGetBlob() to point to the data buffer, and
  // the number of valid bytes
  PinnableSlice* result = nullptr;

  // Status of read
  Status* status = nullptr;

  BlobReadRequest(const Slice& _user_key, uint64_t _offset, size_t _len,
                  CompressionType _compression, PinnableSlice* _result,
                  Status* _status)
      : user_key(&_user_key),
        offset(_offset),
        len(_len),
        compression(_compression),
        result(_result),
        status(_status) {}

  BlobReadRequest() = default;
  BlobReadRequest(const BlobReadRequest& other) = default;
  BlobReadRequest& operator=(const BlobReadRequest& other) = default;
};

using BlobFileReadRequests =
    std::tuple<uint64_t /* file_number */, uint64_t /* file_size */,
               autovector<BlobReadRequest>>;

}  // namespace ROCKSDB_NAMESPACE
