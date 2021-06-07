//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class Version;

class BlobFileMergeCallback {
 public:
  BlobFileMergeCallback(Version* version, ReadOptions read_options)
      : version_(version), read_options_(read_options) {}

  Status OnBlobFileMergeBegin(const Slice& user_key, const Slice& blob_index,
                              PinnableSlice* blob_value, bool* is_blob_index);

 private:
  Version* version_;
  ReadOptions read_options_;
};
}  // namespace ROCKSDB_NAMESPACE