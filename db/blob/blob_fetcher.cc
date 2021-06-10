//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_fetcher.h"

#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFetcher::FetchBlob(const Slice& user_key, const Slice& blob_index,
                              PinnableSlice* blob_value) {
  Status s;
  assert(version_);
  constexpr uint64_t* bytes_read = nullptr;
  s = version_->GetBlob(read_options_, user_key, blob_index, blob_value,
                        bytes_read);
  return s;
}

}  // namespace ROCKSDB_NAMESPACE