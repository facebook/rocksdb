//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_merge_callback.h"

#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

Status BlobFileMergeCallback::OnBlobFileMergeBegin(
    const Slice& user_key, const Slice& blob_index, Slice& blob_value,
    bool* is_blob_index, PinnableSlice* pinnable_val) {
  Status s;

  if (is_blob_index) {
    if (pinnable_val == nullptr) {
      pinnable_val = new PinnableSlice();
    }
    constexpr uint64_t* bytes_read = nullptr;
    s = version_->GetBlob(read_options_, user_key, blob_index, pinnable_val,
                          bytes_read);
    if (s.ok()) {
      blob_value = static_cast<Slice>(*pinnable_val);
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE