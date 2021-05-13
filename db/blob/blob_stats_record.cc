//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_stats_record.h"

#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

void BlobStatsRecord::EncodeTo(std::string* output) {
  PutVarint64(output, blob_file_number_);
  PutVarint64(output, count_);
  PutVarint64(output, bytes_);
}

Status BlobStatsRecord::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "BlobStatsRecord";

  if (!GetVarint64(input, &blob_file_number_)) {
    return Status::Corruption(class_name, "Error decoding blob file number");
  }

  if (!GetVarint64(input, &count_)) {
    return Status::Corruption(class_name, "Error decoding blob count");
  }

  if (!GetVarint64(input, &bytes_)) {
    return Status::Corruption(class_name, "Error decoding blob bytes");
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
