//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "db/blob/blob_constants.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class BlobStatsRecord {
 public:
  BlobStatsRecord() = default;
  BlobStatsRecord(uint64_t blob_file_number, uint64_t count, uint64_t bytes)
      : blob_file_number_(blob_file_number), count_(count), bytes_(bytes) {}

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetCount() const { return count_; }
  uint64_t GetBytes() const { return bytes_; }

  void EncodeTo(std::string* output) {
    PutVarint64(output, blob_file_number_);
    PutVarint64(output, count_);
    PutVarint64(output, bytes_);
  }

  Status DecodeFrom(Slice* input) {
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

 private:
  uint64_t blob_file_number_ = kInvalidBlobFileNumber;
  uint64_t count_ = 0;
  uint64_t bytes_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
