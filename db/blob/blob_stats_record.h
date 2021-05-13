//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>

#include "db/blob/blob_constants.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class Status;

class BlobStatsRecord {
 public:
  BlobStatsRecord() = default;
  BlobStatsRecord(uint64_t blob_file_number, uint64_t count, uint64_t bytes)
      : blob_file_number_(blob_file_number), count_(count), bytes_(bytes) {}

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetCount() const { return count_; }
  uint64_t GetBytes() const { return bytes_; }

  void EncodeTo(std::string* output);
  Status DecodeFrom(Slice* input);

 private:
  uint64_t blob_file_number_ = kInvalidBlobFileNumber;
  uint64_t count_ = 0;
  uint64_t bytes_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
