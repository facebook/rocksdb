//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>
#include <iosfwd>
#include <string>

#include "db/blob/blob_constants.h"
#include "db/blob/blob_log_format.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

class BlobFileAddition {
 public:
  BlobFileAddition() = default;

  BlobFileAddition(uint64_t blob_file_number, uint64_t total_blob_count,
                   uint64_t total_blob_bytes, std::string checksum_method,
                   std::string checksum_value, uint64_t file_size = 0)
      : blob_file_number_(blob_file_number),
        total_blob_count_(total_blob_count),
        total_blob_bytes_(total_blob_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)),
        file_size_(
            ResolveFileSize(blob_file_number, total_blob_bytes, file_size)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
  }

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetTotalBlobCount() const { return total_blob_count_; }
  uint64_t GetTotalBlobBytes() const { return total_blob_bytes_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }
  uint64_t GetFileSize() const { return file_size_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  static uint64_t DefaultFileSize(uint64_t total_blob_bytes) {
    return BlobLogHeader::kSize + total_blob_bytes + BlobLogFooter::kSize;
  }

  static uint64_t ResolveFileSize(uint64_t blob_file_number,
                                  uint64_t total_blob_bytes,
                                  uint64_t file_size) {
    if (file_size != 0) {
      return file_size;
    }
    return blob_file_number == kInvalidBlobFileNumber
               ? 0
               : DefaultFileSize(total_blob_bytes);
  }

  uint64_t blob_file_number_ = kInvalidBlobFileNumber;
  uint64_t total_blob_count_ = 0;
  uint64_t total_blob_bytes_ = 0;
  std::string checksum_method_;
  std::string checksum_value_;
  // Physical sealed file size. This can exceed the logical blob bytes when a
  // direct-write file contains orphaned records that remain on disk.
  uint64_t file_size_ = 0;
};

bool operator==(const BlobFileAddition& lhs, const BlobFileAddition& rhs);
bool operator!=(const BlobFileAddition& lhs, const BlobFileAddition& rhs);

std::ostream& operator<<(std::ostream& os,
                         const BlobFileAddition& blob_file_addition);
JSONWriter& operator<<(JSONWriter& jw,
                       const BlobFileAddition& blob_file_addition);

}  // namespace ROCKSDB_NAMESPACE
