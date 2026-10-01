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
                   std::string checksum_value)
      : blob_file_number_(blob_file_number),
        total_blob_count_(total_blob_count),
        total_blob_bytes_(total_blob_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
  }

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetTotalBlobCount() const { return total_blob_count_; }
  uint64_t GetTotalBlobBytes() const { return total_blob_bytes_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }

  bool HasIndirectionInfo() const {
    return origin_file_number_ != kInvalidBlobFileNumber;
  }
  bool IsIndirectIdentityFile() const {
    return HasIndirectionInfo() && origin_file_number_ == blob_file_number_ &&
           map_size_ == 0;
  }
  bool IsIndirectCarrierFile() const {
    return HasIndirectionInfo() && origin_file_number_ != blob_file_number_ &&
           map_size_ > 0;
  }
  uint64_t GetOriginFileNumber() const { return origin_file_number_; }
  uint64_t GetMapOffset() const { return map_offset_; }
  uint64_t GetMapSize() const { return map_size_; }
  uint32_t GetMapChecksum() const { return map_checksum_; }

  // Marks a v1 physical origin whose indirect BlobIndexes currently resolve
  // by identity. This is persisted as a forward-incompatible field so an old
  // binary fails during MANIFEST replay instead of interpreting stable IDs as
  // terminal physical locations.
  void SetIndirectionIdentity();

  // Marks a v2 physical file as the current carrier for `origin_file_number`.
  // The complete embedded map occupies [map_offset, map_offset + map_size).
  Status SetIndirectionCarrier(uint64_t origin_file_number, uint64_t map_offset,
                               uint64_t map_size, uint32_t map_checksum);

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  uint64_t blob_file_number_ = kInvalidBlobFileNumber;
  uint64_t total_blob_count_ = 0;
  uint64_t total_blob_bytes_ = 0;
  std::string checksum_method_;
  std::string checksum_value_;
  uint64_t origin_file_number_ = kInvalidBlobFileNumber;
  uint64_t map_offset_ = 0;
  uint64_t map_size_ = 0;
  uint32_t map_checksum_ = 0;
};

bool operator==(const BlobFileAddition& lhs, const BlobFileAddition& rhs);
bool operator!=(const BlobFileAddition& lhs, const BlobFileAddition& rhs);

std::ostream& operator<<(std::ostream& os,
                         const BlobFileAddition& blob_file_addition);
JSONWriter& operator<<(JSONWriter& jw,
                       const BlobFileAddition& blob_file_addition);

}  // namespace ROCKSDB_NAMESPACE
