//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

#include <cassert>
#include <cstdint>
#include <iosfwd>
#include <string>

namespace ROCKSDB_NAMESPACE {

constexpr uint64_t kInvalidBlobFileNumber = 0;

class JSONWriter;
class Slice;
class Status;

class BlobFileState {
 public:
  BlobFileState() = default;

  BlobFileState(uint64_t blob_file_number, uint64_t total_blob_count,
                uint64_t total_blob_bytes, std::string checksum_method,
                std::string checksum_value)
      : blob_file_number_(blob_file_number),
        total_blob_count_(total_blob_count),
        total_blob_bytes_(total_blob_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
  }

  BlobFileState(uint64_t blob_file_number, uint64_t total_blob_count,
                uint64_t total_blob_bytes, uint64_t garbage_blob_count,
                uint64_t garbage_blob_bytes, std::string checksum_method,
                std::string checksum_value)
      : blob_file_number_(blob_file_number),
        total_blob_count_(total_blob_count),
        total_blob_bytes_(total_blob_bytes),
        garbage_blob_count_(garbage_blob_count),
        garbage_blob_bytes_(garbage_blob_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
    assert(garbage_blob_count_ <= total_blob_count_);
    assert(garbage_blob_bytes_ <= total_blob_bytes_);
  }

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }

  uint64_t GetTotalBlobCount() const { return total_blob_count_; }
  uint64_t GetTotalBlobBytes() const { return total_blob_bytes_; }

  void AddGarbageBlob(uint64_t size) {
    assert(garbage_blob_count_ < total_blob_count_);
    assert(garbage_blob_bytes_ + size <= total_blob_bytes_);

    ++garbage_blob_count_;
    garbage_blob_bytes_ += size;
  }

  uint64_t GetGarbageBlobCount() const { return garbage_blob_count_; }
  uint64_t GetGarbageBlobBytes() const { return garbage_blob_bytes_; }

  bool IsObsolete() const {
    assert(garbage_blob_count_ <= total_blob_count_);

    return !(garbage_blob_count_ < total_blob_count_);
  }

  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  uint64_t blob_file_number_ = kInvalidBlobFileNumber;
  uint64_t total_blob_count_ = 0;
  uint64_t total_blob_bytes_ = 0;
  uint64_t garbage_blob_count_ = 0;
  uint64_t garbage_blob_bytes_ = 0;
  std::string checksum_method_;
  std::string checksum_value_;
};

bool operator==(const BlobFileState& lhs, const BlobFileState& rhs);
bool operator!=(const BlobFileState& lhs, const BlobFileState& rhs);

std::ostream& operator<<(std::ostream& os,
                         const BlobFileState& blob_file_state);
JSONWriter& operator<<(JSONWriter& jw, const BlobFileState& blob_file_state);

}  // namespace ROCKSDB_NAMESPACE
