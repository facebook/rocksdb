//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

#include <cassert>
#include <memory>

namespace ROCKSDB_NAMESPACE {

class ImmutableBlobFileMetaData {
 public:
  ImmutableBlobFileMetaData(uint64_t blob_file_number, uint64_t total_blob_count,
                uint64_t total_blob_bytes, std::string checksum_method,
                std::string checksum_value)
      : blob_file_number_(blob_file_number),
        total_blob_count_(total_blob_count),
        total_blob_bytes_(total_blob_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
  }

  ~ImmutableBlobFileMetaData() {} // TODO: this is when a blob file becomes obsolete

  ImmutableBlobFileMetaData(const ImmutableBlobFileMetaData&) = delete;
  ImmutableBlobFileMetaData& operator=(const ImmutableBlobFileMetaData&) = delete;

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetTotalBlobCount() const { return total_blob_count_; }
  uint64_t GetTotalBlobBytes() const { return total_blob_bytes_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }

 private:
  uint64_t blob_file_number_;
  uint64_t total_blob_count_;
  uint64_t total_blob_bytes_;
  std::string checksum_method_;
  std::string checksum_value_;
};

class BlobFileMetaData {
 public:
  BlobFileMetaData(std::shared_ptr<ImmutableBlobFileMetaData> immutable_meta,
                   uint64_t garbage_blob_count, uint64_t garbage_blob_bytes)
      : immutable_meta_(std::move(immutable_meta)),
        garbage_blob_count_(garbage_blob_count),
        garbage_blob_bytes_(garbage_blob_bytes) {
          assert(immutable_meta_);
          assert(garbage_blob_count_ <= immutable_meta_->GetTotalBlobCount());
          assert(garbage_blob_bytes_ <= immutable_meta_->GetTotalBlobBytes());
        }

  ~BlobFileMetaData() {}

  BlobFileMetaData(const BlobFileMetaData&) = delete;
  BlobFileMetaData& operator=(const BlobFileMetaData&) = delete;

  const std::shared_ptr<ImmutableBlobFileMetaData>& GetImmutableMeta() const {
    return immutable_meta_;
  }

  uint64_t GetGarbageBlobCount() const { return garbage_blob_count_; }
  uint64_t GetGarbageBlobBytes() const { return garbage_blob_bytes_; }

 private:
  std::shared_ptr<ImmutableBlobFileMetaData> immutable_meta_;
  uint64_t garbage_blob_count_;
  uint64_t garbage_blob_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE
