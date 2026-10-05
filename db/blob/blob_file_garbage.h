//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <iosfwd>
#include <string>

#include "db/blob/blob_constants.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

class BlobFileGarbage {
 public:
  BlobFileGarbage() = default;

  BlobFileGarbage(uint64_t blob_file_number, uint64_t garbage_blob_count,
                  uint64_t garbage_blob_bytes)
      : blob_file_number_(blob_file_number),
        garbage_blob_count_(garbage_blob_count),
        garbage_blob_bytes_(garbage_blob_bytes) {}

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetGarbageBlobCount() const { return garbage_blob_count_; }
  uint64_t GetGarbageBlobBytes() const { return garbage_blob_bytes_; }

  // Transient, non-serialized route selected by VersionBuilder while applying
  // this edit. Compaction listeners use it to report the exact physical
  // file whose accounting changed, even when a concurrent standalone GC
  // replaced that file after the compaction was picked.
  void SetAppliedBlobFileNumber(uint64_t blob_file_number) const {
    applied_blob_file_number_ = blob_file_number;
  }
  bool HasAppliedBlobFileNumber() const {
    return applied_blob_file_number_ != kInvalidBlobFileNumber;
  }
  uint64_t GetAppliedBlobFileNumber() const {
    return applied_blob_file_number_;
  }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  uint64_t blob_file_number_ = kInvalidBlobFileNumber;
  uint64_t garbage_blob_count_ = 0;
  uint64_t garbage_blob_bytes_ = 0;
  mutable uint64_t applied_blob_file_number_ = kInvalidBlobFileNumber;
};

bool operator==(const BlobFileGarbage& lhs, const BlobFileGarbage& rhs);
bool operator!=(const BlobFileGarbage& lhs, const BlobFileGarbage& rhs);

std::ostream& operator<<(std::ostream& os,
                         const BlobFileGarbage& blob_file_garbage);
JSONWriter& operator<<(JSONWriter& jw,
                       const BlobFileGarbage& blob_file_garbage);

}  // namespace ROCKSDB_NAMESPACE
