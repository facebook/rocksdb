//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "db/blob/blob_constants.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/dbformat.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileGarbage;

class BlobGarbageMeter {
 public:
  class BlobStats {
   public:
    void Add(uint64_t bytes) {
      ++count_;
      bytes_ += bytes;
    }
    void Add(uint64_t count, uint64_t bytes) {
      count_ += count;
      bytes_ += bytes;
    }

    uint64_t GetCount() const { return count_; }
    uint64_t GetBytes() const { return bytes_; }

   private:
    uint64_t count_ = 0;
    uint64_t bytes_ = 0;
  };

  class BlobInOutFlow {
   public:
    void AddInFlow(uint64_t bytes) {
      in_flow_.Add(bytes);
      assert(IsValid());
    }
    void AddOutFlow(uint64_t bytes) {
      out_flow_.Add(bytes);
      assert(IsValid());
    }

    bool IsValid() const {
      return in_flow_.GetCount() >= out_flow_.GetCount() &&
             in_flow_.GetBytes() >= out_flow_.GetBytes();
    }
    bool HasGarbage() const {
      assert(IsValid());
      return in_flow_.GetCount() > out_flow_.GetCount();
    }
    uint64_t GetGarbageCount() const {
      assert(IsValid());
      assert(HasGarbage());
      return in_flow_.GetCount() - out_flow_.GetCount();
    }
    uint64_t GetGarbageBytes() const {
      assert(IsValid());
      assert(HasGarbage());
      return in_flow_.GetBytes() - out_flow_.GetBytes();
    }

   private:
    BlobStats in_flow_;
    BlobStats out_flow_;
  };

  Status ProcessInFlow(const Slice& key, const Slice& value) {
    uint64_t blob_file_number = kInvalidBlobFileNumber;
    uint64_t bytes = 0;

    const Status s = Parse(key, value, &blob_file_number, &bytes);
    if (!s.ok()) {
      return s;
    }
    if (blob_file_number == kInvalidBlobFileNumber) {
      return Status::OK();
    }

    flows_[blob_file_number].AddInFlow(bytes);

    return Status::OK();
  }

  Status ProcessOutFlow(const Slice& key, const Slice& value) {
    uint64_t blob_file_number = kInvalidBlobFileNumber;
    uint64_t bytes = 0;

    const Status s = Parse(key, value, &blob_file_number, &bytes);
    if (!s.ok()) {
      return s;
    }
    if (blob_file_number == kInvalidBlobFileNumber) {
      return Status::OK();
    }

    auto it = flows_.find(blob_file_number);
    if (it == flows_.end()) {
      return Status::OK();
    }

    it->second.AddOutFlow(bytes);

    return Status::OK();
  }

  const std::unordered_map<uint64_t, BlobInOutFlow>& flows() const {
    return flows_;
  }

 private:
  static Status Parse(const Slice& key, const Slice& value,
                      uint64_t* blob_file_number, uint64_t* bytes) {
    assert(blob_file_number);
    assert(bytes);

    ParsedInternalKey ikey;

    {
      constexpr bool log_err_key = false;
      const Status s = ParseInternalKey(key, &ikey, log_err_key);
      if (!s.ok()) {
        return s;
      }
    }

    if (ikey.type != kTypeBlobIndex) {
      return Status::OK();
    }

    BlobIndex blob_index;

    {
      const Status s = blob_index.DecodeFrom(value);
      if (!s.ok()) {
        return s;
      }
    }

    if (blob_index.IsInlined() || blob_index.HasTTL()) {
      return Status::Corruption("Unexpected TTL/inlined blob index");
    }

    *blob_file_number = blob_index.file_number();
    *bytes =
        blob_index.size() +
        BlobLogRecord::CalculateAdjustmentForRecordHeader(ikey.user_key.size());

    return Status::OK();
  }

  std::unordered_map<uint64_t, BlobInOutFlow> flows_;
};

}  // namespace ROCKSDB_NAMESPACE
