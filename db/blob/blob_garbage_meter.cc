//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_garbage_meter.h"

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"

namespace ROCKSDB_NAMESPACE {

Status BlobGarbageMeter::ProcessInFlow(const Slice& key, const Slice& value) {
  uint64_t blob_file_number = kInvalidBlobFileNumber;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &blob_file_number, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (blob_file_number != kInvalidBlobFileNumber) {
    flows_[blob_file_number].AddInFlow(bytes);
    return Status::OK();
  }

  return ProcessEntityBlobReferences(key, value, /*is_inflow=*/true);
}

Status BlobGarbageMeter::ProcessOutFlow(const Slice& key, const Slice& value) {
  uint64_t blob_file_number = kInvalidBlobFileNumber;
  uint64_t bytes = 0;

  const Status s = Parse(key, value, &blob_file_number, &bytes);
  if (!s.ok()) {
    return s;
  }

  if (blob_file_number != kInvalidBlobFileNumber) {
    auto it = flows_.find(blob_file_number);
    if (it != flows_.end()) {
      it->second.AddOutFlow(bytes);
    }
    return Status::OK();
  }

  return ProcessEntityBlobReferences(key, value, /*is_inflow=*/false);
}

Status BlobGarbageMeter::Parse(const Slice& key, const Slice& value,
                               uint64_t* blob_file_number, uint64_t* bytes) {
  assert(blob_file_number);
  assert(*blob_file_number == kInvalidBlobFileNumber);
  assert(bytes);
  assert(*bytes == 0);

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

Status BlobGarbageMeter::ProcessEntityBlobReferences(const Slice& key,
                                                     const Slice& value,
                                                     bool is_inflow) {
  ParsedInternalKey ikey;

  {
    constexpr bool log_err_key = false;
    const Status s = ParseInternalKey(key, &ikey, log_err_key);
    if (!s.ok()) {
      return s;
    }
  }

  if (ikey.type != kTypeWideColumnEntity) {
    return Status::OK();
  }

  return WideColumnSerialization::ForEachBlobFileNumber(
      value, [&](const BlobIndex& blob_index) -> Status {
        if (blob_index.IsInlined() || blob_index.HasTTL()) {
          return Status::Corruption("Unexpected TTL/inlined blob index");
        }

        const uint64_t file_number = blob_index.file_number();
        const uint64_t blob_bytes =
            blob_index.size() +
            BlobLogRecord::CalculateAdjustmentForRecordHeader(
                ikey.user_key.size());

        if (is_inflow) {
          flows_[file_number].AddInFlow(blob_bytes);
        } else {
          // Only track outflow for preexisting files (those with inflow).
          auto it = flows_.find(file_number);
          if (it != flows_.end()) {
            it->second.AddOutFlow(blob_bytes);
          }
        }
        return Status::OK();
      });
}

}  // namespace ROCKSDB_NAMESPACE
