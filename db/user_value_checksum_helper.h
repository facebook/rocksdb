//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/dbformat.h"
#include "db/output_validator.h"
#include "db/wide/wide_column_serialization.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/user_value_checksum.h"
#include "seqno_to_time_mapping.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

// Validate user-defined value checksum for a single key-value entry.
// Skips entries that should not be validated: deletion markers, blob indices,
// empty values. For kTypeValuePreferredSeqno (TimedPut), the packed value
// is unpacked before validation. For kTypeMerge, the value type is passed
// as kMergeOperand so implementations can distinguish merge operands from
// resolved values.
//
// Returns:
//   Status::OK()        - entry was validated or skipped (not applicable)
//   Status::Corruption() - checksum mismatch detected
inline Status ValidateUserValueChecksum(const UserValueChecksum& checksum,
                                        const Slice& ikey,
                                        const Slice& raw_value) {
  ParsedInternalKey pikey;
  if (!ParseInternalKey(ikey, &pikey, /*log_err_key=*/false).ok()) {
    // Malformed internal key — skip validation for this entry.
    // The existing OutputValidator or block checksum verification
    // will catch this corruption through hash mismatches.
    return Status::OK();
  }

  // Skip deletion markers and blob indices — these have no user value
  // content to validate.
  if (pikey.type == kTypeDeletion || pikey.type == kTypeSingleDeletion ||
      pikey.type == kTypeRangeDeletion ||
      pikey.type == kTypeDeletionWithTimestamp ||
      pikey.type == kTypeBlobIndex) {
    return Status::OK();
  }

  Slice value = raw_value;
  if (value.empty()) {
    return Status::OK();
  }

  // For kTypeValuePreferredSeqno (TimedPut), unpack the value by stripping
  // the trailing sequence number.
  if (pikey.type == kTypeValuePreferredSeqno) {
    // Bounds check: packed value must be at least sizeof(uint64_t) bytes.
    // A shorter value is structurally malformed (missing trailing seqno).
    if (value.size() < sizeof(uint64_t)) {
      return Status::Corruption(
          "TimedPut value too small to contain packed sequence number");
    }
    value = ParsePackedValueForValue(value);
    if (value.empty()) {
      return Status::OK();
    }
  }

  // For kTypeWideColumnEntity (PutEntity), deserialize the wide columns
  // and call ValidateWideColumns() instead of Validate().
  if (pikey.type == kTypeWideColumnEntity) {
    WideColumns columns;
    Slice input = value;
    Status deserialize_s = WideColumnSerialization::Deserialize(input, columns);
    if (!deserialize_s.ok()) {
      return Status::Corruption(
          "Failed to deserialize wide-column entity for checksum validation",
          deserialize_s.getState());
    }
    return checksum.ValidateWideColumns(pikey.user_key, columns);
  }

  // Determine value type: merge operands vs resolved values.
  ChecksumValueType value_type = (pikey.type == kTypeMerge)
                                     ? ChecksumValueType::kMergeOperand
                                     : ChecksumValueType::kValue;

  return checksum.Validate(pikey.user_key, value, value_type);
}

// Iterates over all entries in the given iterator, optionally computing an
// OutputValidator hash and/or validating user value checksums. Returns the
// first error encountered, or iter->status() if iteration completes.
//
// Parameters:
//   iter                - SeekToFirst is called internally
//   output_validator     - if non-null, validator.Add() is called per entry
//   reference_validator  - if non-null (and output_validator is non-null),
//                          CompareValidator is called after iteration to
//                          verify the hash matches
//   user_value_checksum  - if non-null, ValidateUserValueChecksum() is called
//                          per entry with stats recording
//   stats               - Statistics object for RecordTick (may be null)
//   context             - string for corruption message (e.g. "compaction
//                          output verification")
inline Status IterateAndValidateOutput(
    InternalIterator* iter, OutputValidator* output_validator,
    const OutputValidator* reference_validator,
    const UserValueChecksum* user_value_checksum, Statistics* stats,
    const char* context) {
  Status s;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (output_validator) {
      s = output_validator->Add(iter->key(), iter->value());
      if (!s.ok()) {
        break;
      }
    }
    if (user_value_checksum) {
      s = ValidateUserValueChecksum(*user_value_checksum, iter->key(),
                                    iter->value());
      RecordTick(stats, USER_VALUE_CHECKSUM_COMPUTE_COUNT);
      if (!s.ok()) {
        RecordTick(stats, USER_VALUE_CHECKSUM_MISMATCH_COUNT);
        s = Status::Corruption(
            std::string("User-defined value checksum mismatch during ") +
                context,
            s.getState());
        break;
      }
    }
  }
  if (s.ok()) {
    s = iter->status();
  }
  if (s.ok() && output_validator && reference_validator &&
      !output_validator->CompareValidator(*reference_validator)) {
    s = Status::Corruption(
        "Key-value checksum of output doesn't match what was computed when "
        "written");
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
