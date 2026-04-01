//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

// UserValueChecksum allows applications to provide an end-to-end value
// integrity check. The application computes a checksum over the value content
// and embeds it within the value itself (e.g., as a trailing checksum).
// During compaction and flush, after the SST file is written to disk, the
// file is read back and each value is validated using this interface.
//
// Usage:
// 1. Subclass UserValueChecksum and implement Validate().
// 2. Set user_value_checksum in ColumnFamilyOptions.
// 3. Enable verify_user_value_checksum_on_flush and/or
//    verify_user_value_checksum_on_compaction.
// 4. Ensure all values written to RocksDB (via Put, Merge results, etc.)
//    contain a valid embedded checksum.
//
// Thread safety: The Validate() method must be thread-safe. It will be called
// concurrently from multiple compaction and flush threads. Since it is a const
// method that only reads the key and value, this is typically trivial.
//
// Application responsibility for merge operators and compaction filters:
// If a MergeOperator is configured, the application must ensure that the
// merge operator produces values with valid checksums. If a CompactionFilter
// modifies values (returns kChangeValue or kChangeWideColumnEntity), the
// modified values must also contain valid checksums. Violations will be
// caught at flush/compaction time when verification is enabled, and will
// cause the flush or compaction to fail.
//
// Note: Validation is NOT performed for files created via SstFileWriter
// or ingested via IngestExternalFile(). The application must ensure that
// values written through these paths contain valid checksums. Ingested
// files will be validated during subsequent compaction if
// verify_user_value_checksum_on_compaction is enabled.
//
// Remote compaction: If using CompactionService (remote compaction), the
// UserValueChecksum implementation must be registered in the remote
// worker's ObjectRegistry for CreateFromString() to reconstruct it.
// Without registration, validation will be silently skipped on remote
// workers.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.

// Indicates the origin of the value passed to UserValueChecksum::Validate().
enum class ChecksumValueType : uint8_t {
  // A fully resolved value: from Put(), TimedPut(), or a resolved merge
  // result. The value is expected to contain a valid embedded checksum.
  kValue = 0,
  // An unresolved merge operand: the raw bytes passed to Merge() that have
  // not yet been combined with a base value. These appear in flush output
  // and non-bottommost compaction output. The application may choose to
  // validate or skip these depending on whether merge operands follow the
  // checksum format.
  kMergeOperand = 1,
};

class UserValueChecksum : public Customizable {
 public:
  virtual ~UserValueChecksum() {}
  static const char* Type() { return "UserValueChecksum"; }
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& name,
                                 std::shared_ptr<UserValueChecksum>* result);

  // Returns a name that identifies this checksum implementation.
  const char* Name() const override = 0;

  // Validate the integrity of a value. The checksum is expected to be
  // embedded within the value by the application (e.g., as trailing bytes).
  //
  // key:        The user key (without internal key suffix).
  // value:      The complete value as stored by the application, including
  //             the embedded checksum.
  // value_type: Indicates the origin of the value. See ChecksumValueType.
  //             kValue: a fully resolved value (from Put, TimedPut, or a
  //                     resolved merge result).
  //             kMergeOperand: an unresolved merge operand (raw bytes from
  //                     Merge() not yet combined with a base value).
  //             Implementations may choose to skip validation for merge
  //             operands if they do not follow the checksum format.
  //
  // Return Status::OK() if the checksum is valid (or validation is skipped).
  // Return Status::Corruption() if the checksum does not match.
  //
  // Performance: This method is called for every key-value entry during
  // output verification. It should be lightweight (e.g., a simple checksum
  // comparison) to avoid blocking compaction and flush.
  //
  // Note: Implementations must validate value size before accessing embedded
  // checksums. Non-empty values of any size (including 1 byte) may be passed.
  // For example, if the checksum format requires a 4-byte trailing CRC,
  // Validate() must check value.size() >= 4 before reading the checksum
  // bytes, and return Corruption for values that are too small.
  //
  // This method is NOT called for:
  //   - Deletion markers (no value content)
  //   - Empty values (value.size() == 0)
  //   - Blob index entries (internal references)
  //
  // This method IS called for:
  //   - Plain values (from Put) — with kValue
  //   - Resolved merge results — with kValue
  //   - Unresolved merge operands — with kMergeOperand
  //   - TimedPut values (from WriteBatch::TimedPut) — with kValue.
  //     The trailing internal sequence number is stripped before the value
  //     is passed to Validate(). Only the user value portion is validated.
  //
  // This method is NOT called for wide-column entities (PutEntity).
  // Wide-column entities are validated via ValidateWideColumns() instead.
  virtual Status Validate(const Slice& key, const Slice& value,
                          ChecksumValueType value_type) const = 0;

  // Validate the integrity of a wide-column entity (from PutEntity).
  //
  // key:     The user key (without internal key suffix).
  // columns: The deserialized wide columns (sorted by column name).
  //          Each WideColumn has a name() and value() accessor.
  //          The default column (from Put-style writes that get merged into
  //          wide-column format) has an empty name (kDefaultWideColumnName).
  //
  // Return Status::OK() if the checksum is valid.
  // Return Status::Corruption() if the checksum does not match.
  //
  // A typical use case is storing user data in one column and a checksum
  // in another column, then validating them here.
  //
  // The default implementation returns Status::OK() (no validation),
  // so existing implementations that do not use PutEntity are unaffected.
  //
  // Thread safety: same requirements as Validate().
  virtual Status ValidateWideColumns(const Slice& /*key*/,
                                     const WideColumns& /*columns*/) const {
    return Status::OK();
  }
};

}  // namespace ROCKSDB_NAMESPACE
