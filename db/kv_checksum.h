//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/dbformat.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Holds checksums of user keys/values and copies of internal metadata for
// protecting integrity of data as it is processed.
class KvProtectionInfo {
 public:
  enum class CoverageFlags : unsigned char {
    kKey = 1 << 0,
    kValue = 1 << 1,
    kSequenceNumber = 1 << 2,
    kColumnFamilyId = 1 << 3,
    kValueType = 1 << 4,
  };

  KvProtectionInfo();

  // `*this` must cover a (not necessarily strict) superset of what `expected`
  // covers.
  //
  // Returns a `Status::Corruption` when a mismatch is detected.
  Status VerifyAgainst(const KvProtectionInfo& expected);

  void Clear();

  void SetKeyChecksum(uint64_t key_checksum);
  uint64_t GetKeyChecksum() const;
  bool HasKeyChecksum() const;

  void SetValueChecksum(uint64_t value_checksum);
  uint64_t GetValueChecksum() const;
  bool HasValueChecksum() const;

  void SetSequenceNumber(uint64_t sequence_number);
  uint64_t GetSequenceNumber() const;
  bool HasSequenceNumber() const;

  void SetColumnFamilyId(ColumnFamilyId column_family_id);
  ColumnFamilyId GetColumnFamilyId() const;
  bool HasColumnFamilyId() const;

  void SetValueType(ValueType value_type);
  ValueType GetValueType() const;
  bool HasValueType() const;

 private:
  uint64_t key_checksum_;
  uint64_t value_checksum_;
  SequenceNumber sequence_number_;
  ColumnFamilyId column_family_id_;
  ValueType value_type_;
  CoverageFlags coverage_flags_;
};

}  // namespace ROCKSDB_NAMESPACE
