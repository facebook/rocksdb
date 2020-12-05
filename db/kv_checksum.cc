//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/kv_checksum.h"

#include <type_traits>

namespace ROCKSDB_NAMESPACE {

namespace {

// bitwise operations on an enum class adapted from
// https://softwareengineering.stackexchange.com/a/204566
KvProtectionInfo::CoverageFlags operator|(KvProtectionInfo::CoverageFlags lhs,
                                          KvProtectionInfo::CoverageFlags rhs) {
  using T = std::underlying_type<KvProtectionInfo::CoverageFlags>::type;
  return static_cast<KvProtectionInfo::CoverageFlags>(static_cast<T>(lhs) |
                                                      static_cast<T>(rhs));
}

KvProtectionInfo::CoverageFlags& operator|=(
    KvProtectionInfo::CoverageFlags& lhs, KvProtectionInfo::CoverageFlags rhs) {
  lhs = lhs | rhs;
  return lhs;
}

KvProtectionInfo::CoverageFlags operator&(KvProtectionInfo::CoverageFlags lhs,
                                          KvProtectionInfo::CoverageFlags rhs) {
  using T = std::underlying_type<KvProtectionInfo::CoverageFlags>::type;
  return static_cast<KvProtectionInfo::CoverageFlags>(static_cast<T>(lhs) &
                                                      static_cast<T>(rhs));
}

}  // anonymous namespace

KvProtectionInfo::KvProtectionInfo() { Clear(); }

Status KvProtectionInfo::VerifyAgainst(const KvProtectionInfo& expected) const {
  if (expected.HasKeyChecksum() &&
      GetKeyChecksum() != expected.GetKeyChecksum()) {
    return Status::Corruption("key checksum mismatch");
  }
  if (expected.HasValueChecksum() &&
      GetValueChecksum() != expected.GetValueChecksum()) {
    return Status::Corruption("value checksum mismatch");
  }
  if (expected.HasSequenceNumber() &&
      GetSequenceNumber() != expected.GetSequenceNumber()) {
    return Status::Corruption("sequence number mismatch");
  }
  if (expected.HasColumnFamilyId() &&
      GetColumnFamilyId() != expected.GetColumnFamilyId()) {
    return Status::Corruption("column family ID mismatch");
  }
  if (expected.HasValueType() && GetValueType() != expected.GetValueType()) {
    return Status::Corruption("value type mismatch");
  }
  return Status::OK();
}

void KvProtectionInfo::Clear() {
  coverage_flags_ = static_cast<CoverageFlags>(0);
}

void KvProtectionInfo::SetKeyChecksum(uint64_t key_checksum) {
  key_checksum_ = key_checksum;
  coverage_flags_ |= CoverageFlags::kKey;
}

uint64_t KvProtectionInfo::GetKeyChecksum() const {
  assert(HasKeyChecksum());
  return key_checksum_;
}

bool KvProtectionInfo::HasKeyChecksum() const {
  return (coverage_flags_ & CoverageFlags::kKey) == CoverageFlags::kKey;
}

void KvProtectionInfo::SetTimestampChecksum(uint64_t key_checksum) {
  key_checksum_ = key_checksum;
  coverage_flags_ |= CoverageFlags::kTimestamp;
}

uint64_t KvProtectionInfo::GetTimestampChecksum() const {
  assert(HasTimestampChecksum());
  return key_checksum_;
}

bool KvProtectionInfo::HasTimestampChecksum() const {
  return (coverage_flags_ & CoverageFlags::kTimestamp) ==
         CoverageFlags::kTimestamp;
}

void KvProtectionInfo::SetValueChecksum(uint64_t value_checksum) {
  value_checksum_ = value_checksum;
  coverage_flags_ |= CoverageFlags::kValue;
}

uint64_t KvProtectionInfo::GetValueChecksum() const {
  assert(HasValueChecksum());
  return value_checksum_;
}

bool KvProtectionInfo::HasValueChecksum() const {
  return (coverage_flags_ & CoverageFlags::kValue) == CoverageFlags::kValue;
}

void KvProtectionInfo::SetSequenceNumber(uint64_t sequence_number) {
  sequence_number_ = sequence_number;
  coverage_flags_ |= CoverageFlags::kSequenceNumber;
}

SequenceNumber KvProtectionInfo::GetSequenceNumber() const {
  assert(HasSequenceNumber());
  return sequence_number_;
}

bool KvProtectionInfo::HasSequenceNumber() const {
  return (coverage_flags_ & CoverageFlags::kSequenceNumber) ==
         CoverageFlags::kSequenceNumber;
}

void KvProtectionInfo::SetColumnFamilyId(ColumnFamilyId column_family_id) {
  column_family_id_ = column_family_id;
  coverage_flags_ |= CoverageFlags::kColumnFamilyId;
}

ColumnFamilyId KvProtectionInfo::GetColumnFamilyId() const {
  assert(HasColumnFamilyId());
  return column_family_id_;
}

bool KvProtectionInfo::HasColumnFamilyId() const {
  return (coverage_flags_ & CoverageFlags::kColumnFamilyId) ==
         CoverageFlags::kColumnFamilyId;
}

void KvProtectionInfo::SetValueType(ValueType value_type) {
  value_type_ = value_type;
  coverage_flags_ |= CoverageFlags::kValueType;
}

ValueType KvProtectionInfo::GetValueType() const {
  assert(HasValueType());
  return value_type_;
}

bool KvProtectionInfo::HasValueType() const {
  return (coverage_flags_ & CoverageFlags::kValueType) ==
         CoverageFlags::kValueType;
}

}  // namespace ROCKSDB_NAMESPACE
