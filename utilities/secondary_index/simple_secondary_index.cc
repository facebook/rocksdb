//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>

#include "rocksdb/utilities/secondary_index_simple.h"
#include "util/coding.h"
#include "utilities/secondary_index/secondary_index_helper.h"

namespace ROCKSDB_NAMESPACE {

SimpleSecondaryIndex::SimpleSecondaryIndex(std::string primary_column_name)
    : primary_column_name_(std::move(primary_column_name)) {}

void SimpleSecondaryIndex::SetPrimaryColumnFamily(
    ColumnFamilyHandle* column_family) {
  assert(column_family);
  primary_column_family_ = column_family;
}

void SimpleSecondaryIndex::SetSecondaryColumnFamily(
    ColumnFamilyHandle* column_family) {
  assert(column_family);
  secondary_column_family_ = column_family;
}

ColumnFamilyHandle* SimpleSecondaryIndex::GetPrimaryColumnFamily() const {
  return primary_column_family_;
}

ColumnFamilyHandle* SimpleSecondaryIndex::GetSecondaryColumnFamily() const {
  return secondary_column_family_;
}

Slice SimpleSecondaryIndex::GetPrimaryColumnName() const {
  return primary_column_name_;
}

Status SimpleSecondaryIndex::UpdatePrimaryColumnValue(
    const Slice& /* primary_key */, const Slice& /* primary_column_value */,
    std::optional<std::variant<Slice, std::string>>* /* updated_column_value */)
    const {
  return Status::OK();
}

Status SimpleSecondaryIndex::GetSecondaryKeyPrefix(
    const Slice& /* primary_key */, const Slice& primary_column_value,
    std::variant<Slice, std::string>* secondary_key_prefix) const {
  assert(secondary_key_prefix);

  *secondary_key_prefix = primary_column_value;

  return Status::OK();
}

Status SimpleSecondaryIndex::FinalizeSecondaryKeyPrefix(
    std::variant<Slice, std::string>* secondary_key_prefix) const {
  assert(secondary_key_prefix);

  std::string prefix;
  PutLengthPrefixedSlice(&prefix,
                         SecondaryIndexHelper::AsSlice(*secondary_key_prefix));

  *secondary_key_prefix = std::move(prefix);

  return Status::OK();
}

Status SimpleSecondaryIndex::GetSecondaryValue(
    const Slice& /* primary_key */, const Slice& /* primary_column_value */,
    const Slice& /* original_column_value */,
    std::optional<std::variant<Slice, std::string>>* /* secondary_value */)
    const {
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
