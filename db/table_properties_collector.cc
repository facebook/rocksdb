//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/table_properties_collector.h"

#include "db/dbformat.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

Status InternalKeyPropertiesCollector::InternalAdd(const Slice& key,
                                                   const Slice& value,
                                                   uint64_t file_size) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::InvalidArgument("Invalid internal key");
  }

  // Note: We count both, deletions and single deletions here.
  if (ikey.type == ValueType::kTypeDeletion ||
      ikey.type == ValueType::kTypeSingleDeletion) {
    ++deleted_keys_;
  } else if (ikey.type == ValueType::kTypeMerge) {
    ++merge_operands_;
  }

  return Status::OK();
}

Status InternalKeyPropertiesCollector::Finish(
    UserCollectedProperties* properties) {
  assert(properties);
  assert(properties->find(
        InternalKeyTablePropertiesNames::kDeletedKeys) == properties->end());
  assert(properties->find(InternalKeyTablePropertiesNames::kMergeOperands) ==
         properties->end());

  std::string val_deleted_keys;
  PutVarint64(&val_deleted_keys, deleted_keys_);
  properties->insert(
      {InternalKeyTablePropertiesNames::kDeletedKeys, val_deleted_keys});

  std::string val_merge_operands;
  PutVarint64(&val_merge_operands, merge_operands_);
  properties->insert(
      {InternalKeyTablePropertiesNames::kMergeOperands, val_merge_operands});

  return Status::OK();
}

UserCollectedProperties
InternalKeyPropertiesCollector::GetReadableProperties() const {
  return {{"kDeletedKeys", ToString(deleted_keys_)},
          {"kMergeOperands", ToString(merge_operands_)}};
}

namespace {

EntryType GetEntryType(ValueType value_type) {
  switch (value_type) {
    case kTypeValue:
      return kEntryPut;
    case kTypeDeletion:
      return kEntryDelete;
    case kTypeSingleDeletion:
      return kEntrySingleDelete;
    case kTypeMerge:
      return kEntryMerge;
    default:
      return kEntryOther;
  }
}

uint64_t GetUint64Property(const UserCollectedProperties& props,
                           const std::string property_name,
                           bool* property_present) {
  auto pos = props.find(property_name);
  if (pos == props.end()) {
    *property_present = false;
    return 0;
  }
  Slice raw = pos->second;
  uint64_t val = 0;
  *property_present = true;
  return GetVarint64(&raw, &val) ? val : 0;
}

}  // namespace

Status UserKeyTablePropertiesCollector::InternalAdd(const Slice& key,
                                                    const Slice& value,
                                                    uint64_t file_size) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::InvalidArgument("Invalid internal key");
  }

  return collector_->AddUserKey(ikey.user_key, value, GetEntryType(ikey.type),
                                ikey.sequence, file_size);
}

Status UserKeyTablePropertiesCollector::Finish(
    UserCollectedProperties* properties) {
  return collector_->Finish(properties);
}

UserCollectedProperties
UserKeyTablePropertiesCollector::GetReadableProperties() const {
  return collector_->GetReadableProperties();
}


const std::string InternalKeyTablePropertiesNames::kDeletedKeys
  = "rocksdb.deleted.keys";
const std::string InternalKeyTablePropertiesNames::kMergeOperands =
    "rocksdb.merge.operands";

uint64_t GetDeletedKeys(
    const UserCollectedProperties& props) {
  bool property_present_ignored;
  return GetUint64Property(props, InternalKeyTablePropertiesNames::kDeletedKeys,
                           &property_present_ignored);
}

uint64_t GetMergeOperands(const UserCollectedProperties& props,
                          bool* property_present) {
  return GetUint64Property(
      props, InternalKeyTablePropertiesNames::kMergeOperands, property_present);
}

}  // namespace rocksdb
