//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/table_properties_collector.h"

#include "db/dbformat.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

Status InternalKeyPropertiesCollector::InternalAdd(const Slice& key,
                                                   const Slice& /*value*/,
                                                   uint64_t /*file_size*/) {
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
  assert(properties->find(InternalKeyTablePropertiesNames::kDeletedKeys) ==
         properties->end());
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

Status CompositeSstPropertiesCollector::Finish(
    UserCollectedProperties* properties) {
  assert(properties);
  assert(properties->find(CompositeSstTablePropertiesNames::kSstPurpose) ==
         properties->end());
  assert(properties->find(CompositeSstTablePropertiesNames::kSstDepend) ==
         properties->end());

  auto sst_purpose_value = std::string((const char*)&sst_purpose_, 1);
  properties->insert(
      {CompositeSstTablePropertiesNames::kSstPurpose, sst_purpose_value});

  std::string sst_depend;
  PutVarint64(&sst_depend, sst_depend_->size());
  for (auto sst_id : *sst_depend_) {
    PutVarint64(&sst_depend, sst_id);
  }
  properties->insert(
      {CompositeSstTablePropertiesNames::kSstDepend, sst_depend});

  std::string sst_read_amp;
  PutVarint64(&sst_read_amp, *sst_read_amp_);

  properties->insert(
      {CompositeSstTablePropertiesNames::kSstReadAmp, sst_read_amp});

  return Status::OK();
}

UserCollectedProperties CompositeSstPropertiesCollector::GetReadableProperties()
    const {
  std::string sst_depend;
  if (sst_depend_->empty()) {
    sst_depend += "[]";
  } else {
    sst_depend += '[';
    for (auto sst_id : *sst_depend_) {
      sst_depend += ToString(sst_id);
      sst_depend += ',';
    }
    sst_depend.back() = ']';
  }
  return {{"kSstPurpose", ToString((int)sst_purpose_)},
          {"kSstDepend", sst_depend},
          {"kSstReadAmp", ToString(*sst_read_amp_)}};
}

namespace {

uint64_t GetUint64Property(const UserCollectedProperties& props,
                           const std::string& property_name,
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

UserCollectedProperties UserKeyTablePropertiesCollector::GetReadableProperties()
    const {
  return collector_->GetReadableProperties();
}

const std::string InternalKeyTablePropertiesNames::kDeletedKeys =
    "rocksdb.deleted.keys";
const std::string InternalKeyTablePropertiesNames::kMergeOperands =
    "rocksdb.merge.operands";
const std::string CompositeSstTablePropertiesNames::kSstPurpose =
    "rocksdb.sst.purpose";
const std::string CompositeSstTablePropertiesNames::kSstDepend =
    "rocksdb.sst.depend";
const std::string CompositeSstTablePropertiesNames::kSstReadAmp =
    "rocksdb.sst.read_amp";

uint64_t GetDeletedKeys(const UserCollectedProperties& props) {
  bool property_present_ignored;
  return GetUint64Property(props, InternalKeyTablePropertiesNames::kDeletedKeys,
                           &property_present_ignored);
}

uint64_t GetMergeOperands(const UserCollectedProperties& props,
                          bool* property_present) {
  return GetUint64Property(
      props, InternalKeyTablePropertiesNames::kMergeOperands, property_present);
}

uint8_t GetSstPurpose(const UserCollectedProperties& props) {
  auto pos = props.find(CompositeSstTablePropertiesNames::kSstPurpose);
  if (pos == props.end()) {
    return 0;
  }
  Slice raw = pos->second;
  return raw[0];
}

std::vector<uint64_t> GetSstDepend(const UserCollectedProperties& props) {
  std::vector<uint64_t> result;
  auto pos = props.find(CompositeSstTablePropertiesNames::kSstDepend);
  if (pos == props.end()) {
    return result;
  }
  Slice raw = pos->second;
  uint64_t size;
  if (!GetVarint64(&raw, &size)) {
    return result;
  }
  result.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    uint64_t sst_id;
    if (!GetVarint64(&raw, &sst_id)) {
      return result;
    }
    result.emplace_back(sst_id);
  }
  return result;
}

size_t GetSstReadAmp(const UserCollectedProperties& props) {
  bool ignore;
  return GetUint64Property(
      props, CompositeSstTablePropertiesNames::kSstReadAmp, &ignore);
}

}  // namespace rocksdb
