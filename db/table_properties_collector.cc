//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/table_properties_collector.h"

#include "db/dbformat.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

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

}  // anonymous namespace

Status UserKeyTablePropertiesCollector::InternalAdd(const Slice& key,
                                                    const Slice& value,
                                                    uint64_t file_size) {
  ParsedInternalKey ikey;
  Status s = ParseInternalKey(key, &ikey, false /* log_err_key */);  // TODO
  if (!s.ok()) {
    return s;
  }

  return collector_->AddUserKey(ikey.user_key, value, GetEntryType(ikey.type),
                                ikey.sequence, file_size);
}

void UserKeyTablePropertiesCollector::BlockAdd(
    uint64_t block_uncomp_bytes, uint64_t block_compressed_bytes_fast,
    uint64_t block_compressed_bytes_slow) {
  return collector_->BlockAdd(block_uncomp_bytes, block_compressed_bytes_fast,
                              block_compressed_bytes_slow);
}

Status UserKeyTablePropertiesCollector::Finish(
    UserCollectedProperties* properties) {
  return collector_->Finish(properties);
}

UserCollectedProperties UserKeyTablePropertiesCollector::GetReadableProperties()
    const {
  return collector_->GetReadableProperties();
}

uint64_t GetDeletedKeys(const UserCollectedProperties& props) {
  bool property_present_ignored;
  return GetUint64Property(props, TablePropertiesNames::kDeletedKeys,
                           &property_present_ignored);
}

uint64_t GetMergeOperands(const UserCollectedProperties& props,
                          bool* property_present) {
  return GetUint64Property(props, TablePropertiesNames::kMergeOperands,
                           property_present);
}

}  // namespace ROCKSDB_NAMESPACE
