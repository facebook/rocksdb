//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/table_properties_collector.h"

#include "db/dbformat.h"
#include "util/coding.h"

namespace rocksdb {

namespace {
  void AppendProperty(
      std::string& props,
      const std::string& key,
      const std::string& value,
      const std::string& prop_delim,
      const std::string& kv_delim) {
    props.append(key);
    props.append(kv_delim);
    props.append(value);
    props.append(prop_delim);
  }

  template <class TValue>
  void AppendProperty(
      std::string& props,
      const std::string& key,
      const TValue& value,
      const std::string& prop_delim,
      const std::string& kv_delim) {
    AppendProperty(
        props, key, std::to_string(value), prop_delim, kv_delim
    );
  }
}

std::string TableProperties::ToString(
    const std::string& prop_delim,
    const std::string& kv_delim) const {
  std::string result;
  result.reserve(1024);

  // Basic Info
  AppendProperty(
      result, "# data blocks", num_data_blocks, prop_delim, kv_delim
  );
  AppendProperty(result, "# entries", num_entries, prop_delim, kv_delim);

  AppendProperty(result, "raw key size", raw_key_size, prop_delim, kv_delim);
  AppendProperty(
      result,
      "raw average key size",
      num_entries != 0 ?  1.0 * raw_key_size / num_entries : 0.0,
      prop_delim,
      kv_delim
  );
  AppendProperty(
      result, "raw value size", raw_value_size, prop_delim, kv_delim
  );
  AppendProperty(
      result,
      "raw average value size",
      num_entries != 0 ?  1.0 * raw_value_size / num_entries : 0.0,
      prop_delim,
      kv_delim
  );

  AppendProperty(result, "data block size", data_size, prop_delim, kv_delim);
  AppendProperty(result, "index block size", index_size, prop_delim, kv_delim);
  AppendProperty(
      result, "filter block size", filter_size, prop_delim, kv_delim
  );
  AppendProperty(
      result,
      "(estimated) table size=",
      data_size + index_size + filter_size,
      prop_delim,
      kv_delim
  );

  AppendProperty(
      result,
      "filter policy name",
      filter_policy_name.empty() ? std::string("N/A") : filter_policy_name,
      prop_delim,
      kv_delim
  );

  return result;
}

Status InternalKeyPropertiesCollector::Add(
    const Slice& key, const Slice& value) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::InvalidArgument("Invalid internal key");
  }

  if (ikey.type == ValueType::kTypeDeletion) {
    ++deleted_keys_;
  }

  return Status::OK();
}

Status InternalKeyPropertiesCollector::Finish(
    TableProperties::UserCollectedProperties* properties) {
  assert(properties);
  assert(properties->find(
        InternalKeyTablePropertiesNames::kDeletedKeys) == properties->end());
  std::string val;

  PutVarint64(&val, deleted_keys_);
  properties->insert({ InternalKeyTablePropertiesNames::kDeletedKeys, val });

  return Status::OK();
}

TableProperties::UserCollectedProperties
InternalKeyPropertiesCollector::GetReadableProperties() const {
  return {
    { "kDeletedKeys", std::to_string(deleted_keys_) }
  };
}


Status UserKeyTablePropertiesCollector::Add(
    const Slice& key, const Slice& value) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(key, &ikey)) {
    return Status::InvalidArgument("Invalid internal key");
  }

  return collector_->Add(ikey.user_key, value);
}

Status UserKeyTablePropertiesCollector::Finish(
    TableProperties::UserCollectedProperties* properties) {
  return collector_->Finish(properties);
}

TableProperties::UserCollectedProperties
UserKeyTablePropertiesCollector::GetReadableProperties() const {
  return collector_->GetReadableProperties();
}


const std::string InternalKeyTablePropertiesNames::kDeletedKeys
  = "rocksdb.deleted.keys";

uint64_t GetDeletedKeys(
    const TableProperties::UserCollectedProperties& props) {
  auto pos = props.find(InternalKeyTablePropertiesNames::kDeletedKeys);
  if (pos == props.end()) {
    return 0;
  }
  Slice raw = pos->second;
  uint64_t val = 0;
  return GetVarint64(&raw, &val) ? val : 0;
}

}  // namespace rocksdb
