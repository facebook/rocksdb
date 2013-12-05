//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/meta_blocks.h"

#include <map>

#include "rocksdb/table_properties.h"
#include "table/format.h"
#include "util/coding.h"

namespace rocksdb {

MetaIndexBuilder::MetaIndexBuilder()
    : meta_index_block_(
        new BlockBuilder(1 /* restart interval */, BytewiseComparator())) {
}

void MetaIndexBuilder::Add(const std::string& key,
                           const BlockHandle& handle) {
  std::string handle_encoding;
  handle.EncodeTo(&handle_encoding);
  meta_block_handles_.insert({key, handle_encoding});
}

Slice MetaIndexBuilder::Finish() {
  for (const auto& metablock : meta_block_handles_) {
    meta_index_block_->Add(metablock.first, metablock.second);
  }
  return meta_index_block_->Finish();
}

PropertyBlockBuilder::PropertyBlockBuilder()
  : properties_block_(
      new BlockBuilder(1 /* restart interval */, BytewiseComparator())) {
}

void PropertyBlockBuilder::Add(const std::string& name,
                               const std::string& val) {
  props_.insert({name, val});
}

void PropertyBlockBuilder::Add(const std::string& name, uint64_t val) {
  assert(props_.find(name) == props_.end());

  std::string dst;
  PutVarint64(&dst, val);

  Add(name, dst);
}

void PropertyBlockBuilder::Add(
    const UserCollectedProperties& user_collected_properties) {
  for (const auto& prop : user_collected_properties) {
    Add(prop.first, prop.second);
  }
}

void PropertyBlockBuilder::AddTableProperty(const TableProperties& props) {
  Add(TablePropertiesNames::kRawKeySize, props.raw_key_size);
  Add(TablePropertiesNames::kRawValueSize, props.raw_value_size);
  Add(TablePropertiesNames::kDataSize, props.data_size);
  Add(TablePropertiesNames::kIndexSize, props.index_size);
  Add(TablePropertiesNames::kNumEntries, props.num_entries);
  Add(TablePropertiesNames::kNumDataBlocks, props.num_data_blocks);
  Add(TablePropertiesNames::kFilterSize, props.filter_size);

  if (!props.filter_policy_name.empty()) {
    Add(TablePropertiesNames::kFilterPolicy,
        props.filter_policy_name);
  }
}

Slice PropertyBlockBuilder::Finish() {
  for (const auto& prop : props_) {
    properties_block_->Add(prop.first, prop.second);
  }

  return properties_block_->Finish();
}

void LogPropertiesCollectionError(
    Logger* info_log, const std::string& method, const std::string& name) {
  assert(method == "Add" || method == "Finish");

  std::string msg =
    "[Warning] encountered error when calling TablePropertiesCollector::" +
    method + "() with collector name: " + name;
  Log(info_log, "%s", msg.c_str());
}

bool NotifyCollectTableCollectorsOnAdd(
    const Slice& key,
    const Slice& value,
    const Options::TablePropertiesCollectors& collectors,
    Logger* info_log) {
  bool all_succeeded = true;
  for (auto collector : collectors) {
    Status s = collector->Add(key, value);
    all_succeeded = all_succeeded && s.ok();
    if (!s.ok()) {
      LogPropertiesCollectionError(
          info_log, "Add", /* method */ collector->Name()
      );
    }
  }
  return all_succeeded;
}

bool NotifyCollectTableCollectorsOnFinish(
    const Options::TablePropertiesCollectors& collectors,
    Logger* info_log,
    PropertyBlockBuilder* builder) {
  bool all_succeeded = true;
  for (auto collector : collectors) {
    UserCollectedProperties user_collected_properties;
    Status s = collector->Finish(&user_collected_properties);

    all_succeeded = all_succeeded && s.ok();
    if (!s.ok()) {
      LogPropertiesCollectionError(
          info_log, "Finish", /* method */ collector->Name()
      );
    } else {
      builder->Add(user_collected_properties);
    }
  }

  return all_succeeded;
}

}  // namespace rocksdb
