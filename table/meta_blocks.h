//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <map>
#include <memory>
#include <string>

#include "rocksdb/comparator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/block_builder.h"

namespace rocksdb {

class BlockHandle;
class BlockBuilder;
class Logger;
struct TableProperties;

// An STL style comparator that does the bytewise comparator comparasion
// internally.
struct BytewiseLessThan {
  bool operator()(const std::string& key1, const std::string& key2) const {
    // smaller entries will be placed in front.
    return comparator->Compare(key1, key2) <= 0;
  }

  const Comparator* comparator = BytewiseComparator();
};

// When writing to a block that requires entries to be sorted by
// `BytewiseComparator`, we can buffer the content to `BytewiseSortedMap`
// before writng to store.
typedef std::map<std::string, std::string, BytewiseLessThan> BytewiseSortedMap;

class MetaIndexBuilder {
 public:
  MetaIndexBuilder(const MetaIndexBuilder&) = delete;
  MetaIndexBuilder& operator=(const MetaIndexBuilder&) = delete;

  MetaIndexBuilder();
  void Add(const std::string& key, const BlockHandle& handle);

  // Write all the added key/value pairs to the block and return the contents
  // of the block.
  Slice Finish();

 private:
  //   * Key: meta block name
  //   * Value: block handle to that meta block
  struct Rep;
  Rep* rep_;

  // store the sorted key/handle of the metablocks.
  BytewiseSortedMap meta_block_handles_;
  std::unique_ptr<BlockBuilder> meta_index_block_;
};

class PropertyBlockBuilder {
 public:
  PropertyBlockBuilder(const PropertyBlockBuilder&) = delete;
  PropertyBlockBuilder& operator=(const PropertyBlockBuilder&) = delete;

  PropertyBlockBuilder();

  void AddTableProperty(const TableProperties& props);
  void Add(const std::string& key, uint64_t value);
  void Add(const std::string& key, const std::string& value);
  void Add(const UserCollectedProperties& user_collected_properties);

  // Write all the added entries to the block and return the block contents
  Slice Finish();

 private:
  std::unique_ptr<BlockBuilder> properties_block_;
  BytewiseSortedMap props_;
};

// Were we encounter any error occurs during user-defined statistics collection,
// we'll write the warning message to info log.
void LogPropertiesCollectionError(
    Logger* info_log, const std::string& method, const std::string& name);

// Utility functions help table builder to trigger batch events for user
// defined property collectors.
// Return value indicates if there is any error occurred; if error occurred,
// the warning message will be logged.
// NotifyCollectTableCollectorsOnAdd() triggers the `Add` event for all
// property collectors.
bool NotifyCollectTableCollectorsOnAdd(
    const Slice& key,
    const Slice& value,
    const Options::TablePropertiesCollectors& collectors,
    Logger* info_log);

// NotifyCollectTableCollectorsOnAdd() triggers the `Finish` event for all
// property collectors. The collected properties will be added to `builder`.
bool NotifyCollectTableCollectorsOnFinish(
    const Options::TablePropertiesCollectors& collectors,
    Logger* info_log,
    PropertyBlockBuilder* builder);

}  // namespace rocksdb
