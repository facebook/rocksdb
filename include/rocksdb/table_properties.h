// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>
#include <unordered_map>

#include "rocksdb/status.h"

namespace rocksdb {

// TableProperties contains a bunch of read-only properties of its associated
// table.
struct TableProperties {
 public:
  // Other than basic table properties, each table may also have the user
  // collected properties.
  // The value of the user-collected properties are encoded as raw bytes --
  // users have to interprete these values by themselves.
  typedef
    std::unordered_map<std::string, std::string>
    UserCollectedProperties;

  // the total size of all data blocks.
  uint64_t data_size = 0;
  // the size of index block.
  uint64_t index_size = 0;
  // the size of filter block.
  uint64_t filter_size = 0;
  // total raw key size
  uint64_t raw_key_size = 0;
  // total raw value size
  uint64_t raw_value_size = 0;
  // the number of blocks in this table
  uint64_t num_data_blocks = 0;
  // the number of entries in this table
  uint64_t num_entries = 0;

  // The name of the filter policy used in this table.
  // If no filter policy is used, `filter_policy_name` will be an empty string.
  std::string filter_policy_name;

  // user collected properties
  UserCollectedProperties user_collected_properties;

  // convert this object to a human readable form
  //   @prop_delim: delimiter for each property.
  std::string ToString(
      const std::string& prop_delim = "; ",
      const std::string& kv_delim = "=") const;
};

// `TablePropertiesCollector` provides the mechanism for users to collect
// their own interested properties. This class is essentially a collection
//  of callback functions that will be invoked during table building.
class TablePropertiesCollector {
 public:
  virtual ~TablePropertiesCollector() { }

  // Add() will be called when a new key/value pair is inserted into the table.
  // @params key    the original key that is inserted into the table.
  // @params value  the original value that is inserted into the table.
  virtual Status Add(const Slice& key, const Slice& value) = 0;

  // Finish() will be called when a table has already been built and is ready
  // for writing the properties block.
  // @params properties  User will add their collected statistics to
  // `properties`.
  virtual Status Finish(
      TableProperties::UserCollectedProperties* properties) = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;

  // Return the human-readable properties, where the key is property name and
  // the value is the human-readable form of value.
  virtual TableProperties::UserCollectedProperties
    GetReadableProperties() const = 0;
};

// Extra properties
// Below is a list of non-basic properties that are collected by database
// itself. Especially some properties regarding to the internal keys (which
// is unknown to `table`).
extern uint64_t GetDeletedKeys(
    const TableProperties::UserCollectedProperties& props);

}  // namespace rocksdb
