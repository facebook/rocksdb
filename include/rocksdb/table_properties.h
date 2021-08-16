// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#pragma once

#include <stdint.h>
#include <map>
#include <string>
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// -- Table Properties
// Other than basic table properties, each table may also have the user
// collected properties.
// The value of the user-collected properties are encoded as raw bytes --
// users have to interpret these values by themselves.
// Note: To do prefix seek/scan in `UserCollectedProperties`, you can do
// something similar to:
//
// UserCollectedProperties props = ...;
// for (auto pos = props.lower_bound(prefix);
//      pos != props.end() && pos->first.compare(0, prefix.size(), prefix) == 0;
//      ++pos) {
//   ...
// }
typedef std::map<std::string, std::string> UserCollectedProperties;

// table properties' human-readable names in the property block.
struct TablePropertiesNames {
  static const std::string kDbId;
  static const std::string kDbSessionId;
  static const std::string kDbHostId;
  static const std::string kDataSize;
  static const std::string kIndexSize;
  static const std::string kIndexPartitions;
  static const std::string kTopLevelIndexSize;
  static const std::string kIndexKeyIsUserKey;
  static const std::string kIndexValueIsDeltaEncoded;
  static const std::string kFilterSize;
  static const std::string kRawKeySize;
  static const std::string kRawValueSize;
  static const std::string kNumDataBlocks;
  static const std::string kNumEntries;
  static const std::string kNumFilterEntries;
  static const std::string kDeletedKeys;
  static const std::string kMergeOperands;
  static const std::string kNumRangeDeletions;
  static const std::string kFormatVersion;
  static const std::string kFixedKeyLen;
  static const std::string kFilterPolicy;
  static const std::string kColumnFamilyName;
  static const std::string kColumnFamilyId;
  static const std::string kComparator;
  static const std::string kMergeOperator;
  static const std::string kPrefixExtractorName;
  static const std::string kPropertyCollectors;
  static const std::string kCompression;
  static const std::string kCompressionOptions;
  static const std::string kCreationTime;
  static const std::string kOldestKeyTime;
  static const std::string kFileCreationTime;
  static const std::string kSlowCompressionEstimatedDataSize;
  static const std::string kFastCompressionEstimatedDataSize;
};

extern const std::string kPropertiesBlock;
extern const std::string kCompressionDictBlock;
extern const std::string kRangeDelBlock;

// `TablePropertiesCollector` provides the mechanism for users to collect
// their own properties that they are interested in. This class is essentially
// a collection of callback functions that will be invoked during table
// building. It is constructed with TablePropertiesCollectorFactory. The methods
// don't need to be thread-safe, as we will create exactly one
// TablePropertiesCollector object per table and then call it sequentially
class TablePropertiesCollector {
 public:
  virtual ~TablePropertiesCollector() {}

  // DEPRECATE User defined collector should implement AddUserKey(), though
  //           this old function still works for backward compatible reason.
  // Add() will be called when a new key/value pair is inserted into the table.
  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status Add(const Slice& /*key*/, const Slice& /*value*/) {
    return Status::InvalidArgument(
        "TablePropertiesCollector::Add() deprecated.");
  }

  // AddUserKey() will be called when a new key/value pair is inserted into the
  // table.
  // @params key    the user key that is inserted into the table.
  // @params value  the value that is inserted into the table.
  virtual Status AddUserKey(const Slice& key, const Slice& value,
                            EntryType /*type*/, SequenceNumber /*seq*/,
                            uint64_t /*file_size*/) {
    // For backwards-compatibility.
    return Add(key, value);
  }

  // Called after each new block is cut
  virtual void BlockAdd(uint64_t /* block_raw_bytes */,
                        uint64_t /* block_compressed_bytes_fast */,
                        uint64_t /* block_compressed_bytes_slow */) {
    // Nothing to do here. Callback registers can override.
    return;
  }

  // Finish() will be called when a table has already been built and is ready
  // for writing the properties block.
  // @params properties  User will add their collected statistics to
  // `properties`.
  virtual Status Finish(UserCollectedProperties* properties) = 0;

  // Return the human-readable properties, where the key is property name and
  // the value is the human-readable form of value.
  virtual UserCollectedProperties GetReadableProperties() const = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;

  // EXPERIMENTAL Return whether the output file should be further compacted
  virtual bool NeedCompact() const { return false; }
};

// Constructs TablePropertiesCollector. Internals create a new
// TablePropertiesCollector for each new table
class TablePropertiesCollectorFactory {
 public:
  struct Context {
    uint32_t column_family_id;
    static const uint32_t kUnknownColumnFamily;
  };

  virtual ~TablePropertiesCollectorFactory() {}
  // has to be thread-safe
  virtual TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) = 0;

  // The name of the properties collector can be used for debugging purpose.
  virtual const char* Name() const = 0;

  // Can be overridden by sub-classes to return the Name, followed by
  // configuration info that will // be logged to the info log when the
  // DB is opened
  virtual std::string ToString() const { return Name(); }
};

// TableProperties contains a bunch of read-only properties of its associated
// table.
struct TableProperties {
 public:
  // the total size of all data blocks.
  uint64_t data_size = 0;
  // the size of index block.
  uint64_t index_size = 0;
  // Total number of index partitions if kTwoLevelIndexSearch is used
  uint64_t index_partitions = 0;
  // Size of the top-level index if kTwoLevelIndexSearch is used
  uint64_t top_level_index_size = 0;
  // Whether the index key is user key. Otherwise it includes 8 byte of sequence
  // number added by internal key format.
  uint64_t index_key_is_user_key = 0;
  // Whether delta encoding is used to encode the index values.
  uint64_t index_value_is_delta_encoded = 0;
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
  // the number of unique entries (keys or prefixes) added to filters
  uint64_t num_filter_entries = 0;
  // the number of deletions in the table
  uint64_t num_deletions = 0;
  // the number of merge operands in the table
  uint64_t num_merge_operands = 0;
  // the number of range deletions in this table
  uint64_t num_range_deletions = 0;
  // format version, reserved for backward compatibility
  uint64_t format_version = 0;
  // If 0, key is variable length. Otherwise number of bytes for each key.
  uint64_t fixed_key_len = 0;
  // ID of column family for this SST file, corresponding to the CF identified
  // by column_family_name.
  uint64_t column_family_id = ROCKSDB_NAMESPACE::
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
  // Timestamp of the latest key. 0 means unknown.
  // TODO(sagar0): Should be changed to latest_key_time ... but don't know the
  // full implications of backward compatibility. Hence retaining for now.
  uint64_t creation_time = 0;
  // Timestamp of the earliest key. 0 means unknown.
  uint64_t oldest_key_time = 0;
  // Actual SST file creation time. 0 means unknown.
  uint64_t file_creation_time = 0;
  // Estimated size of data blocks if compressed using a relatively slower
  // compression algorithm (see `ColumnFamilyOptions::sample_for_compression`).
  // 0 means unknown.
  uint64_t slow_compression_estimated_data_size = 0;
  // Estimated size of data blocks if compressed using a relatively faster
  // compression algorithm (see `ColumnFamilyOptions::sample_for_compression`).
  // 0 means unknown.
  uint64_t fast_compression_estimated_data_size = 0;

  // DB identity
  // db_id is an identifier generated the first time the DB is created
  // If DB identity is unset or unassigned, `db_id` will be an empty string.
  std::string db_id;

  // DB session identity
  // db_session_id is an identifier that gets reset every time the DB is opened
  // If DB session identity is unset or unassigned, `db_session_id` will be an
  // empty string.
  std::string db_session_id;

  // Location of the machine hosting the DB instance
  // db_host_id identifies the location of the host in some form
  // (hostname by default, but can also be any string of the user's choosing).
  // It can potentially change whenever the DB is opened
  std::string db_host_id;

  // Name of the column family with which this SST file is associated.
  // If column family is unknown, `column_family_name` will be an empty string.
  std::string column_family_name;

  // The name of the filter policy used in this table.
  // If no filter policy is used, `filter_policy_name` will be an empty string.
  std::string filter_policy_name;

  // The name of the comparator used in this table.
  std::string comparator_name;

  // The name of the merge operator used in this table.
  // If no merge operator is used, `merge_operator_name` will be "nullptr".
  std::string merge_operator_name;

  // The name of the prefix extractor used in this table
  // If no prefix extractor is used, `prefix_extractor_name` will be "nullptr".
  std::string prefix_extractor_name;

  // The names of the property collectors factories used in this table
  // separated by commas
  // {collector_name[1]},{collector_name[2]},{collector_name[3]} ..
  std::string property_collectors_names;

  // The compression algo used to compress the SST files.
  std::string compression_name;

  // Compression options used to compress the SST files.
  std::string compression_options;

  // user collected properties
  UserCollectedProperties user_collected_properties;
  UserCollectedProperties readable_properties;

  // The offset of the value of each property in the file.
  std::map<std::string, uint64_t> properties_offsets;

  // convert this object to a human readable form
  //   @prop_delim: delimiter for each property.
  std::string ToString(const std::string& prop_delim = "; ",
                       const std::string& kv_delim = "=") const;

  // Aggregate the numerical member variables of the specified
  // TableProperties.
  void Add(const TableProperties& tp);

  // Subset of properties that make sense when added together
  // between tables. Keys match field names in this class instead
  // of using full property names.
  std::map<std::string, uint64_t> GetAggregatablePropertiesAsMap() const;
};

// Extra properties
// Below is a list of non-basic properties that are collected by database
// itself. Especially some properties regarding to the internal keys (which
// is unknown to `table`).
//
// DEPRECATED: these properties now belong as TableProperties members. Please
// use TableProperties::num_deletions and TableProperties::num_merge_operands,
// respectively.
extern uint64_t GetDeletedKeys(const UserCollectedProperties& props);
extern uint64_t GetMergeOperands(const UserCollectedProperties& props,
                                 bool* property_present);

}  // namespace ROCKSDB_NAMESPACE
