// Copyright (c) 2013 Facebook
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <string>
#include <unordered_map>

#include "rocksdb/status.h"

namespace rocksdb {

// TableStats contains a bunch of read-only stats of its associated
// table.
struct TableStats {
 public:
  // Other than basic table stats, each table may also have the user
  // collected stats.
  // The value of the user-collected stats are encoded as raw bytes --
  // users have to interprete these values by themselves.
  typedef
    std::unordered_map<std::string, std::string>
    UserCollectedStats;

  // the total size of all data blocks.
  uint64_t data_size = 0;
  // the total size of all index blocks.
  uint64_t index_size = 0;
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

  // user collected stats
  UserCollectedStats user_collected_stats;
};

// `TableStatsCollector` provides the mechanism for users to collect their own
// interested stats. This class is essentially a collection of callback
// functions that will be invoked during table building.
class TableStatsCollector {
 public:
  virtual ~TableStatsCollector() { }
  // Add() will be called when a new key/value pair is inserted into the table.
  // @params key    the original key that is inserted into the table.
  // @params value  the original value that is inserted into the table.
  virtual Status Add(const Slice& key, const Slice& value) = 0;

  // Finish() will be called when a table has already been built and is ready
  // for writing the stats block.
  // @params stats  User will add their collected statistics to `stats`.
  virtual Status Finish(TableStats::UserCollectedStats* stats) = 0;

  // The name of the stats collector can be used for debugging purpose.
  virtual const char* Name() const = 0;
};

}  // namespace rocksdb
