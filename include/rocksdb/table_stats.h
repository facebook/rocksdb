// Copyright (c) 2013 Facebook
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <string>
#include <unordered_map>

namespace rocksdb {

// TableStats contains a bunch of read-only stats of its associated
// table.
struct TableStats {
 public:
  // TODO(kailiu) we do not support user collected stats yet.
  //
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

  // user collected stats
  UserCollectedStats user_collected_stats;
};

}  // namespace rocksdb
