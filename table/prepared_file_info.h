//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "db/dbformat.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

struct PreparedFileInfo {
  uint64_t file_size = 0;
  // Final ingestion bounds, after considering both point keys and range
  // deletions. For range deletion bounds, these use the same boundary shape
  // returned by NewRangeTombstoneIterator(): timestamped range tombstone bounds
  // use max timestamp, while files that did not persist user-defined timestamps
  // omit the timestamp.
  InternalKey smallest;
  InternalKey largest;
  TableProperties table_properties;
};

}  // namespace ROCKSDB_NAMESPACE
