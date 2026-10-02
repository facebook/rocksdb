//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;

// A range to extract from one column family during SplitDB(). The range is
// half-open: begin_key is included and end_key is excluded.
struct ColumnFamilySplit {
  std::string begin_key;
  std::string end_key;
  ColumnFamilyHandle* source_cf = nullptr;
};

struct SplitDBOptions {
  // Must name a path that does not exist. On success it contains the selected
  // column families plus the default column family, which RocksDB requires in
  // every database. Unselected non-default column families are not copied.
  std::string destination_db_path;

  // Each source column family may appear at most once.
  std::vector<ColumnFamilySplit> column_family_splits;

  bool verify_checksums = true;
};

// A range to copy from one source column family to one existing destination
// column family during MergeDB(). The range is half-open.
struct ColumnFamilyMerge {
  std::string begin_key;
  std::string end_key;
  ColumnFamilyHandle* source_cf = nullptr;
  ColumnFamilyHandle* destination_cf = nullptr;
};

struct MergeDBOptions {
  // Temporary directory used for a checkpoint of the source DB. It must not
  // exist. It is removed after success and cleaned up best-effort after an
  // error. Placing it on the destination file system enables hard links during
  // ingestion when the file system supports them.
  std::string checkpoint_directory;

  // Source and destination column families must each be unique across this
  // vector. Every destination column family must already exist.
  std::vector<ColumnFamilyMerge> column_family_merges;

  bool verify_checksums = true;
};

struct SplitMergeResult {
  uint64_t checkpoint_sequence = 0;
  uint64_t transferred_files = 0;
  uint64_t transferred_bytes = 0;
};

// Creates a checkpoint-derived DB containing only the requested ranges in the
// selected column families, then atomically deletes those ranges from source.
// The caller must prevent writes to the selected ranges until this function
// returns and routing has moved to the destination DB.
Status SplitDB(DB* source, const SplitDBOptions& options,
               SplitMergeResult* result);

// Copies the requested ranges from source into existing destination column
// families. All destination changes are committed atomically. The caller must
// prevent writes to all source and destination ranges until this function
// returns. Each destination range must be empty.
Status MergeDB(DB* source, DB* destination, const MergeDBOptions& options,
               SplitMergeResult* result);

}  // namespace ROCKSDB_NAMESPACE
