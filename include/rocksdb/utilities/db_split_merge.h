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
  // every database and which is empty unless selected. Unselected column
  // families are not copied.
  // "<destination_db_path>.tmp" is used for staging, so it must not exist
  // either. The caller must not create either path during the call.
  std::string destination_db_path;

  // Each source column family may appear at most once.
  std::vector<ColumnFamilySplit> column_family_splits;

  // Verifies the checksums of the destination DB after clipping. Ignored by
  // DeleteSplitRanges().
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
  // exist. It is removed before returning, best-effort after an error. If
  // removal fails after a successful merge, the call still succeeds and logs a
  // warning to the destination's info log, and later calls with this path fail
  // until it is removed. Placing it on the destination file system enables hard
  // links during ingestion when the file system supports them.
  // "<checkpoint_directory>.tmp" is used for staging, so it must not exist
  // either. The caller must not create either path during the call.
  std::string checkpoint_directory;

  // Source and destination column families must each be unique across this
  // vector. Every destination column family must already exist.
  std::vector<ColumnFamilyMerge> column_family_merges;

  // Verifies the checksums of the clipped files during ingestion.
  bool verify_checksums = true;
};

struct SplitMergeResult {
  uint64_t checkpoint_sequence = 0;
  uint64_t transferred_files = 0;
  uint64_t transferred_bytes = 0;
};

// Creates a checkpoint-derived DB containing only the requested ranges in the
// selected column families. Source is not modified. Once readers and writers
// of the selected ranges have moved to the destination DB, call
// DeleteSplitRanges() with the same options to remove the ranges from source.
// The caller must prevent writes to the selected ranges from the start of this
// call until DeleteSplitRanges() returns. Clipping fully compacts each selected
// column family in the destination, so the data kept in the selected ranges is
// read and rewritten once.
Status SplitDB(DB* source, const SplitDBOptions& options,
               SplitMergeResult* result);

// Atomically deletes the ranges in options.column_family_splits from source,
// then flushes and compacts them to reclaim space. options.destination_db_path
// and options.verify_checksums are ignored. On a non-OK status the ranges may
// already be deleted; calling again is safe.
Status DeleteSplitRanges(DB* source, const SplitDBOptions& options);

// Copies the requested ranges from source into existing destination column
// families. The destination changes are recorded in one atomic MANIFEST
// update, so after a crash either all or none of them are present. Concurrent
// readers can see some destination column families updated before others
// until this function returns. The caller must prevent writes to all source
// and destination ranges until this function returns. Each destination range
// must be empty. Fails if destination has live snapshots, because merged keys
// keep their source sequence numbers and would become visible in them. The
// ranges are clipped by fully compacting each selected column family in a
// checkpoint of source, so the merged data is read and rewritten once before
// ingestion.
Status MergeDB(DB* source, DB* destination, const MergeDBOptions& options,
               SplitMergeResult* result);

}  // namespace ROCKSDB_NAMESPACE
