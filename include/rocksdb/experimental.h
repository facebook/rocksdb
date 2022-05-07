// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace experimental {

// Supported only for Leveled compaction
Status SuggestCompactRange(DB* db, ColumnFamilyHandle* column_family,
                           const Slice* begin, const Slice* end);
Status SuggestCompactRange(DB* db, const Slice* begin, const Slice* end);

// Move all L0 files to target_level skipping compaction.
// This operation succeeds only if the files in L0 have disjoint ranges; this
// is guaranteed to happen, for instance, if keys are inserted in sorted
// order. Furthermore, all levels between 1 and target_level must be empty.
// If any of the above condition is violated, InvalidArgument will be
// returned.
Status PromoteL0(DB* db, ColumnFamilyHandle* column_family,
                 int target_level = 1);

struct UpdateManifestForFilesStateOptions {
  // When true, read current file temperatures from FileSystem and update in
  // DB manifest when a temperature other than Unknown is reported and
  // inconsistent with manifest.
  bool update_temperatures = true;

  // TODO: new_checksums: to update files to latest file checksum algorithm
};

// Utility for updating manifest of DB directory (not open) for current state
// of files on filesystem. See UpdateManifestForFilesStateOptions.
//
// To minimize interference with ongoing DB operations, only the following
// guarantee is provided, assuming no IO error encountered:
// * Only files live in DB at start AND end of call to
// UpdateManifestForFilesState() are guaranteed to be updated (as needed) in
// manifest.
//   * For example, new files after start of call to
//   UpdateManifestForFilesState() might not be updated, but that is not
//   typically required to achieve goal of manifest consistency/completeness
//   (because current DB configuration would ensure new files get the desired
//   consistent metadata).
Status UpdateManifestForFilesState(
    const DBOptions& db_opts, const std::string& db_name,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const UpdateManifestForFilesStateOptions& opts = {});

}  // namespace experimental
}  // namespace ROCKSDB_NAMESPACE
