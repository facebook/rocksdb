//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once

#include <memory>

#include "db/version_edit.h"
#include "rocksdb/file_system.h"
#include "rocksdb/metadata.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {

struct ImmutableCFOptions;
class TableCache;
class VersionStorageInfo;
class VersionEdit;
struct FileMetaData;
class InternalStats;
class Version;
class VersionSet;
class VersionEditHandler;
class ColumnFamilyData;
class CacheReservationManager;

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionBuilder {
 public:
  VersionBuilder(const FileOptions& file_options,
                 const ImmutableCFOptions* ioptions, TableCache* table_cache,
                 VersionStorageInfo* base_vstorage, VersionSet* version_set,
                 std::shared_ptr<CacheReservationManager>
                     file_metadata_cache_res_mgr = nullptr,
                 ColumnFamilyData* cfd = nullptr,
                 VersionEditHandler* version_edit_handler = nullptr,
                 bool track_found_and_missing_files = false,
                 bool allow_incomplete_valid_version = false);
  ~VersionBuilder();

  bool CheckConsistencyForNumLevels();

  Status Apply(const VersionEdit* edit);

  // Save the current Version to the provided `vstorage`.
  Status SaveTo(VersionStorageInfo* vstorage) const;

  // Load all the table handlers for the current Version in the builder.
  Status LoadTableHandlers(
      InternalStats* internal_stats, int max_threads,
      bool prefetch_index_and_filter_in_cache, bool is_initial_load,
      const std::shared_ptr<const SliceTransform>& prefix_extractor,
      size_t max_file_size_for_l0_meta_pin, const ReadOptions& read_options,
      uint8_t block_protection_bytes_per_key);

  //============APIs only used by VersionEditHandlerPointInTime ============//

  // Creates a save point for the Version that has been built so far. Subsequent
  // VersionEdits applied to the builder will not affect the Version in this
  // save point. VersionBuilder currently only supports creating one save point,
  // so when `CreateOrReplaceSavePoint` is called again, the previous save point
  // is cleared. `ClearSavePoint` can be called explicitly to clear
  // the save point too.
  void CreateOrReplaceSavePoint();

  // The builder can find all the files to build a `Version`. Or if
  // `allow_incomplete_valid_version_` is true and the version history is never
  // edited in an atomic group, and only a suffix of L0 SST files and their
  // associated blob files are missing.
  // From the users' perspective, missing a suffix of L0 files means missing the
  // user's most recently written data. So the remaining available files still
  // presents a valid point in time view, although for some previous time.
  // This validity check result will be cached and reused if the Version is not
  // updated between two validity checks.
  bool ValidVersionAvailable();

  bool HasMissingFiles() const;

  // When applying a sequence of VersionEdit, intermediate files are the ones
  // that are added and then deleted. The caller should clear this intermediate
  // files tracking after calling this API. So that the tracking for subsequent
  // VersionEdits can start over with a clean state.
  std::vector<std::string>& GetAndClearIntermediateFiles();

  // Clearing all the found files in this Version.
  void ClearFoundFiles();

  // Save the Version in the save point to the provided `vstorage`.
  // Non-OK status will be returned if there is not a valid save point.
  Status SaveSavePointTo(VersionStorageInfo* vstorage) const;

  // Load all the table handlers for the Version in the save point.
  // Non-OK status will be returned if there is not a valid save point.
  Status LoadSavePointTableHandlers(
      InternalStats* internal_stats, int max_threads,
      bool prefetch_index_and_filter_in_cache, bool is_initial_load,
      const std::shared_ptr<const SliceTransform>& prefix_extractor,
      size_t max_file_size_for_l0_meta_pin, const ReadOptions& read_options,
      uint8_t block_protection_bytes_per_key);

  void ClearSavePoint();

  //======= End of APIs only used by VersionEditPointInTime==========//

 private:
  class Rep;
  std::unique_ptr<Rep> savepoint_;
  std::unique_ptr<Rep> rep_;
};

// A wrapper of version builder which references the current version in
// constructor and unref it in the destructor.
// Both of the constructor and destructor need to be called inside DB Mutex.
class BaseReferencedVersionBuilder {
 public:
  explicit BaseReferencedVersionBuilder(
      ColumnFamilyData* cfd, VersionEditHandler* version_edit_handler = nullptr,
      bool track_found_and_missing_files = false,
      bool allow_incomplete_valid_version = false);
  BaseReferencedVersionBuilder(
      ColumnFamilyData* cfd, Version* v,
      VersionEditHandler* version_edit_handler = nullptr,
      bool track_found_and_missing_files = false,
      bool allow_incomplete_valid_version = false);
  ~BaseReferencedVersionBuilder();
  VersionBuilder* version_builder() const { return version_builder_.get(); }

 private:
  std::unique_ptr<VersionBuilder> version_builder_;
  Version* version_;
};
}  // namespace ROCKSDB_NAMESPACE
