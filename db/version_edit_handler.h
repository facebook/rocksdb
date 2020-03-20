//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

typedef std::unique_ptr<BaseReferencedVersionBuilder> VersionBuilderUPtr;

// A class used for scanning MANIFEST file.
// VersionEditHandler reads a MANIFEST file, parses the version edits, and
// builds the version set's in-memory state, e.g. the version storage info for
// the versions of column families.
// To use this class and its subclasses,
// 1. Create an object of VersionEditHandler or its subclasses.
//    VersionEditHandler handler(read_only, column_families, version_set,
//                               track_missing_files, ignore_missing_files);
// 2. Status s = handler.Iterate(reader, &db_id);
// 3. Check s and handle possible errors.
//
// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandler is shared by multiple threads.
class VersionEditHandler {
 public:
  explicit VersionEditHandler(
      bool read_only,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      VersionSet* version_set, bool track_missing_files,
      bool ignore_missing_files);

  virtual ~VersionEditHandler() {}

  Status Iterate(log::Reader& reader, std::string* db_id);

  const Status& status() const { return status_; }

  bool HasMissingFiles() const;

 protected:
  Status ApplyVersionEdit(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnColumnFamilyDrop(VersionEdit& edit, ColumnFamilyData** cfd);

  Status OnNonCfOperation(VersionEdit& edit, ColumnFamilyData** cfd);

  Status Initialize();

  void CheckColumnFamilyId(const VersionEdit& edit, bool* cf_in_not_found,
                           bool* cf_in_builders) const;

  virtual void CheckIterationResult(const log::Reader& reader, Status* s);

  ColumnFamilyData* CreateCfAndInit(const ColumnFamilyOptions& cf_options,
                                    const VersionEdit& edit);

  virtual ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit);

  virtual Status MaybeCreateVersion(const VersionEdit& edit,
                                    ColumnFamilyData* cfd,
                                    bool force_create_version);

  Status LoadTables(ColumnFamilyData* cfd,
                    bool prefetch_index_and_filter_in_cache,
                    bool is_initial_load);

  const bool read_only_;
  const std::vector<ColumnFamilyDescriptor>& column_families_;
  Status status_;
  VersionSet* version_set_;
  AtomicGroupReadBuffer read_buffer_;
  std::unordered_map<uint32_t, VersionBuilderUPtr> builders_;
  std::unordered_map<std::string, ColumnFamilyOptions> name_to_options_;
  std::unordered_map<uint32_t, std::string> column_families_not_found_;
  VersionEditParams version_edit_params_;
  const bool track_missing_files_;
  std::unordered_map<uint32_t, std::unordered_set<uint64_t>>
      cf_to_missing_files_;
  bool no_error_if_table_files_missing_;

 private:
  Status ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                    const VersionEdit& edit);

  bool initialized_;
};

// A class similar to its base class, i.e. VersionEditHandler.
// VersionEditHandlerPointInTime restores the versions to the most recent point
// in time such that at this point, the version does not have missing files.
//
// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandlerPointInTime is shared by multiple threads.
class VersionEditHandlerPointInTime : public VersionEditHandler {
 public:
  VersionEditHandlerPointInTime(
      bool read_only,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      VersionSet* version_set);
  ~VersionEditHandlerPointInTime() override;

 protected:
  void CheckIterationResult(const log::Reader& reader, Status* s) override;
  ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit) override;
  Status MaybeCreateVersion(const VersionEdit& edit, ColumnFamilyData* cfd,
                            bool force_create_version) override;

 private:
  std::unordered_map<uint32_t, Version*> versions_;
};

}  // namespace ROCKSDB_NAMESPACE
