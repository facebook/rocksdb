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

// Not thread-safe, external synchronization is necessary if an object of
// VersionEditHandler is shared by multiple threads.
class VersionEditHandler {
 public:
  explicit VersionEditHandler(
      bool read_only,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      VersionSet* version_set, bool create_version_for_each_edit,
      bool load_tables_for_each_edit);
  virtual ~VersionEditHandler() {}

  Status Iterate(bool prefetch_index_and_filter_in_cache, bool is_initial_load,
                 log::Reader& reader, std::string* db_id);
  Status ApplyOneVersionEditToBuilder(bool prefetch_index_and_filter_in_cache,
                                      bool is_initial_load, VersionEdit& edit,
                                      ColumnFamilyData** cfd);
  Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd);
  Status OnColumnFamilyDrop(VersionEdit& edit, ColumnFamilyData** cfd);
  Status OnNonCfOperation(VersionEdit& edit, ColumnFamilyData** cfd);

  const Status& status() const { return status_; }

 protected:
  Status Initialize();
  void CheckColumnFamilyId(const VersionEdit& edit, bool* cf_in_not_found,
                           bool* cf_in_builders) const;
  virtual void CheckIterateResult(const log::Reader& reader, Status* s);

  virtual ColumnFamilyData* CreateCfAndInit(
      const ColumnFamilyOptions& cf_options, const VersionEdit& edit);
  virtual ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit);
  virtual Status CreateVersion(const VersionEdit& edit, ColumnFamilyData* cfd);
  virtual Status LoadTables(ColumnFamilyData* cfd,
                            bool prefetch_index_and_filter_in_cache,
                            bool is_initial_load);
  virtual bool ShouldRetry(Status* /*s*/) { return false; }

  const bool read_only_;
  const std::vector<ColumnFamilyDescriptor>& column_families_;
  Status status_;
  VersionSet* version_set_;
  AtomicGroupReadBuffer read_buffer_;
  std::unordered_map<uint32_t, VersionBuilderUPtr> builders_;
  std::unordered_map<std::string, ColumnFamilyOptions> name_to_options_;
  std::unordered_map<uint32_t, std::string> column_families_not_found_;
  VersionEditParams version_edit_params_;
  const bool create_version_for_each_edit_;
  const bool load_tables_for_each_edit_;

 private:
  Status ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                    const VersionEdit& edit);
  bool initialized_;
};

class VersionEditHandlerPointInTime : public VersionEditHandler {
 public:
  VersionEditHandlerPointInTime(
      bool read_only,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      VersionSet* version_set);
  ~VersionEditHandlerPointInTime() override;
  bool HasMissingFiles() const;

 protected:
  void CheckIterateResult(const log::Reader& reader, Status* s) override;
  ColumnFamilyData* CreateCfAndInit(const ColumnFamilyOptions& cf_options,
                                    const VersionEdit& edit) override;
  ColumnFamilyData* DestroyCfAndCleanup(const VersionEdit& edit) override;
  Status CreateVersion(const VersionEdit& edit, ColumnFamilyData* cfd) override;

 private:
  std::unordered_map<uint32_t, std::unordered_set<uint64_t>>
      cf_to_missing_files_;
  std::unordered_map<uint32_t, Version*> versions_;
};

}  // namespace ROCKSDB_NAMESPACE
