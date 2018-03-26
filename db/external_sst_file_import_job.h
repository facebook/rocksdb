//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/internal_stats.h"
#include "db/snapshot_impl.h"
#include "options/db_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/sst_file_writer.h"
#include "util/autovector.h"

namespace rocksdb {

class ExternalSstFileImportJob {
 public:
  ExternalSstFileImportJob(
      Env* env, VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options, const EnvOptions& env_options,
      SnapshotList* db_snapshots,
      const std::vector<ImportFileMetaData>& files_metadata,
      const ImportExternalFileOptions& import_options)
      : env_(env),
        versions_(versions),
        cfd_(cfd),
        db_options_(db_options),
        env_options_(env_options),
        db_snapshots_(db_snapshots),
        job_start_time_(env_->NowMicros()),
        import_files_metadata_(files_metadata),
        import_options_(import_options) {}

  // Prepare the job by copying external files into the DB.
  Status Prepare();

  // Check if we need to flush the memtable before running the import job
  // This will be true if the files we are importing are overlapping with any
  // key range in the memtable.
  //
  // @param super_version A referenced SuperVersion that will be held for the
  //    duration of this function.
  //
  // Thread-safe
  Status NeedsFlush(bool* flush_needed, SuperVersion* super_version);

  // Will execute the import job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  Status Run();

  // Update column family stats.
  // REQUIRES: Mutex held
  void UpdateStats();

  // Cleanup after successful/failed job
  void Cleanup(const Status& status);

  VersionEdit* edit() { return &edit_; }

  const autovector<IngestedFileInfo>& files_to_import() const {
    return files_to_import_;
  }

  const std::vector<ImportFileMetaData>& import_files_metadata() const {
    return import_files_metadata_;
  }

 private:
  // Open the external file and populate `file_to_import` with all the
  // external information we need to import this file.
  Status GetIngestedFileInfo(const std::string& external_file,
                             IngestedFileInfo* file_to_import);

  // Checks whether the file being imported has any overlap with existing files
  Status CheckLevelOverlapForImportFile(SuperVersion* sv,
                                        IngestedFileInfo* file_to_import);

  Env* env_;
  VersionSet* versions_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const EnvOptions& env_options_;
  SnapshotList* db_snapshots_;
  autovector<IngestedFileInfo> files_to_import_;
  VersionEdit edit_;
  uint64_t job_start_time_;
  std::vector<ImportFileMetaData> import_files_metadata_;
  const ImportExternalFileOptions& import_options_;
};

}  // namespace rocksdb
