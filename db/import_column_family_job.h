#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/snapshot_impl.h"
#include "options/db_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/sst_file_writer.h"
#include "util/autovector.h"

namespace rocksdb {

// Imports a set of sst files as is into a new column family. Logic is similar
// to ExternalSstFileIngestionJob.
class ImportColumnFamilyJob {
 public:
  ImportColumnFamilyJob(
      Env* env, VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options, const EnvOptions& env_options,
      const ImportColumnFamilyOptions& import_options,
      const std::vector<LiveFileMetaData>& metadata)
      : env_(env),
        versions_(versions),
        cfd_(cfd),
        db_options_(db_options),
        env_options_(env_options),
        import_options_(import_options),
        metadata_(metadata) {}

  // Prepare the job by copying external files into the DB.
  Status Prepare(uint64_t next_file_number, SuperVersion* sv);

  // Will execute the import job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  Status Run();

  // Cleanup after successful/failed job
  void Cleanup(const Status& status);

  VersionEdit* edit() { return &edit_; }

  const autovector<IngestedFileInfo>& files_to_import() const {
    return files_to_import_;
  }

 private:
  // Open the external file and populate `file_to_import` with all the
  // external information we need to import this file.
  Status GetIngestedFileInfo(const std::string& external_file,
                             IngestedFileInfo* file_to_import,
                             SuperVersion* sv);

  Env* env_;
  VersionSet* versions_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const EnvOptions& env_options_;
  autovector<IngestedFileInfo> files_to_import_;
  VersionEdit edit_;
  const ImportColumnFamilyOptions& import_options_;
  std::vector<LiveFileMetaData> metadata_;
};

}  // namespace rocksdb
