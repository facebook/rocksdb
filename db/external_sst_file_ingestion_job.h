//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/internal_stats.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "env/file_system_tracer.h"
#include "logging/event_logger.h"
#include "options/db_options.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_writer.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Directories;
class SystemClock;

struct IngestedFileInfo {
  // External file path
  std::string external_file_path;
  // Smallest internal key in external file
  InternalKey smallest_internal_key;
  // Largest internal key in external file
  InternalKey largest_internal_key;
  // Sequence number for keys in external file
  SequenceNumber original_seqno;
  // Offset of the global sequence number field in the file, will
  // be zero if version is 1 (global seqno is not supported)
  size_t global_seqno_offset;
  // External file size
  uint64_t file_size;
  // total number of keys in external file
  uint64_t num_entries;
  // total number of range deletions in external file
  uint64_t num_range_deletions;
  // Id of column family this file shoule be ingested into
  uint32_t cf_id;
  // TableProperties read from external file
  TableProperties table_properties;
  // Version of external file
  int version;

  // FileDescriptor for the file inside the DB
  FileDescriptor fd;
  // file path that we picked for file inside the DB
  std::string internal_file_path;
  // Global sequence number that we picked for the file inside the DB
  SequenceNumber assigned_seqno = 0;
  // Level inside the DB we picked for the external file.
  int picked_level = 0;
  // Whether to copy or link the external sst file. copy_file will be set to
  // false if ingestion_options.move_files is true and underlying FS
  // supports link operation. Need to provide a default value to make the
  // undefined-behavior sanity check of llvm happy. Since
  // ingestion_options.move_files is false by default, thus copy_file is true
  // by default.
  bool copy_file = true;
  // The checksum of ingested file
  std::string file_checksum;
  // The name of checksum function that generate the checksum
  std::string file_checksum_func_name;
  // The temperature of the file to be ingested
  Temperature file_temperature = Temperature::kUnknown;
  // Unique id of the file to be ingested
  UniqueId64x2 unique_id{};
};

class ExternalSstFileIngestionJob {
 public:
  ExternalSstFileIngestionJob(
      VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options,
      const MutableDBOptions& mutable_db_options, const EnvOptions& env_options,
      SnapshotList* db_snapshots,
      const IngestExternalFileOptions& ingestion_options,
      Directories* directories, EventLogger* event_logger,
      const std::shared_ptr<IOTracer>& io_tracer)
      : clock_(db_options.clock),
        fs_(db_options.fs, io_tracer),
        versions_(versions),
        cfd_(cfd),
        db_options_(db_options),
        mutable_db_options_(mutable_db_options),
        env_options_(env_options),
        db_snapshots_(db_snapshots),
        ingestion_options_(ingestion_options),
        directories_(directories),
        event_logger_(event_logger),
        job_start_time_(clock_->NowMicros()),
        consumed_seqno_count_(0),
        io_tracer_(io_tracer) {
    assert(directories != nullptr);
  }

  ~ExternalSstFileIngestionJob() {
    for (const auto& c : file_ingesting_compactions_) {
      cfd_->compaction_picker()->UnregisterCompaction(c);
      delete c;
    }

    for (const auto& f : compaction_input_metdatas_) {
      delete f;
    }
  }

  // Prepare the job by copying external files into the DB.
  Status Prepare(const std::vector<std::string>& external_files_paths,
                 const std::vector<std::string>& files_checksums,
                 const std::vector<std::string>& files_checksum_func_names,
                 const Temperature& file_temperature, uint64_t next_file_number,
                 SuperVersion* sv);

  // Check if we need to flush the memtable before running the ingestion job
  // This will be true if the files we are ingesting are overlapping with any
  // key range in the memtable.
  //
  // @param super_version A referenced SuperVersion that will be held for the
  //    duration of this function.
  //
  // Thread-safe
  Status NeedsFlush(bool* flush_needed, SuperVersion* super_version);

  // Will execute the ingestion job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  Status Run();

  // Register key range involved in this ingestion job
  // to prevent key range conflict with other ongoing compaction/file ingestion
  // REQUIRES: Mutex held
  void RegisterRange();

  // Unregister key range registered for this ingestion job
  // REQUIRES: Mutex held
  void UnregisterRange();

  // Update column family stats.
  // REQUIRES: Mutex held
  void UpdateStats();

  // Cleanup after successful/failed job
  void Cleanup(const Status& status);

  VersionEdit* edit() { return &edit_; }

  const autovector<IngestedFileInfo>& files_to_ingest() const {
    return files_to_ingest_;
  }

  // How many sequence numbers did we consume as part of the ingest job?
  int ConsumedSequenceNumbersCount() const { return consumed_seqno_count_; }

 private:
  // Open the external file and populate `file_to_ingest` with all the
  // external information we need to ingest this file.
  Status GetIngestedFileInfo(const std::string& external_file,
                             uint64_t new_file_number,
                             IngestedFileInfo* file_to_ingest,
                             SuperVersion* sv);

  // Assign `file_to_ingest` the appropriate sequence number and the lowest
  // possible level that it can be ingested to according to compaction_style.
  // REQUIRES: Mutex held
  Status AssignLevelAndSeqnoForIngestedFile(SuperVersion* sv,
                                            bool force_global_seqno,
                                            CompactionStyle compaction_style,
                                            SequenceNumber last_seqno,
                                            IngestedFileInfo* file_to_ingest,
                                            SequenceNumber* assigned_seqno);

  // File that we want to ingest behind always goes to the lowest level;
  // we just check that it fits in the level, that DB allows ingest_behind,
  // and that we don't have 0 seqnums at the upper levels.
  // REQUIRES: Mutex held
  Status CheckLevelForIngestedBehindFile(IngestedFileInfo* file_to_ingest);

  // Set the file global sequence number to `seqno`
  Status AssignGlobalSeqnoForIngestedFile(IngestedFileInfo* file_to_ingest,
                                          SequenceNumber seqno);
  // Generate the file checksum and store in the IngestedFileInfo
  IOStatus GenerateChecksumForIngestedFile(IngestedFileInfo* file_to_ingest);

  // Check if `file_to_ingest` can fit in level `level`
  // REQUIRES: Mutex held
  bool IngestedFileFitInLevel(const IngestedFileInfo* file_to_ingest,
                              int level);

  // Helper method to sync given file.
  template <typename TWritableFile>
  Status SyncIngestedFile(TWritableFile* file);

  // Create equivalent `Compaction` objects to this file ingestion job
  // , which will be used to check range conflict with other ongoing
  // compactions.
  void CreateEquivalentFileIngestingCompactions();

  SystemClock* clock_;
  FileSystemPtr fs_;
  VersionSet* versions_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const MutableDBOptions& mutable_db_options_;
  const EnvOptions& env_options_;
  SnapshotList* db_snapshots_;
  autovector<IngestedFileInfo> files_to_ingest_;
  const IngestExternalFileOptions& ingestion_options_;
  Directories* directories_;
  EventLogger* event_logger_;
  VersionEdit edit_;
  uint64_t job_start_time_;
  int consumed_seqno_count_;
  // Set in ExternalSstFileIngestionJob::Prepare(), if true all files are
  // ingested in L0
  bool files_overlap_{false};
  // Set in ExternalSstFileIngestionJob::Prepare(), if true and DB
  // file_checksum_gen_factory is set, DB will generate checksum each file.
  bool need_generate_file_checksum_{true};
  std::shared_ptr<IOTracer> io_tracer_;

  // Below are variables used in (un)registering range for this ingestion job
  //
  // FileMetaData used in inputs of compactions equivalent to this ingestion
  // job
  std::vector<FileMetaData*> compaction_input_metdatas_;
  // Compactions equivalent to this ingestion job
  std::vector<Compaction*> file_ingesting_compactions_;
};

}  // namespace ROCKSDB_NAMESPACE
