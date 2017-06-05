//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/snapshot_impl.h"
#include "options/db_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_writer.h"
#include "util/autovector.h"

namespace rocksdb {

struct IngestedFileInfo {
  // External file path
  std::string external_file_path;
  // Smallest user key in external file
  std::string smallest_user_key;
  // Largest user key in external file
  std::string largest_user_key;
  // Sequence number for keys in external file
  SequenceNumber original_seqno;
  // Offset of the global sequence number field in the file, will
  // be zero if version is 1 (global seqno is not supported)
  size_t global_seqno_offset;
  // External file size
  uint64_t file_size;
  // total number of keys in external file
  uint64_t num_entries;
  // Id of column family this file shoule be ingested into
  uint32_t cf_id;
  // TableProperties read from external file
  TableProperties table_properties;
  // Version of external file
  int version;

  // FileDescriptor for the file inside the DB
  FileDescriptor fd;
  // file path that we picked for file inside the DB
  std::string internal_file_path = "";
  // Global sequence number that we picked for the file inside the DB
  SequenceNumber assigned_seqno = 0;
  // Level inside the DB we picked for the external file.
  int picked_level = 0;

  InternalKey smallest_internal_key() const {
    return InternalKey(smallest_user_key, assigned_seqno,
                       ValueType::kTypeValue);
  }

  InternalKey largest_internal_key() const {
    return InternalKey(largest_user_key, assigned_seqno, ValueType::kTypeValue);
  }
};

class ExternalSstFileIngestionJob {
 public:
  ExternalSstFileIngestionJob(
      Env* env, VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options, const EnvOptions& env_options,
      SnapshotList* db_snapshots,
      const IngestExternalFileOptions& ingestion_options)
      : env_(env),
        versions_(versions),
        cfd_(cfd),
        db_options_(db_options),
        env_options_(env_options),
        db_snapshots_(db_snapshots),
        ingestion_options_(ingestion_options),
        job_start_time_(env_->NowMicros()) {}

  // Prepare the job by copying external files into the DB.
  Status Prepare(const std::vector<std::string>& external_files_paths);

  // Check if we need to flush the memtable before running the ingestion job
  // This will be true if the files we are ingesting are overlapping with any
  // key range in the memtable.
  // REQUIRES: Mutex held
  Status NeedsFlush(bool* flush_needed);

  // Will execute the ingestion job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  Status Run();

  // Update column family stats.
  // REQUIRES: Mutex held
  void UpdateStats();

  // Cleanup after successful/failed job
  void Cleanup(const Status& status);

  VersionEdit* edit() { return &edit_; }

  const autovector<IngestedFileInfo>& files_to_ingest() const {
    return files_to_ingest_;
  }

 private:
  // Open the external file and populate `file_to_ingest` with all the
  // external information we need to ingest this file.
  Status GetIngestedFileInfo(const std::string& external_file,
                             IngestedFileInfo* file_to_ingest);

  // Check if the files we are ingesting overlap with any memtable.
  // REQUIRES: Mutex held
  Status IngestedFilesOverlapWithMemtables(SuperVersion* sv, bool* overlap);

  // Assign `file_to_ingest` the appropriate sequence number and  the lowest
  // possible level that it can be ingested to according to compaction_style.
  // REQUIRES: Mutex held
  Status AssignLevelAndSeqnoForIngestedFile(SuperVersion* sv,
                                            bool force_global_seqno,
                                            CompactionStyle compaction_style,
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

  // Check if `file_to_ingest` key range overlap with the range `iter` represent
  // REQUIRES: Mutex held
  Status IngestedFileOverlapWithIteratorRange(
      const IngestedFileInfo* file_to_ingest, InternalIterator* iter,
      bool* overlap);

  // Check if `file_to_ingest` key range overlaps with any range deletions
  // specified by `iter`.
  // REQUIRES: Mutex held
  Status IngestedFileOverlapWithRangeDeletions(
      const IngestedFileInfo* file_to_ingest, InternalIterator* range_del_iter,
      bool* overlap);

  // Check if `file_to_ingest` key range overlap with level
  // REQUIRES: Mutex held
  Status IngestedFileOverlapWithLevel(SuperVersion* sv,
    IngestedFileInfo* file_to_ingest, int lvl, bool* overlap_with_level);

  // Check if `file_to_ingest` can fit in level `level`
  // REQUIRES: Mutex held
  bool IngestedFileFitInLevel(const IngestedFileInfo* file_to_ingest,
                              int level);

  Env* env_;
  VersionSet* versions_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const EnvOptions& env_options_;
  SnapshotList* db_snapshots_;
  autovector<IngestedFileInfo> files_to_ingest_;
  const IngestExternalFileOptions& ingestion_options_;
  VersionEdit edit_;
  uint64_t job_start_time_;
};

}  // namespace rocksdb
