//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_completion_callback.h"
#include "db/column_family.h"
#include "db/compaction/compaction_iterator.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/memtable_list.h"
#include "db/range_del_aggregator.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "logging/event_logger.h"
#include "options/cf_options.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/transaction_log.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class ErrorHandler;
class MemTable;
class SnapshotChecker;
class SystemClock;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

// CompactionJob is responsible for executing the compaction. Each (manual or
// automated) compaction corresponds to a CompactionJob object, and usually
// goes through the stages of `Prepare()`->`Run()`->`Install()`. CompactionJob
// will divide the compaction into subcompactions and execute them in parallel
// if needed.
class CompactionJob {
 public:
  CompactionJob(
      int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
      const MutableDBOptions& mutable_db_options,
      const FileOptions& file_options, VersionSet* versions,
      const std::atomic<bool>* shutting_down,
      const SequenceNumber preserve_deletes_seqnum, LogBuffer* log_buffer,
      FSDirectory* db_directory, FSDirectory* output_directory,
      FSDirectory* blob_output_directory, Statistics* stats,
      InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
      std::vector<SequenceNumber> existing_snapshots,
      SequenceNumber earliest_write_conflict_snapshot,
      const SnapshotChecker* snapshot_checker,
      std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
      bool paranoid_file_checks, bool measure_io_stats,
      const std::string& dbname, CompactionJobStats* compaction_job_stats,
      Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
      const std::atomic<int>* manual_compaction_paused = nullptr,
      const std::atomic<bool>* manual_compaction_canceled = nullptr,
      const std::string& db_id = "", const std::string& db_session_id = "",
      std::string full_history_ts_low = "",
      BlobFileCompletionCallback* blob_callback = nullptr);

  virtual ~CompactionJob();

  // no copy/move
  CompactionJob(CompactionJob&& job) = delete;
  CompactionJob(const CompactionJob& job) = delete;
  CompactionJob& operator=(const CompactionJob& job) = delete;

  // REQUIRED: mutex held
  // Prepare for the compaction by setting up boundaries for each subcompaction
  void Prepare();
  // REQUIRED mutex not held
  // Launch threads for each subcompaction and wait for them to finish. After
  // that, verify table is usable and finally do bookkeeping to unify
  // subcompaction results
  Status Run();

  // REQUIRED: mutex held
  // Add compaction input/output to the current version
  Status Install(const MutableCFOptions& mutable_cf_options);

  // Return the IO status
  IOStatus io_status() const { return io_status_; }

 protected:
  struct SubcompactionState;
  // CompactionJob state
  struct CompactionState;

  void AggregateStatistics();
  void UpdateCompactionStats();
  void LogCompaction();
  virtual void RecordCompactionIOStats();
  void CleanupCompaction();

  // Call compaction filter. Then iterate through input and compact the
  // kv-pairs
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);

  CompactionState* compact_;
  InternalStats::CompactionStats compaction_stats_;
  const ImmutableDBOptions& db_options_;
  const MutableDBOptions mutable_db_options_copy_;
  LogBuffer* log_buffer_;
  FSDirectory* output_directory_;
  Statistics* stats_;
  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level_;

  Env::WriteLifeTimeHint write_hint_;

  IOStatus io_status_;

 private:
  // Generates a histogram representing potential divisions of key ranges from
  // the input. It adds the starting and/or ending keys of certain input files
  // to the working set and then finds the approximate size of data in between
  // each consecutive pair of slices. Then it divides these ranges into
  // consecutive groups such that each group has a similar size.
  void GenSubcompactionBoundaries();

  CompactionServiceJobStatus ProcessKeyValueCompactionWithCompactionService(
      SubcompactionState* sub_compact);

  // update the thread status for starting a compaction.
  void ReportStartedCompaction(Compaction* compaction);
  void AllocateCompactionOutputFileNumbers();

  Status FinishCompactionOutputFile(
      const Status& input_status, SubcompactionState* sub_compact,
      CompactionRangeDelAggregator* range_del_agg,
      CompactionIterationStats* range_del_out_stats,
      const Slice* next_table_min_key = nullptr);
  Status InstallCompactionResults(const MutableCFOptions& mutable_cf_options);
  Status OpenCompactionOutputFile(SubcompactionState* sub_compact);
  void UpdateCompactionJobStats(
    const InternalStats::CompactionStats& stats) const;
  void RecordDroppedKeys(const CompactionIterationStats& c_iter_stats,
                         CompactionJobStats* compaction_job_stats = nullptr);

  void UpdateCompactionInputStatsHelper(
      int* num_files, uint64_t* bytes_read, int input_level);

#ifndef ROCKSDB_LITE
  void BuildSubcompactionJobInfo(
      SubcompactionState* sub_compact,
      SubcompactionJobInfo* subcompaction_job_info) const;
#endif  // ROCKSDB_LITE

  void NotifyOnSubcompactionBegin(SubcompactionState* sub_compact);

  void NotifyOnSubcompactionCompleted(SubcompactionState* sub_compact);

  uint32_t job_id_;

  CompactionJobStats* compaction_job_stats_;

  // DBImpl state
  const std::string& dbname_;
  const std::string db_id_;
  const std::string db_session_id_;
  const FileOptions file_options_;

  Env* env_;
  std::shared_ptr<IOTracer> io_tracer_;
  FileSystemPtr fs_;
  // env_option optimized for compaction table reads
  FileOptions file_options_for_read_;
  VersionSet* versions_;
  const std::atomic<bool>* shutting_down_;
  const std::atomic<int>* manual_compaction_paused_;
  const std::atomic<bool>* manual_compaction_canceled_;
  const SequenceNumber preserve_deletes_seqnum_;
  FSDirectory* db_directory_;
  FSDirectory* blob_output_directory_;
  InstrumentedMutex* db_mutex_;
  ErrorHandler* db_error_handler_;
  // If there were two snapshots with seq numbers s1 and
  // s2 and s1 < s2, and if we find two instances of a key k1 then lies
  // entirely within s1 and s2, then the earlier version of k1 can be safely
  // deleted because that version is not visible in any snapshot.
  std::vector<SequenceNumber> existing_snapshots_;

  // This is the earliest snapshot that could be used for write-conflict
  // checking by a transaction.  For any user-key newer than this snapshot, we
  // should make sure not to remove evidence that a write occurred.
  SequenceNumber earliest_write_conflict_snapshot_;

  const SnapshotChecker* const snapshot_checker_;

  std::shared_ptr<Cache> table_cache_;

  EventLogger* event_logger_;

  bool paranoid_file_checks_;
  bool measure_io_stats_;
  // Stores the Slices that designate the boundaries for each subcompaction
  std::vector<Slice> boundaries_;
  // Stores the approx size of keys covered in the range of each subcompaction
  std::vector<uint64_t> sizes_;
  Env::Priority thread_pri_;
  std::string full_history_ts_low_;
  BlobFileCompletionCallback* blob_callback_;

  uint64_t GetCompactionId(SubcompactionState* sub_compact);

  // Get table file name in where it's outputting to, which should also be in
  // `output_directory_`.
  virtual std::string GetTableFileName(uint64_t file_number);
};

// CompactionServiceInput is used the pass compaction information between two
// db instances. It contains the information needed to do a compaction. It
// doesn't contain the LSM tree information, which is passed though MANIFEST
// file.
struct CompactionServiceInput {
  ColumnFamilyDescriptor column_family;

  DBOptions db_options;

  std::vector<SequenceNumber> snapshots;

  // SST files for compaction, it should already be expended to include all the
  // files needed for this compaction, for both input level files and output
  // level files.
  std::vector<std::string> input_files;
  int output_level;

  // information for subcompaction
  bool has_begin = false;
  std::string begin;
  bool has_end = false;
  std::string end;
  uint64_t approx_size = 0;

  // serialization interface to read and write the object
  static Status Read(const std::string& data_str, CompactionServiceInput* obj);
  Status Write(std::string* output);

  // Initialize a dummy ColumnFamilyDescriptor
  CompactionServiceInput() : column_family("", ColumnFamilyOptions()) {}

#ifndef NDEBUG
  bool TEST_Equals(CompactionServiceInput* other);
  bool TEST_Equals(CompactionServiceInput* other, std::string* mismatch);
#endif  // NDEBUG
};

// CompactionServiceOutputFile is the metadata for the output SST file
struct CompactionServiceOutputFile {
  std::string file_name;
  SequenceNumber smallest_seqno;
  SequenceNumber largest_seqno;
  std::string smallest_internal_key;
  std::string largest_internal_key;
  uint64_t oldest_ancester_time;
  uint64_t file_creation_time;
  uint64_t paranoid_hash;
  bool marked_for_compaction;

  CompactionServiceOutputFile() = default;
  CompactionServiceOutputFile(
      const std::string& name, SequenceNumber smallest, SequenceNumber largest,
      std::string _smallest_internal_key, std::string _largest_internal_key,
      uint64_t _oldest_ancester_time, uint64_t _file_creation_time,
      uint64_t _paranoid_hash, bool _marked_for_compaction)
      : file_name(name),
        smallest_seqno(smallest),
        largest_seqno(largest),
        smallest_internal_key(std::move(_smallest_internal_key)),
        largest_internal_key(std::move(_largest_internal_key)),
        oldest_ancester_time(_oldest_ancester_time),
        file_creation_time(_file_creation_time),
        paranoid_hash(_paranoid_hash),
        marked_for_compaction(_marked_for_compaction) {}
};

// CompactionServiceResult contains the compaction result from a different db
// instance, with these information, the primary db instance with write
// permission is able to install the result to the DB.
struct CompactionServiceResult {
  Status status;
  std::vector<CompactionServiceOutputFile> output_files;
  int output_level;

  // location of the output files
  std::string output_path;

  // some statistics about the compaction
  uint64_t num_output_records = 0;
  uint64_t total_bytes = 0;
  uint64_t bytes_read = 0;
  uint64_t bytes_written = 0;
  CompactionJobStats stats;

  // serialization interface to read and write the object
  static Status Read(const std::string& data_str, CompactionServiceResult* obj);
  Status Write(std::string* output);

#ifndef NDEBUG
  bool TEST_Equals(CompactionServiceResult* other);
  bool TEST_Equals(CompactionServiceResult* other, std::string* mismatch);
#endif  // NDEBUG
};

// CompactionServiceCompactionJob is an read-only compaction job, it takes
// input information from `compaction_service_input` and put result information
// in `compaction_service_result`, the SST files are generated to `output_path`.
class CompactionServiceCompactionJob : private CompactionJob {
 public:
  CompactionServiceCompactionJob(
      int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
      const MutableDBOptions& mutable_db_options,
      const FileOptions& file_options, VersionSet* versions,
      const std::atomic<bool>* shutting_down, LogBuffer* log_buffer,
      FSDirectory* output_directory, Statistics* stats,
      InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
      std::vector<SequenceNumber> existing_snapshots,
      std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
      const std::string& dbname, const std::shared_ptr<IOTracer>& io_tracer,
      const std::string& db_id, const std::string& db_session_id,
      const std::string& output_path,
      const CompactionServiceInput& compaction_service_input,
      CompactionServiceResult* compaction_service_result);

  // Run the compaction in current thread and return the result
  Status Run();

  void CleanupCompaction();

  IOStatus io_status() const { return CompactionJob::io_status(); }

 protected:
  void RecordCompactionIOStats() override;

 private:
  // Get table file name in output_path
  std::string GetTableFileName(uint64_t file_number) override;
  // Specific the compaction output path, otherwise it uses default DB path
  const std::string output_path_;

  // Compaction job input
  const CompactionServiceInput& compaction_input_;

  // Compaction job result
  CompactionServiceResult* compaction_result_;
};

}  // namespace ROCKSDB_NAMESPACE
