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
#include "db/compaction/compaction_outputs.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/memtable_list.h"
#include "db/range_del_aggregator.h"
#include "db/seqno_to_time_mapping.h"
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
class CompactionState;
class ErrorHandler;
class MemTable;
class SnapshotChecker;
class SystemClock;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class SubcompactionState;

// CompactionJob is responsible for executing the compaction. Each (manual or
// automated) compaction corresponds to a CompactionJob object, and usually
// goes through the stages of `Prepare()`->`Run()`->`Install()`. CompactionJob
// will divide the compaction into subcompactions and execute them in parallel
// if needed.
//
// CompactionJob has 2 main stats:
// 1. CompactionJobStats compaction_job_stats_
//    CompactionJobStats is a public data structure which is part of Compaction
//    event listener that rocksdb share the job stats with the user.
//    Internally it's an aggregation of all the compaction_job_stats from each
//    `SubcompactionState`:
//                                           +------------------------+
//                                           | SubcompactionState     |
//                                           |                        |
//                                +--------->|   compaction_job_stats |
//                                |          |                        |
//                                |          +------------------------+
// +------------------------+     |
// | CompactionJob          |     |          +------------------------+
// |                        |     |          | SubcompactionState     |
// |   compaction_job_stats +-----+          |                        |
// |                        |     +--------->|   compaction_job_stats |
// |                        |     |          |                        |
// +------------------------+     |          +------------------------+
//                                |
//                                |          +------------------------+
//                                |          | SubcompactionState     |
//                                |          |                        |
//                                +--------->+   compaction_job_stats |
//                                |          |                        |
//                                |          +------------------------+
//                                |
//                                |          +------------------------+
//                                |          |       ...              |
//                                +--------->+                        |
//                                           +------------------------+
//
// 2. CompactionStatsFull compaction_stats_
//    `CompactionStatsFull` is an internal stats about the compaction, which
//    is eventually sent to `ColumnFamilyData::internal_stats_` and used for
//    logging and public metrics.
//    Internally, it's an aggregation of stats_ from each `SubcompactionState`.
//    It has 2 parts, normal stats about the main compaction information and
//    the penultimate level output stats.
//    `SubcompactionState` maintains the CompactionOutputs for normal output and
//    the penultimate level output if exists, the per_level stats is
//    stored with the outputs.
//                                                +---------------------------+
//                                                | SubcompactionState        |
//                                                |                           |
//                                                | +----------------------+  |
//                                                | | CompactionOutputs    |  |
//                                                | | (normal output)      |  |
//                                            +---->|   stats_             |  |
//                                            |   | +----------------------+  |
//                                            |   |                           |
//                                            |   | +----------------------+  |
// +--------------------------------+         |   | | CompactionOutputs    |  |
// | CompactionJob                  |         |   | | (penultimate_level)  |  |
// |                                |    +--------->|   stats_             |  |
// |   compaction_stats_            |    |    |   | +----------------------+  |
// |    +-------------------------+ |    |    |   |                           |
// |    |stats (normal)           |------|----+   +---------------------------+
// |    +-------------------------+ |    |    |
// |                                |    |    |
// |    +-------------------------+ |    |    |   +---------------------------+
// |    |penultimate_level_stats  +------+    |   | SubcompactionState        |
// |    +-------------------------+ |    |    |   |                           |
// |                                |    |    |   | +----------------------+  |
// |                                |    |    |   | | CompactionOutputs    |  |
// +--------------------------------+    |    |   | | (normal output)      |  |
//                                       |    +---->|   stats_             |  |
//                                       |        | +----------------------+  |
//                                       |        |                           |
//                                       |        | +----------------------+  |
//                                       |        | | CompactionOutputs    |  |
//                                       |        | | (penultimate_level)  |  |
//                                       +--------->|   stats_             |  |
//                                                | +----------------------+  |
//                                                |                           |
//                                                +---------------------------+

class CompactionJob {
 public:
  CompactionJob(
      int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
      const MutableDBOptions& mutable_db_options,
      const FileOptions& file_options, VersionSet* versions,
      const std::atomic<bool>* shutting_down, LogBuffer* log_buffer,
      FSDirectory* db_directory, FSDirectory* output_directory,
      FSDirectory* blob_output_directory, Statistics* stats,
      InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
      std::vector<SequenceNumber> existing_snapshots,
      SequenceNumber earliest_write_conflict_snapshot,
      const SnapshotChecker* snapshot_checker, JobContext* job_context,
      std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
      bool paranoid_file_checks, bool measure_io_stats,
      const std::string& dbname, CompactionJobStats* compaction_job_stats,
      Env::Priority thread_pri, const std::shared_ptr<IOTracer>& io_tracer,
      const std::atomic<bool>& manual_compaction_canceled,
      const std::string& db_id = "", const std::string& db_session_id = "",
      std::string full_history_ts_low = "", std::string trim_ts = "",
      BlobFileCompletionCallback* blob_callback = nullptr,
      int* bg_compaction_scheduled = nullptr,
      int* bg_bottom_compaction_scheduled = nullptr);

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
  void UpdateCompactionStats();
  void LogCompaction();
  virtual void RecordCompactionIOStats();
  void CleanupCompaction();

  // Call compaction filter. Then iterate through input and compact the
  // kv-pairs
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);

  CompactionState* compact_;
  InternalStats::CompactionStatsFull compaction_stats_;
  const ImmutableDBOptions& db_options_;
  const MutableDBOptions mutable_db_options_copy_;
  LogBuffer* log_buffer_;
  FSDirectory* output_directory_;
  Statistics* stats_;
  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level_;

  Env::WriteLifeTimeHint write_hint_;

  IOStatus io_status_;

  CompactionJobStats* compaction_job_stats_;

 private:
  friend class CompactionJobTestBase;

  // Generates a histogram representing potential divisions of key ranges from
  // the input. It adds the starting and/or ending keys of certain input files
  // to the working set and then finds the approximate size of data in between
  // each consecutive pair of slices. Then it divides these ranges into
  // consecutive groups such that each group has a similar size.
  void GenSubcompactionBoundaries();

  // Get the number of planned subcompactions based on max_subcompactions and
  // extra reserved resources
  uint64_t GetSubcompactionsLimit();

  // Additional reserved threads are reserved and the number is stored in
  // extra_num_subcompaction_threads_reserved__. For now, this happens only if
  // the compaction priority is round-robin and max_subcompactions is not
  // sufficient (extra resources may be needed)
  void AcquireSubcompactionResources(int num_extra_required_subcompactions);

  // Additional threads may be reserved during IncreaseSubcompactionResources()
  // if num_actual_subcompactions is less than num_planned_subcompactions.
  // Additional threads will be released and the bg_compaction_scheduled_ or
  // bg_bottom_compaction_scheduled_ will be updated if they are used.
  // DB Mutex lock is required.
  void ShrinkSubcompactionResources(uint64_t num_extra_resources);

  // Release all reserved threads and update the compaction limits.
  void ReleaseSubcompactionResources();

  CompactionServiceJobStatus ProcessKeyValueCompactionWithCompactionService(
      SubcompactionState* sub_compact);

  // update the thread status for starting a compaction.
  void ReportStartedCompaction(Compaction* compaction);

  Status FinishCompactionOutputFile(const Status& input_status,
                                    SubcompactionState* sub_compact,
                                    CompactionOutputs& outputs,
                                    const Slice& next_table_min_key);
  Status InstallCompactionResults(const MutableCFOptions& mutable_cf_options);
  Status OpenCompactionOutputFile(SubcompactionState* sub_compact,
                                  CompactionOutputs& outputs);
  void UpdateCompactionJobStats(
      const InternalStats::CompactionStats& stats) const;
  void RecordDroppedKeys(const CompactionIterationStats& c_iter_stats,
                         CompactionJobStats* compaction_job_stats = nullptr);

  void UpdateCompactionInputStatsHelper(int* num_files, uint64_t* bytes_read,
                                        int input_level);

  void NotifyOnSubcompactionBegin(SubcompactionState* sub_compact);

  void NotifyOnSubcompactionCompleted(SubcompactionState* sub_compact);

  uint32_t job_id_;

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
  const std::atomic<bool>& manual_compaction_canceled_;
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

  JobContext* job_context_;

  std::shared_ptr<Cache> table_cache_;

  EventLogger* event_logger_;

  bool paranoid_file_checks_;
  bool measure_io_stats_;
  // Stores the Slices that designate the boundaries for each subcompaction
  std::vector<std::string> boundaries_;
  Env::Priority thread_pri_;
  std::string full_history_ts_low_;
  std::string trim_ts_;
  BlobFileCompletionCallback* blob_callback_;

  uint64_t GetCompactionId(SubcompactionState* sub_compact) const;
  // Stores the number of reserved threads in shared env_ for the number of
  // extra subcompaction in kRoundRobin compaction priority
  int extra_num_subcompaction_threads_reserved_;

  // Stores the pointer to bg_compaction_scheduled_,
  // bg_bottom_compaction_scheduled_ in DBImpl. Mutex is required when accessing
  // or updating it.
  int* bg_compaction_scheduled_;
  int* bg_bottom_compaction_scheduled_;

  // Stores the sequence number to time mapping gathered from all input files
  // it also collects the smallest_seqno -> oldest_ancester_time from the SST.
  SeqnoToTimeMapping seqno_time_mapping_;

  // Minimal sequence number for preserving the time information. The time info
  // older than this sequence number won't be preserved after the compaction and
  // if it's bottommost compaction, the seq num will be zeroed out.
  SequenceNumber preserve_time_min_seqno_ = kMaxSequenceNumber;

  // Minimal sequence number to preclude the data from the last level. If the
  // key has bigger (newer) sequence number than this, it will be precluded from
  // the last level (output to penultimate level).
  SequenceNumber preclude_last_level_min_seqno_ = kMaxSequenceNumber;

  // Get table file name in where it's outputting to, which should also be in
  // `output_directory_`.
  virtual std::string GetTableFileName(uint64_t file_number);
  // The rate limiter priority (io_priority) is determined dynamically here.
  // The Compaction Read and Write priorities are the same for different
  // scenarios, such as write stalled.
  Env::IOPriority GetRateLimiterPriority();
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

  // db_id is used to generate unique id of sst on the remote compactor
  std::string db_id;

  // information for subcompaction
  bool has_begin = false;
  std::string begin;
  bool has_end = false;
  std::string end;

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
  uint64_t epoch_number;
  uint64_t paranoid_hash;
  bool marked_for_compaction;
  UniqueId64x2 unique_id;

  CompactionServiceOutputFile() = default;
  CompactionServiceOutputFile(
      const std::string& name, SequenceNumber smallest, SequenceNumber largest,
      std::string _smallest_internal_key, std::string _largest_internal_key,
      uint64_t _oldest_ancester_time, uint64_t _file_creation_time,
      uint64_t _epoch_number, uint64_t _paranoid_hash,
      bool _marked_for_compaction, UniqueId64x2 _unique_id)
      : file_name(name),
        smallest_seqno(smallest),
        largest_seqno(largest),
        smallest_internal_key(std::move(_smallest_internal_key)),
        largest_internal_key(std::move(_largest_internal_key)),
        oldest_ancester_time(_oldest_ancester_time),
        file_creation_time(_file_creation_time),
        epoch_number(_epoch_number),
        paranoid_hash(_paranoid_hash),
        marked_for_compaction(_marked_for_compaction),
        unique_id(std::move(_unique_id)) {}
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
      const std::atomic<bool>& manual_compaction_canceled,
      const std::string& db_id, const std::string& db_session_id,
      std::string output_path,
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
