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
// 1. CompactionJobStats job_stats_
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
// |   job_stats            +-----+          |                        |
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
// 2. CompactionStatsFull internal_stats_
//    `CompactionStatsFull` is an internal stats about the compaction, which
//    is eventually sent to `ColumnFamilyData::internal_stats_` and used for
//    logging and public metrics.
//    Internally, it's an aggregation of stats_ from each `SubcompactionState`.
//    It has 2 parts, ordinary output level stats and the proximal level output
//    stats.
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
// | CompactionJob                  |         |   | | (proximal_level)     |  |
// |                                |    +--------->|   stats_             |  |
// |   internal_stats_              |    |    |   | +----------------------+  |
// |    +-------------------------+ |    |    |   |                           |
// |    |output_level_stats       |------|----+   +---------------------------+
// |    +-------------------------+ |    |    |
// |                                |    |    |
// |    +-------------------------+ |    |    |   +---------------------------+
// |    |proximal_level_stats     |------+    |   | SubcompactionState        |
// |    +-------------------------+ |    |    |   |                           |
// |                                |    |    |   | +----------------------+  |
// |                                |    |    |   | | CompactionOutputs    |  |
// +--------------------------------+    |    |   | | (normal output)      |  |
//                                       |    +---->|   stats_             |  |
//                                       |        | +----------------------+  |
//                                       |        |                           |
//                                       |        | +----------------------+  |
//                                       |        | | CompactionOutputs    |  |
//                                       |        | | (proximal_level)     |  |
//                                       +--------->|   stats_             |  |
//                                                | +----------------------+  |
//                                                |                           |
//                                                +---------------------------+

class CompactionJob {
 public:
  CompactionJob(int job_id, Compaction* compaction,
                const ImmutableDBOptions& db_options,
                const MutableDBOptions& mutable_db_options,
                const FileOptions& file_options, VersionSet* versions,
                const std::atomic<bool>* shutting_down, LogBuffer* log_buffer,
                FSDirectory* db_directory, FSDirectory* output_directory,
                FSDirectory* blob_output_directory, Statistics* stats,
                InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
                JobContext* job_context, std::shared_ptr<Cache> table_cache,
                EventLogger* event_logger, bool paranoid_file_checks,
                bool measure_io_stats, const std::string& dbname,
                CompactionJobStats* compaction_job_stats,
                Env::Priority thread_pri,
                const std::shared_ptr<IOTracer>& io_tracer,
                const std::atomic<bool>& manual_compaction_canceled,
                const std::string& db_id = "",
                const std::string& db_session_id = "",
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
  // and organizing seqno <-> time info. `known_single_subcompact` is non-null
  // if we already have a known single subcompaction, with optional key bounds
  // (currently for executing a remote compaction).
  //
  // @param compaction_progress Previously saved compaction progress
  //   to resume from. If empty, compaction starts fresh from the
  //   beginning.
  //
  // @param compaction_progress_writer Writer for persisting
  //   subcompaction progress periodically during compaction
  //   execution. If nullptr, progress tracking is disabled and compaction
  //   cannot be resumed later.
  void Prepare(
      std::optional<std::pair<std::optional<Slice>, std::optional<Slice>>>
          known_single_subcompact,
      const CompactionProgress& compaction_progress = CompactionProgress{},
      log::Writer* compaction_progress_writer = nullptr);

  // REQUIRED mutex not held
  // Launch threads for each subcompaction and wait for them to finish. After
  // that, verify table is usable and finally do bookkeeping to unify
  // subcompaction results
  Status Run();

  // REQUIRED: mutex held
  // Add compaction input/output to the current version
  // Releases compaction file through Compaction::ReleaseCompactionFiles().
  // Sets *compaction_released to true if compaction is released.
  Status Install(bool* compaction_released);

  // Return the IO status
  IOStatus io_status() const { return io_status_; }

 protected:
  void UpdateCompactionJobOutputStatsFromInternalStats(
      const Status& status,
      const InternalStats::CompactionStatsFull& internal_stats) const;

  void LogCompaction();
  virtual void RecordCompactionIOStats();
  void CleanupCompaction();

  // Iterate through input and compact the kv-pairs.
  void ProcessKeyValueCompaction(SubcompactionState* sub_compact);

  CompactionState* compact_;
  InternalStats::CompactionStatsFull internal_stats_;
  const ImmutableDBOptions& db_options_;
  const MutableDBOptions mutable_db_options_copy_;
  LogBuffer* log_buffer_;
  FSDirectory* output_directory_;
  Statistics* stats_;
  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level_;

  Env::WriteLifeTimeHint write_hint_;

  IOStatus io_status_;

  CompactionJobStats* job_stats_;

 private:
  friend class CompactionJobTestBase;

  // Collect the following stats from input files and table properties
  // - num_input_files_in_non_output_levels
  // - num_input_files_in_output_level
  // - bytes_read_non_output_levels
  // - bytes_read_output_level
  // - num_input_records
  // - bytes_read_blob
  // - num_dropped_records
  // and set them in internal_stats_.output_level_stats
  //
  // @param num_input_range_del if non-null, will be set to the number of range
  // deletion entries in this compaction input.
  //
  // Returns true iff internal_stats_.output_level_stats.num_input_records and
  // num_input_range_del are calculated successfully.
  //
  // This should be called only once for compactions (not per subcompaction)
  bool UpdateInternalStatsFromInputFiles(
      uint64_t* num_input_range_del = nullptr);

  void UpdateCompactionJobInputStatsFromInternalStats(
      const InternalStats::CompactionStatsFull& internal_stats,
      uint64_t num_input_range_del) const;

  Status VerifyInputRecordCount(uint64_t num_input_range_del) const;
  Status VerifyOutputRecordCount() const;

  // Generates a histogram representing potential divisions of key ranges from
  // the input. It adds the starting and/or ending keys of certain input files
  // to the working set and then finds the approximate size of data in between
  // each consecutive pair of slices. Then it divides these ranges into
  // consecutive groups such that each group has a similar size.
  void GenSubcompactionBoundaries();

  void MaybeAssignCompactionProgressAndWriter(
      const CompactionProgress& compaction_progress,
      log::Writer* compaction_progress_writer);

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

  void InitializeCompactionRun();
  void RunSubcompactions();
  void UpdateTimingStats(uint64_t start_micros);
  void RemoveEmptyOutputs();
  bool HasNewBlobFiles() const;
  Status CollectSubcompactionErrors();
  Status SyncOutputDirectories();
  Status VerifyOutputFiles();
  void SetOutputTableProperties();
  // Aggregates subcompaction output stats to internal stat, and aggregates
  // subcompaction's compaction job stats to the whole entire surrounding
  // compaction job stats.
  void AggregateSubcompactionOutputAndJobStats();
  Status VerifyCompactionRecordCounts(bool stats_built_from_input_table_prop,
                                      uint64_t num_input_range_del);
  void FinalizeCompactionRun(const Status& status,
                             bool stats_built_from_input_table_prop,
                             uint64_t num_input_range_del);

  CompactionServiceJobStatus ProcessKeyValueCompactionWithCompactionService(
      SubcompactionState* sub_compact);

  struct CompactionIOStatsSnapshot {
    PerfLevel prev_perf_level = PerfLevel::kEnableTime;
    uint64_t prev_write_nanos = 0;
    uint64_t prev_fsync_nanos = 0;
    uint64_t prev_range_sync_nanos = 0;
    uint64_t prev_prepare_write_nanos = 0;
    uint64_t prev_cpu_write_nanos = 0;
    uint64_t prev_cpu_read_nanos = 0;
  };

  struct SubcompactionKeyBoundaries {
    const std::optional<const Slice> start;
    const std::optional<const Slice> end;

    // Boundaries without timestamps for read options
    std::optional<Slice> start_without_ts;
    std::optional<Slice> end_without_ts;

    // Timestamp management
    static constexpr char kMaxTs[] =
        "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";
    std::string max_ts;
    Slice ts_slice;

    // Internal key boundaries
    IterKey start_ikey;
    IterKey end_ikey;
    Slice start_internal_key;
    Slice end_internal_key;

    // User key boundaries
    Slice start_user_key;
    Slice end_user_key;

    SubcompactionKeyBoundaries(std::optional<const Slice> start_boundary,
                               std::optional<const Slice> end_boundary)
        : start(start_boundary), end(end_boundary) {}
  };

  struct SubcompactionInternalIterators {
    std::unique_ptr<InternalIterator> raw_input;
    std::unique_ptr<InternalIterator> clip;
    std::unique_ptr<InternalIterator> blob_counter;
    std::unique_ptr<InternalIterator> trim_history_iter;
  };

  struct BlobFileResources {
    std::vector<std::string> blob_file_paths;
    std::unique_ptr<BlobFileBuilder> blob_file_builder;
  };

  bool ShouldUseLocalCompaction(SubcompactionState* sub_compact);
  CompactionIOStatsSnapshot InitializeIOStats();
  Status SetupAndValidateCompactionFilter(
      SubcompactionState* sub_compact,
      const CompactionFilter* configured_compaction_filter,
      const CompactionFilter*& compaction_filter,
      std::unique_ptr<CompactionFilter>& compaction_filter_from_factory);
  void InitializeReadOptionsAndBoundaries(
      size_t ts_sz, ReadOptions& read_options,
      SubcompactionKeyBoundaries& boundaries);
  InternalIterator* CreateInputIterator(
      SubcompactionState* sub_compact, ColumnFamilyData* cfd,
      SubcompactionInternalIterators& iterators,
      SubcompactionKeyBoundaries& boundaries, ReadOptions& read_options);
  void CreateBlobFileBuilder(SubcompactionState* sub_compact,
                             ColumnFamilyData* cfd,
                             BlobFileResources& blob_resources,
                             const WriteOptions& write_options);
  std::unique_ptr<CompactionIterator> CreateCompactionIterator(
      SubcompactionState* sub_compact, ColumnFamilyData* cfd,
      InternalIterator* input_iter, const CompactionFilter* compaction_filter,
      MergeHelper& merge, BlobFileResources& blob_resources,
      const WriteOptions& write_options);
  std::pair<CompactionFileOpenFunc, CompactionFileCloseFunc> CreateFileHandlers(
      SubcompactionState* sub_compact, SubcompactionKeyBoundaries& boundaries);
  Status ProcessKeyValue(SubcompactionState* sub_compact, ColumnFamilyData* cfd,
                         CompactionIterator* c_iter,
                         const CompactionFileOpenFunc& open_file_func,
                         const CompactionFileCloseFunc& close_file_func,
                         uint64_t& prev_cpu_micros);
  void UpdateSubcompactionJobStatsIncrementally(
      CompactionIterator* c_iter, CompactionJobStats* compaction_job_stats,
      uint64_t cur_cpu_micros, uint64_t& prev_cpu_micros);
  void FinalizeSubcompactionJobStats(SubcompactionState* sub_compact,
                                     CompactionIterator* c_iter,
                                     uint64_t start_cpu_micros,
                                     uint64_t prev_cpu_micros,
                                     const CompactionIOStatsSnapshot& io_stats);
  Status FinalizeProcessKeyValueStatus(ColumnFamilyData* cfd,
                                       InternalIterator* input_iter,
                                       CompactionIterator* c_iter,
                                       Status status);
  Status CleanupCompactionFiles(SubcompactionState* sub_compact, Status status,
                                const CompactionFileOpenFunc& open_file_func,
                                const CompactionFileCloseFunc& close_file_func);
  Status FinalizeBlobFiles(SubcompactionState* sub_compact,
                           BlobFileBuilder* blob_file_builder, Status status);
  void FinalizeSubcompaction(SubcompactionState* sub_compact, Status status,
                             const CompactionFileOpenFunc& open_file_func,
                             const CompactionFileCloseFunc& close_file_func,
                             BlobFileBuilder* blob_file_builder,
                             CompactionIterator* c_iter,
                             InternalIterator* input_iter,
                             uint64_t start_cpu_micros,
                             uint64_t prev_cpu_micros,
                             const CompactionIOStatsSnapshot& io_stats);

  // update the thread status for starting a compaction.
  void ReportStartedCompaction(Compaction* compaction);

  Status FinishCompactionOutputFile(
      const Status& input_status,
      const ParsedInternalKey& prev_table_last_internal_key,
      const Slice& next_table_min_key, const Slice* comp_start_user_key,
      const Slice* comp_end_user_key, const CompactionIterator* c_iter,
      SubcompactionState* sub_compact, CompactionOutputs& outputs);
  Status InstallCompactionResults(bool* compaction_released);
  Status OpenCompactionOutputFile(SubcompactionState* sub_compact,
                                  CompactionOutputs& outputs);

  void RecordDroppedKeys(const CompactionIterationStats& c_iter_stats,
                         CompactionJobStats* compaction_job_stats = nullptr);

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

  SequenceNumber earliest_snapshot_;
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
  SeqnoToTimeMapping seqno_to_time_mapping_;

  // Max seqno that can be zeroed out in last level, including for preserving
  // write times.
  SequenceNumber preserve_seqno_after_ = kMaxSequenceNumber;

  // Minimal sequence number to preclude the data from the last level. If the
  // key has bigger (newer) sequence number than this, it will be precluded from
  // the last level (output to proximal level).
  SequenceNumber proximal_after_seqno_ = kMaxSequenceNumber;

  // Options File Number used for Remote Compaction
  // Setting this requires DBMutex.
  uint64_t options_file_number_ = 0;

  // Writer for persisting compaction progress during compaction
  log::Writer* compaction_progress_writer_ = nullptr;

  // Get table file name in where it's outputting to, which should also be in
  // `output_directory_`.
  virtual std::string GetTableFileName(uint64_t file_number);
  // The rate limiter priority (io_priority) is determined dynamically here.
  // The Compaction Read and Write priorities are the same for different
  // scenarios, such as write stalled.
  Env::IOPriority GetRateLimiterPriority();

  Status MaybeResumeSubcompactionProgressOnInputIterator(
      SubcompactionState* sub_compact, InternalIterator* input_iter);

  Status ReadOutputFilesTableProperties(
      const autovector<FileMetaData>& temporary_output_file_allocation,
      const ReadOptions& read_options,
      std::vector<std::shared_ptr<const TableProperties>>&
          output_files_table_properties,
      bool is_proximal_level = false);

  Status ReadTablePropertiesDirectly(
      const ImmutableOptions& ioptions, const MutableCFOptions& moptions,
      const FileMetaData* file_meta, const ReadOptions& read_options,
      std::shared_ptr<const TableProperties>* tp);

  void RestoreCompactionOutputs(
      const ColumnFamilyData* cfd,
      const std::vector<std::shared_ptr<const TableProperties>>&
          output_files_table_properties,
      SubcompactionProgressPerLevel& subcompaction_progress_per_level,
      CompactionOutputs* outputs_to_restore);

  bool ShouldUpdateSubcompactionProgress(
      const SubcompactionState* sub_compact, const CompactionIterator* c_iter,
      const ParsedInternalKey& prev_table_last_internal_key,
      const Slice& next_table_min_internal_key, const FileMetaData* meta) const;

  void UpdateSubcompactionProgress(const CompactionIterator* c_iter,
                                   const Slice next_table_min_key,
                                   SubcompactionState* sub_compact);

  Status PersistSubcompactionProgress(SubcompactionState* sub_compact);

  void UpdateSubcompactionProgressPerLevel(
      SubcompactionState* sub_compact, bool is_proximal_level,
      SubcompactionProgress& subcompaction_progress);
};

// CompactionServiceInput is used the pass compaction information between two
// db instances. It contains the information needed to do a compaction. It
// doesn't contain the LSM tree information, which is passed though MANIFEST
// file.
struct CompactionServiceInput {
  std::string cf_name;

  std::vector<SequenceNumber> snapshots;

  // SST files for compaction, it should already be expended to include all the
  // files needed for this compaction, for both input level files and output
  // level files.
  std::vector<std::string> input_files;
  int output_level = 0;

  // db_id is used to generate unique id of sst on the remote compactor
  std::string db_id;

  // information for subcompaction
  bool has_begin = false;
  std::string begin;
  bool has_end = false;
  std::string end;

  uint64_t options_file_number = 0;

  // serialization interface to read and write the object
  static Status Read(const std::string& data_str, CompactionServiceInput* obj);
  Status Write(std::string* output);

#ifndef NDEBUG
  bool TEST_Equals(CompactionServiceInput* other);
  bool TEST_Equals(CompactionServiceInput* other, std::string* mismatch);
#endif  // NDEBUG
};

// CompactionServiceOutputFile is the metadata for the output SST file
struct CompactionServiceOutputFile {
  std::string file_name;
  uint64_t file_size{};
  SequenceNumber smallest_seqno{};
  SequenceNumber largest_seqno{};
  std::string smallest_internal_key;
  std::string largest_internal_key;
  uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;
  uint64_t file_creation_time = kUnknownFileCreationTime;
  uint64_t epoch_number = kUnknownEpochNumber;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
  uint64_t paranoid_hash{};
  bool marked_for_compaction;
  UniqueId64x2 unique_id{};
  TableProperties table_properties;
  bool is_proximal_level_output;
  Temperature file_temperature = Temperature::kUnknown;

  CompactionServiceOutputFile() = default;
  CompactionServiceOutputFile(
      const std::string& name, uint64_t size, SequenceNumber smallest,
      SequenceNumber largest, std::string _smallest_internal_key,
      std::string _largest_internal_key, uint64_t _oldest_ancester_time,
      uint64_t _file_creation_time, uint64_t _epoch_number,
      const std::string& _file_checksum,
      const std::string& _file_checksum_func_name, uint64_t _paranoid_hash,
      bool _marked_for_compaction, UniqueId64x2 _unique_id,
      const TableProperties& _table_properties, bool _is_proximal_level_output,
      Temperature _file_temperature)
      : file_name(name),
        file_size(size),
        smallest_seqno(smallest),
        largest_seqno(largest),
        smallest_internal_key(std::move(_smallest_internal_key)),
        largest_internal_key(std::move(_largest_internal_key)),
        oldest_ancester_time(_oldest_ancester_time),
        file_creation_time(_file_creation_time),
        epoch_number(_epoch_number),
        file_checksum(_file_checksum),
        file_checksum_func_name(_file_checksum_func_name),
        paranoid_hash(_paranoid_hash),
        marked_for_compaction(_marked_for_compaction),
        unique_id(std::move(_unique_id)),
        table_properties(_table_properties),
        is_proximal_level_output(_is_proximal_level_output),
        file_temperature(_file_temperature) {}
};

// CompactionServiceResult contains the compaction result from a different db
// instance, with these information, the primary db instance with write
// permission is able to install the result to the DB.
struct CompactionServiceResult {
  Status status;
  std::vector<CompactionServiceOutputFile> output_files;
  int output_level = 0;

  // location of the output files
  std::string output_path;

  uint64_t bytes_read = 0;
  uint64_t bytes_written = 0;

  // Job-level Compaction Stats.
  //
  // NOTE: Job level stats cannot be rebuilt from scratch by simply aggregating
  // per-level stats due to some fields populated directly during compaction
  // (e.g. RecordDroppedKeys()). This is why we need both job-level stats and
  // per-level in the serialized result. If rebuilding job-level stats from
  // per-level stats become possible in the future, consider deprecating this
  // field.
  CompactionJobStats stats;

  // Per-level Compaction Stats for both output_level_stats and
  // proximal_level_stats
  InternalStats::CompactionStatsFull internal_stats;

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
      JobContext* job_context, std::shared_ptr<Cache> table_cache,
      EventLogger* event_logger, const std::string& dbname,
      const std::shared_ptr<IOTracer>& io_tracer,
      const std::atomic<bool>& manual_compaction_canceled,
      const std::string& db_id, const std::string& db_session_id,
      std::string output_path,
      const CompactionServiceInput& compaction_service_input,
      CompactionServiceResult* compaction_service_result);

  // REQUIRED: mutex held
  // Like CompactionJob::Prepare()
  void Prepare(
      const CompactionProgress& compaction_progress = CompactionProgress{},
      log::Writer* compaction_progress_writer = nullptr);

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
