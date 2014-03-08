//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <set>
#include <utility>
#include <vector>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "memtable_list.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/transaction_log.h"
#include "util/autovector.h"
#include "util/stats_logger.h"
#include "util/thread_local.h"
#include "db/internal_stats.h"

namespace rocksdb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const DBOptions& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  using DB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value);
  using DB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value);
  using DB::Delete;
  virtual Status Delete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family, const Slice& key);
  using DB::Write;
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value);
  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  virtual Status CreateColumnFamily(const ColumnFamilyOptions& options,
                                    const std::string& column_family,
                                    ColumnFamilyHandle** handle);
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family);

  // Returns false if key doesn't exist in the database and true if it may.
  // If value_found is not passed in as null, then return the value if found in
  // memory. On return, if value was found, then value_found will be set to true
  // , otherwise false.
  using DB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value, bool* value_found = nullptr);
  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family);
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const Slice& property, std::string* value);
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes);
  using DB::CompactRange;
  virtual Status CompactRange(ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end,
                              bool reduce_level = false, int target_level = -1);

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family);
  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(ColumnFamilyHandle* column_family);
  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(ColumnFamilyHandle* column_family);
  virtual const std::string& GetName() const;
  virtual Env* GetEnv() const;
  using DB::GetOptions;
  virtual const Options& GetOptions(ColumnFamilyHandle* column_family) const;
  using DB::Flush;
  virtual Status Flush(const FlushOptions& options,
                       ColumnFamilyHandle* column_family);
  virtual Status DisableFileDeletions();
  virtual Status EnableFileDeletions(bool force);
  // All the returned filenames start with "/"
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true);
  virtual Status GetSortedWalFiles(VectorLogPtr& files);
  virtual SequenceNumber GetLatestSequenceNumber() const;
  virtual Status GetUpdatesSince(
      SequenceNumber seq_number, unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions&
          read_options = TransactionLogIterator::ReadOptions());
  virtual Status DeleteFile(std::string name);

  virtual void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata);

  virtual Status GetDbIdentity(std::string& identity);

  Status RunManualCompaction(ColumnFamilyData* cfd, int input_level,
                             int output_level, const Slice* begin,
                             const Slice* end);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin, *end]
  Status TEST_CompactRange(int level, const Slice* begin, const Slice* end,
                           ColumnFamilyHandle* column_family = nullptr);

  // Force current memtable contents to be flushed.
  Status TEST_FlushMemTable();

  // Wait for memtable compaction
  Status TEST_WaitForFlushMemTable(ColumnFamilyHandle* column_family = nullptr);

  // Wait for any compaction
  Status TEST_WaitForCompact();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator(ColumnFamilyHandle* column_family =
                                         nullptr);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes(ColumnFamilyHandle* column_family =
                                                nullptr);

  // Simulate a db crash, no elegant closing of database.
  void TEST_Destroy_DBImpl();

  // Return the current manifest file no.
  uint64_t TEST_Current_Manifest_FileNo();

  // Trigger's a background call for testing.
  void TEST_PurgeObsoleteteWAL();

  // get total level0 file size. Only for testing.
  uint64_t TEST_GetLevel0TotalSize();

  void TEST_SetDefaultTimeToCheck(uint64_t default_interval_to_delete_obsolete_WAL)
  {
    default_interval_to_delete_obsolete_WAL_ = default_interval_to_delete_obsolete_WAL;
  }

  void TEST_GetFilesMetaData(ColumnFamilyHandle* column_family,
                             std::vector<std::vector<FileMetaData>>* metadata);

  // needed for CleanupIteratorState
  struct DeletionState {
    inline bool HaveSomethingToDelete() const {
      return  candidate_files.size() ||
        sst_delete_files.size() ||
        log_delete_files.size();
    }

    // a list of all files that we'll consider deleting
    // (every once in a while this is filled up with all files
    // in the DB directory)
    std::vector<std::string> candidate_files;

    // the list of all live sst files that cannot be deleted
    std::vector<uint64_t> sst_live;

    // a list of sst files that we need to delete
    std::vector<FileMetaData*> sst_delete_files;

    // a list of log files that we need to delete
    std::vector<uint64_t> log_delete_files;

    // a list of memtables to be free
    autovector<MemTable*> memtables_to_free;

    autovector<SuperVersion*> superversions_to_free;

    SuperVersion* new_superversion;  // if nullptr no new superversion

    // the current manifest_file_number, log_number and prev_log_number
    // that corresponds to the set of files in 'live'.
    uint64_t manifest_file_number, log_number, prev_log_number;

    explicit DeletionState(bool create_superversion = false) {
      manifest_file_number = 0;
      log_number = 0;
      prev_log_number = 0;
      new_superversion = create_superversion ? new SuperVersion() : nullptr;
    }

    ~DeletionState() {
      // free pending memtables
      for (auto m : memtables_to_free) {
        delete m;
      }
      // free superversions
      for (auto s : superversions_to_free) {
        delete s;
      }
      // if new_superversion was not used, it will be non-nullptr and needs
      // to be freed here
      delete new_superversion;
    }
  };

  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'candidate_files'.
  // If force == false and the last call was less than
  // options_.delete_obsolete_files_period_micros microseconds ago,
  // it will not fill up the deletion_state
  void FindObsoleteFiles(DeletionState& deletion_state,
                         bool force,
                         bool no_full_scan = false);

  // Diffs the files listed in filenames and those that do not
  // belong to live files are posibly removed. Also, removes all the
  // files in sst_delete_files and log_delete_files.
  // It is not necessary to hold the mutex when invoking this method.
  void PurgeObsoleteFiles(DeletionState& deletion_state);

  ColumnFamilyHandle* DefaultColumnFamily() const;

 protected:
  Env* const env_;
  const std::string dbname_;
  unique_ptr<VersionSet> versions_;
  const DBOptions options_;

  Iterator* NewInternalIterator(const ReadOptions&, ColumnFamilyData* cfd,
                                SuperVersion* super_version);

 private:
  friend class DB;
  friend class TailingIterator;
  struct CompactionState;
  struct Writer;

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only = false, bool error_if_log_file_exist = false);

  void MaybeIgnoreError(Status* s) const;

  const Status CreateArchivalDirectory();

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status FlushMemTableToOutputFile(ColumnFamilyData* cfd, bool* madeProgress,
                                   DeletionState& deletion_state);

  Status RecoverLogFile(uint64_t log_number, SequenceNumber* max_sequence,
                        bool read_only);

  // The following two methods are used to flush a memtable to
  // storage. The first one is used atdatabase RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(ColumnFamilyData* cfd, MemTable* mem,
                                     VersionEdit* edit);
  Status WriteLevel0Table(ColumnFamilyData* cfd, autovector<MemTable*>& mems,
                          VersionEdit* edit, uint64_t* filenumber);

  uint64_t SlowdownAmount(int n, double bottom, double top);
  Status MakeRoomForWrite(ColumnFamilyData* cfd,
                          bool force /* flush even if there is room? */);
  void BuildBatchGroup(Writer** last_writer,
                       autovector<WriteBatch*>* write_batch_group);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(ColumnFamilyData* cfd, const FlushOptions& options);

  // Wait for memtable flushed
  Status WaitForFlushMemTable(ColumnFamilyData* cfd);

  void MaybeScheduleLogDBDeployStats();
  static void BGLogDBDeployStats(void* db);
  void LogDBDeployStats();

  void MaybeScheduleFlushOrCompaction();
  static void BGWorkCompaction(void* db);
  static void BGWorkFlush(void* db);
  void BackgroundCallCompaction();
  void BackgroundCallFlush();
  Status BackgroundCompaction(bool* madeProgress, DeletionState& deletion_state,
                              LogBuffer* log_buffer);
  Status BackgroundFlush(bool* madeProgress, DeletionState& deletion_state);
  void CleanupCompaction(CompactionState* compact, Status status);
  Status DoCompactionWork(CompactionState* compact,
                          DeletionState& deletion_state);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact);
  void AllocateCompactionOutputFileNumbers(CompactionState* compact);
  void ReleaseCompactionUnusedFileNumbers(CompactionState* compact);

  void PurgeObsoleteWALFiles();

  Status AppendSortedWalsOfType(const std::string& path,
                                VectorLogPtr& log_files,
                                WalFileType type);

  // Requires: all_logs should be sorted with earliest log file first
  // Retains all log files in all_logs which contain updates with seq no.
  // Greater Than or Equal to the requested SequenceNumber.
  Status RetainProbableWalFiles(VectorLogPtr& all_logs,
                                const SequenceNumber target);
  //  return true if
  bool CheckWalFileExistsAndEmpty(const WalFileType type,
                                  const uint64_t number);

  Status ReadFirstRecord(const WalFileType type, const uint64_t number,
                         WriteBatch* const result);

  Status ReadFirstLine(const std::string& fname, WriteBatch* const batch);

  void PrintStatistics();

  // dump rocksdb.stats to LOG
  void MaybeDumpStats();

  // Return the minimum empty level that could hold the total data in the
  // input level. Return the input level, if such level could not be found.
  int FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd, int level);

  // Move the files in the input level to the target level.
  // If target_level < 0, automatically calculate the minimum level that could
  // hold the data set.
  Status ReFitLevel(ColumnFamilyData* cfd, int level, int target_level = -1);

  // Returns a pair of iterators (mutable-only and immutable-only) used
  // internally by TailingIterator and stores cfd->GetSuperVersionNumber() in
  // *superversion_number. These iterators are always up-to-date, i.e. can
  // be used to read new data.
  std::pair<Iterator*, Iterator*> GetTailingIteratorPair(
      const ReadOptions& options, ColumnFamilyData* cfd,
      uint64_t* superversion_number);

  // table_cache_ provides its own synchronization
  std::shared_ptr<Cache> table_cache_;

  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  uint64_t logfile_number_;
  unique_ptr<log::Writer> log_;
  ColumnFamilyHandleImpl* default_cf_handle_;
  unique_ptr<ColumnFamilyMemTablesImpl> column_family_memtables_;
  std::deque<uint64_t> alive_log_files_;

  std::string host_name_;

  std::unique_ptr<Directory> db_directory_;

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // count how many background compactions are running or have been scheduled
  int bg_compaction_scheduled_;

  // If non-zero, MaybeScheduleFlushOrCompaction() will only schedule manual
  // compactions (if manual_compaction_ is not null). This mechanism enables
  // manual compactions to wait until all other compactions are finished.
  int bg_manual_only_;

  // number of background memtable flush jobs, submitted to the HIGH pool
  int bg_flush_scheduled_;

  // Has a background stats log thread scheduled?
  bool bg_logstats_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    ColumnFamilyData* cfd;
    int input_level;
    int output_level;
    bool done;
    Status status;
    bool in_progress;           // compaction request being processed?
    const InternalKey* begin;   // nullptr means beginning of key range
    const InternalKey* end;     // nullptr means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  std::unique_ptr<StatsLogger> logger_;

  int64_t volatile last_log_ts;

  // shall we disable deletion of obsolete files
  // if 0 the deletion is enabled.
  // if non-zero, files will not be getting deleted
  // This enables two different threads to call
  // EnableFileDeletions() and DisableFileDeletions()
  // without any synchronization
  int disable_delete_obsolete_files_;

  // last time when DeleteObsoleteFiles was invoked
  uint64_t delete_obsolete_files_last_run_;

  // last time when PurgeObsoleteWALFiles ran.
  uint64_t purge_wal_files_last_run_;

  // last time stats were dumped to LOG
  std::atomic<uint64_t> last_stats_dump_time_microsec_;

  // obsolete files will be deleted every this seconds if ttl deletion is
  // enabled and archive size_limit is disabled.
  uint64_t default_interval_to_delete_obsolete_WAL_;

  bool flush_on_destroy_; // Used when disableWAL is true.

  static const int KEEP_LOG_FILE_NUM = 1000;
  std::string db_absolute_path_;

  // count of the number of contiguous delaying writes
  int delayed_writes_;

  // The options to access storage files
  const EnvOptions storage_options_;

  // A value of true temporarily disables scheduling of background work
  bool bg_work_gate_closed_;

  // Guard against multiple concurrent refitting
  bool refitting_level_;

  // Indicate DB was opened successfully
  bool opened_successfully_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  // dump the delayed_writes_ to the log file and reset counter.
  void DelayLoggingAndReset();

  // Return the earliest snapshot where seqno is visible.
  // Store the snapshot right before that, if any, in prev_snapshot
  inline SequenceNumber findEarliestVisibleSnapshot(
    SequenceNumber in,
    std::vector<SequenceNumber>& snapshots,
    SequenceNumber* prev_snapshot);

  // Background threads call this function, which is just a wrapper around
  // the cfd->InstallSuperVersion() function. Background threads carry
  // deletion_state which can have new_superversion already allocated.
  void InstallSuperVersion(ColumnFamilyData* cfd,
                           DeletionState& deletion_state);

  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                          TablePropertiesCollection* props)
      override;

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, std::string* value,
                 bool* value_found = nullptr);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);
extern DBOptions SanitizeOptions(const std::string& db, const DBOptions& src);

// Determine compression type, based on user options, level of the output
// file and whether compression is disabled.
// If enable_compression is false, then compression is always disabled no
// matter what the values of the other two parameters are.
// Otherwise, the compression type is determined based on options and level.
CompressionType GetCompressionType(const Options& options, int level,
                                   const bool enable_compression);

// Determine compression type for L0 file written by memtable flush.
CompressionType GetCompressionFlush(const Options& options);

}  // namespace rocksdb
