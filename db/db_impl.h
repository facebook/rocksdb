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

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
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
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Merge(const WriteOptions&, const Slice& key,
                       const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual std::vector<Status> MultiGet(const ReadOptions& options,
                                       const std::vector<Slice>& keys,
                                       std::vector<std::string>* values);

  // Returns false if key doesn't exist in the database and true if it may.
  // If value_found is not passed in as null, then return the value if found in
  // memory. On return, if value was found, then value_found will be set to true
  // , otherwise false.
  virtual bool KeyMayExist(const ReadOptions& options,
                           const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual Status CompactRange(const Slice* begin, const Slice* end,
                              bool reduce_level = false, int target_level = -1);
  virtual int NumberLevels();
  virtual int MaxMemCompactionLevel();
  virtual int Level0StopWriteTrigger();
  virtual const std::string& GetName() const;
  virtual Env* GetEnv() const;
  virtual const Options& GetOptions() const;
  virtual Status Flush(const FlushOptions& options);
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

  virtual void GetLiveFilesMetaData(
    std::vector<LiveFileMetaData> *metadata);

  virtual Status GetDbIdentity(std::string& identity);

  Status RunManualCompaction(int input_level,
                             int output_level,
                             const Slice* begin,
                             const Slice* end);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin, *end]
  Status TEST_CompactRange(int level,
                           const Slice* begin,
                           const Slice* end);

  // Force current memtable contents to be flushed.
  Status TEST_FlushMemTable();

  // Wait for memtable compaction
  Status TEST_WaitForFlushMemTable();

  // Wait for any compaction
  Status TEST_WaitForCompact();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

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

  void TEST_GetFilesMetaData(std::vector<std::vector<FileMetaData>>* metadata);

  // holds references to memtable, all immutable memtables and version
  struct SuperVersion {
    MemTable* mem;
    MemTableListVersion* imm;
    Version* current;
    std::atomic<uint32_t> refs;
    // We need to_delete because during Cleanup(), imm->Unref() returns
    // all memtables that we need to free through this vector. We then
    // delete all those memtables outside of mutex, during destruction
    autovector<MemTable*> to_delete;
    // Version number of the current SuperVersion
    uint64_t version_number;
    DBImpl* db;

    // should be called outside the mutex
    SuperVersion() = default;
    ~SuperVersion();
    SuperVersion* Ref();
    // Returns true if this was the last reference and caller should
    // call Clenaup() and delete the object
    bool Unref();

    // call these two methods with db mutex held
    // Cleanup unrefs mem, imm and current. Also, it stores all memtables
    // that needs to be deleted in to_delete vector. Unrefing those
    // objects needs to be done in the mutex
    void Cleanup();
    void Init(MemTable* new_mem, MemTableListVersion* new_imm,
              Version* new_current);

    // The value of dummy is not actually used. kSVInUse takes its address as a
    // mark in the thread local storage to indicate the SuperVersion is in use
    // by thread. This way, the value of kSVInUse is guaranteed to have no
    // conflict with SuperVersion object address and portable on different
    // platform.
    static int dummy;
    static void* const kSVInUse;
    static void* const kSVObsolete;
  };

  static void SuperVersionUnrefHandle(void* ptr) {
    // UnrefHandle is called when a thread exists or a ThreadLocalPtr gets
    // destroyed. When former happens, the thread shouldn't see kSVInUse.
    // When latter happens, we are in ~DBImpl(), no get should happen as well.
    assert(ptr != SuperVersion::kSVInUse);
    DBImpl::SuperVersion* sv = static_cast<DBImpl::SuperVersion*>(ptr);
    if (sv->Unref()) {
      sv->db->mutex_.Lock();
      sv->Cleanup();
      sv->db->mutex_.Unlock();
      delete sv;
    }
  }

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

    SuperVersion* new_superversion; // if nullptr no new superversion

    // the current manifest_file_number, log_number and prev_log_number
    // that corresponds to the set of files in 'live'.
    uint64_t manifest_file_number, log_number, prev_log_number;

    explicit DeletionState(bool create_superversion = false) {
      manifest_file_number = 0;
      log_number = 0;
      prev_log_number = 0;
      new_superversion =
          create_superversion ? new SuperVersion() : nullptr;
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

 protected:
  Env* const env_;
  const std::string dbname_;
  unique_ptr<VersionSet> versions_;
  const InternalKeyComparator internal_comparator_;
  const Options options_;  // options_.comparator == &internal_comparator_

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  SuperVersion* GetSuperVersion() {
    return super_version_;
  }

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot);

 private:
  friend class DB;
  friend class TailingIterator;
  friend struct SuperVersion;
  struct CompactionState;
  struct Writer;

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.
  Status Recover(bool read_only = false, bool error_if_log_file_exist = false);

  void MaybeIgnoreError(Status* s) const;

  const Status CreateArchivalDirectory();

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status FlushMemTableToOutputFile(bool* madeProgress,
                                   DeletionState& deletion_state,
                                   LogBuffer* log_buffer);

  Status RecoverLogFile(uint64_t log_number, SequenceNumber* max_sequence,
                        bool read_only);

  // The following two methods are used to flush a memtable to
  // storage. The first one is used atdatabase RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(MemTable* mem, VersionEdit* edit);
  Status WriteLevel0Table(autovector<MemTable*>& mems, VersionEdit* edit,
                          uint64_t* filenumber,
                          LogBuffer* log_buffer);

  uint64_t SlowdownAmount(int n, double bottom, double top);
  // MakeRoomForWrite will return superversion_to_free through an arugment,
  // which the caller needs to delete. We do it because caller can delete
  // the superversion outside of mutex
  Status MakeRoomForWrite(bool force /* compact even if there is room? */,
                          SuperVersion** superversion_to_free);
  void BuildBatchGroup(Writer** last_writer,
                       autovector<WriteBatch*>* write_batch_group);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(const FlushOptions& options);

  // Wait for memtable flushed
  Status WaitForFlushMemTable();

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
  Status BackgroundFlush(bool* madeProgress, DeletionState& deletion_state,
                         LogBuffer* log_buffer);
  void CleanupCompaction(CompactionState* compact, Status status);
  Status DoCompactionWork(CompactionState* compact,
                          DeletionState& deletion_state,
                          LogBuffer* log_buffer);

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
  int FindMinimumEmptyLevelFitting(int level);

  // Move the files in the input level to the target level.
  // If target_level < 0, automatically calculate the minimum level that could
  // hold the data set.
  Status ReFitLevel(int level, int target_level = -1);

  // Returns the current SuperVersion number.
  uint64_t CurrentVersionNumber() const;

  // Returns a pair of iterators (mutable-only and immutable-only) used
  // internally by TailingIterator and stores CurrentVersionNumber() in
  // *superversion_number. These iterators are always up-to-date, i.e. can
  // be used to read new data.
  std::pair<Iterator*, Iterator*> GetTailingIteratorPair(
    const ReadOptions& options,
    uint64_t* superversion_number);

  // Constant after construction
  const InternalFilterPolicy internal_filter_policy_;
  bool owns_info_log_;

  // table_cache_ provides its own synchronization
  unique_ptr<TableCache> table_cache_;

  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;          // Signalled when background work finishes
  MemTable* mem_;
  MemTableList imm_;             // Memtable that are not changing
  uint64_t logfile_number_;
  unique_ptr<log::Writer> log_;

  SuperVersion* super_version_;

  // An ordinal representing the current SuperVersion. Updated by
  // InstallSuperVersion(), i.e. incremented every time super_version_
  // changes.
  std::atomic<uint64_t> super_version_number_;
  // Thread's local copy of SuperVersion pointer
  // This needs to be destructed after mutex_
  ThreadLocalPtr* local_sv_;

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

  InternalStats internal_stats_;

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

  // will return a pointer to SuperVersion* if previous SuperVersion
  // if its reference count is zero and needs deletion or nullptr if not
  // As argument takes a pointer to allocated SuperVersion
  // Foreground threads call this function directly (they don't carry
  // deletion state and have to handle their own creation and deletion
  // of SuperVersion)
  SuperVersion* InstallSuperVersion(SuperVersion* new_superversion);
  // Background threads call this function, which is just a wrapper around
  // the InstallSuperVersion() function above. Background threads carry
  // deletion_state which can have new_superversion already allocated.
  void InstallSuperVersion(DeletionState& deletion_state);

  void ResetThreadLocalSuperVersions(DeletionState* deletion_state);

  virtual Status GetPropertiesOfAllTables(TablePropertiesCollection* props)
      override;

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  Status GetImpl(const ReadOptions& options,
                 const Slice& key,
                 std::string* value,
                 bool* value_found = nullptr);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);


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
