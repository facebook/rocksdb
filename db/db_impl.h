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
#include <limits>
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
#include "util/stop_watch.h"
#include "util/thread_local.h"
#include "db/internal_stats.h"

namespace rocksdb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class CompactionFilterV2;
class Arena;

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
  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, uint64_t* value) override;
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes);
  using DB::CompactRange;
  virtual Status CompactRange(ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end,
                              bool reduce_level = false, int target_level = -1,
                              uint32_t target_path_id = 0);

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

  virtual SequenceNumber GetLatestSequenceNumber() const;

#ifndef ROCKSDB_LITE
  virtual Status DisableFileDeletions();
  virtual Status EnableFileDeletions(bool force);
  virtual int IsFileDeletionsEnabled() const;
  // All the returned filenames start with "/"
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true);
  virtual Status GetSortedWalFiles(VectorLogPtr& files);

  virtual Status GetUpdatesSince(
      SequenceNumber seq_number, unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions&
          read_options = TransactionLogIterator::ReadOptions());
  virtual Status DeleteFile(std::string name);

  virtual void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata);
#endif  // ROCKSDB_LITE

  // checks if all live files exist on file system and that their file sizes
  // match to our in-memory records
  virtual Status CheckConsistency();

  virtual Status GetDbIdentity(std::string& identity);

  Status RunManualCompaction(ColumnFamilyData* cfd, int input_level,
                             int output_level, uint32_t output_path_id,
                             const Slice* begin, const Slice* end);

#ifndef ROCKSDB_LITE
  // Extra methods (for testing) that are not in the public DB interface
  // Implemented in db_impl_debug.cc

  // Compact any files in the named level that overlap [*begin, *end]
  Status TEST_CompactRange(int level, const Slice* begin, const Slice* end,
                           ColumnFamilyHandle* column_family = nullptr);

  // Force current memtable contents to be flushed.
  Status TEST_FlushMemTable(bool wait = true);

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

  Status TEST_ReadFirstRecord(const WalFileType type, const uint64_t number,
                              SequenceNumber* sequence);

  Status TEST_ReadFirstLine(const std::string& fname, SequenceNumber* sequence);
#endif  // NDEBUG

  // Structure to store information for candidate files to delete.
  struct CandidateFileInfo {
    std::string file_name;
    uint32_t path_id;
    CandidateFileInfo(std::string name, uint32_t path)
        : file_name(name), path_id(path) {}
    bool operator==(const CandidateFileInfo& other) const {
      return file_name == other.file_name && path_id == other.path_id;
    }
  };

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
    std::vector<CandidateFileInfo> candidate_files;

    // the list of all live sst files that cannot be deleted
    std::vector<FileDescriptor> sst_live;

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
    uint64_t manifest_file_number, pending_manifest_file_number, log_number,
        prev_log_number;

    explicit DeletionState(bool create_superversion = false) {
      manifest_file_number = 0;
      pending_manifest_file_number = 0;
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
  Statistics* stats_;

  Iterator* NewInternalIterator(const ReadOptions&, ColumnFamilyData* cfd,
                                SuperVersion* super_version,
                                Arena* arena = nullptr);

 private:
  friend class DB;
  friend class InternalStats;
#ifndef ROCKSDB_LITE
  friend class TailingIterator;
  friend class ForwardIterator;
#endif
  friend struct SuperVersion;
  struct CompactionState;
  struct Writer;
  struct WriteContext;

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
                                   DeletionState& deletion_state,
                                   LogBuffer* log_buffer);

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
                          VersionEdit* edit, uint64_t* filenumber,
                          LogBuffer* log_buffer);

  uint64_t SlowdownAmount(int n, double bottom, double top);

  // Before applying write operation (such as DBImpl::Write, DBImpl::Flush)
  // thread should grab the mutex_ and be the first on writers queue.
  // BeginWrite is used for it.
  // Be aware! Writer's job can be done by other thread (see DBImpl::Write
  // for examples), so check it via w.done before applying changes.
  //
  // Writer* w:                writer to be placed in the queue
  // uint64_t expiration_time: maximum time to be in the queue
  // See also: EndWrite
  Status BeginWrite(Writer* w, uint64_t expiration_time);

  // After doing write job, we need to remove already used writers from
  // writers_ queue and notify head of the queue about it.
  // EndWrite is used for this.
  //
  // Writer* w:           Writer, that was added by BeginWrite function
  // Writer* last_writer: Since we can join a few Writers (as DBImpl::Write
  //                      does)
  //                      we should pass last_writer as a parameter to
  //                      EndWrite
  //                      (if you don't touch other writers, just pass w)
  // Status status:       Status of write operation
  // See also: BeginWrite
  void EndWrite(Writer* w, Writer* last_writer, Status status);

  Status MakeRoomForWrite(ColumnFamilyData* cfd,
                          WriteContext* context,
                          uint64_t expiration_time);

  Status SetNewMemtableAndNewLogFile(ColumnFamilyData* cfd,
                                     WriteContext* context);

  void BuildBatchGroup(Writer** last_writer,
                       autovector<WriteBatch*>* write_batch_group);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(ColumnFamilyData* cfd, const FlushOptions& options);

  // Wait for memtable flushed
  Status WaitForFlushMemTable(ColumnFamilyData* cfd);

  void RecordFlushIOStats();
  void RecordCompactionIOStats();

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

  // This function is called as part of compaction. It enables Flush process to
  // preempt compaction, since it's higher prioirty
  // Returns: micros spent executing
  uint64_t CallFlushDuringCompaction(ColumnFamilyData* cfd,
                                     DeletionState& deletion_state,
                                     LogBuffer* log_buffer);

  // Call compaction filter if is_compaction_v2 is not true. Then iterate
  // through input and compact the kv-pairs
  Status ProcessKeyValueCompaction(
    bool is_snapshot_supported,
    SequenceNumber visible_at_tip,
    SequenceNumber earliest_snapshot,
    SequenceNumber latest_snapshot,
    DeletionState& deletion_state,
    bool bottommost_level,
    int64_t& imm_micros,
    Iterator* input,
    CompactionState* compact,
    bool is_compaction_v2,
    LogBuffer* log_buffer);

  // Call compaction_filter_v2->Filter() on kv-pairs in compact
  void CallCompactionFilterV2(CompactionState* compact,
    CompactionFilterV2* compaction_filter_v2);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact,
                                  LogBuffer* log_buffer);
  void AllocateCompactionOutputFileNumbers(CompactionState* compact);
  void ReleaseCompactionUnusedFileNumbers(CompactionState* compact);

#ifdef ROCKSDB_LITE
  void PurgeObsoleteWALFiles() {
    // this function is used for archiving WAL files. we don't need this in
    // ROCKSDB_LITE
  }
#else
  void PurgeObsoleteWALFiles();

  Status GetSortedWalsOfType(const std::string& path,
                             VectorLogPtr& log_files,
                             WalFileType type);

  // Requires: all_logs should be sorted with earliest log file first
  // Retains all log files in all_logs which contain updates with seq no.
  // Greater Than or Equal to the requested SequenceNumber.
  Status RetainProbableWalFiles(VectorLogPtr& all_logs,
                                const SequenceNumber target);

  Status ReadFirstRecord(const WalFileType type, const uint64_t number,
                         SequenceNumber* sequence);

  Status ReadFirstLine(const std::string& fname, SequenceNumber* sequence);
#endif  // ROCKSDB_LITE

  void PrintStatistics();

  // dump rocksdb.stats to LOG
  void MaybeDumpStats();

  // Return true if the current db supports snapshot.  If the current
  // DB does not support snapshot, then calling GetSnapshot() will always
  // return nullptr.
  //
  // @see GetSnapshot()
  virtual bool IsSnapshotSupported() const;

  // Return the minimum empty level that could hold the total data in the
  // input level. Return the input level, if such level could not be found.
  int FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd, int level);

  // Move the files in the input level to the target level.
  // If target_level < 0, automatically calculate the minimum level that could
  // hold the data set.
  Status ReFitLevel(ColumnFamilyData* cfd, int level, int target_level = -1);

  // table_cache_ provides its own synchronization
  std::shared_ptr<Cache> table_cache_;

  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  // This condition variable is signaled on these conditions:
  // * whenever bg_compaction_scheduled_ goes down to 0
  // * if bg_manual_only_ > 0, whenever a compaction finishes, even if it hasn't
  // made any progress
  // * whenever a compaction made any progress
  // * whenever bg_flush_scheduled_ value decreases (i.e. whenever a flush is
  // done, even if it didn't make any progress)
  // * whenever there is an error in background flush or compaction
  port::CondVar bg_cv_;
  uint64_t logfile_number_;
  unique_ptr<log::Writer> log_;
  bool log_empty_;
  ColumnFamilyHandleImpl* default_cf_handle_;
  InternalStats* default_cf_internal_stats_;
  unique_ptr<ColumnFamilyMemTablesImpl> column_family_memtables_;
  struct LogFileNumberSize {
    explicit LogFileNumberSize(uint64_t _number)
        : number(_number), size(0), getting_flushed(false) {}
    void AddSize(uint64_t new_size) { size += new_size; }
    uint64_t number;
    uint64_t size;
    bool getting_flushed;
  };
  std::deque<LogFileNumberSize> alive_log_files_;
  uint64_t total_log_size_;
  // only used for dynamically adjusting max_total_wal_size. it is a sum of
  // [write_buffer_size * max_write_buffer_number] over all column families
  uint64_t max_total_in_memory_state_;
  // If true, we have only one (default) column family. We use this to optimize
  // some code-paths
  bool single_column_family_mode_;

  std::unique_ptr<Directory> db_directory_;

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch tmp_batch_;

  SnapshotList snapshots_;

  // cache for ReadFirstRecord() calls
  std::unordered_map<uint64_t, SequenceNumber> read_first_record_cache_;
  port::Mutex read_first_record_cache_mutex_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  // map from pending file number ID to their path IDs.
  FileNumToPathIdMap pending_outputs_;

  // At least one compaction or flush job is pending but not yet scheduled
  // because of the max background thread limit.
  bool bg_schedule_needed_;

  // count how many background compactions are running or have been scheduled
  int bg_compaction_scheduled_;

  // If non-zero, MaybeScheduleFlushOrCompaction() will only schedule manual
  // compactions (if manual_compaction_ is not null). This mechanism enables
  // manual compactions to wait until all other compactions are finished.
  int bg_manual_only_;

  // number of background memtable flush jobs, submitted to the HIGH pool
  int bg_flush_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    ColumnFamilyData* cfd;
    int input_level;
    int output_level;
    uint32_t output_path_id;
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
  static const uint64_t kNoTimeOut = std::numeric_limits<uint64_t>::max();
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

  // Find Super version and reference it. Based on options, it might return
  // the thread local cached one.
  inline SuperVersion* GetAndRefSuperVersion(ColumnFamilyData* cfd);

  // Un-reference the super version and return it to thread local cache if
  // needed. If it is the last reference of the super version. Clean it up
  // after un-referencing it.
  inline void ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd,
                                           SuperVersion* sv);

#ifndef ROCKSDB_LITE
  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                          TablePropertiesCollection* props)
      override;
#endif  // ROCKSDB_LITE

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, std::string* value,
                 bool* value_found = nullptr);

  bool GetIntPropertyInternal(ColumnFamilyHandle* column_family,
                              DBPropertyType property_type,
                              bool need_out_of_mutex, uint64_t* value);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const Options& src);
extern DBOptions SanitizeOptions(const std::string& db, const DBOptions& src);

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

// Dump db file summary, implemented in util/
extern void DumpDBFileSummary(const DBOptions& options,
                              const std::string& dbname);

}  // namespace rocksdb
