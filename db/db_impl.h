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
#include <list>
#include <utility>
#include <list>
#include <vector>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "db/wal_manager.h"
#include "memtable_list.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/transaction_log.h"
#include "util/autovector.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"
#include "util/scoped_arena_iterator.h"
#include "db/internal_stats.h"
#include "db/write_controller.h"
#include "db/flush_scheduler.h"
#include "db/write_thread.h"
#include "db/job_context.h"

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

  using DB::CompactFiles;
  virtual Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names,
      const int output_level, const int output_path_id = -1);

  using DB::SetOptions;
  Status SetOptions(ColumnFamilyHandle* column_family,
      const std::unordered_map<std::string, std::string>& options_map);

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

  // Obtains the meta data of the specified column family of the DB.
  // Status::NotFound() will be returned if the current DB does not have
  // any column family match the specified name.
  // TODO(yhchiang): output parameter is placed in the end in this codebase.
  virtual void GetColumnFamilyMetaData(
      ColumnFamilyHandle* column_family,
      ColumnFamilyMetaData* metadata) override;

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
  Iterator* TEST_NewInternalIterator(
      Arena* arena, ColumnFamilyHandle* column_family = nullptr);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes(ColumnFamilyHandle* column_family =
                                                nullptr);

  // Return the current manifest file no.
  uint64_t TEST_Current_Manifest_FileNo();

  // get total level0 file size. Only for testing.
  uint64_t TEST_GetLevel0TotalSize();

  void TEST_GetFilesMetaData(ColumnFamilyHandle* column_family,
                             std::vector<std::vector<FileMetaData>>* metadata);

  void TEST_LockMutex();

  void TEST_UnlockMutex();

  // REQUIRES: mutex locked
  void* TEST_BeginWrite();

  // REQUIRES: mutex locked
  // pass the pointer that you got from TEST_BeginWrite()
  void TEST_EndWrite(void* w);
#endif  // ROCKSDB_LITE

  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'candidate_files'.
  // If force == false and the last call was less than
  // db_options_.delete_obsolete_files_period_micros microseconds ago,
  // it will not fill up the job_context
  void FindObsoleteFiles(JobContext* job_context, bool force,
                         bool no_full_scan = false);

  // Diffs the files listed in filenames and those that do not
  // belong to live files are posibly removed. Also, removes all the
  // files in sst_delete_files and log_delete_files.
  // It is not necessary to hold the mutex when invoking this method.
  void PurgeObsoleteFiles(const JobContext& background_contet);

  ColumnFamilyHandle* DefaultColumnFamily() const;

 protected:
  Env* const env_;
  const std::string dbname_;
  unique_ptr<VersionSet> versions_;
  const DBOptions db_options_;
  Statistics* stats_;

  Iterator* NewInternalIterator(const ReadOptions&, ColumnFamilyData* cfd,
                                SuperVersion* super_version, Arena* arena);

  void NotifyOnFlushCompleted(ColumnFamilyData* cfd, uint64_t file_number);

 private:
  friend class DB;
  friend class InternalStats;
#ifndef ROCKSDB_LITE
  friend class ForwardIterator;
#endif
  friend struct SuperVersion;
  friend class CompactedDBImpl;
  struct CompactionState;

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

  // Background process needs to call
  //     auto x = CaptureCurrentFileNumberInPendingOutputs()
  //     <do something>
  //     ReleaseFileNumberFromPendingOutputs(x)
  // This will protect any temporary files created while <do something> is
  // executing from being deleted.
  // -----------
  // This function will capture current file number and append it to
  // pending_outputs_. This will prevent any background process to delete any
  // file created after this point.
  std::list<uint64_t>::iterator CaptureCurrentFileNumberInPendingOutputs();
  // This function should be called with the result of
  // CaptureCurrentFileNumberInPendingOutputs(). It then marks that any file
  // created between the calls CaptureCurrentFileNumberInPendingOutputs() and
  // ReleaseFileNumberFromPendingOutputs() can now be deleted (if it's not live
  // and blocked by any other pending_outputs_ calls)
  void ReleaseFileNumberFromPendingOutputs(std::list<uint64_t>::iterator v);

  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status FlushMemTableToOutputFile(ColumnFamilyData* cfd,
                                   const MutableCFOptions& mutable_cf_options,
                                   bool* madeProgress, JobContext* job_context,
                                   LogBuffer* log_buffer);

  // REQUIRES: log_numbers are sorted in ascending order
  Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                         SequenceNumber* max_sequence, bool read_only);

  // The following two methods are used to flush a memtable to
  // storage. The first one is used atdatabase RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(ColumnFamilyData* cfd, MemTable* mem,
                                     VersionEdit* edit);
  Status DelayWrite(uint64_t expiration_time);

  Status ScheduleFlushes(WriteContext* context);

  Status SetNewMemtableAndNewLogFile(ColumnFamilyData* cfd,
                                     WriteContext* context);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(ColumnFamilyData* cfd, const FlushOptions& options);

  // Wait for memtable flushed
  Status WaitForFlushMemTable(ColumnFamilyData* cfd);

  void RecordFlushIOStats();
  void RecordCompactionIOStats();

#ifndef ROCKSDB_LITE
  Status CompactFilesImpl(
      const CompactionOptions& compact_options, ColumnFamilyData* cfd,
      Version* version, const std::vector<std::string>& input_file_names,
      const int output_level, int output_path_id);
#endif  // ROCKSDB_LITE

  ColumnFamilyData* GetColumnFamilyDataByName(const std::string& cf_name);

  void MaybeScheduleFlushOrCompaction();
  static void BGWorkCompaction(void* db);
  static void BGWorkFlush(void* db);
  void BackgroundCallCompaction();
  void BackgroundCallFlush();
  Status BackgroundCompaction(bool* madeProgress, JobContext* job_context,
                              LogBuffer* log_buffer);
  Status BackgroundFlush(bool* madeProgress, JobContext* job_context,
                         LogBuffer* log_buffer);

  // This function is called as part of compaction. It enables Flush process to
  // preempt compaction, since it's higher prioirty
  uint64_t CallFlushDuringCompaction(ColumnFamilyData* cfd,
                                     const MutableCFOptions& mutable_cf_options,
                                     JobContext* job_context,
                                     LogBuffer* log_buffer);

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
  int FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd,
      const MutableCFOptions& mutable_cf_options, int level);

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
  std::atomic<bool> shutting_down_;
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

  WriteThread write_thread_;

  WriteBatch tmp_batch_;

  WriteController write_controller_;
  FlushScheduler flush_scheduler_;

  SnapshotList snapshots_;

  // For each background job, pending_outputs_ keeps the current file number at
  // the time that background job started.
  // FindObsoleteFiles()/PurgeObsoleteFiles() never deletes any file that has
  // number bigger than any of the file number in pending_outputs_. Since file
  // numbers grow monotonically, this also means that pending_outputs_ is always
  // sorted. After a background job is done executing, its file number is
  // deleted from pending_outputs_, which allows PurgeObsoleteFiles() to clean
  // it up.
  // State is protected with db mutex.
  std::list<uint64_t> pending_outputs_;

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

  // last time stats were dumped to LOG
  std::atomic<uint64_t> last_stats_dump_time_microsec_;

  bool flush_on_destroy_; // Used when disableWAL is true.

  static const int KEEP_LOG_FILE_NUM = 1000;
  std::string db_absolute_path_;

  // The options to access storage files
  const EnvOptions env_options_;

#ifndef ROCKSDB_LITE
  WalManager wal_manager_;
#endif  // ROCKSDB_LITE

  // A value of true temporarily disables scheduling of background work
  bool bg_work_gate_closed_;

  // Guard against multiple concurrent refitting
  bool refitting_level_;

  // Indicate DB was opened successfully
  bool opened_successfully_;

  // The list of registered event listeners.
  std::list<EventListener*> listeners_;

  // count how many events are currently being notified.
  int notifying_events_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  // Return the earliest snapshot where seqno is visible.
  // Store the snapshot right before that, if any, in prev_snapshot
  inline SequenceNumber findEarliestVisibleSnapshot(
    SequenceNumber in,
    std::vector<SequenceNumber>& snapshots,
    SequenceNumber* prev_snapshot);

  // Background threads call this function, which is just a wrapper around
  // the InstallSuperVersion() function. Background threads carry
  // job_context which can have new_superversion already
  // allocated.
  void InstallSuperVersionBackground(
      ColumnFamilyData* cfd, JobContext* job_context,
      const MutableCFOptions& mutable_cf_options);

  SuperVersion* InstallSuperVersion(
    ColumnFamilyData* cfd, SuperVersion* new_sv,
    const MutableCFOptions& mutable_cf_options);

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

}  // namespace rocksdb
