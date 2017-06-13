//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/compaction_job.h"
#include "db/dbformat.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/flush_job.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/log_writer.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/wal_manager.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "memtable_list.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/event_logger.h"
#include "util/hash.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace rocksdb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class Arena;
class WriteCallback;
struct JobContext;
struct ExternalSstFileInfo;
struct MemTableInfo;

class DBImpl : public DB {
 public:
  DBImpl(const DBOptions& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  using DB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) override;
  using DB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;
  using DB::Delete;
  virtual Status Delete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override;
  using DB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key) override;
  using DB::Write;
  virtual Status Write(const WriteOptions& options,
                       WriteBatch* updates) override;

  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;
  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  virtual Status CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                    const std::string& column_family,
                                    ColumnFamilyHandle** handle) override;
  virtual Status CreateColumnFamilies(
      const ColumnFamilyOptions& cf_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles) override;
  virtual Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles) override;
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family) override;
  virtual Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  // Returns false if key doesn't exist in the database and true if it may.
  // If value_found is not passed in as null, then return the value if found in
  // memory. On return, if value was found, then value_found will be set to true
  // , otherwise false.
  using DB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override;
  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override;
  virtual const Snapshot* GetSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;
  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const Slice& property, std::string* value) override;
  using DB::GetMapProperty;
  virtual bool GetMapProperty(ColumnFamilyHandle* column_family,
                              const Slice& property,
                              std::map<std::string, double>* value) override;
  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, uint64_t* value) override;
  using DB::GetAggregatedIntProperty;
  virtual bool GetAggregatedIntProperty(const Slice& property,
                                        uint64_t* aggregated_value) override;
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes,
                                   uint8_t include_flags
                                   = INCLUDE_FILES) override;
  using DB::GetApproximateMemTableStats;
  virtual void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                           const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) override;
  using DB::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end) override;

  using DB::CompactFiles;
  virtual Status CompactFiles(const CompactionOptions& compact_options,
                              ColumnFamilyHandle* column_family,
                              const std::vector<std::string>& input_file_names,
                              const int output_level,
                              const int output_path_id = -1) override;

  virtual Status PauseBackgroundWork() override;
  virtual Status ContinueBackgroundWork() override;

  virtual Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;

  using DB::SetOptions;
  Status SetOptions(
      ColumnFamilyHandle* column_family,
      const std::unordered_map<std::string, std::string>& options_map) override;

  virtual Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& options_map) override;

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family) override;
  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(ColumnFamilyHandle* column_family) override;
  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(
      ColumnFamilyHandle* column_family) override;
  virtual const std::string& GetName() const override;
  virtual Env* GetEnv() const override;
  using DB::GetOptions;
  virtual Options GetOptions(ColumnFamilyHandle* column_family) const override;
  using DB::GetDBOptions;
  virtual DBOptions GetDBOptions() const override;
  using DB::Flush;
  virtual Status Flush(const FlushOptions& options,
                       ColumnFamilyHandle* column_family) override;
  virtual Status SyncWAL() override;

  virtual SequenceNumber GetLatestSequenceNumber() const override;

  bool HasActiveSnapshotLaterThanSN(SequenceNumber sn);

#ifndef ROCKSDB_LITE
  using DB::ResetStats;
  virtual Status ResetStats() override;
  virtual Status DisableFileDeletions() override;
  virtual Status EnableFileDeletions(bool force) override;
  virtual int IsFileDeletionsEnabled() const;
  // All the returned filenames start with "/"
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;
  virtual Status GetSortedWalFiles(VectorLogPtr& files) override;

  virtual Status GetUpdatesSince(
      SequenceNumber seq_number, unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions&
          read_options = TransactionLogIterator::ReadOptions()) override;
  virtual Status DeleteFile(std::string name) override;
  Status DeleteFilesInRange(ColumnFamilyHandle* column_family,
                            const Slice* begin, const Slice* end);

  virtual void GetLiveFilesMetaData(
      std::vector<LiveFileMetaData>* metadata) override;

  // Obtains the meta data of the specified column family of the DB.
  // Status::NotFound() will be returned if the current DB does not have
  // any column family match the specified name.
  // TODO(yhchiang): output parameter is placed in the end in this codebase.
  virtual void GetColumnFamilyMetaData(
      ColumnFamilyHandle* column_family,
      ColumnFamilyMetaData* metadata) override;

  // experimental API
  Status SuggestCompactRange(ColumnFamilyHandle* column_family,
                             const Slice* begin, const Slice* end);

  Status PromoteL0(ColumnFamilyHandle* column_family, int target_level);

  // Similar to Write() but will call the callback once on the single write
  // thread to determine whether it is safe to perform the write.
  virtual Status WriteWithCallback(const WriteOptions& write_options,
                                   WriteBatch* my_batch,
                                   WriteCallback* callback);

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into the current
  // memtables. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  //
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  SequenceNumber GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                   bool include_history);

  // For a given key, check to see if there are any records for this key
  // in the memtables, including memtable history.  If cache_only is false,
  // SST files will also be checked.
  //
  // If a key is found, *found_record_for_key will be set to true and
  // *seq will be set to the stored sequence number for the latest
  // operation on this key or kMaxSequenceNumber if unknown.
  // If no key is found, *found_record_for_key will be set to false.
  //
  // Note: If cache_only=false, it is possible for *seq to be set to 0 if
  // the sequence number has been cleared from the record.  If the caller is
  // holding an active db snapshot, we know the missing sequence must be less
  // than the snapshot's sequence number (sequence numbers are only cleared
  // when there are no earlier active snapshots).
  //
  // If NotFound is returned and found_record_for_key is set to false, then no
  // record for this key was found.  If the caller is holding an active db
  // snapshot, we know that no key could have existing after this snapshot
  // (since we do not compact keys that have an earlier snapshot).
  //
  // Returns OK or NotFound on success,
  // other status on unexpected error.
  // TODO(andrewkr): this API need to be aware of range deletion operations
  Status GetLatestSequenceForKey(SuperVersion* sv, const Slice& key,
                                 bool cache_only, SequenceNumber* seq,
                                 bool* found_record_for_key);

  using DB::IngestExternalFile;
  virtual Status IngestExternalFile(
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& external_files,
      const IngestExternalFileOptions& ingestion_options) override;

#endif  // ROCKSDB_LITE

  // Similar to GetSnapshot(), but also lets the db know that this snapshot
  // will be used for transaction write-conflict checking.  The DB can then
  // make sure not to compact any keys that would prevent a write-conflict from
  // being detected.
  const Snapshot* GetSnapshotForWriteConflictBoundary();

  // checks if all live files exist on file system and that their file sizes
  // match to our in-memory records
  virtual Status CheckConsistency();

  virtual Status GetDbIdentity(std::string& identity) const override;

  Status RunManualCompaction(ColumnFamilyData* cfd, int input_level,
                             int output_level, uint32_t output_path_id,
                             const Slice* begin, const Slice* end,
                             bool exclusive,
                             bool disallow_trivial_move = false);

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  InternalIterator* NewInternalIterator(
      Arena* arena, RangeDelAggregator* range_del_agg,
      ColumnFamilyHandle* column_family = nullptr);

#ifndef NDEBUG
  // Extra methods (for testing) that are not in the public DB interface
  // Implemented in db_impl_debug.cc

  // Compact any files in the named level that overlap [*begin, *end]
  Status TEST_CompactRange(int level, const Slice* begin, const Slice* end,
                           ColumnFamilyHandle* column_family = nullptr,
                           bool disallow_trivial_move = false);

  void TEST_HandleWALFull();

  bool TEST_UnableToFlushOldestLog() {
    return unable_to_flush_oldest_log_;
  }

  bool TEST_IsLogGettingFlushed() {
    return alive_log_files_.begin()->getting_flushed;
  }

  // Force current memtable contents to be flushed.
  Status TEST_FlushMemTable(bool wait = true,
                            ColumnFamilyHandle* cfh = nullptr);

  // Wait for memtable compaction
  Status TEST_WaitForFlushMemTable(ColumnFamilyHandle* column_family = nullptr);

  // Wait for any compaction
  Status TEST_WaitForCompact();

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

  uint64_t TEST_MaxTotalInMemoryState() const {
    return max_total_in_memory_state_;
  }

  size_t TEST_LogsToFreeSize();

  uint64_t TEST_LogfileNumber();

  uint64_t TEST_total_log_size() const { return total_log_size_; }

  // Returns column family name to ImmutableCFOptions map.
  Status TEST_GetAllImmutableCFOptions(
      std::unordered_map<std::string, const ImmutableCFOptions*>* iopts_map);

  // Return the lastest MutableCFOptions of a column family
  Status TEST_GetLatestMutableCFOptions(ColumnFamilyHandle* column_family,
                                        MutableCFOptions* mutable_cf_options);

  Cache* TEST_table_cache() { return table_cache_.get(); }

  WriteController& TEST_write_controler() { return write_controller_; }

  uint64_t TEST_FindMinLogContainingOutstandingPrep();
  uint64_t TEST_FindMinPrepLogReferencedByMemTable();

  int TEST_BGCompactionsAllowed() const;
  int TEST_BGFlushesAllowed() const;

#endif  // NDEBUG

  struct BGJobLimits {
    int max_flushes;
    int max_compactions;
  };
  // Returns maximum background flushes and compactions allowed to be scheduled
  BGJobLimits GetBGJobLimits() const;
  // Need a static version that can be called during SanitizeOptions().
  static BGJobLimits GetBGJobLimits(int max_background_flushes,
                                    int max_background_compactions,
                                    int max_background_jobs,
                                    bool parallelize_compactions);

  // move logs pending closing from job_context to the DB queue and
  // schedule a purge
  void ScheduleBgLogWriterClose(JobContext* job_context);

  uint64_t MinLogNumberToKeep();

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
  void PurgeObsoleteFiles(const JobContext& background_contet,
                          bool schedule_only = false);

  void SchedulePurge();

  ColumnFamilyHandle* DefaultColumnFamily() const override;

  const SnapshotList& snapshots() const { return snapshots_; }

  const ImmutableDBOptions& immutable_db_options() const {
    return immutable_db_options_;
  }

  void CancelAllBackgroundWork(bool wait);

  // Find Super version and reference it. Based on options, it might return
  // the thread local cached one.
  // Call ReturnAndCleanupSuperVersion() when it is no longer needed.
  SuperVersion* GetAndRefSuperVersion(ColumnFamilyData* cfd);

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.
  SuperVersion* GetAndRefSuperVersion(uint32_t column_family_id);

  // Un-reference the super version and return it to thread local cache if
  // needed. If it is the last reference of the super version. Clean it up
  // after un-referencing it.
  void ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd, SuperVersion* sv);

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread.
  void ReturnAndCleanupSuperVersion(uint32_t colun_family_id, SuperVersion* sv);

  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.  Return value only valid until next call to this function or
  // mutex is released.
  ColumnFamilyHandle* GetColumnFamilyHandle(uint32_t column_family_id);

  // Same as above, should called without mutex held and not on write thread.
  ColumnFamilyHandle* GetColumnFamilyHandleUnlocked(uint32_t column_family_id);

  // Returns the number of currently running flushes.
  // REQUIREMENT: mutex_ must be held when calling this function.
  int num_running_flushes() {
    mutex_.AssertHeld();
    return num_running_flushes_;
  }

  // Returns the number of currently running compactions.
  // REQUIREMENT: mutex_ must be held when calling this function.
  int num_running_compactions() {
    mutex_.AssertHeld();
    return num_running_compactions_;
  }

  const WriteController& write_controller() { return write_controller_; }

  // hollow transactions shell used for recovery.
  // these will then be passed to TransactionDB so that
  // locks can be reacquired before writing can resume.
  struct RecoveredTransaction {
    uint64_t log_number_;
    std::string name_;
    WriteBatch* batch_;
    explicit RecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch)
        : log_number_(log), name_(name), batch_(batch) {}

    ~RecoveredTransaction() { delete batch_; }
  };

  bool allow_2pc() const { return immutable_db_options_.allow_2pc; }

  std::unordered_map<std::string, RecoveredTransaction*>
  recovered_transactions() {
    return recovered_transactions_;
  }

  RecoveredTransaction* GetRecoveredTransaction(const std::string& name) {
    auto it = recovered_transactions_.find(name);
    if (it == recovered_transactions_.end()) {
      return nullptr;
    } else {
      return it->second;
    }
  }

  void InsertRecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch) {
    recovered_transactions_[name] = new RecoveredTransaction(log, name, batch);
    MarkLogAsContainingPrepSection(log);
  }

  void DeleteRecoveredTransaction(const std::string& name) {
    auto it = recovered_transactions_.find(name);
    assert(it != recovered_transactions_.end());
    auto* trx = it->second;
    recovered_transactions_.erase(it);
    MarkLogAsHavingPrepSectionFlushed(trx->log_number_);
    delete trx;
  }

  void DeleteAllRecoveredTransactions() {
    for (auto it = recovered_transactions_.begin();
         it != recovered_transactions_.end(); it++) {
      delete it->second;
    }
    recovered_transactions_.clear();
  }

  void MarkLogAsHavingPrepSectionFlushed(uint64_t log);
  void MarkLogAsContainingPrepSection(uint64_t log);
  void AddToLogsToFreeQueue(log::Writer* log_writer) {
    logs_to_free_queue_.push_back(log_writer);
  }

  Status NewDB();

 protected:
  Env* const env_;
  const std::string dbname_;
  unique_ptr<VersionSet> versions_;
  const DBOptions initial_db_options_;
  const ImmutableDBOptions immutable_db_options_;
  MutableDBOptions mutable_db_options_;
  Statistics* stats_;
  std::unordered_map<std::string, RecoveredTransaction*>
      recovered_transactions_;

  InternalIterator* NewInternalIterator(const ReadOptions&,
                                        ColumnFamilyData* cfd,
                                        SuperVersion* super_version,
                                        Arena* arena,
                                        RangeDelAggregator* range_del_agg);

  // Except in DB::Open(), WriteOptionsFile can only be called when:
  // Persist options to options file.
  // If need_mutex_lock = false, the method will lock DB mutex.
  // If need_enter_write_thread = false, the method will enter write thread.
  Status WriteOptionsFile(bool need_mutex_lock, bool need_enter_write_thread);

  // The following two functions can only be called when:
  // 1. WriteThread::Writer::EnterUnbatched() is used.
  // 2. db_mutex is NOT held
  Status RenameTempFileToOptionsFile(const std::string& file_name);
  Status DeleteObsoleteOptionsFiles();

  void NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                          const MutableCFOptions& mutable_cf_options,
                          int job_id, TableProperties prop);

  void NotifyOnFlushCompleted(ColumnFamilyData* cfd, FileMetaData* file_meta,
                              const MutableCFOptions& mutable_cf_options,
                              int job_id, TableProperties prop);

  void NotifyOnCompactionCompleted(ColumnFamilyData* cfd,
                                   Compaction *c, const Status &st,
                                   const CompactionJobStats& job_stats,
                                   int job_id);
  void NotifyOnMemTableSealed(ColumnFamilyData* cfd,
                              const MemTableInfo& mem_table_info);

#ifndef ROCKSDB_LITE
  void NotifyOnExternalFileIngested(
      ColumnFamilyData* cfd, const ExternalSstFileIngestionJob& ingestion_job);
#endif  // !ROCKSDB_LITE

  void NewThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusDbInfo() const;

  Status WriteImpl(const WriteOptions& options, WriteBatch* updates,
                   WriteCallback* callback = nullptr,
                   uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                   bool disable_memtable = false);

  Status PipelinedWriteImpl(const WriteOptions& options, WriteBatch* updates,
                            WriteCallback* callback = nullptr,
                            uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                            bool disable_memtable = false);

  uint64_t FindMinLogContainingOutstandingPrep();
  uint64_t FindMinPrepLogReferencedByMemTable();

 private:
  friend class DB;
  friend class InternalStats;
  friend class TransactionImpl;
#ifndef ROCKSDB_LITE
  friend class ForwardIterator;
#endif
  friend struct SuperVersion;
  friend class CompactedDBImpl;
#ifndef NDEBUG
  friend class XFTransactionWriteHandler;
#endif
  struct CompactionState;

  struct WriteContext {
    autovector<SuperVersion*> superversions_to_free_;
    autovector<MemTable*> memtables_to_free_;

    ~WriteContext() {
      for (auto& sv : superversions_to_free_) {
        delete sv;
      }
      for (auto& m : memtables_to_free_) {
        delete m;
      }
    }
  };

  struct PurgeFileInfo;

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only = false, bool error_if_log_file_exist = false,
                 bool error_if_data_exists_in_logs = false);

  void MaybeIgnoreError(Status* s) const;

  const Status CreateArchivalDirectory();

  Status CreateColumnFamilyImpl(const ColumnFamilyOptions& cf_options,
                                const std::string& cf_name,
                                ColumnFamilyHandle** handle);

  Status DropColumnFamilyImpl(ColumnFamilyHandle* column_family);

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();
  // Delete obsolete files and log status and information of file deletion
  void DeleteObsoleteFileImpl(Status file_deletion_status, int job_id,
                              const std::string& fname, FileType type,
                              uint64_t number, uint32_t path_id);

  // Background process needs to call
  //     auto x = CaptureCurrentFileNumberInPendingOutputs()
  //     auto file_num = versions_->NewFileNumber();
  //     <do something>
  //     ReleaseFileNumberFromPendingOutputs(x)
  // This will protect any file with number `file_num` or greater from being
  // deleted while <do something> is running.
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

  Status SyncClosedLogs(JobContext* job_context);

  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status FlushMemTableToOutputFile(ColumnFamilyData* cfd,
                                   const MutableCFOptions& mutable_cf_options,
                                   bool* madeProgress, JobContext* job_context,
                                   LogBuffer* log_buffer);

  // REQUIRES: log_numbers are sorted in ascending order
  Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                         SequenceNumber* next_sequence, bool read_only);

  // The following two methods are used to flush a memtable to
  // storage. The first one is used at database RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(int job_id, ColumnFamilyData* cfd,
                                     MemTable* mem, VersionEdit* edit);

  // num_bytes: for slowdown case, delay time is calculated based on
  //            `num_bytes` going through.
  Status DelayWrite(uint64_t num_bytes, const WriteOptions& write_options);

  Status ThrottleLowPriWritesIfNeeded(const WriteOptions& write_options,
                                      WriteBatch* my_batch);

  Status ScheduleFlushes(WriteContext* context);

  Status SwitchMemtable(ColumnFamilyData* cfd, WriteContext* context);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(ColumnFamilyData* cfd, const FlushOptions& options,
                       bool writes_stopped = false);

  // Wait for memtable flushed
  Status WaitForFlushMemTable(ColumnFamilyData* cfd);

  // REQUIRES: mutex locked
  Status HandleWALFull(WriteContext* write_context);

  // REQUIRES: mutex locked
  Status HandleWriteBufferFull(WriteContext* write_context);

  // REQUIRES: mutex locked
  Status PreprocessWrite(const WriteOptions& write_options, bool* need_log_sync,
                         WriteContext* write_context);

  Status WriteToWAL(const WriteThread::WriteGroup& write_group,
                    log::Writer* log_writer, bool need_log_sync,
                    bool need_log_dir_sync, SequenceNumber sequence);

  // Used by WriteImpl to update bg_error_ if paranoid check is enabled.
  void ParanoidCheck(const Status& status);

  // Used by WriteImpl to update bg_error_ in case of memtable insert error.
  void MemTableInsertStatusCheck(const Status& memtable_insert_status);

#ifndef ROCKSDB_LITE

  Status CompactFilesImpl(const CompactionOptions& compact_options,
                          ColumnFamilyData* cfd, Version* version,
                          const std::vector<std::string>& input_file_names,
                          const int output_level, int output_path_id,
                          JobContext* job_context, LogBuffer* log_buffer);

  // Wait for current IngestExternalFile() calls to finish.
  // REQUIRES: mutex_ held
  void WaitForIngestFile();

#else
  // IngestExternalFile is not supported in ROCKSDB_LITE so this function
  // will be no-op
  void WaitForIngestFile() {}
#endif  // ROCKSDB_LITE

  ColumnFamilyData* GetColumnFamilyDataByName(const std::string& cf_name);

  void MaybeScheduleFlushOrCompaction();
  void SchedulePendingFlush(ColumnFamilyData* cfd);
  void SchedulePendingCompaction(ColumnFamilyData* cfd);
  void SchedulePendingPurge(std::string fname, FileType type, uint64_t number,
                            uint32_t path_id, int job_id);
  static void BGWorkCompaction(void* arg);
  static void BGWorkFlush(void* db);
  static void BGWorkPurge(void* arg);
  static void UnscheduleCallback(void* arg);
  void BackgroundCallCompaction(void* arg);
  void BackgroundCallFlush();
  void BackgroundCallPurge();
  Status BackgroundCompaction(bool* madeProgress, JobContext* job_context,
                              LogBuffer* log_buffer, void* m = 0);
  Status BackgroundFlush(bool* madeProgress, JobContext* job_context,
                         LogBuffer* log_buffer);

  void PrintStatistics();

  // dump rocksdb.stats to LOG
  void MaybeDumpStats();

  // Return the minimum empty level that could hold the total data in the
  // input level. Return the input level, if such level could not be found.
  int FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd,
      const MutableCFOptions& mutable_cf_options, int level);

  // Move the files in the input level to the target level.
  // If target_level < 0, automatically calculate the minimum level that could
  // hold the data set.
  Status ReFitLevel(ColumnFamilyData* cfd, int level, int target_level = -1);

  // helper functions for adding and removing from flush & compaction queues
  void AddToCompactionQueue(ColumnFamilyData* cfd);
  ColumnFamilyData* PopFirstFromCompactionQueue();
  void AddToFlushQueue(ColumnFamilyData* cfd);
  ColumnFamilyData* PopFirstFromFlushQueue();

  // helper function to call after some of the logs_ were synced
  void MarkLogsSynced(uint64_t up_to, bool synced_dir, const Status& status);

  const Snapshot* GetSnapshotImpl(bool is_write_conflict_boundary);

  uint64_t GetMaxTotalWalSize() const;

  // table_cache_ provides its own synchronization
  std::shared_ptr<Cache> table_cache_;

  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  FileLock* db_lock_;

  // The mutex for options file related operations.
  // NOTE: should never acquire options_file_mutex_ and mutex_ at the
  //       same time.
  InstrumentedMutex options_files_mutex_;
  // State below is protected by mutex_
  mutable InstrumentedMutex mutex_;

  std::atomic<bool> shutting_down_;
  // This condition variable is signaled on these conditions:
  // * whenever bg_compaction_scheduled_ goes down to 0
  // * if AnyManualCompaction, whenever a compaction finishes, even if it hasn't
  // made any progress
  // * whenever a compaction made any progress
  // * whenever bg_flush_scheduled_ or bg_purge_scheduled_ value decreases
  // (i.e. whenever a flush is done, even if it didn't make any progress)
  // * whenever there is an error in background purge, flush or compaction
  // * whenever num_running_ingest_file_ goes to 0.
  InstrumentedCondVar bg_cv_;
  uint64_t logfile_number_;
  std::deque<uint64_t>
      log_recycle_files;  // a list of log files that we can recycle
  bool log_dir_synced_;
  bool log_empty_;
  ColumnFamilyHandleImpl* default_cf_handle_;
  InternalStats* default_cf_internal_stats_;
  unique_ptr<ColumnFamilyMemTablesImpl> column_family_memtables_;
  struct LogFileNumberSize {
    explicit LogFileNumberSize(uint64_t _number)
        : number(_number) {}
    void AddSize(uint64_t new_size) { size += new_size; }
    uint64_t number;
    uint64_t size = 0;
    bool getting_flushed = false;
  };
  struct LogWriterNumber {
    // pass ownership of _writer
    LogWriterNumber(uint64_t _number, log::Writer* _writer)
        : number(_number), writer(_writer) {}

    log::Writer* ReleaseWriter() {
      auto* w = writer;
      writer = nullptr;
      return w;
    }
    void ClearWriter() {
      delete writer;
      writer = nullptr;
    }

    uint64_t number;
    // Visual Studio doesn't support deque's member to be noncopyable because
    // of a unique_ptr as a member.
    log::Writer* writer;  // own
    // true for some prefix of logs_
    bool getting_synced = false;
  };
  std::deque<LogFileNumberSize> alive_log_files_;
  // Log files that aren't fully synced, and the current log file.
  // Synchronization:
  //  - push_back() is done from write thread with locked mutex_,
  //  - pop_front() is done from any thread with locked mutex_,
  //  - back() and items with getting_synced=true are not popped,
  //  - it follows that write thread with unlocked mutex_ can safely access
  //    back() and items with getting_synced=true.
  std::deque<LogWriterNumber> logs_;
  // Signaled when getting_synced becomes false for some of the logs_.
  InstrumentedCondVar log_sync_cv_;
  std::atomic<uint64_t> total_log_size_;
  // only used for dynamically adjusting max_total_wal_size. it is a sum of
  // [write_buffer_size * max_write_buffer_number] over all column families
  uint64_t max_total_in_memory_state_;
  // If true, we have only one (default) column family. We use this to optimize
  // some code-paths
  bool single_column_family_mode_;
  // If this is non-empty, we need to delete these log files in background
  // threads. Protected by db mutex.
  autovector<log::Writer*> logs_to_free_;

  bool is_snapshot_supported_;

  // Class to maintain directories for all database paths other than main one.
  class Directories {
   public:
    Status SetDirectories(Env* env, const std::string& dbname,
                          const std::string& wal_dir,
                          const std::vector<DbPath>& data_paths);

    Directory* GetDataDir(size_t path_id);

    Directory* GetWalDir() {
      if (wal_dir_) {
        return wal_dir_.get();
      }
      return db_dir_.get();
    }

    Directory* GetDbDir() { return db_dir_.get(); }

   private:
    std::unique_ptr<Directory> db_dir_;
    std::vector<std::unique_ptr<Directory>> data_dirs_;
    std::unique_ptr<Directory> wal_dir_;

    Status CreateAndNewDirectory(Env* env, const std::string& dirname,
                                 std::unique_ptr<Directory>* directory) const;
  };

  Directories directories_;

  WriteBufferManager* write_buffer_manager_;

  WriteThread write_thread_;

  WriteBatch tmp_batch_;

  WriteController write_controller_;

  unique_ptr<RateLimiter> low_pri_write_rate_limiter_;

  // Size of the last batch group. In slowdown mode, next write needs to
  // sleep if it uses up the quota.
  uint64_t last_batch_group_size_;

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

  // PurgeFileInfo is a structure to hold information of files to be deleted in
  // purge_queue_
  struct PurgeFileInfo {
    std::string fname;
    FileType type;
    uint64_t number;
    uint32_t path_id;
    int job_id;
    PurgeFileInfo(std::string fn, FileType t, uint64_t num, uint32_t pid,
                  int jid)
        : fname(fn), type(t), number(num), path_id(pid), job_id(jid) {}
  };

  // flush_queue_ and compaction_queue_ hold column families that we need to
  // flush and compact, respectively.
  // A column family is inserted into flush_queue_ when it satisfies condition
  // cfd->imm()->IsFlushPending()
  // A column family is inserted into compaction_queue_ when it satisfied
  // condition cfd->NeedsCompaction()
  // Column families in this list are all Ref()-erenced
  // TODO(icanadi) Provide some kind of ReferencedColumnFamily class that will
  // do RAII on ColumnFamilyData
  // Column families are in this queue when they need to be flushed or
  // compacted. Consumers of these queues are flush and compaction threads. When
  // column family is put on this queue, we increase unscheduled_flushes_ and
  // unscheduled_compactions_. When these variables are bigger than zero, that
  // means we need to schedule background threads for compaction and thread.
  // Once the background threads are scheduled, we decrease unscheduled_flushes_
  // and unscheduled_compactions_. That way we keep track of number of
  // compaction and flush threads we need to schedule. This scheduling is done
  // in MaybeScheduleFlushOrCompaction()
  // invariant(column family present in flush_queue_ <==>
  // ColumnFamilyData::pending_flush_ == true)
  std::deque<ColumnFamilyData*> flush_queue_;
  // invariant(column family present in compaction_queue_ <==>
  // ColumnFamilyData::pending_compaction_ == true)
  std::deque<ColumnFamilyData*> compaction_queue_;

  // A queue to store filenames of the files to be purged
  std::deque<PurgeFileInfo> purge_queue_;

  // A queue to store log writers to close
  std::deque<log::Writer*> logs_to_free_queue_;
  int unscheduled_flushes_;
  int unscheduled_compactions_;

  // count how many background compactions are running or have been scheduled
  int bg_compaction_scheduled_;

  // stores the number of compactions are currently running
  int num_running_compactions_;

  // number of background memtable flush jobs, submitted to the HIGH pool
  int bg_flush_scheduled_;

  // stores the number of flushes are currently running
  int num_running_flushes_;

  // number of background obsolete file purge jobs, submitted to the HIGH pool
  int bg_purge_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    ColumnFamilyData* cfd;
    int input_level;
    int output_level;
    uint32_t output_path_id;
    Status status;
    bool done;
    bool in_progress;             // compaction request being processed?
    bool incomplete;              // only part of requested range compacted
    bool exclusive;               // current behavior of only one manual
    bool disallow_trivial_move;   // Force actual compaction to run
    const InternalKey* begin;     // nullptr means beginning of key range
    const InternalKey* end;       // nullptr means end of key range
    InternalKey* manual_end;      // how far we are compacting
    InternalKey tmp_storage;      // Used to keep track of compaction progress
    InternalKey tmp_storage1;     // Used to keep track of compaction progress
    Compaction* compaction;
  };
  std::deque<ManualCompaction*> manual_compaction_dequeue_;

  struct CompactionArg {
    DBImpl* db;
    ManualCompaction* m;
  };

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // shall we disable deletion of obsolete files
  // if 0 the deletion is enabled.
  // if non-zero, files will not be getting deleted
  // This enables two different threads to call
  // EnableFileDeletions() and DisableFileDeletions()
  // without any synchronization
  int disable_delete_obsolete_files_;

  // last time when DeleteObsoleteFiles with full scan was executed. Originaly
  // initialized with startup time.
  uint64_t delete_obsolete_files_last_run_;

  // last time stats were dumped to LOG
  std::atomic<uint64_t> last_stats_dump_time_microsec_;

  // Each flush or compaction gets its own job id. this counter makes sure
  // they're unique
  std::atomic<int> next_job_id_;

  // A flag indicating whether the current rocksdb database has any
  // data that is not yet persisted into either WAL or SST file.
  // Used when disableWAL is true.
  std::atomic<bool> has_unpersisted_data_;

  // if an attempt was made to flush all column families that
  // the oldest log depends on but uncommited data in the oldest
  // log prevents the log from being released.
  // We must attempt to free the dependent memtables again
  // at a later time after the transaction in the oldest
  // log is fully commited.
  bool unable_to_flush_oldest_log_;

  static const int KEEP_LOG_FILE_NUM = 1000;
  // MSVC version 1800 still does not have constexpr for ::max()
  static const uint64_t kNoTimeOut = port::kMaxUint64;

  std::string db_absolute_path_;

  // The options to access storage files
  const EnvOptions env_options_;

  // Number of running IngestExternalFile() calls.
  // REQUIRES: mutex held
  int num_running_ingest_file_;

#ifndef ROCKSDB_LITE
  WalManager wal_manager_;
#endif  // ROCKSDB_LITE

  // Unified interface for logging events
  EventLogger event_logger_;

  // A value of > 0 temporarily disables scheduling of background work
  int bg_work_paused_;

  // A value of > 0 temporarily disables scheduling of background compaction
  int bg_compaction_paused_;

  // Guard against multiple concurrent refitting
  bool refitting_level_;

  // Indicate DB was opened successfully
  bool opened_successfully_;

  // minimum log number still containing prepared data.
  // this is used by FindObsoleteFiles to determine which
  // flushed logs we must keep around because they still
  // contain prepared data which has not been flushed or rolled back
  std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>
      min_log_with_prep_;

  // to be used in conjunction with min_log_with_prep_.
  // once a transaction with data in log L is committed or rolled back
  // rather than removing the value from the heap we add that value
  // to prepared_section_completed_ which maps LOG -> instance_count
  // since a log could contain multiple prepared sections
  //
  // when trying to determine the minimum log still active we first
  // consult min_log_with_prep_. while that root value maps to
  // a value > 0 in prepared_section_completed_ we decrement the
  // instance_count for that log and pop the root value in
  // min_log_with_prep_. This will work the same as a min_heap
  // where we are deleteing arbitrary elements and the up heaping.
  std::unordered_map<uint64_t, uint64_t> prepared_section_completed_;
  std::mutex prep_heap_mutex_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  // Background threads call this function, which is just a wrapper around
  // the InstallSuperVersion() function. Background threads carry
  // job_context which can have new_superversion already
  // allocated.
  void InstallSuperVersionAndScheduleWorkWrapper(
      ColumnFamilyData* cfd, JobContext* job_context,
      const MutableCFOptions& mutable_cf_options);

  // All ColumnFamily state changes go through this function. Here we analyze
  // the new state and we schedule background work if we detect that the new
  // state needs flush or compaction.
  SuperVersion* InstallSuperVersionAndScheduleWork(
      ColumnFamilyData* cfd, SuperVersion* new_sv,
      const MutableCFOptions& mutable_cf_options);

#ifndef ROCKSDB_LITE
  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                          TablePropertiesCollection* props)
      override;
  virtual Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) override;

#endif  // ROCKSDB_LITE

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, PinnableSlice* value,
                 bool* value_found = nullptr);

  bool GetIntPropertyInternal(ColumnFamilyData* cfd,
                              const DBPropertyInfo& property_info,
                              bool is_locked, uint64_t* value);

  bool HasPendingManualCompaction();
  bool HasExclusiveManualCompaction();
  void AddManualCompaction(ManualCompaction* m);
  void RemoveManualCompaction(ManualCompaction* m);
  bool ShouldntRunManualCompaction(ManualCompaction* m);
  bool HaveManualCompaction(ColumnFamilyData* cfd);
  bool MCOverlap(ManualCompaction* m, ManualCompaction* m1);

  size_t GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;
};

extern Options SanitizeOptions(const std::string& db,
                               const Options& src);

extern DBOptions SanitizeOptions(const std::string& db, const DBOptions& src);

extern CompressionType GetCompressionFlush(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

}  // namespace rocksdb
