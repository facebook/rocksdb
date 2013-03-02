// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_file.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/stats_logger.h"
#include "memtablelist.h"

#ifdef USE_SCRIBE
#include "scribe/scribe_logger.h"
#endif

namespace leveldb {

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
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual int NumberLevels();
  virtual int MaxMemCompactionLevel();
  virtual int Level0StopWriteTrigger();
  virtual Status Flush(const FlushOptions& options);
  virtual Status DisableFileDeletions();
  virtual Status EnableFileDeletions();
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size);
  virtual SequenceNumber GetLatestSequenceNumber();
  virtual Status GetUpdatesSince(SequenceNumber seq_number,
                                 unique_ptr<TransactionLogIterator>* iter);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Wait for memtable compaction
  Status TEST_WaitForCompactMemTable();

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
 protected:
  Env* const env_;
  const std::string dbname_;
  unique_ptr<VersionSet> versions_;
  const InternalKeyComparator internal_comparator_;
  const Options options_;  // options_.comparator == &internal_comparator_

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  MemTable* GetMemTable() {
    return mem_;
  }

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;
  struct DeletionState;

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, MemTable* external_table = nullptr,
      bool error_if_log_file_exist = false);

  void MaybeIgnoreError(Status* s) const;

  const Status CreateArchivalDirectory();

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status CompactMemTable(bool* madeProgress = nullptr);

  Status RecoverLogFile(uint64_t log_number,
                        VersionEdit* edit,
                        SequenceNumber* max_sequence,
                        MemTable* external_table);

  // The following two methods are used to flush a memtable to
  // storage. The first one is used atdatabase RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(MemTable* mem, VersionEdit* edit);
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                uint64_t* filenumber);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(const FlushOptions& options);

  // Wait for memtable compaction
  Status WaitForCompactMemTable();

  void MaybeScheduleLogDBDeployStats();
  static void BGLogDBDeployStats(void* db);
  void LogDBDeployStats();

  void MaybeScheduleCompaction();
  static void BGWork(void* db);
  void BackgroundCall();
  Status BackgroundCompaction(bool* madeProgress, DeletionState& deletion_state);
  void CleanupCompaction(CompactionState* compact);
  Status DoCompactionWork(CompactionState* compact);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact);
  void AllocateCompactionOutputFileNumbers(CompactionState* compact);
  void ReleaseCompactionUnusedFileNumbers(CompactionState* compact);


  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'allfiles'.
  void FindObsoleteFiles(DeletionState& deletion_state);

  // Diffs the files listed in filenames and those that do not
  // belong to live files are posibly removed. If the removed file
  // is a sst file, then it returns the file number in files_to_evict.
  void PurgeObsoleteFiles(DeletionState& deletion_state);

  // Removes the file listed in files_to_evict from the table_cache
  void EvictObsoleteFiles(DeletionState& deletion_state);

  void PurgeObsoleteWALFiles();

  Status ListAllWALFiles(const std::string& path,
                         std::vector<LogFile>* logFiles,
                         WalFileType type);

  //  Find's all the log files which contain updates with seq no.
  //  Greater Than or Equal to the requested SequenceNumber
  Status FindProbableWALFiles(std::vector<LogFile>* const allLogs,
                              std::vector<LogFile>* const result,
                              const SequenceNumber target);


  Status ReadFirstRecord(const LogFile& file, WriteBatch* const result);


  Status ReadFirstLine(const std::string& fname, WriteBatch* const batch);
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

  std::string host_name_;

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // count how many background compaction been scheduled or is running?
  int bg_compaction_scheduled_;

  // Has a background stats log thread scheduled?
  bool bg_logstats_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    bool in_progress;           // compaction request being processed?
    const InternalKey* begin;   // nullptr means beginning of key range
    const InternalKey* end;     // nullptr means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };
  ManualCompaction* manual_compaction_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  StatsLogger* logger_;

  int64_t volatile last_log_ts;

  // shall we disable deletion of obsolete files
  bool disable_delete_obsolete_files_;

  // last time when DeleteObsoleteFiles was invoked
  uint64_t delete_obsolete_files_last_run_;

  // These count the number of microseconds for which MakeRoomForWrite stalls.
  uint64_t stall_level0_slowdown_;
  uint64_t stall_memtable_compaction_;
  uint64_t stall_level0_num_files_;
  std::vector<uint64_t> stall_leveln_slowdown_;

  // Time at which this instance was started.
  const uint64_t started_at_;

  bool flush_on_destroy_; // Used when disableWAL is true.

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    uint64_t micros;

    // Bytes read from level N during compaction between levels N and N+1
    int64_t bytes_readn;

    // Bytes read from level N+1 during compaction between levels N and N+1
    int64_t bytes_readnp1;

    // Total bytes written during compaction between levels N and N+1
    int64_t bytes_written;

    // Files read from level N during compaction between levels N and N+1
    int     files_in_leveln;

    // Files read from level N+1 during compaction between levels N and N+1
    int     files_in_levelnp1;

    // Files written during compaction between levels N and N+1
    int     files_out_levelnp1;

    // Number of compactions done
    int     count;

    CompactionStats() : micros(0), bytes_readn(0), bytes_readnp1(0),
                        bytes_written(0), files_in_leveln(0),
                        files_in_levelnp1(0), files_out_levelnp1(0),
                        count(0) { }

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_readn += c.bytes_readn;
      this->bytes_readnp1 += c.bytes_readnp1;
      this->bytes_written += c.bytes_written;
      this->files_in_leveln += c.files_in_leveln;
      this->files_in_levelnp1 += c.files_in_levelnp1;
      this->files_out_levelnp1 += c.files_out_levelnp1;
      this->count += 1;
    }
  };

  CompactionStats* stats_;

  static const int KEEP_LOG_FILE_NUM = 1000;
  std::string db_absolute_path_;

  // count of the number of contiguous delaying writes
  int delayed_writes_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  // dump the delayed_writes_ to the log file and reset counter.
  void DelayLoggingAndReset();

  // find the earliest snapshot where seqno is visible
  inline SequenceNumber findEarliestVisibleSnapshot(SequenceNumber in,
    std::vector<SequenceNumber>& snapshots);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
