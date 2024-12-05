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
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/compaction/compaction_iterator.h"
#include "db/compaction/compaction_job.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/flush_job.h"
#include "db/flush_scheduler.h"
#include "db/import_column_family_job.h"
#include "db/internal_stats.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable_list.h"
#include "db/periodic_task_scheduler.h"
#include "db/post_memtable_callback.h"
#include "db/pre_release_callback.h"
#include "db/range_del_aggregator.h"
#include "db/read_callback.h"
#include "db/seqno_to_time_mapping.h"
#include "db/snapshot_checker.h"
#include "db/snapshot_impl.h"
#include "db/trim_history_scheduler.h"
#include "db/version_edit.h"
#include "db/wal_manager.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "logging/event_logger.h"
#include "memtable/wbwi_memtable.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/attribute_groups.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/user_write_callback.h"
#include "rocksdb/utilities/replayer.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/hash.h"
#include "util/repeatable_thread.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class ArenaWrappedDBIter;
class InMemoryStatsHistoryIterator;
class MemTable;
class PersistentStatsHistoryIterator;
class TableCache;
class TaskLimiterToken;
class Version;
class VersionEdit;
class VersionSet;
class WriteCallback;
struct JobContext;
struct ExternalSstFileInfo;
struct MemTableInfo;

// Class to maintain directories for all database paths other than main one.
class Directories {
 public:
  IOStatus SetDirectories(FileSystem* fs, const std::string& dbname,
                          const std::string& wal_dir,
                          const std::vector<DbPath>& data_paths);

  FSDirectory* GetDataDir(size_t path_id) const {
    assert(path_id < data_dirs_.size());
    FSDirectory* ret_dir = data_dirs_[path_id].get();
    if (ret_dir == nullptr) {
      // Should use db_dir_
      return db_dir_.get();
    }
    return ret_dir;
  }

  FSDirectory* GetWalDir() {
    if (wal_dir_) {
      return wal_dir_.get();
    }
    return db_dir_.get();
  }

  FSDirectory* GetDbDir() { return db_dir_.get(); }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) {
    // close all directories for all database paths
    IOStatus s = IOStatus::OK();

    // The default implementation for Close() in Directory/FSDirectory class
    // "NotSupported" status, the upper level interface should be able to
    // handle this error so that Close() does not fail after upgrading when
    // run on FileSystems that have not implemented `Directory::Close()` or
    // `FSDirectory::Close()` yet

    if (db_dir_) {
      IOStatus temp_s = db_dir_->Close(options, dbg);
      if (!temp_s.ok() && !temp_s.IsNotSupported() && s.ok()) {
        s = std::move(temp_s);
      }
    }

    // Attempt to close everything even if one fails
    s.PermitUncheckedError();

    if (wal_dir_) {
      IOStatus temp_s = wal_dir_->Close(options, dbg);
      if (!temp_s.ok() && !temp_s.IsNotSupported() && s.ok()) {
        s = std::move(temp_s);
      }
    }

    s.PermitUncheckedError();

    for (auto& data_dir_ptr : data_dirs_) {
      if (data_dir_ptr) {
        IOStatus temp_s = data_dir_ptr->Close(options, dbg);
        if (!temp_s.ok() && !temp_s.IsNotSupported() && s.ok()) {
          s = std::move(temp_s);
        }
      }
    }

    // Ready for caller
    s.MustCheck();
    return s;
  }

 private:
  std::unique_ptr<FSDirectory> db_dir_;
  std::vector<std::unique_ptr<FSDirectory>> data_dirs_;
  std::unique_ptr<FSDirectory> wal_dir_;
};

// While DB is the public interface of RocksDB, and DBImpl is the actual
// class implementing it. It's the entrance of the core RocksdB engine.
// All other DB implementations, e.g. TransactionDB, BlobDB, etc, wrap a
// DBImpl internally.
// Other than functions implementing the DB interface, some public
// functions are there for other internal components to call. For
// example, TransactionDB directly calls DBImpl::WriteImpl() and
// BlobDB directly calls DBImpl::GetImpl(). Some other functions
// are for sub-components to call. For example, ColumnFamilyHandleImpl
// calls DBImpl::FindObsoleteFiles().
//
// Since it's a very large class, the definition of the functions is
// divided in several db_impl_*.cc files, besides db_impl.cc.
class DBImpl : public DB {
 public:
  DBImpl(const DBOptions& options, const std::string& dbname,
         const bool seq_per_batch = false, const bool batch_per_txn = true,
         bool read_only = false);
  // No copying allowed
  DBImpl(const DBImpl&) = delete;
  void operator=(const DBImpl&) = delete;

  virtual ~DBImpl();

  // ---- Implementations of the DB interface ----

  using DB::Resume;
  Status Resume() override;

  using DB::Put;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& value) override;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& ts, const Slice& value) override;

  using DB::PutEntity;
  Status PutEntity(const WriteOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns) override;
  Status PutEntity(const WriteOptions& options, const Slice& key,
                   const AttributeGroups& attribute_groups) override;

  using DB::Merge;
  Status Merge(const WriteOptions& options, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& value) override;
  Status Merge(const WriteOptions& options, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& ts, const Slice& value) override;

  using DB::Delete;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key) override;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key, const Slice& ts) override;

  using DB::SingleDelete;
  Status SingleDelete(const WriteOptions& options,
                      ColumnFamilyHandle* column_family,
                      const Slice& key) override;
  Status SingleDelete(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts) override;

  using DB::DeleteRange;
  Status DeleteRange(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key) override;
  Status DeleteRange(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key, const Slice& ts) override;

  using DB::Write;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;

  using DB::WriteWithCallback;
  Status WriteWithCallback(const WriteOptions& options, WriteBatch* updates,
                           UserWriteCallback* user_write_cb) override;

  using DB::Get;
  Status Get(const ReadOptions& _read_options,
             ColumnFamilyHandle* column_family, const Slice& key,
             PinnableSlice* value, std::string* timestamp) override;

  using DB::GetEntity;
  Status GetEntity(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableWideColumns* columns) override;
  Status GetEntity(const ReadOptions& options, const Slice& key,
                   PinnableAttributeGroups* result) override;

  using DB::GetMergeOperands;
  Status GetMergeOperands(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          PinnableSlice* merge_operands,
                          GetMergeOperandsOptions* get_merge_operands_options,
                          int* number_of_operands) override {
    GetImplOptions get_impl_options;
    get_impl_options.column_family = column_family;
    get_impl_options.merge_operands = merge_operands;
    get_impl_options.get_merge_operands_options = get_merge_operands_options;
    get_impl_options.number_of_operands = number_of_operands;
    get_impl_options.get_value = false;
    return GetImpl(options, key, get_impl_options);
  }

  using DB::MultiGet;
  // This MultiGet is a batched version, which may be faster than calling Get
  // multiple times, especially if the keys have some spatial locality that
  // enables them to be queried in the same SST files/set of files. The larger
  // the batch size, the more scope for batching and performance improvement
  // The values and statuses parameters are arrays with number of elements
  // equal to keys.size(). This allows the storage for those to be alloacted
  // by the caller on the stack for small batches
  void MultiGet(const ReadOptions& _read_options, const size_t num_keys,
                ColumnFamilyHandle** column_families, const Slice* keys,
                PinnableSlice* values, std::string* timestamps,
                Status* statuses, const bool sorted_input = false) override;

  void MultiGetWithCallback(
      const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
      ReadCallback* callback,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys);

  using DB::MultiGetEntity;

  void MultiGetEntity(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, size_t num_keys,
                      const Slice* keys, PinnableWideColumns* results,
                      Status* statuses, bool sorted_input) override;

  void MultiGetEntity(const ReadOptions& options, size_t num_keys,
                      ColumnFamilyHandle** column_families, const Slice* keys,
                      PinnableWideColumns* results, Status* statuses,
                      bool sorted_input) override;
  void MultiGetEntity(const ReadOptions& options, size_t num_keys,
                      const Slice* keys,
                      PinnableAttributeGroups* results) override;

  void MultiGetEntityWithCallback(
      const ReadOptions& read_options, ColumnFamilyHandle* column_family,
      ReadCallback* callback,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys);

  Status CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                            const std::string& column_family,
                            ColumnFamilyHandle** handle) override {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    return CreateColumnFamily(ReadOptions(), WriteOptions(), cf_options,
                              column_family, handle);
  }
  virtual Status CreateColumnFamily(const ReadOptions& read_options,
                                    const WriteOptions& write_options,
                                    const ColumnFamilyOptions& cf_options,
                                    const std::string& column_family,
                                    ColumnFamilyHandle** handle);
  Status CreateColumnFamilies(
      const ColumnFamilyOptions& cf_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles) override {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    return CreateColumnFamilies(ReadOptions(), WriteOptions(), cf_options,
                                column_family_names, handles);
  }
  virtual Status CreateColumnFamilies(
      const ReadOptions& read_options, const WriteOptions& write_options,
      const ColumnFamilyOptions& cf_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles);

  Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles) override {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    return CreateColumnFamilies(ReadOptions(), WriteOptions(), column_families,
                                handles);
  }
  virtual Status CreateColumnFamilies(
      const ReadOptions& read_options, const WriteOptions& write_options,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles);
  Status DropColumnFamily(ColumnFamilyHandle* column_family) override;
  Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  // Returns false if key doesn't exist in the database and true if it may.
  // If value_found is not passed in as null, then return the value if found in
  // memory. On return, if value was found, then value_found will be set to true
  // , otherwise false.
  using DB::KeyMayExist;
  bool KeyMayExist(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   std::string* value, std::string* timestamp,
                   bool* value_found = nullptr) override;

  using DB::NewIterator;
  Iterator* NewIterator(const ReadOptions& _read_options,
                        ColumnFamilyHandle* column_family) override;
  Status NewIterators(const ReadOptions& _read_options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override;

  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;

  std::unique_ptr<Iterator> NewCoalescingIterator(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  std::unique_ptr<AttributeGroupIterator> NewAttributeGroupIterator(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  // Create a timestamped snapshot. This snapshot can be shared by multiple
  // readers. If any of them uses it for write conflict checking, then
  // is_write_conflict_boundary is true. For simplicity, set it to true by
  // default.
  std::pair<Status, std::shared_ptr<const Snapshot>> CreateTimestampedSnapshot(
      SequenceNumber snapshot_seq, uint64_t ts);
  std::shared_ptr<const SnapshotImpl> GetTimestampedSnapshot(uint64_t ts) const;
  void ReleaseTimestampedSnapshotsOlderThan(
      uint64_t ts, size_t* remaining_total_ss = nullptr);
  Status GetTimestampedSnapshots(uint64_t ts_lb, uint64_t ts_ub,
                                 std::vector<std::shared_ptr<const Snapshot>>&
                                     timestamped_snapshots) const;

  using DB::GetProperty;
  bool GetProperty(ColumnFamilyHandle* column_family, const Slice& property,
                   std::string* value) override;
  using DB::GetMapProperty;
  bool GetMapProperty(ColumnFamilyHandle* column_family, const Slice& property,
                      std::map<std::string, std::string>* value) override;
  using DB::GetIntProperty;
  bool GetIntProperty(ColumnFamilyHandle* column_family, const Slice& property,
                      uint64_t* value) override;
  using DB::GetAggregatedIntProperty;
  bool GetAggregatedIntProperty(const Slice& property,
                                uint64_t* aggregated_value) override;
  using DB::GetApproximateSizes;
  Status GetApproximateSizes(const SizeApproximationOptions& options,
                             ColumnFamilyHandle* column_family,
                             const Range* range, int n,
                             uint64_t* sizes) override;
  using DB::GetApproximateMemTableStats;
  void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                   const Range& range, uint64_t* const count,
                                   uint64_t* const size) override;
  using DB::CompactRange;
  Status CompactRange(const CompactRangeOptions& options,
                      ColumnFamilyHandle* column_family, const Slice* begin,
                      const Slice* end) override;

  using DB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override;

  Status PauseBackgroundWork() override;
  Status ContinueBackgroundWork() override;

  Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;

  void EnableManualCompaction() override;
  void DisableManualCompaction() override;

  using DB::SetOptions;
  Status SetOptions(
      ColumnFamilyHandle* column_family,
      const std::unordered_map<std::string, std::string>& options_map) override;

  Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& options_map) override;

  using DB::NumberLevels;
  int NumberLevels(ColumnFamilyHandle* column_family) override;
  using DB::MaxMemCompactionLevel;
  int MaxMemCompactionLevel(ColumnFamilyHandle* column_family) override;
  using DB::Level0StopWriteTrigger;
  int Level0StopWriteTrigger(ColumnFamilyHandle* column_family) override;
  const std::string& GetName() const override;
  Env* GetEnv() const override;
  FileSystem* GetFileSystem() const override;
  using DB::GetOptions;
  Options GetOptions(ColumnFamilyHandle* column_family) const override;
  using DB::GetDBOptions;
  DBOptions GetDBOptions() const override;
  using DB::Flush;
  Status Flush(const FlushOptions& options,
               ColumnFamilyHandle* column_family) override;
  Status Flush(
      const FlushOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;
  Status FlushWAL(bool sync) override {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    return FlushWAL(WriteOptions(), sync);
  }

  virtual Status FlushWAL(const WriteOptions& write_options, bool sync);
  bool WALBufferIsEmpty();
  Status SyncWAL() override;
  Status LockWAL() override;
  Status UnlockWAL() override;

  SequenceNumber GetLatestSequenceNumber() const override;

  // IncreaseFullHistoryTsLow(ColumnFamilyHandle*, std::string) will acquire
  // and release db_mutex
  Status IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                  std::string ts_low) override;

  // GetFullHistoryTsLow(ColumnFamilyHandle*, std::string*) will acquire and
  // release db_mutex
  Status GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                             std::string* ts_low) override;

  Status GetDbIdentity(std::string& identity) const override;

  virtual Status GetDbIdentityFromIdentityFile(const IOOptions& opts,
                                               std::string* identity) const;

  Status GetDbSessionId(std::string& session_id) const override;

  ColumnFamilyHandle* DefaultColumnFamily() const override;

  ColumnFamilyHandle* PersistentStatsColumnFamily() const;

  Status Close() override;

  Status DisableFileDeletions() override;

  Status EnableFileDeletions() override;

  virtual bool IsFileDeletionsEnabled() const;

  Status GetStatsHistory(
      uint64_t start_time, uint64_t end_time,
      std::unique_ptr<StatsHistoryIterator>* stats_iterator) override;

  using DB::ResetStats;
  Status ResetStats() override;
  // All the returned filenames start with "/"
  Status GetLiveFiles(std::vector<std::string>&, uint64_t* manifest_file_size,
                      bool flush_memtable = true) override;
  Status GetSortedWalFiles(VectorWalPtr& files) override;
  Status GetSortedWalFilesImpl(VectorWalPtr& files, bool need_seqnos);

  // Get the known flushed sizes of WALs that might still be written to
  // or have pending sync.
  // NOTE: unlike alive_log_files_, this function includes WALs that might
  // be obsolete (but not obsolete to a pending Checkpoint) and not yet fully
  // synced.
  Status GetOpenWalSizes(std::map<uint64_t, uint64_t>& number_to_size);
  Status GetCurrentWalFile(std::unique_ptr<WalFile>* current_log_file) override;
  Status GetCreationTimeOfOldestFile(uint64_t* creation_time) override;

  Status GetUpdatesSince(
      SequenceNumber seq_number, std::unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions& read_options =
          TransactionLogIterator::ReadOptions()) override;
  Status DeleteFile(std::string name) override;
  Status DeleteFilesInRanges(ColumnFamilyHandle* column_family,
                             const RangePtr* ranges, size_t n,
                             bool include_end = true);

  void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) override;

  Status GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) override;

  Status GetLiveFilesStorageInfo(
      const LiveFilesStorageInfoOptions& opts,
      std::vector<LiveFileStorageInfo>* files) override;

  // Obtains the meta data of the specified column family of the DB.
  // TODO(yhchiang): output parameter is placed in the end in this codebase.
  void GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                               ColumnFamilyMetaData* metadata) override;

  void GetAllColumnFamilyMetaData(
      std::vector<ColumnFamilyMetaData>* metadata) override;

  Status SuggestCompactRange(ColumnFamilyHandle* column_family,
                             const Slice* begin, const Slice* end) override;

  Status PromoteL0(ColumnFamilyHandle* column_family,
                   int target_level) override;

  using DB::IngestExternalFile;
  Status IngestExternalFile(
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& external_files,
      const IngestExternalFileOptions& ingestion_options) override;

  using DB::IngestExternalFiles;
  Status IngestExternalFiles(
      const std::vector<IngestExternalFileArg>& args) override;

  using DB::CreateColumnFamilyWithImport;
  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& options, const std::string& column_family_name,
      const ImportColumnFamilyOptions& import_options,
      const std::vector<const ExportImportFilesMetaData*>& metadatas,
      ColumnFamilyHandle** handle) override;

  using DB::ClipColumnFamily;
  Status ClipColumnFamily(ColumnFamilyHandle* column_family,
                          const Slice& begin_key,
                          const Slice& end_key) override;

  using DB::VerifyFileChecksums;
  Status VerifyFileChecksums(const ReadOptions& read_options) override;

  using DB::VerifyChecksum;
  Status VerifyChecksum(const ReadOptions& /*read_options*/) override;
  // Verify the checksums of files in db. Currently only tables are checked.
  //
  // read_options: controls file I/O behavior, e.g. read ahead size while
  //               reading all the live table files.
  //
  // use_file_checksum: if false, verify the block checksums of all live table
  //                    in db. Otherwise, obtain the file checksums and compare
  //                    with the MANIFEST. Currently, file checksums are
  //                    recomputed by reading all table files.
  //
  // Returns: OK if there is no file whose file or block checksum mismatches.
  Status VerifyChecksumInternal(const ReadOptions& read_options,
                                bool use_file_checksum);

  Status VerifyFullFileChecksum(const std::string& file_checksum_expected,
                                const std::string& func_name_expected,
                                const std::string& fpath,
                                const ReadOptions& read_options);

  using DB::StartTrace;
  Status StartTrace(const TraceOptions& options,
                    std::unique_ptr<TraceWriter>&& trace_writer) override;

  using DB::EndTrace;
  Status EndTrace() override;

  using DB::NewDefaultReplayer;
  Status NewDefaultReplayer(const std::vector<ColumnFamilyHandle*>& handles,
                            std::unique_ptr<TraceReader>&& reader,
                            std::unique_ptr<Replayer>* replayer) override;

  using DB::StartBlockCacheTrace;
  Status StartBlockCacheTrace(
      const TraceOptions& trace_options,
      std::unique_ptr<TraceWriter>&& trace_writer) override;

  Status StartBlockCacheTrace(
      const BlockCacheTraceOptions& options,
      std::unique_ptr<BlockCacheTraceWriter>&& trace_writer) override;

  using DB::EndBlockCacheTrace;
  Status EndBlockCacheTrace() override;

  using DB::StartIOTrace;
  Status StartIOTrace(const TraceOptions& options,
                      std::unique_ptr<TraceWriter>&& trace_writer) override;

  using DB::EndIOTrace;
  Status EndIOTrace() override;

  using DB::GetPropertiesOfAllTables;
  Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                  TablePropertiesCollection* props) override;
  Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) override;

  // ---- End of implementations of the DB interface ----
  SystemClock* GetSystemClock() const;

  struct GetImplOptions {
    ColumnFamilyHandle* column_family = nullptr;
    PinnableSlice* value = nullptr;
    PinnableWideColumns* columns = nullptr;
    std::string* timestamp = nullptr;
    bool* value_found = nullptr;
    ReadCallback* callback = nullptr;
    bool* is_blob_index = nullptr;
    // If true return value associated with key via value pointer else return
    // all merge operands for key via merge_operands pointer
    bool get_value = true;
    // Pointer to an array of size
    // get_merge_operands_options.expected_max_number_of_operands allocated by
    // user
    PinnableSlice* merge_operands = nullptr;
    GetMergeOperandsOptions* get_merge_operands_options = nullptr;
    int* number_of_operands = nullptr;
  };

  Status GetImpl(const ReadOptions& read_options,
                 ColumnFamilyHandle* column_family, const Slice& key,
                 PinnableSlice* value);

  Status GetImpl(const ReadOptions& read_options,
                 ColumnFamilyHandle* column_family, const Slice& key,
                 PinnableSlice* value, std::string* timestamp);

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  // This function is also called by GetMergeOperands
  // If get_impl_options.get_value = true get value associated with
  // get_impl_options.key via get_impl_options.value
  // If get_impl_options.get_value = false get merge operands associated with
  // get_impl_options.key via get_impl_options.merge_operands
  virtual Status GetImpl(const ReadOptions& options, const Slice& key,
                         GetImplOptions& get_impl_options);

  // If `snapshot` == kMaxSequenceNumber, set a recent one inside the file.
  ArenaWrappedDBIter* NewIteratorImpl(const ReadOptions& options,
                                      ColumnFamilyHandleImpl* cfh,
                                      SuperVersion* sv, SequenceNumber snapshot,
                                      ReadCallback* read_callback,
                                      bool expose_blob_index = false,
                                      bool allow_refresh = true);

  virtual SequenceNumber GetLastPublishedSequence() const {
    if (last_seq_same_as_publish_seq_) {
      return versions_->LastSequence();
    } else {
      return versions_->LastPublishedSequence();
    }
  }

  // REQUIRES: joined the main write queue if two_write_queues is disabled, and
  // the second write queue otherwise.
  virtual void SetLastPublishedSequence(SequenceNumber seq);
  // Returns LastSequence in last_seq_same_as_publish_seq_
  // mode and LastAllocatedSequence otherwise. This is useful when visiblility
  // depends also on data written to the WAL but not to the memtable.
  SequenceNumber TEST_GetLastVisibleSequence() const;

  // Similar to Write() but will call the callback once on the single write
  // thread to determine whether it is safe to perform the write.
  virtual Status WriteWithCallback(const WriteOptions& write_options,
                                   WriteBatch* my_batch,
                                   WriteCallback* callback,
                                   UserWriteCallback* user_write_cb = nullptr);

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
  // `key` should NOT have user-defined timestamp appended to user key even if
  // timestamp is enabled.
  //
  // If a key is found, *found_record_for_key will be set to true and
  // *seq will be set to the stored sequence number for the latest
  // operation on this key or kMaxSequenceNumber if unknown. If user-defined
  // timestamp is enabled for this column family and timestamp is not nullptr,
  // then *timestamp will be set to the stored timestamp for the latest
  // operation on this key.
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
  // Only records newer than or at `lower_bound_seq` are guaranteed to be
  // returned. Memtables and files may not be checked if it only contains data
  // older than `lower_bound_seq`.
  //
  // Returns OK or NotFound on success,
  // other status on unexpected error.
  // TODO(andrewkr): this API need to be aware of range deletion operations
  Status GetLatestSequenceForKey(SuperVersion* sv, const Slice& key,
                                 bool cache_only,
                                 SequenceNumber lower_bound_seq,
                                 SequenceNumber* seq, std::string* timestamp,
                                 bool* found_record_for_key,
                                 bool* is_blob_index);

  Status TraceIteratorSeek(const uint32_t& cf_id, const Slice& key,
                           const Slice& lower_bound, const Slice upper_bound);
  Status TraceIteratorSeekForPrev(const uint32_t& cf_id, const Slice& key,
                                  const Slice& lower_bound,
                                  const Slice upper_bound);

  // Similar to GetSnapshot(), but also lets the db know that this snapshot
  // will be used for transaction write-conflict checking.  The DB can then
  // make sure not to compact any keys that would prevent a write-conflict from
  // being detected.
  const Snapshot* GetSnapshotForWriteConflictBoundary();

  // checks if all live files exist on file system and that their file sizes
  // match to our in-memory records
  virtual Status CheckConsistency();

  // max_file_num_to_ignore allows bottom level compaction to filter out newly
  // compacted SST files. Setting max_file_num_to_ignore to kMaxUint64 will
  // disable the filtering
  // If `final_output_level` is not nullptr, it is set to manual compaction's
  // output level if returned status is OK, and it may or may not be set to
  // manual compaction's output level if returned status is not OK.
  Status RunManualCompaction(ColumnFamilyData* cfd, int input_level,
                             int output_level,
                             const CompactRangeOptions& compact_range_options,
                             const Slice* begin, const Slice* end,
                             bool exclusive, bool disallow_trivial_move,
                             uint64_t max_file_num_to_ignore,
                             const std::string& trim_ts,
                             int* final_output_level = nullptr);

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  // If allow_unprepared_value is true, the returned iterator may defer reading
  // the value and so will require PrepareValue() to be called before value();
  // allow_unprepared_value = false is convenient when this optimization is not
  // useful, e.g. when reading the whole column family.
  //
  // read_options.ignore_range_deletions determines whether range tombstones are
  // processed in the returned interator internally, i.e., whether range
  // tombstone covered keys are in this iterator's output.
  // @param read_options Must outlive the returned iterator.
  InternalIterator* NewInternalIterator(
      const ReadOptions& read_options, Arena* arena, SequenceNumber sequence,
      ColumnFamilyHandle* column_family = nullptr,
      bool allow_unprepared_value = false);

  // Note: to support DB iterator refresh, memtable range tombstones in the
  // underlying merging iterator needs to be refreshed. If db_iter is not
  // nullptr, db_iter->SetMemtableRangetombstoneIter() is called with the
  // memtable range tombstone iterator used by the underlying merging iterator.
  // This range tombstone iterator can be refreshed later by db_iter.
  // @param read_options Must outlive the returned iterator.
  InternalIterator* NewInternalIterator(const ReadOptions& read_options,
                                        ColumnFamilyData* cfd,
                                        SuperVersion* super_version,
                                        Arena* arena, SequenceNumber sequence,
                                        bool allow_unprepared_value,
                                        ArenaWrappedDBIter* db_iter = nullptr);

  LogsWithPrepTracker* logs_with_prep_tracker() {
    return &logs_with_prep_tracker_;
  }

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

  uint64_t MinLogNumberToRecycle();

  // Returns the lower bound file number for SSTs that won't be deleted, even if
  // they're obsolete. This lower bound is used internally to prevent newly
  // created flush/compaction output files from being deleted before they're
  // installed. This technique avoids the need for tracking the exact numbers of
  // files pending creation, although it prevents more files than necessary from
  // being deleted.
  uint64_t MinObsoleteSstNumberToKeep();

  uint64_t GetObsoleteSstFilesSize();

  uint64_t MinOptionsFileNumberToKeep();

  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'candidate_files'.
  // If force == false and the last call was less than
  // db_options_.delete_obsolete_files_period_micros microseconds ago,
  // it will not fill up the job_context
  void FindObsoleteFiles(JobContext* job_context, bool force,
                         bool no_full_scan = false);

  // Diffs the files listed in filenames and those that do not
  // belong to live files are possibly removed. Also, removes all the
  // files in sst_delete_files and log_delete_files.
  // It is not necessary to hold the mutex when invoking this method.
  // If FindObsoleteFiles() was run, we need to also run
  // PurgeObsoleteFiles(), even if disable_delete_obsolete_files_ is true
  void PurgeObsoleteFiles(JobContext& background_contet,
                          bool schedule_only = false);

  // Schedule a background job to actually delete obsolete files.
  void SchedulePurge();

  const SnapshotList& snapshots() const { return snapshots_; }

  // load list of snapshots to `snap_vector` that is no newer than `max_seq`
  // in ascending order.
  // `oldest_write_conflict_snapshot` is filled with the oldest snapshot
  // which satisfies SnapshotImpl.is_write_conflict_boundary_ = true.
  void LoadSnapshots(std::vector<SequenceNumber>* snap_vector,
                     SequenceNumber* oldest_write_conflict_snapshot,
                     const SequenceNumber& max_seq) const {
    InstrumentedMutexLock l(mutex());
    snapshots().GetAll(snap_vector, oldest_write_conflict_snapshot, max_seq);
  }

  const ImmutableDBOptions& immutable_db_options() const {
    return immutable_db_options_;
  }

  // Cancel all background jobs, including flush, compaction, background
  // purging, stats dumping threads, etc. If `wait` = true, wait for the
  // running jobs to abort or finish before returning. Otherwise, only
  // sends the signals.
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

  // Un-reference the super version and clean it up if it is the last reference.
  void CleanupSuperVersion(SuperVersion* sv);

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
  std::unique_ptr<ColumnFamilyHandle> GetColumnFamilyHandleUnlocked(
      uint32_t column_family_id);

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
    std::string name_;
    bool unprepared_;

    struct BatchInfo {
      uint64_t log_number_;
      // TODO(lth): For unprepared, the memory usage here can be big for
      // unprepared transactions. This is only useful for rollbacks, and we
      // can in theory just keep keyset for that.
      WriteBatch* batch_;
      // Number of sub-batches. A new sub-batch is created if txn attempts to
      // insert a duplicate key,seq to memtable. This is currently used in
      // WritePreparedTxn/WriteUnpreparedTxn.
      size_t batch_cnt_;
    };

    // This maps the seq of the first key in the batch to BatchInfo, which
    // contains WriteBatch and other information relevant to the batch.
    //
    // For WriteUnprepared, batches_ can have size greater than 1, but for
    // other write policies, it must be of size 1.
    std::map<SequenceNumber, BatchInfo> batches_;

    explicit RecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch, SequenceNumber seq,
                                  size_t batch_cnt, bool unprepared)
        : name_(name), unprepared_(unprepared) {
      batches_[seq] = {log, batch, batch_cnt};
    }

    ~RecoveredTransaction() {
      for (auto& it : batches_) {
        delete it.second.batch_;
      }
    }

    void AddBatch(SequenceNumber seq, uint64_t log_number, WriteBatch* batch,
                  size_t batch_cnt, bool unprepared) {
      assert(batches_.count(seq) == 0);
      batches_[seq] = {log_number, batch, batch_cnt};
      // Prior state must be unprepared, since the prepare batch must be the
      // last batch.
      assert(unprepared_);
      unprepared_ = unprepared;
    }
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
                                  WriteBatch* batch, SequenceNumber seq,
                                  size_t batch_cnt, bool unprepared_batch) {
    // For WriteUnpreparedTxn, InsertRecoveredTransaction is called multiple
    // times for every unprepared batch encountered during recovery.
    //
    // If the transaction is prepared, then the last call to
    // InsertRecoveredTransaction will have unprepared_batch = false.
    auto rtxn = recovered_transactions_.find(name);
    if (rtxn == recovered_transactions_.end()) {
      recovered_transactions_[name] = new RecoveredTransaction(
          log, name, batch, seq, batch_cnt, unprepared_batch);
    } else {
      rtxn->second->AddBatch(seq, log, batch, batch_cnt, unprepared_batch);
    }
    logs_with_prep_tracker_.MarkLogAsContainingPrepSection(log);
  }

  void DeleteRecoveredTransaction(const std::string& name) {
    auto it = recovered_transactions_.find(name);
    assert(it != recovered_transactions_.end());
    auto* trx = it->second;
    recovered_transactions_.erase(it);
    for (const auto& info : trx->batches_) {
      logs_with_prep_tracker_.MarkLogAsHavingPrepSectionFlushed(
          info.second.log_number_);
    }
    delete trx;
  }

  void DeleteAllRecoveredTransactions() {
    for (auto it = recovered_transactions_.begin();
         it != recovered_transactions_.end(); ++it) {
      delete it->second;
    }
    recovered_transactions_.clear();
  }

  void AddToLogsToFreeQueue(log::Writer* log_writer) {
    mutex_.AssertHeld();
    logs_to_free_queue_.push_back(log_writer);
  }

  void AddSuperVersionsToFreeQueue(SuperVersion* sv) {
    superversions_to_free_queue_.push_back(sv);
  }

  void SetSnapshotChecker(SnapshotChecker* snapshot_checker);

  // Fill JobContext with snapshot information needed by flush and compaction.
  void GetSnapshotContext(JobContext* job_context,
                          std::vector<SequenceNumber>* snapshot_seqs,
                          SequenceNumber* earliest_write_conflict_snapshot,
                          SnapshotChecker** snapshot_checker);

  // Not thread-safe.
  void SetRecoverableStatePreReleaseCallback(PreReleaseCallback* callback);

  InstrumentedMutex* mutex() const { return &mutex_; }

  // Initialize a brand new DB. The DB directory is expected to be empty before
  // calling it. Push new manifest file name into `new_filenames`.
  Status NewDB(std::vector<std::string>* new_filenames);

  // This is to be used only by internal rocksdb classes.
  static Status Open(const DBOptions& db_options, const std::string& name,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
                     const bool seq_per_batch, const bool batch_per_txn,
                     const bool is_retry, bool* can_retry);

  static IOStatus CreateAndNewDirectory(
      FileSystem* fs, const std::string& dirname,
      std::unique_ptr<FSDirectory>* directory);

  // find stats map from stats_history_ with smallest timestamp in
  // the range of [start_time, end_time)
  bool FindStatsByTime(uint64_t start_time, uint64_t end_time,
                       uint64_t* new_time,
                       std::map<std::string, uint64_t>* stats_map);

  // Print information of all tombstones of all iterators to the std::string
  // This is only used by ldb. The output might be capped. Tombstones
  // printed out are not guaranteed to be in any order.
  Status TablesRangeTombstoneSummary(ColumnFamilyHandle* column_family,
                                     int max_entries_to_print,
                                     std::string* out_str);

  VersionSet* GetVersionSet() const { return versions_.get(); }

  Status WaitForCompact(
      const WaitForCompactOptions& wait_for_compact_options) override;

#ifndef NDEBUG
  // Compact any files in the named level that overlap [*begin, *end]
  Status TEST_CompactRange(int level, const Slice* begin, const Slice* end,
                           ColumnFamilyHandle* column_family = nullptr,
                           bool disallow_trivial_move = false);

  Status TEST_SwitchWAL();

  bool TEST_UnableToReleaseOldestLog() { return unable_to_release_oldest_log_; }

  bool TEST_IsLogGettingFlushed() {
    return alive_log_files_.begin()->getting_flushed;
  }

  Status TEST_SwitchMemtable(ColumnFamilyData* cfd = nullptr);

  // Force current memtable contents to be flushed.
  Status TEST_FlushMemTable(bool wait = true, bool allow_write_stall = false,
                            ColumnFamilyHandle* cfh = nullptr);

  Status TEST_FlushMemTable(ColumnFamilyData* cfd,
                            const FlushOptions& flush_opts);

  // Flush (multiple) ColumnFamilyData without using ColumnFamilyHandle. This
  // is because in certain cases, we can flush column families, wait for the
  // flush to complete, but delete the column family handle before the wait
  // finishes. For example in CompactRange.
  Status TEST_AtomicFlushMemTables(
      const autovector<ColumnFamilyData*>& provided_candidate_cfds,
      const FlushOptions& flush_opts);

  // Wait for background threads to complete scheduled work.
  Status TEST_WaitForBackgroundWork();

  // Wait for memtable compaction
  Status TEST_WaitForFlushMemTable(ColumnFamilyHandle* column_family = nullptr);

  Status TEST_WaitForCompact();
  Status TEST_WaitForCompact(
      const WaitForCompactOptions& wait_for_compact_options);

  // Wait for any background purge
  Status TEST_WaitForPurge();

  // Get the background error status
  Status TEST_GetBGError();

  bool TEST_IsRecoveryInProgress();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  uint64_t TEST_MaxNextLevelOverlappingBytes(
      ColumnFamilyHandle* column_family = nullptr);

  // Return the current manifest file no.
  uint64_t TEST_Current_Manifest_FileNo();

  // Returns the number that'll be assigned to the next file that's created.
  uint64_t TEST_Current_Next_FileNo();

  // get total level0 file size. Only for testing.
  uint64_t TEST_GetLevel0TotalSize();

  void TEST_GetFilesMetaData(
      ColumnFamilyHandle* column_family,
      std::vector<std::vector<FileMetaData>>* metadata,
      std::vector<std::shared_ptr<BlobFileMetaData>>* blob_metadata = nullptr);

  void TEST_LockMutex();

  void TEST_UnlockMutex();

  InstrumentedMutex* TEST_Mutex() { return &mutex_; }

  void TEST_SignalAllBgCv();

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

  void TEST_GetAllBlockCaches(std::unordered_set<const Cache*>* cache_set);

  // Return the lastest MutableCFOptions of a column family
  Status TEST_GetLatestMutableCFOptions(ColumnFamilyHandle* column_family,
                                        MutableCFOptions* mutable_cf_options);

  Cache* TEST_table_cache() { return table_cache_.get(); }

  WriteController& TEST_write_controler() { return write_controller_; }

  uint64_t TEST_FindMinLogContainingOutstandingPrep();
  uint64_t TEST_FindMinPrepLogReferencedByMemTable();
  size_t TEST_PreparedSectionCompletedSize();
  size_t TEST_LogsWithPrepSize();

  int TEST_BGCompactionsAllowed() const;
  int TEST_BGFlushesAllowed() const;
  size_t TEST_GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;
  void TEST_WaitForPeriodicTaskRun(std::function<void()> callback) const;
  SeqnoToTimeMapping TEST_GetSeqnoToTimeMapping() const;
  const autovector<uint64_t>& TEST_GetFilesToQuarantine() const;
  size_t TEST_EstimateInMemoryStatsHistorySize() const;

  uint64_t TEST_GetCurrentLogNumber() const {
    InstrumentedMutexLock l(mutex());
    assert(!logs_.empty());
    return logs_.back().number;
  }

  void TEST_DeleteObsoleteFiles();

  const std::unordered_set<uint64_t>& TEST_GetFilesGrabbedForPurge() const {
    return files_grabbed_for_purge_;
  }

  const PeriodicTaskScheduler& TEST_GetPeriodicTaskScheduler() const;

  static Status TEST_ValidateOptions(const DBOptions& db_options) {
    return ValidateOptions(db_options);
  }
#endif  // NDEBUG

  // In certain configurations, verify that the table/blob file cache only
  // contains entries for live files, to check for effective leaks of open
  // files. This can only be called when purging of obsolete files has
  // "settled," such as during parts of DB Close().
  void TEST_VerifyNoObsoleteFilesCached(bool db_mutex_already_held) const;

  // persist stats to column family "_persistent_stats"
  void PersistStats();

  // dump rocksdb.stats to LOG
  void DumpStats();

  // flush LOG out of application buffer
  void FlushInfoLog();

  // record current sequence number to time mapping. If
  // populate_historical_seconds > 0 then pre-populate all the
  // sequence numbers from [1, last] to map to [now minus
  // populate_historical_seconds, now].
  void RecordSeqnoToTimeMapping(uint64_t populate_historical_seconds);

  // Everytime DB's seqno to time mapping changed (which already hold the db
  // mutex), we install a new SuperVersion in each column family with a shared
  // copy of the new mapping while holding the db mutex.
  // This is done for all column families even though the column family does not
  // explicitly enabled the
  // `preclude_last_level_data_seconds` or `preserve_internal_time_seconds`
  // features.
  // This mapping supports iterators to fulfill the
  // "rocksdb.iterator.write-time" iterator property for entries in memtables.
  //
  // Since this new SuperVersion doesn't involve an LSM tree shape change, we
  // don't schedule work after installing this SuperVersion. It returns the used
  // `SuperVersionContext` for clean up after release mutex.
  void InstallSeqnoToTimeMappingInSV(
      std::vector<SuperVersionContext>* sv_contexts);

  // Interface to block and signal the DB in case of stalling writes by
  // WriteBufferManager. Each DBImpl object contains ptr to WBMStallInterface.
  // When DB needs to be blocked or signalled by WriteBufferManager,
  // state_ is changed accordingly.
  class WBMStallInterface : public StallInterface {
   public:
    enum State {
      BLOCKED = 0,
      RUNNING,
    };

    WBMStallInterface() : state_cv_(&state_mutex_) {
      MutexLock lock(&state_mutex_);
      state_ = State::RUNNING;
    }

    void SetState(State state) {
      MutexLock lock(&state_mutex_);
      state_ = state;
    }

    // Change the state_ to State::BLOCKED and wait until its state is
    // changed by WriteBufferManager. When stall is cleared, Signal() is
    // called to change the state and unblock the DB.
    void Block() override {
      MutexLock lock(&state_mutex_);
      while (state_ == State::BLOCKED) {
        TEST_SYNC_POINT("WBMStallInterface::BlockDB");
        state_cv_.Wait();
      }
    }

    // Called from WriteBufferManager. This function changes the state_
    // to State::RUNNING indicating the stall is cleared and DB can proceed.
    void Signal() override {
      {
        MutexLock lock(&state_mutex_);
        state_ = State::RUNNING;
      }
      state_cv_.Signal();
    }

   private:
    // Conditional variable and mutex to block and
    // signal the DB during stalling process.
    port::Mutex state_mutex_;
    port::CondVar state_cv_;
    // state represting whether DB is running or blocked because of stall by
    // WriteBufferManager.
    State state_;
  };

  static void TEST_ResetDbSessionIdGen();
  static std::string GenerateDbSessionId(Env* env);

  bool seq_per_batch() const { return seq_per_batch_; }

 protected:
  const std::string dbname_;
  // TODO(peterd): unify with VersionSet::db_id_
  std::string db_id_;
  // db_session_id_ is an identifier that gets reset
  // every time the DB is opened
  std::string db_session_id_;
  std::unique_ptr<VersionSet> versions_;
  // Flag to check whether we allocated and own the info log file
  bool own_info_log_;
  Status init_logger_creation_s_;
  const DBOptions initial_db_options_;
  Env* const env_;
  std::shared_ptr<IOTracer> io_tracer_;
  const ImmutableDBOptions immutable_db_options_;
  FileSystemPtr fs_;
  MutableDBOptions mutable_db_options_;
  Statistics* stats_;
  std::unordered_map<std::string, RecoveredTransaction*>
      recovered_transactions_;
  std::unique_ptr<Tracer> tracer_;
  InstrumentedMutex trace_mutex_;
  BlockCacheTracer block_cache_tracer_;

  // constant false canceled flag, used when the compaction is not manual
  const std::atomic<bool> kManualCompactionCanceledFalse_{false};

  // State below is protected by mutex_
  // With two_write_queues enabled, some of the variables that accessed during
  // WriteToWAL need different synchronization: log_empty_, alive_log_files_,
  // logs_, logfile_number_. Refer to the definition of each variable below for
  // more description.
  //
  // `mutex_` can be a hot lock in some workloads, so it deserves dedicated
  // cachelines.
  mutable CacheAlignedInstrumentedMutex mutex_;

  ColumnFamilyHandleImpl* default_cf_handle_;
  InternalStats* default_cf_internal_stats_;

  // table_cache_ provides its own synchronization
  std::shared_ptr<Cache> table_cache_;

  ErrorHandler error_handler_;

  // Unified interface for logging events
  EventLogger event_logger_;

  // only used for dynamically adjusting max_total_wal_size. it is a sum of
  // [write_buffer_size * max_write_buffer_number] over all column families
  std::atomic<uint64_t> max_total_in_memory_state_;

  // The options to access storage files
  const FileOptions file_options_;

  // Additonal options for compaction and flush
  FileOptions file_options_for_compaction_;

  std::unique_ptr<ColumnFamilyMemTablesImpl> column_family_memtables_;

  // Increase the sequence number after writing each batch, whether memtable is
  // disabled for that or not. Otherwise the sequence number is increased after
  // writing each key into memtable. This implies that when disable_memtable is
  // set, the seq is not increased at all.
  //
  // Default: false
  const bool seq_per_batch_;
  // This determines during recovery whether we expect one writebatch per
  // recovered transaction, or potentially multiple writebatches per
  // transaction. For WriteUnprepared, this is set to false, since multiple
  // batches can exist per transaction.
  //
  // Default: true
  const bool batch_per_txn_;

  // Each flush or compaction gets its own job id. this counter makes sure
  // they're unique
  std::atomic<int> next_job_id_;

  std::atomic<bool> shutting_down_;

  // No new background jobs can be queued if true. This is used to prevent new
  // background jobs from being queued after WaitForCompact() completes waiting
  // all background jobs then attempts to close when close_db_ option is true.
  bool reject_new_background_jobs_;

  // RecoveryContext struct stores the context about version edits along
  // with corresponding column_family_data and column_family_options.
  class RecoveryContext {
   public:
    ~RecoveryContext() {
      for (auto& edit_list : edit_lists_) {
        for (auto* edit : edit_list) {
          delete edit;
        }
      }
    }

    void UpdateVersionEdits(ColumnFamilyData* cfd, const VersionEdit& edit) {
      assert(cfd != nullptr);
      if (map_.find(cfd->GetID()) == map_.end()) {
        uint32_t size = static_cast<uint32_t>(map_.size());
        map_.emplace(cfd->GetID(), size);
        cfds_.emplace_back(cfd);
        mutable_cf_opts_.emplace_back(cfd->GetLatestMutableCFOptions());
        edit_lists_.emplace_back(autovector<VersionEdit*>());
      }
      uint32_t i = map_[cfd->GetID()];
      edit_lists_[i].emplace_back(new VersionEdit(edit));
    }

    std::unordered_map<uint32_t, uint32_t> map_;  // cf_id to index;
    autovector<ColumnFamilyData*> cfds_;
    autovector<const MutableCFOptions*> mutable_cf_opts_;
    autovector<autovector<VersionEdit*>> edit_lists_;
    // All existing data files (SST files and Blob files) found during DB::Open.
    std::vector<std::string> existing_data_files_;
    bool is_new_db_ = false;
  };

  // Persist options to options file. Must be holding options_mutex_.
  // Will lock DB mutex if !db_mutex_already_held.
  Status WriteOptionsFile(const WriteOptions& write_options,
                          bool db_mutex_already_held);

  Status CompactRangeInternal(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end,
                              const std::string& trim_ts);

  // The following two functions can only be called when:
  // 1. WriteThread::Writer::EnterUnbatched() is used.
  // 2. db_mutex is NOT held
  Status RenameTempFileToOptionsFile(const std::string& file_name,
                                     bool is_remote_compaction_enabled);
  Status DeleteObsoleteOptionsFiles();

  void NotifyOnManualFlushScheduled(autovector<ColumnFamilyData*> cfds,
                                    FlushReason flush_reason);

  void NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                          const MutableCFOptions& mutable_cf_options,
                          int job_id, FlushReason flush_reason);

  void NotifyOnFlushCompleted(
      ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
      std::list<std::unique_ptr<FlushJobInfo>>* flush_jobs_info);

  void NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                               const Status& st,
                               const CompactionJobStats& job_stats, int job_id);

  void NotifyOnCompactionCompleted(ColumnFamilyData* cfd, Compaction* c,
                                   const Status& st,
                                   const CompactionJobStats& job_stats,
                                   int job_id);
  void NotifyOnMemTableSealed(ColumnFamilyData* cfd,
                              const MemTableInfo& mem_table_info);

  void NotifyOnExternalFileIngested(
      ColumnFamilyData* cfd, const ExternalSstFileIngestionJob& ingestion_job);

  Status FlushAllColumnFamilies(const FlushOptions& flush_options,
                                FlushReason flush_reason);

  virtual Status FlushForGetLiveFiles();

  void NewThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusDbInfo() const;

  // For CFs that has updates in `wbwi`, their memtable will be switched,
  // and `wbwi` will be added as the latest immutable memtable.
  //
  // REQUIRES: this thread is currently at the front of the main writer queue.
  // @param prep_log refers to the WAL that contains prepare record
  // for the transaction based on wbwi.
  // @param assigned_seqno Sequence numbers for the ingested memtable.
  // @param last_seqno the value of versions_->LastSequence() after the write
  // ingests `wbwi` is done.
  // @param memtable_updated Whether the same write that ingests wbwi has
  // updated memtable. This is useful for determining whether to set bg
  // error when IngestWBWI fails.
  Status IngestWBWI(std::shared_ptr<WriteBatchWithIndex> wbwi,
                    const WBWIMemTable::SeqnoRange& assigned_seqno,
                    uint64_t min_prep_log, SequenceNumber last_seqno,
                    bool memtable_updated, bool ignore_missing_cf);

  // If disable_memtable is set the application logic must guarantee that the
  // batch will still be skipped from memtable during the recovery. An excption
  // to this is seq_per_batch_ mode, in which since each batch already takes one
  // seq, it is ok for the batch to write to memtable during recovery as long as
  // it only takes one sequence number: i.e., no duplicate keys.
  // In WriteCommitted it is guarnateed since disable_memtable is used for
  // prepare batch which will be written to memtable later during the commit,
  // and in WritePrepared it is guaranteed since it will be used only for WAL
  // markers which will never be written to memtable. If the commit marker is
  // accompanied with CommitTimeWriteBatch that is not written to memtable as
  // long as it has no duplicate keys, it does not violate the one-seq-per-batch
  // policy.
  // batch_cnt is expected to be non-zero in seq_per_batch mode and
  // indicates the number of sub-patches. A sub-patch is a subset of the write
  // batch that does not have duplicate keys.
  // `callback` is called before WAL write.
  // See more in comment above WriteCallback::Callback().
  // pre_release_callback is called after WAL write and before memtable write.
  // See more in comment above PreReleaseCallback::Callback().
  // post_memtable_callback is called after memtable write but before publishing
  // the sequence number to readers.
  //
  // The main write queue. This is the only write queue that updates
  // LastSequence. When using one write queue, the same sequence also indicates
  // the last published sequence.
  Status WriteImpl(const WriteOptions& options, WriteBatch* updates,
                   WriteCallback* callback = nullptr,
                   UserWriteCallback* user_write_cb = nullptr,
                   uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                   bool disable_memtable = false, uint64_t* seq_used = nullptr,
                   size_t batch_cnt = 0,
                   PreReleaseCallback* pre_release_callback = nullptr,
                   PostMemTableCallback* post_memtable_callback = nullptr,
                   std::shared_ptr<WriteBatchWithIndex> wbwi = nullptr,
                   uint64_t min_prep_log = 0);

  Status PipelinedWriteImpl(const WriteOptions& options, WriteBatch* updates,
                            WriteCallback* callback = nullptr,
                            UserWriteCallback* user_write_cb = nullptr,
                            uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                            bool disable_memtable = false,
                            uint64_t* seq_used = nullptr);

  // Write only to memtables without joining any write queue
  Status UnorderedWriteMemtable(const WriteOptions& write_options,
                                WriteBatch* my_batch, WriteCallback* callback,
                                uint64_t log_ref, SequenceNumber seq,
                                const size_t sub_batch_cnt);

  // Whether the batch requires to be assigned with an order
  enum AssignOrder : bool { kDontAssignOrder, kDoAssignOrder };
  // Whether it requires publishing last sequence or not
  enum PublishLastSeq : bool { kDontPublishLastSeq, kDoPublishLastSeq };

  // Join the write_thread to write the batch only to the WAL. It is the
  // responsibility of the caller to also write the write batch to the memtable
  // if it required.
  //
  // sub_batch_cnt is expected to be non-zero when assign_order = kDoAssignOrder
  // indicating the number of sub-batches in my_batch. A sub-patch is a subset
  // of the write batch that does not have duplicate keys. When seq_per_batch is
  // not set, each key is a separate sub_batch. Otherwise each duplicate key
  // marks start of a new sub-batch.
  Status WriteImplWALOnly(
      WriteThread* write_thread, const WriteOptions& options,
      WriteBatch* updates, WriteCallback* callback,
      UserWriteCallback* user_write_cb, uint64_t* log_used,
      const uint64_t log_ref, uint64_t* seq_used, const size_t sub_batch_cnt,
      PreReleaseCallback* pre_release_callback, const AssignOrder assign_order,
      const PublishLastSeq publish_last_seq, const bool disable_memtable);

  // write cached_recoverable_state_ to memtable if it is not empty
  // The writer must be the leader in write_thread_ and holding mutex_
  Status WriteRecoverableState();

  // Actual implementation of Close()
  virtual Status CloseImpl();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  // recovered_seq is set to less than kMaxSequenceNumber if the log's tail is
  // skipped.
  // recovery_ctx stores the context about version edits and all those
  // edits are persisted to new Manifest after successfully syncing the new WAL.
  virtual Status Recover(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      bool read_only = false, bool error_if_wal_file_exists = false,
      bool error_if_data_exists_in_wals = false, bool is_retry = false,
      uint64_t* recovered_seq = nullptr,
      RecoveryContext* recovery_ctx = nullptr, bool* can_retry = nullptr);

  virtual bool OwnTablesAndLogs() const { return true; }

  // Read/create DB identity file (as appropriate), and write DB ID to
  // version_edit if provided.
  Status SetupDBId(const WriteOptions& write_options, bool read_only,
                   bool is_new_db, bool is_retry, VersionEdit* version_edit);
  // Assign db_id_ and write DB ID to version_edit if provided.
  void SetDBId(std::string&& id, bool read_only, VersionEdit* version_edit);

  // Collect a deduplicated collection of paths used by this DB, including
  // dbname_, DBOptions.db_paths, ColumnFamilyOptions.cf_paths.
  std::set<std::string> CollectAllDBPaths();

  // REQUIRES: db mutex held when calling this function, but the db mutex can
  // be released and re-acquired. Db mutex will be held when the function
  // returns.
  // It stores all existing data files (SST and Blob) in RecoveryContext. In
  // the meantime, we find out the largest file number present in the paths, and
  // bump up the version set's next_file_number_ to be 1 + largest_file_number.
  // recovery_ctx stores the context about version edits. All those edits are
  // persisted to new Manifest after successfully syncing the new WAL.
  Status MaybeUpdateNextFileNumber(RecoveryContext* recovery_ctx);

  // Track existing data files, including both referenced and unreferenced SST
  // and Blob files in SstFileManager. This is only called during DB::Open and
  // it's called before any file deletion start so that their deletion can be
  // properly rate limited.
  // Files may not be referenced in the MANIFEST because (e.g.
  // 1. It's best effort recovery;
  // 2. The VersionEdits referencing the SST files are appended to
  // RecoveryContext, DB crashes when syncing the MANIFEST, the VersionEdits are
  // still not synced to MANIFEST during recovery.)
  //
  // If the file is referenced in Manifest (typically that's the
  // vast majority of all files), since it already has the file size
  // on record, we don't need to query the file system. Otherwise, we query the
  // file system for the size of an unreferenced file.
  // REQUIRES: mutex unlocked
  void TrackExistingDataFiles(
      const std::vector<std::string>& existing_data_files);

  // Untrack data files in sst manager. This is only called during DB::Close on
  // an unowned SstFileManager, to return it to a consistent state.
  // REQUIRES: mutex unlocked
  void UntrackDataFiles();

  // SetDbSessionId() should be called in the constuctor DBImpl()
  // to ensure that db_session_id_ gets updated every time the DB is opened
  void SetDbSessionId();

  Status FailIfCfHasTs(const ColumnFamilyHandle* column_family) const;
  Status FailIfTsMismatchCf(ColumnFamilyHandle* column_family,
                            const Slice& ts) const;

  // Check that the read timestamp `ts` is at or above the `full_history_ts_low`
  // timestamp in a `SuperVersion`. It's necessary to do this check after
  // grabbing the SuperVersion. If the check passed, the referenced SuperVersion
  // this read holds on to can ensure the read won't be affected if
  // `full_history_ts_low` is increased concurrently, and this achieves that
  // without explicitly locking by piggybacking the SuperVersion.
  Status FailIfReadCollapsedHistory(const ColumnFamilyData* cfd,
                                    const SuperVersion* sv,
                                    const Slice& ts) const;

  // recovery_ctx stores the context about version edits and
  // LogAndApplyForRecovery persist all those edits to new Manifest after
  // successfully syncing new WAL.
  // LogAndApplyForRecovery should be called only once during recovery and it
  // should be called when RocksDB writes to a first new MANIFEST since this
  // recovery.
  Status LogAndApplyForRecovery(const RecoveryContext& recovery_ctx);

  void InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap();

  // Return true to proceed with current WAL record whose content is stored in
  // `batch`. Return false to skip current WAL record.
  bool InvokeWalFilterIfNeededOnWalRecord(uint64_t wal_number,
                                          const std::string& wal_fname,
                                          log::Reader::Reporter& reporter,
                                          Status& status, bool& stop_replay,
                                          WriteBatch& batch);

 private:
  friend class DB;
  friend class ErrorHandler;
  friend class InternalStats;
  friend class PessimisticTransaction;
  friend class TransactionBaseImpl;
  friend class WriteCommittedTxn;
  friend class WritePreparedTxn;
  friend class WritePreparedTxnDB;
  friend class WriteBatchWithIndex;
  friend class WriteUnpreparedTxnDB;
  friend class WriteUnpreparedTxn;

  friend class ForwardIterator;
  friend struct SuperVersion;
  friend class CompactedDBImpl;
  friend class DBImplFollower;
#ifndef NDEBUG
  friend class DBTest_ConcurrentFlushWAL_Test;
  friend class DBTest_MixedSlowdownOptionsStop_Test;
  friend class DBCompactionTest_CompactBottomLevelFilesWithDeletions_Test;
  friend class DBCompactionTest_CompactionDuringShutdown_Test;
  friend class DBCompactionTest_DelayCompactBottomLevelFilesWithDeletions_Test;
  friend class DBCompactionTest_DisableCompactBottomLevelFiles_Test;
  friend class StatsHistoryTest_PersistentStatsCreateColumnFamilies_Test;
  friend class DBTest2_ReadCallbackTest_Test;
  friend class WriteCallbackPTest_WriteWithCallbackTest_Test;
  friend class XFTransactionWriteHandler;
  friend class DBBlobIndexTest;
  friend class WriteUnpreparedTransactionTest_RecoveryTest_Test;
  friend class CompactionServiceTest_PreservedOptionsLocalCompaction_Test;
  friend class CompactionServiceTest_PreservedOptionsRemoteCompaction_Test;
#endif

  struct CompactionState;
  struct PrepickedCompaction;
  struct PurgeFileInfo;

  struct WriteContext {
    SuperVersionContext superversion_context;
    autovector<ReadOnlyMemTable*> memtables_to_free_;

    explicit WriteContext(bool create_superversion = false)
        : superversion_context(create_superversion) {}

    ~WriteContext() {
      superversion_context.Clean();
      for (auto& m : memtables_to_free_) {
        delete m;
      }
    }
  };

  struct LogFileNumberSize {
    explicit LogFileNumberSize(uint64_t _number) : number(_number) {}
    LogFileNumberSize() {}
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
    Status ClearWriter() {
      Status s;
      if (writer->file()) {
        // TODO: plumb Env::IOActivity, Env::IOPriority
        s = writer->WriteBuffer(WriteOptions());
      }
      delete writer;
      writer = nullptr;
      return s;
    }

    bool IsSyncing() { return getting_synced; }

    uint64_t GetPreSyncSize() {
      assert(getting_synced);
      return pre_sync_size;
    }

    void PrepareForSync() {
      assert(!getting_synced);
      // Ensure the head of logs_ is marked as getting_synced if any is.
      getting_synced = true;
      // If last sync failed on a later WAL, this could be a fully synced
      // and closed WAL that just needs to be recorded as synced in the
      // manifest.
      if (writer->file()) {
        // Size is expected to be monotonically increasing.
        assert(writer->file()->GetFlushedSize() >= pre_sync_size);
        pre_sync_size = writer->file()->GetFlushedSize();
      }
    }

    void FinishSync() {
      assert(getting_synced);
      getting_synced = false;
    }

    uint64_t number;
    // Visual Studio doesn't support deque's member to be noncopyable because
    // of a std::unique_ptr as a member.
    log::Writer* writer;  // own

   private:
    // true for some prefix of logs_
    bool getting_synced = false;
    // The size of the file before the sync happens. This amount is guaranteed
    // to be persisted even if appends happen during sync so it can be used for
    // tracking the synced size in MANIFEST.
    uint64_t pre_sync_size = 0;
  };

  struct LogContext {
    explicit LogContext(bool need_sync = false)
        : need_log_sync(need_sync), need_log_dir_sync(need_sync) {}
    bool need_log_sync = false;
    bool need_log_dir_sync = false;
    log::Writer* writer = nullptr;
    LogFileNumberSize* log_file_number_size = nullptr;
  };

  // PurgeFileInfo is a structure to hold information of files to be deleted in
  // purge_files_
  struct PurgeFileInfo {
    std::string fname;
    std::string dir_to_sync;
    FileType type;
    uint64_t number;
    int job_id;
    PurgeFileInfo(std::string fn, std::string d, FileType t, uint64_t num,
                  int jid)
        : fname(fn), dir_to_sync(d), type(t), number(num), job_id(jid) {}
  };

  // Argument required by background flush thread.
  struct BGFlushArg {
    BGFlushArg()
        : cfd_(nullptr),
          max_memtable_id_(0),
          superversion_context_(nullptr),
          flush_reason_(FlushReason::kOthers) {}
    BGFlushArg(ColumnFamilyData* cfd, uint64_t max_memtable_id,
               SuperVersionContext* superversion_context,
               FlushReason flush_reason)
        : cfd_(cfd),
          max_memtable_id_(max_memtable_id),
          superversion_context_(superversion_context),
          flush_reason_(flush_reason) {}

    // Column family to flush.
    ColumnFamilyData* cfd_;
    // Maximum ID of memtable to flush. In this column family, memtables with
    // IDs smaller than this value must be flushed before this flush completes.
    uint64_t max_memtable_id_;
    // Pointer to a SuperVersionContext object. After flush completes, RocksDB
    // installs a new superversion for the column family. This operation
    // requires a SuperVersionContext object (currently embedded in JobContext).
    SuperVersionContext* superversion_context_;
    FlushReason flush_reason_;
  };

  // Argument passed to flush thread.
  struct FlushThreadArg {
    DBImpl* db_;

    Env::Priority thread_pri_;
  };

  // Information for a manual compaction
  struct ManualCompactionState {
    ManualCompactionState(ColumnFamilyData* _cfd, int _input_level,
                          int _output_level, uint32_t _output_path_id,
                          bool _exclusive, bool _disallow_trivial_move,
                          std::atomic<bool>* _canceled)
        : cfd(_cfd),
          input_level(_input_level),
          output_level(_output_level),
          output_path_id(_output_path_id),
          exclusive(_exclusive),
          disallow_trivial_move(_disallow_trivial_move),
          canceled(_canceled ? *_canceled : canceled_internal_storage) {}
    // When _canceled is not provided by ther user, we assign the reference of
    // canceled_internal_storage to it to consolidate canceled and
    // manual_compaction_paused since DisableManualCompaction() might be
    // called

    ColumnFamilyData* cfd;
    int input_level;
    int output_level;
    uint32_t output_path_id;
    Status status;
    bool done = false;
    bool in_progress = false;    // compaction request being processed?
    bool incomplete = false;     // only part of requested range compacted
    bool exclusive;              // current behavior of only one manual
    bool disallow_trivial_move;  // Force actual compaction to run
    const InternalKey* begin = nullptr;  // nullptr means beginning of key range
    const InternalKey* end = nullptr;    // nullptr means end of key range
    InternalKey* manual_end = nullptr;   // how far we are compacting
    InternalKey tmp_storage;      // Used to keep track of compaction progress
    InternalKey tmp_storage1;     // Used to keep track of compaction progress

    // When the user provides a canceled pointer in CompactRangeOptions, the
    // above varaibe is the reference of the user-provided
    // `canceled`, otherwise, it is the reference of canceled_internal_storage
    std::atomic<bool> canceled_internal_storage = false;
    std::atomic<bool>& canceled;  // Compaction canceled pointer reference
  };
  struct PrepickedCompaction {
    // background compaction takes ownership of `compaction`.
    Compaction* compaction;
    // caller retains ownership of `manual_compaction_state` as it is reused
    // across background compactions.
    ManualCompactionState* manual_compaction_state;  // nullptr if non-manual
    // task limiter token is requested during compaction picking.
    std::unique_ptr<TaskLimiterToken> task_token;
  };

  struct CompactionArg {
    // caller retains ownership of `db`.
    DBImpl* db;
    // background compaction takes ownership of `prepicked_compaction`.
    PrepickedCompaction* prepicked_compaction;
    Env::Priority compaction_pri_;
  };

  static bool IsRecoveryFlush(FlushReason flush_reason) {
    return flush_reason == FlushReason::kErrorRecoveryRetryFlush ||
           flush_reason == FlushReason::kErrorRecovery;
  }
  // Initialize the built-in column family for persistent stats. Depending on
  // whether on-disk persistent stats have been enabled before, it may either
  // create a new column family and column family handle or just a column family
  // handle.
  // Required: DB mutex held
  Status InitPersistStatsColumnFamily();

  // Persistent Stats column family has two format version key which are used
  // for compatibility check. Write format version if it's created for the
  // first time, read format version and check compatibility if recovering
  // from disk. This function requires DB mutex held at entrance but may
  // release and re-acquire DB mutex in the process.
  // Required: DB mutex held
  Status PersistentStatsProcessFormatVersion();

  Status ResumeImpl(DBRecoverContext context);

  void MaybeIgnoreError(Status* s) const;

  const Status CreateArchivalDirectory();

  // Create a column family, without some of the follow-up work yet
  Status CreateColumnFamilyImpl(const ReadOptions& read_options,
                                const WriteOptions& write_options,
                                const ColumnFamilyOptions& cf_options,
                                const std::string& cf_name,
                                ColumnFamilyHandle** handle);

  // Follow-up work to user creating a column family or (families)
  Status WrapUpCreateColumnFamilies(
      const ReadOptions& read_options, const WriteOptions& write_options,
      const std::vector<const ColumnFamilyOptions*>& cf_options);

  Status DropColumnFamilyImpl(ColumnFamilyHandle* column_family);

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();
  // Delete obsolete files and log status and information of file deletion
  void DeleteObsoleteFileImpl(int job_id, const std::string& fname,
                              const std::string& path_to_sync, FileType type,
                              uint64_t number);

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
  void ReleaseFileNumberFromPendingOutputs(
      std::unique_ptr<std::list<uint64_t>::iterator>& v);

  // Similar to pending_outputs, preserve OPTIONS file. Used for remote
  // compaction.
  std::list<uint64_t>::iterator CaptureOptionsFileNumber();
  void ReleaseOptionsFileNumber(
      std::unique_ptr<std::list<uint64_t>::iterator>& v);

  // Sets bg error if there is an error writing to WAL.
  IOStatus SyncClosedWals(const WriteOptions& write_options,
                          JobContext* job_context, VersionEdit* synced_wals,
                          bool error_recovery_in_prog);

  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful. Then
  // installs a new super version for the column family.
  Status FlushMemTableToOutputFile(
      ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
      bool* madeProgress, JobContext* job_context, FlushReason flush_reason,
      SuperVersionContext* superversion_context,
      std::vector<SequenceNumber>& snapshot_seqs,
      SequenceNumber earliest_write_conflict_snapshot,
      SnapshotChecker* snapshot_checker, LogBuffer* log_buffer,
      Env::Priority thread_pri);

  // Flush the memtables of (multiple) column families to multiple files on
  // persistent storage.
  Status FlushMemTablesToOutputFiles(
      const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
      JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri);

  Status AtomicFlushMemTablesToOutputFiles(
      const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
      JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri);

  // REQUIRES: log_numbers are sorted in ascending order
  // corrupted_log_found is set to true if we recover from a corrupted log file.
  Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                         SequenceNumber* next_sequence, bool read_only,
                         bool is_retry, bool* corrupted_log_found,
                         RecoveryContext* recovery_ctx);

  // The following two methods are used to flush a memtable to
  // storage. The first one is used at database RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(int job_id, ColumnFamilyData* cfd,
                                     MemTable* mem, VersionEdit* edit);

  // Get the size of a log file and, if truncate is true, truncate the
  // log file to its actual size, thereby freeing preallocated space.
  // Return success even if truncate fails
  Status GetLogSizeAndMaybeTruncate(uint64_t wal_number, bool truncate,
                                    LogFileNumberSize* log);

  // Restore alive_log_files_ and total_log_size_ after recovery.
  // It needs to run only when there's no flush during recovery
  // (e.g. avoid_flush_during_recovery=true). May also trigger flush
  // in case total_log_size > max_total_wal_size.
  Status RestoreAliveLogFiles(const std::vector<uint64_t>& log_numbers);

  // num_bytes: for slowdown case, delay time is calculated based on
  //            `num_bytes` going through.
  Status DelayWrite(uint64_t num_bytes, WriteThread& write_thread,
                    const WriteOptions& write_options);

  // Begin stalling of writes when memory usage increases beyond a certain
  // threshold.
  void WriteBufferManagerStallWrites();

  Status ThrottleLowPriWritesIfNeeded(const WriteOptions& write_options,
                                      WriteBatch* my_batch);

  // REQUIRES: mutex locked and in write thread.
  Status ScheduleFlushes(WriteContext* context);

  void MaybeFlushStatsCF(autovector<ColumnFamilyData*>* cfds);

  Status TrimMemtableHistory(WriteContext* context);

  // Switches the current live memtable to immutable/read-only memtable.
  // A new WAL is created if the current WAL is not empty.
  // If `new_imm` is not nullptr, it will be added as the newest immutable
  // memtable, if and only if OK status is returned.
  // `last_seqno` needs to be provided if `new_imm` is not nullptr. It is
  // the value of versions_->LastSequence() after the write that ingests new_imm
  // is done.
  //
  // REQUIRES: mutex_ is held
  // REQUIRES: this thread is currently at the front of the writer queue
  // REQUIRES: this thread is currently at the front of the 2nd writer queue if
  // two_write_queues_ is true (This is to simplify the reasoning.)
  Status SwitchMemtable(ColumnFamilyData* cfd, WriteContext* context,
                        ReadOnlyMemTable* new_imm = nullptr,
                        SequenceNumber last_seqno = 0);

  // Select and output column families qualified for atomic flush in
  // `selected_cfds`. If `provided_candidate_cfds` is non-empty, it will be used
  // as candidate CFs to select qualified ones from. Otherwise, all column
  // families are used as candidate to select from.
  //
  // REQUIRES: mutex held
  void SelectColumnFamiliesForAtomicFlush(
      autovector<ColumnFamilyData*>* selected_cfds,
      const autovector<ColumnFamilyData*>& provided_candidate_cfds = {},
      FlushReason flush_reason = FlushReason::kOthers);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(ColumnFamilyData* cfd, const FlushOptions& options,
                       FlushReason flush_reason,
                       bool entered_write_thread = false);

  // Atomic-flush memtables from quanlified CFs among `provided_candidate_cfds`
  // (if non-empty) or amomg all column families and atomically record the
  // result to the MANIFEST.
  Status AtomicFlushMemTables(
      const FlushOptions& options, FlushReason flush_reason,
      const autovector<ColumnFamilyData*>& provided_candidate_cfds = {},
      bool entered_write_thread = false);

  Status RetryFlushesForErrorRecovery(FlushReason flush_reason, bool wait);

  // Wait until flushing this column family won't stall writes
  Status WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                           bool* flush_needed);

  // Wait for memtable flushed.
  // If flush_memtable_id is non-null, wait until the memtable with the ID
  // gets flush. Otherwise, wait until the column family don't have any
  // memtable pending flush.
  // resuming_from_bg_err indicates whether the caller is attempting to resume
  // from background error.
  Status WaitForFlushMemTable(
      ColumnFamilyData* cfd, const uint64_t* flush_memtable_id = nullptr,
      bool resuming_from_bg_err = false,
      std::optional<FlushReason> flush_reason = std::nullopt) {
    return WaitForFlushMemTables({cfd}, {flush_memtable_id},
                                 resuming_from_bg_err, flush_reason);
  }
  // Wait for memtables to be flushed for multiple column families.
  Status WaitForFlushMemTables(
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const uint64_t*>& flush_memtable_ids,
      bool resuming_from_bg_err, std::optional<FlushReason> flush_reason);

  inline void WaitForPendingWrites() {
    mutex_.AssertHeld();
    TEST_SYNC_POINT("DBImpl::WaitForPendingWrites:BeforeBlock");
    // In case of pipelined write is enabled, wait for all pending memtable
    // writers.
    if (immutable_db_options_.enable_pipelined_write) {
      // Memtable writers may call DB::Get in case max_successive_merges > 0,
      // which may lock mutex. Unlocking mutex here to avoid deadlock.
      mutex_.Unlock();
      write_thread_.WaitForMemTableWriters();
      mutex_.Lock();
    }

    if (immutable_db_options_.unordered_write) {
      // Wait for the ones who already wrote to the WAL to finish their
      // memtable write.
      if (pending_memtable_writes_.load() != 0) {
        // XXX: suspicious wait while holding DB mutex?
        std::unique_lock<std::mutex> guard(switch_mutex_);
        switch_cv_.wait(guard,
                        [&] { return pending_memtable_writes_.load() == 0; });
      }
    } else {
      // (Writes are finished before the next write group starts.)
    }

    // Wait for any LockWAL to clear
    while (lock_wal_count_ > 0) {
      bg_cv_.Wait();
    }
  }

  // TaskType is used to identify tasks in thread-pool, currently only
  // differentiate manual compaction, which could be unscheduled from the
  // thread-pool.
  enum class TaskType : uint8_t {
    kDefault = 0,
    kManualCompaction = 1,
    kCount = 2,
  };

  // Task tag is used to identity tasks in thread-pool, which is
  // dbImpl obj address + type
  inline void* GetTaskTag(TaskType type) {
    return GetTaskTag(static_cast<uint8_t>(type));
  }

  inline void* GetTaskTag(uint8_t type) {
    return static_cast<uint8_t*>(static_cast<void*>(this)) + type;
  }

  // REQUIRES: mutex locked and in write thread.
  void AssignAtomicFlushSeq(const autovector<ColumnFamilyData*>& cfds);

  // REQUIRES: mutex locked and in write thread.
  Status SwitchWAL(WriteContext* write_context);

  // REQUIRES: mutex locked and in write thread.
  Status HandleWriteBufferManagerFlush(WriteContext* write_context);

  // REQUIRES: mutex locked
  Status PreprocessWrite(const WriteOptions& write_options,
                         LogContext* log_context, WriteContext* write_context);

  // Merge write batches in the write group into merged_batch.
  // Returns OK if merge is successful.
  // Returns Corruption if corruption in write batch is detected.
  Status MergeBatch(const WriteThread::WriteGroup& write_group,
                    WriteBatch* tmp_batch, WriteBatch** merged_batch,
                    size_t* write_with_wal, WriteBatch** to_be_cached_state);

  IOStatus WriteToWAL(const WriteBatch& merged_batch,
                      const WriteOptions& write_options,
                      log::Writer* log_writer, uint64_t* log_used,
                      uint64_t* log_size,
                      LogFileNumberSize& log_file_number_size);

  IOStatus WriteToWAL(const WriteThread::WriteGroup& write_group,
                      log::Writer* log_writer, uint64_t* log_used,
                      bool need_log_sync, bool need_log_dir_sync,
                      SequenceNumber sequence,
                      LogFileNumberSize& log_file_number_size);

  IOStatus ConcurrentWriteToWAL(const WriteThread::WriteGroup& write_group,
                                uint64_t* log_used,
                                SequenceNumber* last_sequence, size_t seq_inc);

  // Used by WriteImpl to update bg_error_ if paranoid check is enabled.
  // Caller must hold mutex_.
  void WriteStatusCheckOnLocked(const Status& status);

  // Used by WriteImpl to update bg_error_ if paranoid check is enabled.
  void WriteStatusCheck(const Status& status);

  // Used by WriteImpl to update bg_error_ when IO error happens, e.g., write
  // WAL, sync WAL fails, if paranoid check is enabled.
  void WALIOStatusCheck(const IOStatus& status);

  // Used by WriteImpl to update bg_error_ in case of memtable insert error.
  void MemTableInsertStatusCheck(const Status& memtable_insert_status);

  Status CompactFilesImpl(const CompactionOptions& compact_options,
                          ColumnFamilyData* cfd, Version* version,
                          const std::vector<std::string>& input_file_names,
                          std::vector<std::string>* const output_file_names,
                          const int output_level, int output_path_id,
                          JobContext* job_context, LogBuffer* log_buffer,
                          CompactionJobInfo* compaction_job_info);

  // REQUIRES: mutex unlocked
  void TrackOrUntrackFiles(const std::vector<std::string>& existing_data_files,
                           bool track);

  void MaybeScheduleFlushOrCompaction();

  struct FlushRequest {
    FlushReason flush_reason;
    // A map from column family to flush to largest memtable id to persist for
    // each column family. Once all the memtables whose IDs are smaller than or
    // equal to this per-column-family specified value, this flush request is
    // considered to have completed its work of flushing this column family.
    // After completing the work for all column families in this request, this
    // flush is considered complete.
    std::unordered_map<ColumnFamilyData*, uint64_t>
        cfd_to_max_mem_id_to_persist;

#ifndef NDEBUG
    int reschedule_count = 1;
#endif /* !NDEBUG */
  };

  // In case of atomic flush, generates a `FlushRequest` for the latest atomic
  // cuts for these `cfds`. Atomic cuts are recorded in
  // `AssignAtomicFlushSeq()`. For each entry in `cfds`, all CFDs sharing the
  // same latest atomic cut must also be present.
  //
  // REQUIRES: mutex held
  void GenerateFlushRequest(const autovector<ColumnFamilyData*>& cfds,
                            FlushReason flush_reason, FlushRequest* req);

  // Below functions are for executing flush, compaction in the background. A
  // dequeue is the communication channel between threads that asks for the work
  // to be done and the available threads in the thread pool that pick it up to
  // execute it. We use these terminologies to describe the state of the work
  // and its transitions:
  // 1) It becomes pending once it's successfully enqueued into the
  //    corresponding dequeue, a work in this state is also called unscheduled.
  //    Counter `unscheduled_*_` counts work in this state.
  // 2) When `MaybeScheduleFlushOrCompaction` schedule a thread to run `BGWork*`
  //    for the work, it becomes scheduled
  //    Counter `bg_*_scheduled_` counts work in this state.
  // 3) Once the thread start to execute `BGWork*`, the work is popped from the
  //    dequeue, it is now in running state
  //    Counter `num_running_*_` counts work in this state.
  // 4) Eventually, the work is finished. We don't need to specifically track
  //    finished work.

  // Returns true if `req` is successfully enqueued.
  bool EnqueuePendingFlush(const FlushRequest& req);

  void EnqueuePendingCompaction(ColumnFamilyData* cfd);
  void SchedulePendingPurge(std::string fname, std::string dir_to_sync,
                            FileType type, uint64_t number, int job_id);
  static void BGWorkCompaction(void* arg);
  // Runs a pre-chosen universal compaction involving bottom level in a
  // separate, bottom-pri thread pool.
  static void BGWorkBottomCompaction(void* arg);
  static void BGWorkFlush(void* arg);
  static void BGWorkPurge(void* arg);
  static void UnscheduleCompactionCallback(void* arg);
  static void UnscheduleFlushCallback(void* arg);
  void BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                Env::Priority thread_pri);
  void BackgroundCallFlush(Env::Priority thread_pri);
  void BackgroundCallPurge();
  Status BackgroundCompaction(bool* madeProgress, JobContext* job_context,
                              LogBuffer* log_buffer,
                              PrepickedCompaction* prepicked_compaction,
                              Env::Priority thread_pri);
  Status BackgroundFlush(bool* madeProgress, JobContext* job_context,
                         LogBuffer* log_buffer, FlushReason* reason,
                         bool* flush_rescheduled_to_retain_udt,
                         Env::Priority thread_pri);

  bool EnoughRoomForCompaction(ColumnFamilyData* cfd,
                               const std::vector<CompactionInputFiles>& inputs,
                               bool* sfm_bookkeeping, LogBuffer* log_buffer);

  // Request compaction tasks token from compaction thread limiter.
  // It always succeeds if force = true or limiter is disable.
  bool RequestCompactionToken(ColumnFamilyData* cfd, bool force,
                              std::unique_ptr<TaskLimiterToken>* token,
                              LogBuffer* log_buffer);

  // Return true if the `FlushRequest` can be rescheduled to retain the UDT.
  // Only true if there are user-defined timestamps in the involved MemTables
  // with newer than cutoff timestamp `full_history_ts_low` and not flushing
  // immediately will not cause entering write stall mode.
  bool ShouldRescheduleFlushRequestToRetainUDT(const FlushRequest& flush_req);

  // Schedule background tasks
  Status StartPeriodicTaskScheduler();

  // Cancel scheduled periodic tasks
  Status CancelPeriodicTaskScheduler();

  Status RegisterRecordSeqnoTimeWorker(const ReadOptions& read_options,
                                       const WriteOptions& write_options,
                                       bool is_new_db);

  void PrintStatistics();

  size_t EstimateInMemoryStatsHistorySize() const;

  // Return the minimum empty level that could hold the total data in the
  // input level. Return the input level, if such level could not be found.
  int FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level);

  // Move the files in the input level to the target level.
  // If target_level < 0, automatically calculate the minimum level that could
  // hold the data set.
  Status ReFitLevel(ColumnFamilyData* cfd, int level, int target_level = -1);

  // helper functions for adding and removing from flush & compaction queues
  void AddToCompactionQueue(ColumnFamilyData* cfd);
  ColumnFamilyData* PopFirstFromCompactionQueue();
  FlushRequest PopFirstFromFlushQueue();

  // Pick the first unthrottled compaction with task token from queue.
  ColumnFamilyData* PickCompactionFromQueue(
      std::unique_ptr<TaskLimiterToken>* token, LogBuffer* log_buffer);

  IOStatus SyncWalImpl(bool include_current_wal,
                       const WriteOptions& write_options,
                       JobContext* job_context, VersionEdit* synced_wals,
                       bool error_recovery_in_prog);

  // helper function to call after some of the logs_ were synced
  void MarkLogsSynced(uint64_t up_to, bool synced_dir, VersionEdit* edit);
  Status ApplyWALToManifest(const ReadOptions& read_options,
                            const WriteOptions& write_options,
                            VersionEdit* edit);
  // WALs with log number up to up_to are not synced successfully.
  void MarkLogsNotSynced(uint64_t up_to);

  SnapshotImpl* GetSnapshotImpl(bool is_write_conflict_boundary,
                                bool lock = true);

  // If snapshot_seq != kMaxSequenceNumber, then this function can only be
  // called from the write thread that publishes sequence numbers to readers.
  // For 1) write-committed, or 2) write-prepared + one-write-queue, this will
  // be the write thread performing memtable writes. For write-prepared with
  // two write queues, this will be the write thread writing commit marker to
  // the WAL.
  // If snapshot_seq == kMaxSequenceNumber, this function is called by a caller
  // ensuring no writes to the database.
  std::pair<Status, std::shared_ptr<const SnapshotImpl>>
  CreateTimestampedSnapshotImpl(SequenceNumber snapshot_seq, uint64_t ts,
                                bool lock = true);

  uint64_t GetMaxTotalWalSize() const;

  FSDirectory* GetDataDir(ColumnFamilyData* cfd, size_t path_id) const;

  Status MaybeReleaseTimestampedSnapshotsAndCheck();

  Status CloseHelper();

  void WaitForBackgroundWork();

  // Background threads call this function, which is just a wrapper around
  // the InstallSuperVersion() function. Background threads carry
  // sv_context which can have new_superversion already
  // allocated.
  // All ColumnFamily state changes go through this function. Here we analyze
  // the new state and we schedule background work if we detect that the new
  // state needs flush or compaction.
  void InstallSuperVersionAndScheduleWork(
      ColumnFamilyData* cfd, SuperVersionContext* sv_context,
      const MutableCFOptions& mutable_cf_options);

  bool GetIntPropertyInternal(ColumnFamilyData* cfd,
                              const DBPropertyInfo& property_info,
                              bool is_locked, uint64_t* value);
  bool GetPropertyHandleOptionsStatistics(std::string* value);

  bool HasPendingManualCompaction();
  bool HasExclusiveManualCompaction();
  void AddManualCompaction(ManualCompactionState* m);
  void RemoveManualCompaction(ManualCompactionState* m);
  bool ShouldntRunManualCompaction(ManualCompactionState* m);
  bool HaveManualCompaction(ColumnFamilyData* cfd);
  bool MCOverlap(ManualCompactionState* m, ManualCompactionState* m1);
  void UpdateDeletionCompactionStats(const std::unique_ptr<Compaction>& c);

  // May open and read table files for table property.
  // Should not be called while holding mutex_.
  void BuildCompactionJobInfo(const ColumnFamilyData* cfd, Compaction* c,
                              const Status& st,
                              const CompactionJobStats& compaction_job_stats,
                              const int job_id,
                              CompactionJobInfo* compaction_job_info) const;
  // Reserve the next 'num' file numbers for to-be-ingested external SST files,
  // and return the current file_number in 'next_file_number'.
  // Write a version edit to the MANIFEST.
  Status ReserveFileNumbersBeforeIngestion(
      ColumnFamilyData* cfd, uint64_t num,
      std::unique_ptr<std::list<uint64_t>::iterator>& pending_output_elem,
      uint64_t* next_file_number);

  bool ShouldPurge(uint64_t file_number) const;
  void MarkAsGrabbedForPurge(uint64_t file_number);

  size_t GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;
  Env::WriteLifeTimeHint CalculateWALWriteHint() { return Env::WLTH_SHORT; }

  IOStatus CreateWAL(const WriteOptions& write_options, uint64_t log_file_num,
                     uint64_t recycle_log_number, size_t preallocate_block_size,
                     log::Writer** new_log);

  // Validate self-consistency of DB options
  static Status ValidateOptions(const DBOptions& db_options);
  // Validate self-consistency of DB options and its consistency with cf options
  static Status ValidateOptions(
      const DBOptions& db_options,
      const std::vector<ColumnFamilyDescriptor>& column_families);

  // Utility function to do some debug validation and sort the given vector
  // of MultiGet keys
  void PrepareMultiGetKeys(
      const size_t num_keys, bool sorted,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* key_ptrs);

  void MultiGetCommon(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const size_t num_keys,
                      const Slice* keys, PinnableSlice* values,
                      PinnableWideColumns* columns, std::string* timestamps,
                      Status* statuses, bool sorted_input);

  void MultiGetCommon(const ReadOptions& options, const size_t num_keys,
                      ColumnFamilyHandle** column_families, const Slice* keys,
                      PinnableSlice* values, PinnableWideColumns* columns,
                      std::string* timestamps, Status* statuses,
                      bool sorted_input);

  // A structure to hold the information required to process MultiGet of keys
  // belonging to one column family. For a multi column family MultiGet, there
  // will be a container of these objects.
  struct MultiGetKeyRangePerCf {
    // For the batched MultiGet which relies on sorted keys, start specifies
    // the index of first key belonging to this column family in the sorted
    // list.
    size_t start;

    // For the batched MultiGet case, num_keys specifies the number of keys
    // belonging to this column family in the sorted list
    size_t num_keys;

    MultiGetKeyRangePerCf() : start(0), num_keys(0) {}

    MultiGetKeyRangePerCf(size_t first, size_t count)
        : start(first), num_keys(count) {}
  };

  // A structure to contain ColumnFamilyData and the SuperVersion obtained for
  // the consistent view of DB
  struct ColumnFamilySuperVersionPair {
    ColumnFamilyHandleImpl* cfh;
    ColumnFamilyData* cfd;

    // SuperVersion for the column family obtained in a manner that ensures a
    // consistent view across all column families in the DB
    SuperVersion* super_version;
    ColumnFamilySuperVersionPair(ColumnFamilyHandle* column_family,
                                 SuperVersion* sv)
        : cfh(static_cast<ColumnFamilyHandleImpl*>(column_family)),
          cfd(cfh->cfd()),
          super_version(sv) {}

    ColumnFamilySuperVersionPair() = default;
  };

  // A common function to obtain a consistent snapshot, which can be implicit
  // if the user doesn't specify a snapshot in read_options, across
  // multiple column families. It will attempt to get an implicit
  // snapshot without acquiring the db_mutes, but will give up after a few
  // tries and acquire the mutex if a memtable flush happens. The template
  // allows both the batched and non-batched MultiGet to call this with
  // either an std::unordered_map or autovector of column families.
  //
  // If callback is non-null, the callback is refreshed with the snapshot
  // sequence number
  //
  // `extra_sv_ref` is used to indicate whether thread-local SuperVersion
  // should be obtained with an extra ref (by GetReferencedSuperVersion()) or
  // not (by GetAndRefSuperVersion()). For instance, point lookup like MultiGet
  // does not require SuperVersion to be re-acquired throughout the entire
  // invocation (no need extra ref), while MultiCfIterators may need the
  // SuperVersion to be updated during Refresh() (requires extra ref).
  //
  // `sv_from_thread_local` being set to false indicates that the SuperVersion
  // obtained from the ColumnFamilyData, whereas true indicates they are thread
  // local.
  //
  // A non-OK status will be returned if for a column family that enables
  // user-defined timestamp feature, the specified `ReadOptions.timestamp`
  // attemps to read collapsed history.
  template <class T, typename IterDerefFuncType>
  Status MultiCFSnapshot(const ReadOptions& read_options,
                         ReadCallback* callback,
                         IterDerefFuncType iter_deref_func, T* cf_list,
                         bool extra_sv_ref, SequenceNumber* snapshot,
                         bool* sv_from_thread_local);

  // The actual implementation of the batching MultiGet. The caller is expected
  // to have acquired the SuperVersion and pass in a snapshot sequence number
  // in order to construct the LookupKeys. The start_key and num_keys specify
  // the range of keys in the sorted_keys vector for a single column family.
  Status MultiGetImpl(
      const ReadOptions& read_options, size_t start_key, size_t num_keys,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys,
      SuperVersion* sv, SequenceNumber snap_seqnum, ReadCallback* callback);

  void MultiGetWithCallbackImpl(
      const ReadOptions& read_options, ColumnFamilyHandle* column_family,
      ReadCallback* callback,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys);

  Status DisableFileDeletionsWithLock();

  Status IncreaseFullHistoryTsLowImpl(ColumnFamilyData* cfd,
                                      std::string ts_low);

  bool ShouldReferenceSuperVersion(const MergeContext& merge_context);

  template <typename IterType, typename ImplType,
            typename ErrorIteratorFuncType>
  std::unique_ptr<IterType> NewMultiCfIterator(
      const ReadOptions& _read_options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      ErrorIteratorFuncType error_iterator_func);

  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  FileLock* db_lock_;

  // Guards changes to DB and CF options to ensure consistency between
  // * In-memory options objects
  // * Settings in effect
  // * Options file contents
  // while allowing the DB mutex to be released during slow operations like
  // persisting options file or modifying global periodic task timer.
  // Always acquired *before* DB mutex when this one is applicable.
  InstrumentedMutex options_mutex_;

  // Guards reads and writes to in-memory stats_history_.
  InstrumentedMutex stats_history_mutex_;

  // In addition to mutex_, log_write_mutex_ protects writes to logs_ and
  // logfile_number_. With two_write_queues it also protects alive_log_files_,
  // and log_empty_. Refer to the definition of each variable below for more
  // details.
  // Note: to avoid deadlock, if needed to acquire both log_write_mutex_ and
  // mutex_, the order should be first mutex_ and then log_write_mutex_.
  InstrumentedMutex log_write_mutex_;

  // If zero, manual compactions are allowed to proceed. If non-zero, manual
  // compactions may still be running, but will quickly fail with
  // `Status::Incomplete`. The value indicates how many threads have paused
  // manual compactions. It is accessed in read mode outside the DB mutex in
  // compaction code paths.
  std::atomic<int> manual_compaction_paused_;

  // This condition variable is signaled on these conditions:
  // * whenever bg_compaction_scheduled_ goes down to 0
  // * if AnyManualCompaction, whenever a compaction finishes, even if it hasn't
  // made any progress
  // * whenever a compaction made any progress
  // * whenever bg_flush_scheduled_ or bg_purge_scheduled_ value decreases
  // (i.e. whenever a flush is done, even if it didn't make any progress)
  // * whenever there is an error in background purge, flush or compaction
  // * whenever num_running_ingest_file_ goes to 0.
  // * whenever pending_purge_obsolete_files_ goes to 0.
  // * whenever disable_delete_obsolete_files_ goes to 0.
  // * whenever SetOptions successfully updates options.
  // * whenever a column family is dropped.
  InstrumentedCondVar bg_cv_;
  // Writes are protected by locking both mutex_ and log_write_mutex_, and reads
  // must be under either mutex_ or log_write_mutex_. Since after ::Open,
  // logfile_number_ is currently updated only in write_thread_, it can be read
  // from the same write_thread_ without any locks.
  uint64_t logfile_number_;
  // Log files that we can recycle. Must be protected by db mutex_.
  std::deque<uint64_t> log_recycle_files_;
  // The minimum log file number taht can be recycled, if log recycling is
  // enabled. This is used to ensure that log files created by previous
  // instances of the database are not recycled, as we cannot be sure they
  // were created in the recyclable format.
  uint64_t min_log_number_to_recycle_;
  // Protected by log_write_mutex_.
  bool log_dir_synced_;
  // Without two_write_queues, read and writes to log_empty_ are protected by
  // mutex_. Since it is currently updated/read only in write_thread_, it can be
  // accessed from the same write_thread_ without any locks. With
  // two_write_queues writes, where it can be updated in different threads,
  // read and writes are protected by log_write_mutex_ instead. This is to avoid
  // expensive mutex_ lock during WAL write, which update log_empty_.
  bool log_empty_;

  ColumnFamilyHandleImpl* persist_stats_cf_handle_;

  bool persistent_stats_cfd_exists_ = true;

  // The current WAL file and those that have not been found obsolete from
  // memtable flushes. A WAL not on this list might still be pending writer
  // flush and/or sync and close and might still be in logs_. alive_log_files_
  // is protected by mutex_ and log_write_mutex_ with details as follows:
  // 1. read by FindObsoleteFiles() which can be called in either application
  //    thread or RocksDB bg threads, both mutex_ and log_write_mutex_ are
  //    held.
  // 2. pop_front() by FindObsoleteFiles(), both mutex_ and log_write_mutex_
  //    are held.
  // 3. push_back() by DBImpl::Open() and DBImpl::RestoreAliveLogFiles()
  //    (actually called by Open()), only mutex_ is held because at this point,
  //    the DB::Open() call has not returned success to application, and the
  //    only other thread(s) that can conflict are bg threads calling
  //    FindObsoleteFiles() which ensure that both mutex_ and log_write_mutex_
  //    are held when accessing alive_log_files_.
  // 4. read by DBImpl::Open() is protected by mutex_.
  // 5. push_back() by SwitchMemtable(). Both mutex_ and log_write_mutex_ are
  //    held. This is done by the write group leader. Note that in the case of
  //    two-write-queues, another WAL-only write thread can be writing to the
  //    WAL concurrently. See 9.
  // 6. read by SwitchWAL() with both mutex_ and log_write_mutex_ held. This is
  //    done by write group leader.
  // 7. read by ConcurrentWriteToWAL() by the write group leader in the case of
  //    two-write-queues. Only log_write_mutex_ is held to protect concurrent
  //    pop_front() by FindObsoleteFiles().
  // 8. read by PreprocessWrite() by the write group leader. log_write_mutex_
  //    is held to protect the data structure from concurrent pop_front() by
  //    FindObsoleteFiles().
  // 9. read by ConcurrentWriteToWAL() by a WAL-only write thread in the case
  //    of two-write-queues. Only log_write_mutex_ is held. This suffices to
  //    protect the data structure from concurrent push_back() by current
  //    write group leader as well as pop_front() by FindObsoleteFiles().
  std::deque<LogFileNumberSize> alive_log_files_;

  // Log files that aren't fully synced, and the current log file.
  // Synchronization:
  // 1. read by FindObsoleteFiles() which can be called either in application
  //    thread or RocksDB bg threads. log_write_mutex_ is always held, while
  //    some reads are performed without mutex_.
  // 2. pop_front() by FindObsoleteFiles() with only log_write_mutex_ held.
  // 3. read by DBImpl::Open() with both mutex_ and log_write_mutex_.
  // 4. emplace_back() by DBImpl::Open() with both mutex_ and log_write_mutex.
  //    Note that at this point, DB::Open() has not returned success to
  //    application, thus the only other thread(s) that can conflict are bg
  //    threads calling FindObsoleteFiles(). See 1.
  // 5. iteration and clear() from CloseHelper() always hold log_write_mutex
  //    and mutex_.
  // 6. back() called by APIs FlushWAL() and LockWAL() are protected by only
  //    log_write_mutex_. These two can be called by application threads after
  //    DB::Open() returns success to applications.
  // 7. read by SyncWAL(), another API, protected by only log_write_mutex_.
  // 8. read by MarkLogsNotSynced() and MarkLogsSynced() are protected by
  //    log_write_mutex_.
  // 9. erase() by MarkLogsSynced() protected by log_write_mutex_.
  // 10. read by SyncClosedWals() protected by only log_write_mutex_. This can
  //     happen in bg flush threads after DB::Open() returns success to
  //     applications.
  // 11. reads, e.g. front(), iteration, and back() called by PreprocessWrite()
  //     holds only the log_write_mutex_. This is done by the write group
  //     leader. A bg thread calling FindObsoleteFiles() or MarkLogsSynced()
  //     can happen concurrently. This is fine because log_write_mutex_ is used
  //     by all parties. See 2, 5, 9.
  // 12. reads, empty(), back() called by SwitchMemtable() hold both mutex_ and
  //     log_write_mutex_. This happens in the write group leader.
  // 13. emplace_back() by SwitchMemtable() hold both mutex_ and
  //     log_write_mutex_. This happens in the write group leader. Can conflict
  //     with bg threads calling FindObsoleteFiles(), MarkLogsSynced(),
  //     SyncClosedWals(), etc. as well as application threads calling
  //     FlushWAL(), SyncWAL(), LockWAL(). This is fine because all parties
  //     require at least log_write_mutex_.
  // 14. iteration called in WriteToWAL(write_group) protected by
  //     log_write_mutex_. This is done by write group leader when
  //     two-write-queues is disabled and write needs to sync logs.
  // 15. back() called in ConcurrentWriteToWAL() protected by log_write_mutex_.
  //     This can be done by the write group leader if two-write-queues is
  //     enabled. It can also be done by another WAL-only write thread.
  //
  // Other observations:
  //  - back() and items with getting_synced=true are not popped,
  //  - The same thread that sets getting_synced=true will reset it.
  //  - it follows that the object referred by back() can be safely read from
  //  the write_thread_ without using mutex. Note that calling back() without
  //  mutex may be unsafe because different implementations of deque::back() may
  //  access other member variables of deque, causing undefined behaviors.
  //  Generally, do not access stl containers without proper synchronization.
  //  - it follows that the items with getting_synced=true can be safely read
  //  from the same thread that has set getting_synced=true
  std::deque<LogWriterNumber> logs_;

  // Signaled when getting_synced becomes false for some of the logs_.
  InstrumentedCondVar log_sync_cv_;
  // This is the app-level state that is written to the WAL but will be used
  // only during recovery. Using this feature enables not writing the state to
  // memtable on normal writes and hence improving the throughput. Each new
  // write of the state will replace the previous state entirely even if the
  // keys in the two consecutive states do not overlap.
  // It is protected by log_write_mutex_ when two_write_queues_ is enabled.
  // Otherwise only the heaad of write_thread_ can access it.
  WriteBatch cached_recoverable_state_;
  std::atomic<bool> cached_recoverable_state_empty_ = {true};
  std::atomic<uint64_t> total_log_size_;

  // If this is non-empty, we need to delete these log files in background
  // threads. Protected by log_write_mutex_.
  autovector<log::Writer*> logs_to_free_;

  bool is_snapshot_supported_;

  std::map<uint64_t, std::map<std::string, uint64_t>> stats_history_;

  std::map<std::string, uint64_t> stats_slice_;

  bool stats_slice_initialized_ = false;

  Directories directories_;

  WriteBufferManager* write_buffer_manager_;

  WriteThread write_thread_;
  WriteBatch tmp_batch_;
  // The write thread when the writers have no memtable write. This will be used
  // in 2PC to batch the prepares separately from the serial commit.
  WriteThread nonmem_write_thread_;

  WriteController write_controller_;

  // Size of the last batch group. In slowdown mode, next write needs to
  // sleep if it uses up the quota.
  // Note: This is to protect memtable and compaction. If the batch only writes
  // to the WAL its size need not to be included in this.
  uint64_t last_batch_group_size_;

  FlushScheduler flush_scheduler_;

  TrimHistoryScheduler trim_history_scheduler_;

  SnapshotList snapshots_;

  TimestampedSnapshotList timestamped_snapshots_;

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

  // Similar to pending_outputs_, FindObsoleteFiles()/PurgeObsoleteFiles() never
  // deletes any OPTIONS file that has number bigger than any of the file number
  // in min_options_file_numbers_.
  std::list<uint64_t> min_options_file_numbers_;

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
  // means we need to schedule background threads for flush and compaction.
  // Once the background threads are scheduled, we decrease unscheduled_flushes_
  // and unscheduled_compactions_. That way we keep track of number of
  // compaction and flush threads we need to schedule. This scheduling is done
  // in MaybeScheduleFlushOrCompaction()
  // invariant(column family present in flush_queue_ <==>
  // ColumnFamilyData::pending_flush_ == true)
  std::deque<FlushRequest> flush_queue_;
  // invariant(column family present in compaction_queue_ <==>
  // ColumnFamilyData::pending_compaction_ == true)
  std::deque<ColumnFamilyData*> compaction_queue_;

  // A map to store file numbers and filenames of the files to be purged
  std::unordered_map<uint64_t, PurgeFileInfo> purge_files_;

  // A vector to store the file numbers that have been assigned to certain
  // JobContext. Current implementation tracks table and blob files only.
  std::unordered_set<uint64_t> files_grabbed_for_purge_;

  // A queue to store log writers to close. Protected by db mutex_.
  std::deque<log::Writer*> logs_to_free_queue_;

  std::deque<SuperVersion*> superversions_to_free_queue_;

  int unscheduled_flushes_;

  int unscheduled_compactions_;

  // count how many background compactions are running or have been scheduled in
  // the BOTTOM pool
  int bg_bottom_compaction_scheduled_;

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

  std::deque<ManualCompactionState*> manual_compaction_dequeue_;

  // shall we disable deletion of obsolete files
  // if 0 the deletion is enabled.
  // if non-zero, files will not be getting deleted
  // This enables two different threads to call
  // EnableFileDeletions() and DisableFileDeletions()
  // without any synchronization
  int disable_delete_obsolete_files_;

  // Number of times FindObsoleteFiles has found deletable files and the
  // corresponding call to PurgeObsoleteFiles has not yet finished.
  int pending_purge_obsolete_files_;

  // last time when DeleteObsoleteFiles with full scan was executed. Originally
  // initialized with startup time.
  uint64_t delete_obsolete_files_last_run_;

  // The thread that wants to switch memtable, can wait on this cv until the
  // pending writes to memtable finishes.
  std::condition_variable switch_cv_;
  // The mutex used by switch_cv_. mutex_ should be acquired beforehand.
  std::mutex switch_mutex_;
  // Number of threads intending to write to memtable
  std::atomic<size_t> pending_memtable_writes_ = {};

  // A flag indicating whether the current rocksdb database has any
  // data that is not yet persisted into either WAL or SST file.
  // Used when disableWAL is true.
  std::atomic<bool> has_unpersisted_data_;

  // if an attempt was made to flush all column families that
  // the oldest log depends on but uncommitted data in the oldest
  // log prevents the log from being released.
  // We must attempt to free the dependent memtables again
  // at a later time after the transaction in the oldest
  // log is fully commited.
  bool unable_to_release_oldest_log_;

  // Number of running IngestExternalFile() or CreateColumnFamilyWithImport()
  // calls.
  // REQUIRES: mutex held
  int num_running_ingest_file_;

  WalManager wal_manager_;

  // A value of > 0 temporarily disables scheduling of background work
  int bg_work_paused_;

  // A value of > 0 temporarily disables scheduling of background compaction
  int bg_compaction_paused_;

  // Guard against multiple concurrent refitting
  bool refitting_level_;

  // Indicate DB was opened successfully
  bool opened_successfully_;

  // The min threshold to triggere bottommost compaction for removing
  // garbages, among all column families.
  SequenceNumber bottommost_files_mark_threshold_ = kMaxSequenceNumber;

  // The min threshold to trigger compactions for standalone range deletion
  // files that are marked for compaction.
  SequenceNumber standalone_range_deletion_files_mark_threshold_ =
      kMaxSequenceNumber;

  LogsWithPrepTracker logs_with_prep_tracker_;

  // Callback for compaction to check if a key is visible to a snapshot.
  // REQUIRES: mutex held
  std::unique_ptr<SnapshotChecker> snapshot_checker_;

  // Callback for when the cached_recoverable_state_ is written to memtable
  // Only to be set during initialization
  std::unique_ptr<PreReleaseCallback> recoverable_state_pre_release_callback_;

  // Scheduler to run DumpStats(), PersistStats(), and FlushInfoLog().
  // Currently, internally it has a global timer instance for running the tasks.
  PeriodicTaskScheduler periodic_task_scheduler_;

  // It contains the implementations for each periodic task.
  std::map<PeriodicTaskType, const PeriodicTaskFunc> periodic_task_functions_;

  // When set, we use a separate queue for writes that don't write to memtable.
  // In 2PC these are the writes at Prepare phase.
  const bool two_write_queues_;
  const bool manual_wal_flush_;

  // LastSequence also indicates last published sequence visibile to the
  // readers. Otherwise LastPublishedSequence should be used.
  const bool last_seq_same_as_publish_seq_;
  // It indicates that a customized gc algorithm must be used for
  // flush/compaction and if it is not provided vis SnapshotChecker, we should
  // disable gc to be safe.
  const bool use_custom_gc_;
  // Flag to indicate that the DB instance shutdown has been initiated. This
  // different from shutting_down_ atomic in that it is set at the beginning
  // of shutdown sequence, specifically in order to prevent any background
  // error recovery from going on in parallel. The latter, shutting_down_,
  // is set a little later during the shutdown after scheduling memtable
  // flushes
  std::atomic<bool> shutdown_initiated_;
  // Flag to indicate whether sst_file_manager object was allocated in
  // DB::Open() or passed to us
  bool own_sfm_;

  // Flag to check whether Close() has been called on this DB
  bool closed_;
  // save the closing status, for re-calling the close()
  Status closing_status_;
  // mutex for DB::Close()
  InstrumentedMutex closing_mutex_;

  // Conditional variable to coordinate installation of atomic flush results.
  // With atomic flush, each bg thread installs the result of flushing multiple
  // column families, and different threads can flush different column
  // families. It's difficult to rely on one thread to perform batch
  // installation for all threads. This is different from the non-atomic flush
  // case.
  // atomic_flush_install_cv_ makes sure that threads install atomic flush
  // results sequentially. Flush results of memtables with lower IDs get
  // installed to MANIFEST first.
  InstrumentedCondVar atomic_flush_install_cv_;

  bool wal_in_db_path_;
  std::atomic<uint64_t> max_total_wal_size_;

  BlobFileCompletionCallback blob_callback_;

  // Pointer to WriteBufferManager stalling interface.
  std::unique_ptr<StallInterface> wbm_stall_;

  // seqno_to_time_mapping_ stores the sequence number to time mapping, it's not
  // thread safe, both read and write need db mutex hold.
  SeqnoToTimeMapping seqno_to_time_mapping_;

  // Stop write token that is acquired when first LockWAL() is called.
  // Destroyed when last UnlockWAL() is called. Controlled by DB mutex.
  // See lock_wal_count_
  std::unique_ptr<WriteControllerToken> lock_wal_write_token_;

  // The number of LockWAL called without matching UnlockWAL call.
  // See also lock_wal_write_token_
  uint32_t lock_wal_count_;
};

class GetWithTimestampReadCallback : public ReadCallback {
 public:
  explicit GetWithTimestampReadCallback(SequenceNumber seq)
      : ReadCallback(seq) {}
  bool IsVisibleFullCheck(SequenceNumber seq) override {
    return seq <= max_visible_seq_;
  }
};

Options SanitizeOptions(const std::string& db, const Options& src,
                        bool read_only = false,
                        Status* logger_creation_s = nullptr);

DBOptions SanitizeOptions(const std::string& db, const DBOptions& src,
                          bool read_only = false,
                          Status* logger_creation_s = nullptr);

CompressionType GetCompressionFlush(const ImmutableCFOptions& ioptions,
                                    const MutableCFOptions& mutable_cf_options);

// Return a VersionEdit for the DB's recovery when the `memtables` of the
// specified column family are obsolete. Specifically, the min log number to
// keep, and the WAL files that can be deleted.
VersionEdit GetDBRecoveryEditForObsoletingMemTables(
    VersionSet* vset, const ColumnFamilyData& cfd,
    const autovector<VersionEdit*>& edit_list,
    const autovector<ReadOnlyMemTable*>& memtables,
    LogsWithPrepTracker* prep_tracker);

// Return the earliest log file to keep after the memtable flush is
// finalized.
// `cfd_to_flush` is the column family whose memtable (specified in
// `memtables_to_flush`) will be flushed and thus will not depend on any WAL
// file.
// The function is only applicable to 2pc mode.
uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list,
    const autovector<ReadOnlyMemTable*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker);
// For atomic flush.
uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    const autovector<const autovector<ReadOnlyMemTable*>*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker);

// In non-2PC mode, WALs with log number < the returned number can be
// deleted after the cfd_to_flush column family is flushed successfully.
uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list);
// For atomic flush.
uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists);

// `cfd_to_flush` is the column family whose memtable will be flushed and thus
// will not depend on any WAL file. nullptr means no memtable is being flushed.
// The function is only applicable to 2pc mode.
uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const autovector<ReadOnlyMemTable*>& memtables_to_flush);
// For atomic flush.
uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset,
    const autovector<const autovector<ReadOnlyMemTable*>*>& memtables_to_flush);

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

inline Status DBImpl::FailIfCfHasTs(
    const ColumnFamilyHandle* column_family) const {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be null");
  }
  assert(column_family);
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  if (ucmp->timestamp_size() > 0) {
    std::ostringstream oss;
    oss << "cannot call this method on column family "
        << column_family->GetName() << " that enables timestamp";
    return Status::InvalidArgument(oss.str());
  }
  return Status::OK();
}

inline Status DBImpl::FailIfTsMismatchCf(ColumnFamilyHandle* column_family,
                                         const Slice& ts) const {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be null");
  }
  assert(column_family);
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  if (0 == ucmp->timestamp_size()) {
    std::stringstream oss;
    oss << "cannot call this method on column family "
        << column_family->GetName() << " that does not enable timestamp";
    return Status::InvalidArgument(oss.str());
  }
  const size_t ts_sz = ts.size();
  if (ts_sz != ucmp->timestamp_size()) {
    std::stringstream oss;
    oss << "Timestamp sizes mismatch: expect " << ucmp->timestamp_size() << ", "
        << ts_sz << " given";
    return Status::InvalidArgument(oss.str());
  }
  return Status::OK();
}

inline Status DBImpl::FailIfReadCollapsedHistory(const ColumnFamilyData* cfd,
                                                 const SuperVersion* sv,
                                                 const Slice& ts) const {
  // Reaching to this point means the timestamp size matching sanity check in
  // `DBImpl::FailIfTsMismatchCf` already passed. So we skip that and assume
  // column family has the same user-defined timestamp format as `ts`.
  const Comparator* const ucmp = cfd->user_comparator();
  assert(ucmp);
  const std::string& full_history_ts_low = sv->full_history_ts_low;
  assert(full_history_ts_low.empty() ||
         full_history_ts_low.size() == ts.size());
  if (!full_history_ts_low.empty() &&
      ucmp->CompareTimestamp(ts, full_history_ts_low) < 0) {
    std::stringstream oss;
    oss << "Read timestamp: " << ucmp->TimestampToString(ts)
        << " is smaller than full_history_ts_low: "
        << ucmp->TimestampToString(full_history_ts_low) << std::endl;
    return Status::InvalidArgument(oss.str());
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
