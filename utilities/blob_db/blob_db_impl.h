//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>
#include <condition_variable>
#include <limits>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/db_iter.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/wal_filter.h"
#include "util/mpsc.h"
#include "util/mutexlock.h"
#include "util/timer_queue.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/blob_db/blob_file.h"
#include "utilities/blob_db/blob_log_format.h"
#include "utilities/blob_db/blob_log_reader.h"
#include "utilities/blob_db/blob_log_writer.h"

namespace rocksdb {

class DBImpl;
class ColumnFamilyHandle;
class ColumnFamilyData;
struct FlushJobInfo;

namespace blob_db {

class BlobFile;
class BlobDBImpl;

class BlobDBFlushBeginListener : public EventListener {
 public:
  explicit BlobDBFlushBeginListener() : impl_(nullptr) {}

  void OnFlushBegin(DB* db, const FlushJobInfo& info) override;

  void SetImplPtr(BlobDBImpl* p) { impl_ = p; }

 protected:
  BlobDBImpl* impl_;
};

// this implements the callback from the WAL which ensures that the
// blob record is present in the blob log. If fsync/fdatasync in not
// happening on every write, there is the probability that keys in the
// blob log can lag the keys in blobs
class BlobReconcileWalFilter : public WalFilter {
 public:
  virtual WalFilter::WalProcessingOption LogRecordFound(
      unsigned long long log_number, const std::string& log_file_name,
      const WriteBatch& batch, WriteBatch* new_batch,
      bool* batch_changed) override;

  virtual const char* Name() const override { return "BlobDBWalReconciler"; }

  void SetImplPtr(BlobDBImpl* p) { impl_ = p; }

 protected:
  BlobDBImpl* impl_;
};

class EvictAllVersionsCompactionListener : public EventListener {
 public:
  class InternalListener : public CompactionEventListener {
    friend class BlobDBImpl;

   public:
    virtual void OnCompaction(int level, const Slice& key,
                              CompactionListenerValueType value_type,
                              const Slice& existing_value,
                              const SequenceNumber& sn, bool is_new) override;

    void SetImplPtr(BlobDBImpl* p) { impl_ = p; }

   private:
    BlobDBImpl* impl_;
  };

  explicit EvictAllVersionsCompactionListener()
      : internal_listener_(new InternalListener()) {}

  virtual CompactionEventListener* GetCompactionEventListener() override {
    return internal_listener_.get();
  }

  void SetImplPtr(BlobDBImpl* p) { internal_listener_->SetImplPtr(p); }

 private:
  std::unique_ptr<InternalListener> internal_listener_;
};

#if 0
class EvictAllVersionsFilterFactory : public CompactionFilterFactory {
 private:
  BlobDBImpl* impl_;

 public:
  EvictAllVersionsFilterFactory() : impl_(nullptr) {}

  void SetImplPtr(BlobDBImpl* p) { impl_ = p; }

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;

  virtual const char* Name() const override {
    return "EvictAllVersionsFilterFactory";
  }
};
#endif

// Comparator to sort "TTL" aware Blob files based on the lower value of
// TTL range.
struct blobf_compare_ttl {
  bool operator()(const std::shared_ptr<BlobFile>& lhs,
                  const std::shared_ptr<BlobFile>& rhs) const;
};

struct GCStats {
  uint64_t blob_count = 0;
  uint64_t num_keys_overwritten = 0;
  uint64_t num_keys_expired = 0;
  uint64_t num_keys_relocated = 0;
  uint64_t bytes_overwritten = 0;
  uint64_t bytes_expired = 0;
  uint64_t bytes_relocated = 0;
};

/**
 * The implementation class for BlobDB. This manages the value
 * part in TTL aware sequentially written files. These files are
 * Garbage Collected.
 */
class BlobDBImpl : public BlobDB {
  friend class BlobDBFlushBeginListener;
  friend class EvictAllVersionsCompactionListener;
  friend class BlobDB;
  friend class BlobFile;
  friend class BlobDBIterator;

 public:
  // deletions check period
  static constexpr uint32_t kDeleteCheckPeriodMillisecs = 2 * 1000;

  // gc percentage each check period
  static constexpr uint32_t kGCFilePercentage = 100;

  // gc period
  static constexpr uint32_t kGCCheckPeriodMillisecs = 60 * 1000;

  // sanity check task
  static constexpr uint32_t kSanityCheckPeriodMillisecs = 20 * 60 * 1000;

  // how many random access open files can we tolerate
  static constexpr uint32_t kOpenFilesTrigger = 100;

  // how many periods of stats do we keep.
  static constexpr uint32_t kWriteAmplificationStatsPeriods = 24;

  // we will garbage collect blob files in
  // which entire files have expired. However if the
  // ttl_range of files is very large say a day, we
  // would have to wait for the entire day, before we
  // recover most of the space.
  static constexpr uint32_t kPartialExpirationGCRangeSecs = 4 * 3600;

  // this should be based on allowed Write Amplification
  // if 50% of the space of a blob file has been deleted/expired,
  static constexpr uint32_t kPartialExpirationPercentage = 75;

  // how often should we schedule a job to fsync open files
  static constexpr uint32_t kFSyncFilesPeriodMillisecs = 10 * 1000;

  // how often to schedule reclaim open files.
  static constexpr uint32_t kReclaimOpenFilesPeriodMillisecs = 1 * 1000;

  // how often to schedule delete obs files periods
  static constexpr uint32_t kDeleteObsoleteFilesPeriodMillisecs = 10 * 1000;

  // how often to schedule check seq files period
  static constexpr uint32_t kCheckSeqFilesPeriodMillisecs = 10 * 1000;

  // when should oldest file be evicted:
  // on reaching 90% of blob_dir_size
  static constexpr double kEvictOldestFileAtSize = 0.9;

  using BlobDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using BlobDB::Delete;
  Status Delete(const WriteOptions& options, const Slice& key) override;

  using BlobDB::Get;
  Status Get(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value) override;

  using BlobDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& read_options) override;

  using BlobDB::NewIterators;
  virtual Status NewIterators(
      const ReadOptions& read_options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override {
    return Status::NotSupported("Not implemented");
  }

  using BlobDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& read_options,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;
  virtual void GetLiveFilesMetaData(
      std::vector<LiveFileMetaData>* ) override;

  using BlobDB::PutWithTTL;
  Status PutWithTTL(const WriteOptions& options, const Slice& key,
                    const Slice& value, uint64_t ttl) override;

  using BlobDB::PutUntil;
  Status PutUntil(const WriteOptions& options, const Slice& key,
                  const Slice& value, uint64_t expiration) override;

  Status LinkToBaseDB(DB* db) override;

  BlobDBOptions GetBlobDBOptions() const override;

  BlobDBImpl(DB* db, const BlobDBOptions& bdb_options);

  BlobDBImpl(const std::string& dbname, const BlobDBOptions& bdb_options,
             const DBOptions& db_options);

  ~BlobDBImpl();

#ifndef NDEBUG
  Status TEST_GetBlobValue(const Slice& key, const Slice& index_entry,
                           PinnableSlice* value);

  std::vector<std::shared_ptr<BlobFile>> TEST_GetBlobFiles() const;

  std::vector<std::shared_ptr<BlobFile>> TEST_GetObsoleteFiles() const;

  Status TEST_CloseBlobFile(std::shared_ptr<BlobFile>& bfile);

  Status TEST_GCFileAndUpdateLSM(std::shared_ptr<BlobFile>& bfile,
                                 GCStats* gc_stats);

  void TEST_RunGC();

  void TEST_DeleteObsoleteFiles();
#endif  //  !NDEBUG

 private:
  class GarbageCollectionWriteCallback;
  class BlobInserter;

  Status OpenPhase1();

  // Create a snapshot if there isn't one in read options.
  // Return true if a snapshot is created.
  bool SetSnapshotIfNeeded(ReadOptions* read_options);

  Status GetImpl(const ReadOptions& read_options,
                 ColumnFamilyHandle* column_family, const Slice& key,
                 PinnableSlice* value);

  Status GetBlobValue(const Slice& key, const Slice& index_entry,
                      PinnableSlice* value);

  Slice GetCompressedSlice(const Slice& raw,
                           std::string* compression_output) const;

  // Just before flush starts acting on memtable files,
  // this handler is called.
  void OnFlushBeginHandler(DB* db, const FlushJobInfo& info);

  // is this file ready for Garbage collection. if the TTL of the file
  // has expired or if threshold of the file has been evicted
  // tt - current time
  // last_id - the id of the non-TTL file to evict
  bool ShouldGCFile(std::shared_ptr<BlobFile> bfile, uint64_t now,
                    bool is_oldest_non_ttl_file, std::string* reason);

  // collect all the blob log files from the blob directory
  Status GetAllLogFiles(std::set<std::pair<uint64_t, std::string>>* file_nums);

  // Close a file by appending a footer, and removes file from open files list.
  Status CloseBlobFile(std::shared_ptr<BlobFile> bfile);

  // Close a file if its size exceeds blob_file_size
  Status CloseBlobFileIfNeeded(std::shared_ptr<BlobFile>& bfile);

  uint64_t ExtractExpiration(const Slice& key, const Slice& value,
                             Slice* value_slice, std::string* new_value);

  Status PutBlobValue(const WriteOptions& options, const Slice& key,
                      const Slice& value, uint64_t expiration,
                      SequenceNumber sequence, WriteBatch* batch);

  Status AppendBlob(const std::shared_ptr<BlobFile>& bfile,
                    const std::string& headerbuf, const Slice& key,
                    const Slice& value, uint64_t expiration,
                    std::string* index_entry);

  // find an existing blob log file based on the expiration unix epoch
  // if such a file does not exist, return nullptr
  std::shared_ptr<BlobFile> SelectBlobFileTTL(uint64_t expiration);

  // find an existing blob log file to append the value to
  std::shared_ptr<BlobFile> SelectBlobFile();

  std::shared_ptr<BlobFile> FindBlobFileLocked(uint64_t expiration) const;

  void Shutdown();

  // periodic sanity check. Bunch of checks
  std::pair<bool, int64_t> SanityCheck(bool aborted);

  // delete files which have been garbage collected and marked
  // obsolete. Check whether any snapshots exist which refer to
  // the same
  std::pair<bool, int64_t> DeleteObsoleteFiles(bool aborted);

  // Major task to garbage collect expired and deleted blobs
  std::pair<bool, int64_t> RunGC(bool aborted);

  // asynchronous task to fsync/fdatasync the open blob files
  std::pair<bool, int64_t> FsyncFiles(bool aborted);

  // periodically check if open blob files and their TTL's has expired
  // if expired, close the sequential writer and make the file immutable
  std::pair<bool, int64_t> CheckSeqFiles(bool aborted);

  // if the number of open files, approaches ULIMIT's this
  // task will close random readers, which are kept around for
  // efficiency
  std::pair<bool, int64_t> ReclaimOpenFiles(bool aborted);

  // background task to do book-keeping of deleted keys
  std::pair<bool, int64_t> EvictDeletions(bool aborted);

  std::pair<bool, int64_t> EvictCompacted(bool aborted);

  std::pair<bool, int64_t> RemoveTimerQ(TimerQueue* tq, bool aborted);

  // Adds the background tasks to the timer queue
  void StartBackgroundTasks();

  // add a new Blob File
  std::shared_ptr<BlobFile> NewBlobFile(const std::string& reason);

  Status OpenAllFiles();

  // hold write mutex on file and call
  // creates a Random Access reader for GET call
  std::shared_ptr<RandomAccessFileReader> GetOrOpenRandomAccessReader(
      const std::shared_ptr<BlobFile>& bfile, Env* env,
      const EnvOptions& env_options);

  // hold write mutex on file and call.
  // Close the above Random Access reader
  void CloseRandomAccessLocked(const std::shared_ptr<BlobFile>& bfile);

  // hold write mutex on file and call
  // creates a sequential (append) writer for this blobfile
  Status CreateWriterLocked(const std::shared_ptr<BlobFile>& bfile);

  // returns a Writer object for the file. If writer is not
  // already present, creates one. Needs Write Mutex to be held
  std::shared_ptr<Writer> CheckOrCreateWriterLocked(
      const std::shared_ptr<BlobFile>& bfile);

  // Iterate through keys and values on Blob and write into
  // separate file the remaining blobs and delete/update pointers
  // in LSM atomically
  Status GCFileAndUpdateLSM(const std::shared_ptr<BlobFile>& bfptr,
                            GCStats* gcstats);

  // checks if there is no snapshot which is referencing the
  // blobs
  bool VisibleToActiveSnapshot(const std::shared_ptr<BlobFile>& file);
  bool FileDeleteOk_SnapshotCheckLocked(const std::shared_ptr<BlobFile>& bfile);

  bool MarkBlobDeleted(const Slice& key, const Slice& lsmValue);

  bool FindFileAndEvictABlob(uint64_t file_number, uint64_t key_size,
                             uint64_t blob_offset, uint64_t blob_size);

  void CopyBlobFiles(
      std::vector<std::shared_ptr<BlobFile>>* bfiles_copy,
      std::function<bool(const std::shared_ptr<BlobFile>&)> predicate = {});

  void FilterSubsetOfFiles(
      const std::vector<std::shared_ptr<BlobFile>>& blob_files,
      std::vector<std::shared_ptr<BlobFile>>* to_process, uint64_t epoch,
      size_t files_to_collect);

  uint64_t EpochNow() { return env_->NowMicros() / 1000000; }

  Status CheckSize(size_t blob_size);

  std::shared_ptr<BlobFile> GetOldestBlobFile();

  bool EvictOldestBlobFile();

  // the base DB
  DBImpl* db_impl_;
  Env* env_;
  TTLExtractor* ttl_extractor_;

  // the options that govern the behavior of Blob Storage
  BlobDBOptions bdb_options_;
  DBOptions db_options_;
  EnvOptions env_options_;

  // Raw pointer of statistic. db_options_ has a shared_ptr to hold ownership.
  Statistics* statistics_;

  // name of the database directory
  std::string dbname_;

  // by default this is "blob_dir" under dbname_
  // but can be configured
  std::string blob_dir_;

  // pointer to directory
  std::unique_ptr<Directory> dir_ent_;

  std::atomic<bool> dir_change_;

  // Read Write Mutex, which protects all the data structures
  // HEAVILY TRAFFICKED
  mutable port::RWMutex mutex_;

  // Writers has to hold write_mutex_ before writing.
  mutable port::Mutex write_mutex_;

  // counter for blob file number
  std::atomic<uint64_t> next_file_number_;

  // entire metadata of all the BLOB files memory
  std::map<uint64_t, std::shared_ptr<BlobFile>> blob_files_;

  // epoch or version of the open files.
  std::atomic<uint64_t> epoch_of_;

  // opened non-TTL blob file.
  std::shared_ptr<BlobFile> open_non_ttl_file_;

  // all the blob files which are currently being appended to based
  // on variety of incoming TTL's
  std::multiset<std::shared_ptr<BlobFile>, blobf_compare_ttl> open_ttl_files_;

  // packet of information to put in lockess delete(s) queue
  struct delete_packet_t {
    ColumnFamilyHandle* cfh_;
    std::string key_;
    SequenceNumber dsn_;
  };

  struct override_packet_t {
    uint64_t file_number_;
    uint64_t key_size_;
    uint64_t blob_offset_;
    uint64_t blob_size_;
    SequenceNumber dsn_;
  };

  // LOCKLESS multiple producer single consumer queue to quickly append
  // deletes without taking lock. Can rapidly grow in size!!
  // deletes happen in LSM, but minor book-keeping needs to happen on
  // BLOB side (for triggering eviction)
  mpsc_queue_t<delete_packet_t> delete_keys_q_;

  // LOCKLESS multiple producer single consumer queue for values
  // that are being compacted
  mpsc_queue_t<override_packet_t> override_vals_q_;

  // atomic bool to represent shutdown
  std::atomic<bool> shutdown_;

  // timer based queue to execute tasks
  TimerQueue tqueue_;

  // only accessed in GC thread, hence not atomic. The epoch of the
  // GC task. Each execution is one epoch. Helps us in allocating
  // files to one execution
  uint64_t current_epoch_;

  // number of files opened for random access/GET
  // counter is used to monitor and close excess RA files.
  std::atomic<uint32_t> open_file_count_;

  // total size of all blob files at a given time
  std::atomic<uint64_t> total_blob_space_;
  std::list<std::shared_ptr<BlobFile>> obsolete_files_;
  bool open_p1_done_;

  uint32_t debug_level_;

  std::atomic<bool> oldest_file_evicted_;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
