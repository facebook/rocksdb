//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once


#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "db/blob_log_format.h"
#include "db/blob_log_reader.h"
#include "db/blob_log_writer.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/wal_filter.h"
#include "util/cf_options.h"
#include "util/file_reader_writer.h"
#include "util/mpsc.h"
#include "util/mutexlock.h"
#include "util/timer_queue.h"
#include "utilities/blob_db/blob_db.h"

namespace rocksdb {

class BlobFile;
class DBImpl;
class ColumnFamilyHandle;
class OptimisticTransactionDBImpl;
class FlushJobInfo;
class BlobDBImpl;

class BlobDBFlushBeginListener : public EventListener {
 public:
  explicit BlobDBFlushBeginListener(): impl_(nullptr) {}

  void OnFlushBegin(
      DB* db, const FlushJobInfo& info) override;

  void setImplPtr(BlobDBImpl *p) { impl_ = p; }

 protected:
  BlobDBImpl *impl_;
};

// this implements the callback from the WAL which ensures that the
// blob record is present in the blob log. If fsync/fdatasync in not
// happening on every write, there is the probability that keys in the
// blob log can lag the keys in blobs
class BlobReconcileWalFilter : public WalFilter {
public:

  virtual WalFilter::WalProcessingOption LogRecordFound(unsigned long long log_number,
    const std::string& log_file_name, const WriteBatch& batch,
    WriteBatch* new_batch, bool* batch_changed) override;

  virtual const char* Name() const override {
    return "BlobDBWalReconciler";
  }

  void setImplPtr(BlobDBImpl *p) { impl_ = p; }

 protected:
  BlobDBImpl *impl_;
};

// Comparator to sort "TTL" aware Blob files based on the lower value of
// TTL range.
struct blobf_compare_ttl {
  bool operator() (const std::shared_ptr<BlobFile>& lhs,
    const std::shared_ptr<BlobFile>& rhs) const;
};

typedef std::pair<uint32_t, uint32_t> ttlrange_t;
typedef std::pair<uint64_t, uint64_t> tsrange_t;
typedef std::pair<rocksdb::SequenceNumber, rocksdb::SequenceNumber> snrange_t;

/**
 * The implementation class for BlobDB. This manages the value
 * part in TTL aware sequentially written files. These files are
 * Garbage Collected.
 */
class BlobDBImpl : public BlobDB {
  friend class BlobDBFlushBeginListener;
  friend class BlobDB;
  friend class BlobFile;
 public:
  using rocksdb::StackableDB::Put;
  Status Put(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& value) override;

  using rocksdb::StackableDB::Delete;
  Status Delete(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key)
    override;

  using rocksdb::StackableDB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& wopts,
    ColumnFamilyHandle* column_family,
    const Slice& key) override;

  using rocksdb::StackableDB::Get;
  Status Get(const ReadOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    std::string* value) override;

  using rocksdb::StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys,
    std::vector<std::string>* values) override;

  virtual Status Write(const WriteOptions& opts,
    WriteBatch* updates) override;

  using rocksdb::BlobDB::PutWithTTL;
  Status PutWithTTL(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& value, int32_t ttl) override;

  using rocksdb::BlobDB::PutUntil;
  Status PutUntil(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& value, int32_t expiration) override;

  Status LinkToBaseDB(DB* db) override;

  BlobDBImpl(DB* db, const BlobDBOptions& bdb_options);

  BlobDBImpl(const std::string& dbname, const BlobDBOptions& bdb_options,
    const Options& options, const DBOptions& db_options,
    const EnvOptions& env_options);

  ~BlobDBImpl();

 private:

  static bool extractTTLFromBlob(const Slice& value, Slice *newval,
    int32_t *ttl_val);

  Status openPhase1();

  // Just before flush starts acting on memtable files,
  // this handler is called.
  void OnFlushBeginHandler(DB* db, const FlushJobInfo& info);

  // timer queue callback to close a file by appending a footer
  // removes file from open files list
  std::pair<bool, int64_t> closeSeqWrite(std::shared_ptr<BlobFile> bfile,
    bool aborted);

  // is this file ready for Garbage collection. if the TTL of the file
  // has expired or if threshold of the file has been evicted
  // tt - current time
  // last_id - the id of the non-TTL file to evict
  bool shouldGCFile_locked(std::shared_ptr<BlobFile> bfile, std::time_t tt,
    uint64_t last_id, std::string *reason);

  // collect all the blob log files from the blob directory
  Status getAllLogFiles(std::set<std::pair<uint64_t, std::string>>* file_nums);

  // appends a task into timer queue to close the file
  void closeIf(const std::shared_ptr<BlobFile>& bfile);

  Status appendBlob(std::shared_ptr<BlobFile>& bfile,
    const char *headerbuf, const Slice& key, const Slice& value,
    std::string *index_entry);

  Status appendSN(std::shared_ptr<BlobFile>& bfile, const SequenceNumber& sn);

  // find an existing blob log file based on the expiration unix epoch
  // if such a file does not exist, return nullptr
  std::shared_ptr<BlobFile> selectBlobFileTTL(uint32_t expiration);

  // find an existing blob log file to append the value to
  std::shared_ptr<BlobFile> selectBlobFile();

  std::shared_ptr<BlobFile> findBlobFile_locked(uint32_t expiration) const;

  void updateWriteOptions(const WriteOptions& options);

  void shutdown();

  // periodic sanity check. Bunch of checks
  std::pair<bool, int64_t> sanityCheck(bool aborted);

  // delete files which have been garbage collected and marked
  // obsolete. Check whether any snapshots exist which refer to
  // the same
  std::pair<bool, int64_t> deleteObsFiles(bool aborted);

  // Major task to garbage collect expired and deleted blobs
  std::pair<bool, int64_t> runGC(bool aborted);

  // asynchronous task to fsync/fdatasync the open blob files
  std::pair<bool, int64_t> fsyncFiles(bool aborted);

  // periodically check if open blob files and their TTL's has expired
  // if expired, close the sequential writer and make the file immutable
  std::pair<bool, int64_t> checkSeqFiles(bool aborted);

  // if the number of open files, approaches ULIMIT's this
  // task will close random readers, which are kept around for
  // efficiency
  std::pair<bool, int64_t> reclaimOpenFiles(bool aborted);

  // periodically print write amplification statistics
  std::pair<bool, int64_t> waStats(bool aborted);

  // background task to do book-keeping of deleted keys
  std::pair<bool, int64_t> evictDeletions(bool aborted);

  // Adds the background tasks to the timer queue
  void startBackgroundTasks();

  // add a new Blob File
  std::shared_ptr<BlobFile> newBlobFile(const std::string& reason);

  Status openAllFiles();

  // hold write mutex on file and call
  // creates a Random Access reader for GET call
  std::shared_ptr<RandomAccessFileReader> openRandomAccess_locked(
    std::shared_ptr<BlobFile>& bfile, Env *env,
    const EnvOptions& env_options);

  // hold write mutex on file and call.
  // Close the above Random Access reader
  void closeRandomAccess_locked(std::shared_ptr<BlobFile>& bfile);

  // hold write mutex on file and call
  // creates a sequential (append) writer for this blobfile
  Status createWriter_locked(std::shared_ptr<BlobFile> &bfile,
    bool reopen = false);

  // returns a Writer object for the file. If writer is not
  // already present, creates one. Needs Write Mutex to be held
  std::shared_ptr<blob_log::Writer>
    checkOrCreateWriter_locked(std::shared_ptr<BlobFile>& bfile);

  // Iterate through keys and values on Blob and write into
  // separate file the remaining blobd and delete the keys
  // atomically
  Status writeBatchOfDeleteKeys(std::shared_ptr<BlobFile>& bfptr,
    std::time_t tt);

  // checks if there is no snapshot which is referencing the
  // blobs
  bool FileDeleteOk_SnapshotCheck_locked(const std::shared_ptr<BlobFile>& bfile);

 private:

  // the base DB
  DBImpl* db_impl_;

  Env *myenv_;

  // Optimistic Transaction DB used during Garbage collection
  // for atomicity
  std::shared_ptr<OptimisticTransactionDBImpl> opt_db_;

  // a boolean to capture whether write_options has been set
  std::atomic<bool> wo_set_;
  WriteOptions write_options_;

  // the options that govern the behavior of Blob Storage
  BlobDBOptions bdb_options_;

  ImmutableCFOptions ioptions_;
  DBOptions db_options_;
  EnvOptions env_options_;

  // name of the database directory
  std::string dbname_;

  // by default this is "blob_dir" under dbname_
  // but can be configured
  std::string blob_dir_;

  // Read Write Mutex, which protects all the data structures
  // HEAVILY TRAFFICKED
  port::RWMutex mutex_;

  // counter for blob file number
  std::atomic<uint64_t> next_file_number_;

  // entire metadata of all the BLOB files memory
  std::unordered_map<uint64_t, std::shared_ptr<BlobFile>> blob_files_;

  // epoch or version of the open files.
  std::atomic<uint64_t> epoch_of_;

  // typically we keep 4 open blob files (simple i.e. no TTL)
  std::vector<std::shared_ptr<BlobFile> > open_simple_files_;

  // all the blob files which are currently being appended to based
  // on variety of incoming TTL's
  std::multiset<std::shared_ptr<BlobFile>, blobf_compare_ttl> open_blob_files_;

  // packet of information to put in lockess delete(s) queue
  struct delete_packet_t {
    ColumnFamilyHandle *cfh_;
    std::string key_;
    SequenceNumber dsn_;
  };

  // LOCKLESS multiple producer single consumer queue to quickly append
  // deletes without taking lock. Can rapidly grow in size!!
  // deletes happen in LSM, but minor book-keeping needs to happen on
  // BLOB side (for triggering eviction)
  mpsc_queue_t<delete_packet_t> delete_keys_q_;

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

  // should hold mutex to modify
  // STATISTICS for WA of Blob Files due to GC
  // collect by default 24 hourly periods
  std::list<uint64_t> all_periods_write_;
  std::list<uint64_t> all_periods_ampl_;

  std::atomic<uint64_t> last_period_write_;
  std::atomic<uint64_t> last_period_ampl_;

  uint64_t total_periods_write_;
  uint64_t total_periods_ampl_;

  // total size of all blob files at a given time
  std::atomic<uint64_t> total_blob_space_;
  std::list<std::shared_ptr<BlobFile>> obsolete_files_;
  bool open_p1_done_;
};

class BlobFile {

  friend class BlobDBImpl;
  friend struct blobf_compare_ttl;

private:
  // access to parent
  const BlobDBImpl *parent_;

  // path to blob directory
  std::string path_to_dir_;

  // the id of the file.
  // the above 2 are created during file creation and never changed
  // after that
  uint64_t file_number_;

  // number of blobs in the file
  std::atomic<uint64_t> blob_count_;

  // the file will be selected for GC in this future epoch
  std::atomic<int64_t> gc_epoch_;

  // size of the file
  std::atomic<uint64_t> file_size_;

  // number of blobs in this particular file which have been evicted
  uint64_t deleted_count_;

  // size of deleted blobs (used by heuristic to select file for GC)
  uint64_t deleted_size_;

  blob_log::BlobLogHeader header_;

  // closed_ = true implies the file is no more mutable
  // no more blobs will be appended and the footer has been written out
  std::atomic<bool> closed_;

  // has a pass of garbage collection successfully finished on this file
  // can_be_deleted_ still needs to do iterator/snapshot checks
  std::atomic<bool> can_be_deleted_;

  // et - lt of the blobs
  ttlrange_t ttl_range_;

  // et - lt of the timestamp of the KV pairs.
  tsrange_t time_range_;

  // ESN - LSN of the blobs
  snrange_t sn_range_;

  // Sequential/Append writer for blobs
  std::shared_ptr<blob_log::Writer> log_writer_;

  // random access file reader for GET calls
  std::shared_ptr<RandomAccessFileReader> ra_file_reader_;

  // This Read-Write mutex is per file specific and protects
  // all the datastructures
  port::RWMutex mutex_;

  // time when the random access reader was last created.
  std::atomic<std::time_t> last_access_;

  // last time file was fsync'd/fdatasyncd
  std::atomic<uint64_t> last_fsync_;

  bool header_valid_;
public:

  BlobFile();

  BlobFile(const BlobDBImpl *parent, const std::string& bdir, uint64_t fnum);

  ~BlobFile();

  ColumnFamilyHandle *GetColumnFamily(DB *db);

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = blob_dir/000003.blob
  std::string PathName() const;

  // Primary identifier for blob file.
  // once the file is created, this never changes
  uint64_t BlobFileNumber() const { return file_number_; }

  // the following functions are atomic, and don't need
  // read lock
  uint64_t BlobCount() const { return blob_count_.load(std::memory_order_acquire); }

  // if the file has gone through GC and blobs have been relocated
  bool Obsolete() const { return can_be_deleted_.load(); }

  // if the file is not taking any more appends.
  bool Immutable() const { return closed_.load(); }

  // we will assume this is atomic
  bool NeedsFsync(bool hard, uint64_t bytes_per_sync) const;

  uint64_t GetFileSize() const { return file_size_.load(std::memory_order_acquire); }

  // All Get functions which are not atomic, will need ReadLock on the mutex
  tsrange_t GetTimeRange() const { assert(HasTimestamps()); return time_range_; }

  ttlrange_t GetTTLRange() const { return ttl_range_; }

  snrange_t GetSNRange() const { return sn_range_; }

  bool HasTTL() const { assert(header_valid_); return header_.HasTTL(); }

  bool HasTimestamps() const { assert(header_valid_); return header_.HasTimestamps(); }

  std::shared_ptr<blob_log::Writer> GetWriter() const { return log_writer_; }

  void Fsync();

 private:

  std::shared_ptr<blob_log::Reader> openSequentialReader(
    Env *env, const DBOptions& db_options,
    const EnvOptions& env_options) const;

  Status readFooter_locked(blob_log::BlobLogFooter& footer);

  Status writeFooterAndClose_locked();

  std::shared_ptr<RandomAccessFileReader> openRandomAccess_locked(
    Env *env, const EnvOptions& env_options, bool* fresh_open);

  void closeRandomAccess_locked();

  // this is used, when you are reading only the footer of a
  // previously closed file
  void setFromFooter_locked(const blob_log::BlobLogFooter& footer);

  void setHeader_locked(const blob_log::BlobLogHeader& hdr);

  // The following set functions will need write lock on the file.
  void setTTL() { header_.setTTL(); }

  void setTimestamps() { header_.setTimestamps(); }

  void setTimeRange(const tsrange_t& tr) { time_range_ = tr; }

  void setTTLRange(const ttlrange_t& ttl) { ttl_range_ = ttl; }

  void setSNRange(const snrange_t& snr) { sn_range_ = snr; }

  // The following functions are atomic, and don't need locks
  void setFileSize(uint64_t fs) { file_size_ = fs; }

  void setBlobCount(uint64_t bc) { blob_count_ = bc; }

  void setCanBeDeleted() { can_be_deleted_ = true; }
};

}
