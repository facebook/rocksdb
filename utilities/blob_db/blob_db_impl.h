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

class BlobReconcileWalFilter : public WalFilter {
public:

  virtual WalFilter::WalProcessingOption LogRecordFound(unsigned long long log_number,
    const std::string& log_file_name,
    const WriteBatch& batch,
    WriteBatch* new_batch,
    bool* batch_changed) override;

  virtual const char* Name() const override {
    return "BlobDBWalReconciler";
  }
};

struct blobf_compare_ttl {
    bool operator() (const std::shared_ptr<BlobFile>& lhs,
      const std::shared_ptr<BlobFile>& rhs) const;
};

typedef std::pair<uint32_t, uint32_t> ttlrange_t;
typedef std::pair<uint64_t, uint64_t> tsrange_t;
typedef std::pair<rocksdb::SequenceNumber, rocksdb::SequenceNumber> snrange_t;

class BlobDBImpl : public BlobDB {
  friend class BlobDBFlushBeginListener;
 public:
  using rocksdb::StackableDB::Put;
  Status Put(const WriteOptions& options,
             ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;

  using rocksdb::StackableDB::Delete;
  Status Delete(const WriteOptions& options,
             ColumnFamilyHandle* column_family, const Slice& key)
             override;

  using rocksdb::StackableDB::Get;
  Status Get(const ReadOptions& options,
             ColumnFamilyHandle* column_family, const Slice& key,
             std::string* value) override;

  Status PutWithTTL(const WriteOptions& options,
             ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value, uint32_t ttl) override;

  Status PutUntil(const WriteOptions& options,
             ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value, uint32_t expiration) override;

  void OnFlushBeginHandler(DB* db, const FlushJobInfo& info);

  Status Open();

  Status OpenP1();

  void setDBImplPtr(DB* db);

  BlobDBImpl(DB* db, const BlobDBOptions& bdb_options);

  BlobDBImpl(const std::string& dbname, const BlobDBOptions& bdb_options,
    const Options& options, const DBOptions& db_options,
    const EnvOptions& env_options);

  ~BlobDBImpl();

 private:

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

  Status getAllLogFiles(std::set<std::pair<uint64_t, std::string>>* file_nums);

  // appends a task into timer queue to close the file
  void closeIf(const std::shared_ptr<BlobFile>& bfile);

  Status PutCommon(std::shared_ptr<BlobFile>& bfile,
    const WriteOptions& options, ColumnFamilyHandle *column_family,
    const char *headerbuf, const Slice& key, const Slice& val);

  std::shared_ptr<BlobFile> selectBlobFileTTL(uint32_t expiration);

  std::shared_ptr<BlobFile> selectBlobFile();

  void updateWriteOptions(const WriteOptions& options);

  void shutdown();

  // periodic sanity check
  std::pair<bool, int64_t> sanityCheck(bool aborted);

  std::pair<bool, int64_t> deleteObsFiles(bool aborted);

  std::pair<bool, int64_t> runGC(bool aborted);

  std::pair<bool, int64_t> fsyncFiles(bool aborted);

  // periodically check if seq files and their TTL's has expired
  std::pair<bool, int64_t> checkSeqFiles(bool aborted);

  std::pair<bool, int64_t> reclaimOpenFiles(bool aborted);

  std::pair<bool, int64_t> waStats(bool aborted);

  void startGCThreads();

  std::shared_ptr<BlobFile> findBlobFile_locked(uint32_t expiration) const;

  std::shared_ptr<BlobFile> openNewFile_P1();

  Status addNewFile();

  Status openAllFiles();

  Status getSortedBlobLogs(const std::string& path);

  // this holds BlobFile mutex
  Status createWriter_locked(BlobFile *bfile, bool reopen = false);

  Status writeBatchOfDeleteKeys(BlobFile *bfptr, std::time_t tt);

  bool DeleteFileOK_locked(const std::shared_ptr<BlobFile>& bfile);

  std::shared_ptr<blob_log::Writer> checkOrCreateWriter_locked(BlobFile *bfile);

  std::pair<bool, int64_t> evictDeletions(bool aborted);

 private:

  DBImpl* db_impl_;
  Env *myenv_;
  std::shared_ptr<OptimisticTransactionDBImpl> opt_db_;

  // a boolean to capture whether write_options has been set
  std::atomic<bool> wo_set_;
  WriteOptions write_options_;
  BlobDBOptions bdb_options_;
  ImmutableCFOptions ioptions_;
  DBOptions db_options_;
  EnvOptions env_options_;

  std::string dbname_;
  std::string blob_dir_;

  port::RWMutex mutex_;

  std::unique_ptr<blob_log::Writer> current_log_writer_;

  std::atomic<uint64_t> next_file_number_;

  // entire metadata in memory
  std::unordered_map<uint64_t, std::shared_ptr<BlobFile>> blob_files_;

  std::atomic<uint64_t> epoch_of_;

  std::vector<std::shared_ptr<BlobFile> > open_simple_files_;

  std::set<std::shared_ptr<BlobFile>, blobf_compare_ttl> open_blob_files_;

  struct delete_packet_t {
    ColumnFamilyHandle *cfh_;
    std::string key_;
    SequenceNumber dsn_;
  };

  mpsc_queue_t<delete_packet_t> delete_keys_q_;

  std::vector<std::thread> gc_threads_;
  std::atomic_bool shutdown_;
  std::condition_variable gc_cv_;
  std::mutex gc_mutex_;
  TimerQueue tqueue_;
  std::uint64_t current_epoch_;
  std::atomic<std::uint32_t> open_file_count_;

  // should hold mutex to modify
  // STATISTICS for WA of Blob Files due to GC
  // collect by default 24 hourly periods
  std::list<uint64_t> all_periods_write_;
  std::list<uint64_t> all_periods_ampl_;

  std::atomic<uint64_t> last_period_write_;
  std::atomic<uint64_t> last_period_ampl_;

  uint64_t total_periods_write_;
  uint64_t total_periods_ampl_;

  std::atomic<uint64_t> total_blob_space_;
  std::list<std::shared_ptr<BlobFile>> obsolete_files_;
  bool open_p1_done_;
};

class BlobFile {

  friend class BlobDBImpl;
  friend struct blobf_compare_ttl;

private:
  std::string path_to_dir_;
  std::atomic<uint64_t> blob_count_;
  std::time_t last_gc_;
  std::atomic<int64_t> gc_epoch_;
  uint64_t file_number_;
  std::atomic<uint64_t> file_size_;
  uint64_t deleted_count_;
  uint64_t deleted_size_;

  blob_log::BlobLogHeader header_;

  std::atomic<bool> seq_open_;
  std::atomic<bool> closed_;
  bool header_read_;
  std::atomic<bool> can_be_deleted_;

  ttlrange_t ttl_range_;
  tsrange_t time_range_;
  snrange_t sn_range_;

  std::shared_ptr<blob_log::Writer> log_writer_;

  std::shared_ptr<RandomAccessFileReader> ra_file_reader_;

  std::shared_ptr<blob_log::Reader> log_reader_;

  port::RWMutex mutex_;

  std::atomic<std::time_t> last_access_;

  std::atomic<uint64_t> last_fsync_;

public:

  BlobFile();

  BlobFile(const std::string& bdir, uint64_t fnum);

  ~BlobFile();

  // we will assume this is atomic
  bool NeedsFsync(bool hard, uint64_t bytes_per_sync) const;

  bool Obsolete() const { return can_be_deleted_.load(); }

  ColumnFamilyHandle *GetColumnFamily(DB *db);

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = blob_dir/000003.blob
  std::string PathName() const;

  // Primary identifier for blob file.
  uint64_t BlobFileNumber() const { return file_number_; }

  uint64_t BlobCount() const { return blob_count_.load(std::memory_order_acquire); }

  std::shared_ptr<blob_log::Reader> GetReader() const { return log_reader_; }

  std::shared_ptr<blob_log::Writer> GetWriter() const { return log_writer_; }

  bool Immutable() const { return closed_.load(); }

  bool ActiveForAppend() const { return !Immutable(); }

  static void Fsync(void *arg);

  void Fsync_member();

  tsrange_t GetTimeRange() const { assert(HasTimestamps()); return time_range_; }

  ttlrange_t GetTTLRange() const { return ttl_range_; }

  snrange_t GetSNRange() const { return sn_range_; }

  bool HasTTL() const { return header_.HasTTL(); }

  bool HasTimestamps() const { return header_.HasTimestamps(); }

  blob_log::BlobLogHeader& Header() { return header_; }

  Status ReadHeader();

  Status WriteFooterAndClose_locked();

  uint64_t GetFileSize() const { return file_size_.load(std::memory_order_acquire); }

  Status ReadFooter(blob_log::BlobLogFooter& footer, bool close_reader = false);

 private:

  std::shared_ptr<blob_log::Reader> openSequentialReader_locked(
    Env *env, const DBOptions& db_options,
    const EnvOptions& env_options, bool rewind = false);

  void canBeDeleted() { can_be_deleted_ = true; }

  void closeRandomAccess_locked();

  std::shared_ptr<RandomAccessFileReader> openRandomAccess_locked(
    Env *env, const EnvOptions& env_options, bool* fresh_open);

  // this is used, when you are reading only the footer of a
  // previously closed file
  void setFromFooter(const blob_log::BlobLogFooter& footer);

  void setBlobCount(uint64_t bc) { blob_count_ = bc; }

  void setTTL() { header_.setTTL(); }

  void setTimestamps() { header_.setTimestamps(); }

  void setTimeRange(const tsrange_t& tr) { time_range_ = tr; }

  void setTTLRange(const ttlrange_t& ttl) { ttl_range_ = ttl; }

  void setSNRange(const snrange_t& snr) { sn_range_ = snr; }

  void setFileSize(uint64_t fs) { file_size_ = fs; }
};

}
