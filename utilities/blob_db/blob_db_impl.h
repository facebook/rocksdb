#pragma once


#include <string>
#include <memory>
#include <atomic>
#include <set>
#include <condition_variable>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "utilities/blob_db/blob_db.h"
#include "util/mutexlock.h"
#include "util/cf_options.h"
#include "util/mpsc.h"
#include "db/blob_log_writer.h"
#include "db/blob_log_reader.h"
#include "db/blob_log_format.h"
#include "util/file_reader_writer.h"


namespace rocksdb {

class BlobFile;
class DBImpl;
class ColumnFamilyHandle;
class OptimisticTransactionDBImpl;

struct blobf_compare_ttl {
    bool operator() (const std::shared_ptr<BlobFile>& lhs,
      const std::shared_ptr<BlobFile>& rhs) const;
};

typedef std::pair<uint32_t, uint32_t> ttlrange_t;
typedef std::pair<uint64_t, uint64_t> tsrange_t;
typedef std::pair<rocksdb::SequenceNumber, rocksdb::SequenceNumber> snrange_t;

class BlobDBImpl : public BlobDB {
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

  Status Open();

  BlobDBImpl(DB* db, const BlobDBOptions& bdb_options);

  ~BlobDBImpl();

 private:

  void closeIf(std::shared_ptr<BlobFile>& bfile);

  Status PutCommon(std::shared_ptr<BlobFile>& bfile,
    const WriteOptions& options, ColumnFamilyHandle *column_family,
    const char *headerbuf, const Slice& key, const Slice& val);

  std::shared_ptr<BlobFile> selectBlobFileTTL(uint32_t expiration);

  std::shared_ptr<BlobFile> selectBlobFile();

  void updateWriteOptions(const WriteOptions& options);

  void shutdown();

  // periodic sanity check
  void sanityCheck();

  void runGC();

  Status startGCThreads();

  std::shared_ptr<BlobFile> findBlobFile_locked(uint32_t expiration) const;

  std::shared_ptr<BlobFile> openNewFile_P1();

  Status addNewFile();

  Status openAllFiles();

  Status getSortedBlobLogs(const std::string& path);

  // this holds BlobFile mutex
  Status createWriter_locked(BlobFile *bfile, bool reopen = false);

  Status writeBatchOfDeleteKeys(BlobFile *bfptr);

  bool TryDeleteFile(std::shared_ptr<BlobFile>& bfile);

  std::shared_ptr<blob_log::Writer> checkOrCreateWriter_locked(BlobFile *bfile);

  void evictDeletions();

 private:

  DBImpl* db_impl_;
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
};

class BlobFile {

  friend class BlobDBImpl;
  friend struct blobf_compare_ttl;

private:
  std::string path_to_dir_;
  std::atomic<uint64_t> blob_count_;
  uint64_t file_number_;
  std::atomic<uint64_t> file_size_;
  uint64_t deleted_count_;
  uint64_t deleted_size_;

  blob_log::BlobLogHeader header_;

  bool closed_;
  bool header_read_;
  bool can_be_deleted_;

  ttlrange_t ttl_range_;
  tsrange_t time_range_;
  snrange_t sn_range_;

  std::shared_ptr<blob_log::Writer> log_writer_;

  std::shared_ptr<RandomAccessFileReader> ra_file_reader_;

  std::shared_ptr<blob_log::Reader> log_reader_;

  port::RWMutex mutex_;

  std::shared_ptr<blob_log::Reader> openSequentialReader_locked(
    Env *env, const DBOptions& db_options,
    const EnvOptions& env_options, bool rewind = false);

  void canBeDeleted() { can_be_deleted_ = true; }

public:

  BlobFile();

  BlobFile(const std::string& bdir, uint64_t fnum);

  ~BlobFile();

  bool Obsolete() const { return can_be_deleted_; }

  ColumnFamilyHandle *GetColumnFamily(DB *db);

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = blob_dir/000003.blob
  std::string PathName() const;

  // Primary identifier for blob file.
  uint64_t BlobFileNumber() const { return file_number_; }

  uint64_t BlobCount() const { return blob_count_.load(std::memory_order_acquire); }

  std::shared_ptr<blob_log::Reader> GetReader() const { return log_reader_; }

  std::shared_ptr<blob_log::Writer> GetWriter() const { return log_writer_; }

  bool Immutable() const { return closed_; }

  bool ActiveForAppend() const { return !Immutable(); }

  static void Fsync(void *arg);

  void Fsync_member();

  tsrange_t GetTimeRange() const { assert(HasTimestamps()); return time_range_; }

  ttlrange_t GetTTLRange() const { assert(HasTTL()); return ttl_range_; }

  snrange_t GetSNRange() const { return sn_range_; }

  bool HasTTL() const { return header_.HasTTL(); }

  bool HasTimestamps() const { return header_.HasTimestamps(); }

  blob_log::BlobLogHeader& Header() { return header_; }

  Status ReadHeader();

  Status WriteFooterAndClose_locked();

  uint64_t GetFileSize() const { return file_size_.load(std::memory_order_acquire); }

  Status ReadFooter(blob_log::BlobLogFooter& footer, bool close_reader = false);

 private:

  std::shared_ptr<RandomAccessFileReader> openRandomAccess_locked(Env *env, const EnvOptions& env_options);

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
