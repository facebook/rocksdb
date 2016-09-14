#pragma once


#include <string>
#include <memory>
#include <atomic>
#include <set>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "utilities/blob_db/blob_db.h"
#include "util/instrumented_mutex.h"
#include "db/blob_log_writer.h"
#include "db/blob_log_reader.h"
#include "db/blob_log_format.h"
//#include "db/filename.h"
//#include "db/write_batch_internal.h"
//#include "rocksdb/convenience.h"
//#include "rocksdb/env.h"
//#include "rocksdb/iterator.h"
//#include "rocksdb/utilities/stackable_db.h"
//#include "table/block.h"
#include "util/file_reader_writer.h"


namespace rocksdb {

class BlobFile;

struct blobf_compare_ttl {
    bool operator() (const BlobFile* lhs, const BlobFile* rhs) const;
};

class BlobDBImpl : public BlobDB {
 public:
  using rocksdb::StackableDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using rocksdb::StackableDB::Get;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  
  Status PutWithTTL(const WriteOptions& options, const Slice& key,
             const Slice& value, uint32_t ttl) override;

  Status PutUntil(const WriteOptions& options, const Slice& key,
             const Slice& value, uint32_t expiration) override;

  Status Open();

  BlobDBImpl(DB* db, const BlobDBOptions& bdb_options);

  BlobFile *findBlobFile(uint32_t expiration) const;

  ~BlobDBImpl();

 private:

  Status openNewFile(BlobFile *& bfile_ret);

  Status openNewFileWithTTL(std::pair<uint32_t, uint32_t>& ttl_guess, BlobFile *& bfile_ret);

  Status openNewFile_P1_lock(std::unique_ptr<BlobFile>& bfile);

  Status addNewFile();

  Status openAllFiles();

  Status getSortedBlobLogs(const std::string& path);

  Status createWriter(BlobFile *bfile, Env *env, const EnvOptions& env_options, bool reopen = false);

  Status ReadFooter(BlobFile *bfile, blob_log::BlobLogFooter& footer);

 private:

  BlobDBOptions bdb_options_;

  std::string dbname_;
  std::string blob_dir_;

  ImmutableCFOptions ioptions_;
  DBOptions db_options_;
  InstrumentedMutex mutex_;

  std::unique_ptr<blob_log::Writer> current_log_writer_;

  std::atomic<uint64_t> next_file_number_;

  // entire metadata in memory
  std::unordered_map<uint64_t, std::unique_ptr<BlobFile>> blob_files_;

  BlobFile *open_simple_file_;

  std::set<BlobFile*, blobf_compare_ttl> open_blob_files_;
};

class BlobFile {

   friend class BlobDBImpl;
   friend struct blobf_compare_ttl;

 private:
   std::string path_to_dir_;
   uint64_t blob_count_;
   uint64_t file_number_;
   uint64_t file_size_;

   blob_log::BlobLogHeader header_;
   bool closed_;
   bool header_read_;

   std::pair<uint64_t, uint64_t> time_range_;
   std::pair<uint32_t, uint32_t> ttl_range_;
   std::pair<uint32_t, uint32_t> sn_range_;

   std::unique_ptr<WritableFile> wfile_;
   std::unique_ptr<WritableFileWriter> file_writer_;
   std::unique_ptr<blob_log::Writer> log_writer_;

   std::unique_ptr<RandomAccessFileReader> ra_file_reader_;

   std::unique_ptr<SequentialFile> sfile_;
   std::unique_ptr<SequentialFileReader> sfile_reader_;
   std::unique_ptr<blob_log::Reader> log_reader_;

   Status createSequentialReader(Env *env, const DBOptions& db_options, const EnvOptions& env_options);


 public:

  BlobFile() { }

  BlobFile(const std::string& bdir, uint64_t fn);
  
  ~BlobFile() {}

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = blob_dir/000003.blob
  std::string PathName() const;

  // Primary identifier for blob file.
  uint64_t BlobFileNumber() const { return file_number_; }

  uint64_t BlobCount() const { return blob_count_; }

  blob_log::Reader* GetReader() const { return log_reader_.get(); }

  blob_log::Writer* GetWriter() const { return log_writer_.get(); }

  bool Immutable() const { return closed_; }

  bool ActiveForAppend() const { return !Immutable(); }

  std::pair<uint64_t, uint64_t> GetTimeRange() const { assert(HasTimestamps()); return time_range_; }

  std::pair<uint32_t, uint32_t> GetTTLRange() const { assert(HasTTL()); return ttl_range_; }

  std::pair<uint64_t, uint64_t> GetSNRange() const { return sn_range_; }

  bool HasTTL() const { return header_.HasTTL(); }

  bool HasTimestamps() const { return header_.HasTimestamps(); }

  blob_log::BlobLogHeader& Header() { return header_; }

  Status ReadHeader();

  Status WriteFooterAndClose();

  uint64_t GetFileSize() const { return file_size_; }

 private:

  RandomAccessFileReader* openRandomAccess(Env *env, const EnvOptions& env_options);

  // this is used, when you are reading only the footer of a 
  // previously closed file
  void setFromFooter(const blob_log::BlobLogFooter& footer);

  void setBlobCount(uint64_t bc) { blob_count_ = bc; }

  void setTTL() { header_.setTTL(); }

  void setTimestamps() { header_.setTimestamps(); }

  void setTimeRange(const std::pair<uint64_t, uint64_t>& tr) { time_range_ = tr; }

  void setTTLRange(const std::pair<uint32_t, uint32_t>& ttl) { ttl_range_ = ttl; }

  void setSNRange(const std::pair<uint32_t, uint32_t>& snr) { sn_range_ = snr; }

  void setFileSize(uint64_t fs) { file_size_ = fs; }
};

}
