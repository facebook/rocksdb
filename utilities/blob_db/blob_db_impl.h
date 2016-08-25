#pragma once


#include <string>
#include <memory>
#include <atomic>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "utilities/blob_db/blob_db.h"
#include "util/instrumented_mutex.h"
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

class BlobDBImpl : public BlobDB {
 public:
  using rocksdb::StackableDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using rocksdb::StackableDB::Get;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  
  BlobDBImpl(DB* db, const BlobDBOptions& bdb_options);

  Status PutWithTTL(const WriteOptions& options, const Slice& key,
             const Slice& value, uint32_t ttl) override;

  Status PutUntil(const WriteOptions& options, const Slice& key,
             const Slice& value, uint32_t expiration) override;

  Status Open();

 private:

  Status openNewFile();

  Status addNewFile();

  Status openAllFiles();

  Status GetSortedBlobLogs(const std::string& path);

 private:

  BlobDBOptions bdb_options_;
  std::string dbname_;
  std::string blob_dir_;
  ImmutableCFOptions ioptions_;
  InstrumentedMutex mutex_;
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  size_t writer_offset_;
  size_t next_sync_offset_;
  std::atomic<uint64_t> next_file_number_;
  std::unordered_map<uint64_t, std::unique_ptr<BlobFile>> blob_files_;

  static const std::string kFileName;
  static const size_t kBlockHeaderSize;
  static const size_t kBytesPerSync;
};

class BlobFile {

 private:
   std::string path_to_dir_;
   uint64_t blob_count_;
   uint64_t file_number_;
   uint64_t file_size_;
   bool has_ttl_;
   bool has_timestamps_;
   std::pair<uint32_t, uint32_t> time_range_;

 public:

  BlobFile() { }

  BlobFile(const std::string& bdir, uint64_t fn);
  
  void setTTL() { has_ttl_ = true; }

  void setTimestamps() { has_timestamps_ = true; }

  void setTimeRange(const std::pair<uint32_t, uint32_t>& tr) { time_range_ = tr; }

  void setFileSize(uint64_t fs) { file_size_ = fs; }

  ~BlobFile() {}

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = blob_dir/000003.blob
  std::string PathName() const;

  // Primary identifier for blob file.
  uint64_t BlobFileNumber() const { return file_number_; }

  uint64_t BlobCount() const { return blob_count_; }

  bool Immutable() const;

  std::pair<uint32_t, uint32_t> GetTimeRange() const;

  bool HasTTL() const { return has_ttl_; }

  bool HasTimestamps() const { return has_timestamps_; }

  // Size of log file on disk in Bytes
  uint64_t SizeFileBytes() const { return file_size_; }
};

}
