#pragma once


#include <string>
#include <memory>

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

class BlobDBImpl : public BlobDB {
 public:
  using rocksdb::StackableDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using rocksdb::StackableDB::Get;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  
  BlobDBImpl(DB* db, const BlobDBOptions& bdb_options);

  Status Open();

 private:

  BlobDBOptions bdb_options_;
  std::string dbname_;
  ImmutableCFOptions ioptions_;
  InstrumentedMutex mutex_;
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  size_t writer_offset_;
  size_t next_sync_offset_;

  static const std::string kFileName;
  static const size_t kBlockHeaderSize;
  static const size_t kBytesPerSync;
};


}
