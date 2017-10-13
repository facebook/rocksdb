//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <atomic>
#include <memory>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "util/file_reader_writer.h"
#include "utilities/blob_db/blob_log_format.h"
#include "utilities/blob_db/blob_log_writer.h"

namespace rocksdb {
namespace blob_db {

class BlobFile {
  friend class BlobDBImpl;
  friend struct blobf_compare_ttl;

 private:
  // access to parent
  const BlobDBImpl* parent_;

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

  BlobLogHeader header_;

  // closed_ = true implies the file is no more mutable
  // no more blobs will be appended and the footer has been written out
  std::atomic<bool> closed_;

  // has a pass of garbage collection successfully finished on this file
  // can_be_deleted_ still needs to do iterator/snapshot checks
  std::atomic<bool> can_be_deleted_;

  // should this file been gc'd once to reconcile lost deletes/compactions
  std::atomic<bool> gc_once_after_open_;

  // et - lt of the blobs
  ttlrange_t ttl_range_;

  // et - lt of the timestamp of the KV pairs.
  tsrange_t time_range_;

  // ESN - LSN of the blobs
  snrange_t sn_range_;

  // Sequential/Append writer for blobs
  std::shared_ptr<Writer> log_writer_;

  // random access file reader for GET calls
  std::shared_ptr<RandomAccessFileReader> ra_file_reader_;

  // This Read-Write mutex is per file specific and protects
  // all the datastructures
  mutable port::RWMutex mutex_;

  // time when the random access reader was last created.
  std::atomic<std::int64_t> last_access_;

  // last time file was fsync'd/fdatasyncd
  std::atomic<uint64_t> last_fsync_;

  bool header_valid_;

 public:
  BlobFile();

  BlobFile(const BlobDBImpl* parent, const std::string& bdir, uint64_t fnum);

  ~BlobFile();

  ColumnFamilyHandle* GetColumnFamily(DB* db);

  // Returns log file's pathname relative to the main db dir
  // Eg. For a live-log-file = blob_dir/000003.blob
  std::string PathName() const;

  // Primary identifier for blob file.
  // once the file is created, this never changes
  uint64_t BlobFileNumber() const { return file_number_; }

  // the following functions are atomic, and don't need
  // read lock
  uint64_t BlobCount() const {
    return blob_count_.load(std::memory_order_acquire);
  }

  std::string DumpState() const;

  // if the file has gone through GC and blobs have been relocated
  bool Obsolete() const { return can_be_deleted_.load(); }

  // if the file is not taking any more appends.
  bool Immutable() const { return closed_.load(); }

  // we will assume this is atomic
  bool NeedsFsync(bool hard, uint64_t bytes_per_sync) const;

  uint64_t GetFileSize() const {
    return file_size_.load(std::memory_order_acquire);
  }

  // All Get functions which are not atomic, will need ReadLock on the mutex
  tsrange_t GetTimeRange() const {
    assert(HasTimestamp());
    return time_range_;
  }

  ttlrange_t GetTTLRange() const { return ttl_range_; }

  snrange_t GetSNRange() const { return sn_range_; }

  bool HasTTL() const {
    assert(header_valid_);
    return header_.HasTTL();
  }

  bool HasTimestamp() const {
    assert(header_valid_);
    return header_.HasTimestamp();
  }

  std::shared_ptr<Writer> GetWriter() const { return log_writer_; }

  void Fsync();

 private:
  std::shared_ptr<Reader> OpenSequentialReader(
      Env* env, const DBOptions& db_options,
      const EnvOptions& env_options) const;

  Status ReadFooter(BlobLogFooter* footer);

  Status WriteFooterAndCloseLocked();

  std::shared_ptr<RandomAccessFileReader> GetOrOpenRandomAccessReader(
      Env* env, const EnvOptions& env_options, bool* fresh_open);

  void CloseRandomAccessLocked();

  // this is used, when you are reading only the footer of a
  // previously closed file
  Status SetFromFooterLocked(const BlobLogFooter& footer);

  void set_time_range(const tsrange_t& tr) { time_range_ = tr; }

  void set_ttl_range(const ttlrange_t& ttl) { ttl_range_ = ttl; }

  void SetSNRange(const snrange_t& snr) { sn_range_ = snr; }

  // The following functions are atomic, and don't need locks
  void SetFileSize(uint64_t fs) { file_size_ = fs; }

  void SetBlobCount(uint64_t bc) { blob_count_ = bc; }

  void SetCanBeDeleted() { can_be_deleted_ = true; }
};
}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
