//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <functional>
#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/stackable_db.h"

namespace rocksdb {

namespace blob_db {

// A wrapped database which puts values of KV pairs in a separate log
// and store location to the log in the underlying DB.
//
// The factory needs to be moved to include/rocksdb/utilities to allow
// users to use blob DB.

struct BlobDBOptions {
  // name of the directory under main db, where blobs will be stored.
  // default is "blob_dir"
  std::string blob_dir = "blob_dir";

  // whether the blob_dir path is relative or absolute.
  bool path_relative = true;

  // When max_db_size is reached, evict blob files to free up space
  // instead of returnning NoSpace error on write. Blob files will be
  // evicted from oldest to newest, based on file creation time.
  bool is_fifo = false;

  // Maximum size of the database (including SST files and blob files).
  //
  // Default: 0 (no limits)
  uint64_t max_db_size = 0;

  // a new bucket is opened, for ttl_range. So if ttl_range is 600seconds
  // (10 minutes), and the first bucket starts at 1471542000
  // then the blob buckets will be
  // first bucket is 1471542000 - 1471542600
  // second bucket is 1471542600 - 1471543200
  // and so on
  uint64_t ttl_range_secs = 3600;

  // The smallest value to store in blob log. Values smaller than this threshold
  // will be inlined in base DB together with the key.
  uint64_t min_blob_size = 0;

  // Allows OS to incrementally sync blob files to disk for every
  // bytes_per_sync bytes written. Users shouldn't rely on it for
  // persistency guarantee.
  uint64_t bytes_per_sync = 512 * 1024;

  // the target size of each blob file. File will become immutable
  // after it exceeds that size
  uint64_t blob_file_size = 256 * 1024 * 1024;

  // what compression to use for Blob's
  CompressionType compression = kNoCompression;

  // If enabled, BlobDB cleans up stale blobs in non-TTL files during compaction
  // by rewriting the remaining live blobs to new files.
  bool enable_garbage_collection = false;

  // The cutoff in terms of blob file age for garbage collection. Blobs in
  // the oldest N non-TTL blob files will be rewritten when encountered during
  // compaction, where N = garbage_collection_cutoff * number_of_non_TTL_files.
  double garbage_collection_cutoff = 0.25;

  // Disable all background job. Used for test only.
  bool disable_background_tasks = false;

  void Dump(Logger* log) const;
};

class BlobDB : public StackableDB {
 public:
  using rocksdb::StackableDB::Put;
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) override = 0;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    return Put(options, key, value);
  }

  using rocksdb::StackableDB::Delete;
  virtual Status Delete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    assert(db_ != nullptr);
    return db_->Delete(options, column_family, key);
  }

  virtual Status PutWithTTL(const WriteOptions& options, const Slice& key,
                            const Slice& value, uint64_t ttl) = 0;
  virtual Status PutWithTTL(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value, uint64_t ttl) {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    return PutWithTTL(options, key, value, ttl);
  }

  // Put with expiration. Key with expiration time equal to
  // std::numeric_limits<uint64_t>::max() means the key don't expire.
  virtual Status PutUntil(const WriteOptions& options, const Slice& key,
                          const Slice& value, uint64_t expiration) = 0;
  virtual Status PutUntil(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& value, uint64_t expiration) {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    return PutUntil(options, key, value, expiration);
  }

  using rocksdb::StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override = 0;

  // Get value and expiration.
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value, uint64_t* expiration) = 0;
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     PinnableSlice* value, uint64_t* expiration) {
    return Get(options, DefaultColumnFamily(), key, value, expiration);
  }

  using rocksdb::StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override = 0;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override {
    for (auto column_family : column_families) {
      if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
        return std::vector<Status>(
            column_families.size(),
            Status::NotSupported(
                "Blob DB doesn't support non-default column family."));
      }
    }
    return MultiGet(options, keys, values);
  }
  virtual void MultiGet(const ReadOptions& /*options*/,
                        ColumnFamilyHandle* /*column_family*/,
                        const size_t num_keys, const Slice* /*keys*/,
                        PinnableSlice* /*values*/, Status* statuses,
                        const bool /*sorted_input*/ = false) override {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = Status::NotSupported(
          "Blob DB doesn't support batched MultiGet");
    }
  }

  using rocksdb::StackableDB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& /*wopts*/,
                              ColumnFamilyHandle* /*column_family*/,
                              const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  using rocksdb::StackableDB::Merge;
  virtual Status Merge(const WriteOptions& /*options*/,
                       ColumnFamilyHandle* /*column_family*/,
                       const Slice& /*key*/, const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  virtual Status Write(const WriteOptions& opts,
                       WriteBatch* updates) override = 0;
  using rocksdb::StackableDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options) override = 0;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      // Blob DB doesn't support non-default column family.
      return nullptr;
    }
    return NewIterator(options);
  }

  Status CompactFiles(
      const CompactionOptions& compact_options,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override = 0;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override {
    if (column_family->GetID() != DefaultColumnFamily()->GetID()) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }

    return CompactFiles(compact_options, input_file_names, output_level,
                        output_path_id, output_file_names, compaction_job_info);
  }

  using rocksdb::StackableDB::Close;
  virtual Status Close() override = 0;

  // Opening blob db.
  static Status Open(const Options& options, const BlobDBOptions& bdb_options,
                     const std::string& dbname, BlobDB** blob_db);

  static Status Open(const DBOptions& db_options,
                     const BlobDBOptions& bdb_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     BlobDB** blob_db);

  virtual BlobDBOptions GetBlobDBOptions() const = 0;

  virtual Status SyncBlobFiles() = 0;

  virtual ~BlobDB() {}

 protected:
  explicit BlobDB();
};

// Destroy the content of the database.
Status DestroyBlobDB(const std::string& dbname, const Options& options,
                     const BlobDBOptions& bdb_options);

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
