//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

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
// It lacks lots of importatant functionalities, e.g. DB restarts,
// garbage collection, iterators, etc.
//
// The factory needs to be moved to include/rocksdb/utilities to allow
// users to use blob DB.

struct BlobDBOptions {
  // name of the directory under main db, where blobs will be stored.
  // default is "blob_dir"
  std::string blob_dir;

  // whether the blob_dir path is relative or absolute.
  bool path_relative;

  // is the eviction strategy fifo based
  bool is_fifo;

  // maximum size of the blob dir. Once this gets used, up
  // evict the blob file which is oldest (is_fifo )
  // 0 means no limits
  uint64_t blob_dir_size;

  // a new bucket is opened, for ttl_range. So if ttl_range is 600seconds
  // (10 minutes), and the first bucket starts at 1471542000
  // then the blob buckets will be
  // first bucket is 1471542000 - 1471542600
  // second bucket is 1471542600 - 1471543200
  // and so on
  uint32_t ttl_range_secs;

  // at what size will the blobs be stored in separate log rather than
  // inline
  uint64_t min_blob_size;

  // at what bytes will the blob files be synced to blob log.
  uint64_t bytes_per_sync;

  // the target size of each blob file. File will become immutable
  // after it exceeds that size
  uint64_t blob_file_size;

  // how many files to use for simple blobs at one time
  uint32_t num_concurrent_simple_blobs;

  // this function is to be provided by client if they intend to
  // use Put API to provide TTL.
  // the first argument is the value in the Put API
  // in case you want to do some modifications to the value,
  // return a new Slice in the second.
  // otherwise just copy the input value into output.
  // the ttl should be extracted and returned in last pointer.
  // otherwise assign it to -1
  std::function<bool(const Slice&, Slice*, int32_t*)> extract_ttl_fn;

  // eviction callback.
  // this function will be called for every blob that is getting
  // evicted.
  std::function<void(const ColumnFamilyHandle*, const Slice&, const Slice&)>
      gc_evict_cb_fn;

  // default ttl extactor
  bool default_ttl_extractor;

  // what compression to use for Blob's
  CompressionType compression;

  // default constructor
  BlobDBOptions();

  BlobDBOptions(const BlobDBOptions& in) = default;

  virtual ~BlobDBOptions() = default;
};

class BlobDB : public StackableDB {
 public:
  // the suffix to a blob value to represent "ttl:TTLVAL"
  static const uint64_t kTTLSuffixLength = 8;

 public:
  using rocksdb::StackableDB::Put;

  // This function needs to be called before destroying
  // the base DB
  static Status DestroyBlobDB(const std::string& dbname, const Options& options,
                              const BlobDBOptions& bdb_options);

  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) override = 0;

  using rocksdb::StackableDB::Delete;
  virtual Status Delete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override = 0;

  virtual Status PutWithTTL(const WriteOptions& options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value, int32_t ttl) = 0;

  virtual Status PutWithTTL(const WriteOptions& options, const Slice& key,
                            const Slice& value, int32_t ttl) {
    return PutWithTTL(options, DefaultColumnFamily(), key, value, ttl);
  }

  virtual Status PutUntil(const WriteOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& value, int32_t expiration) = 0;

  virtual Status PutUntil(const WriteOptions& options, const Slice& key,
                          const Slice& value, int32_t expiration) {
    return PutUntil(options, DefaultColumnFamily(), key, value, expiration);
  }

  using rocksdb::StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) override = 0;

  using rocksdb::StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override = 0;

  using rocksdb::StackableDB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& wopts,
                              ColumnFamilyHandle* column_family,
                              const Slice& key) override = 0;

  using rocksdb::StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  virtual Status Write(const WriteOptions& opts,
                       WriteBatch* updates) override = 0;

  // Starting point for opening a Blob DB.
  // changed_options - critical. Blob DB loads and inserts listeners
  // into options which are necessary for recovery and atomicity
  // Use this pattern if you need control on step 2, i.e. your
  // BaseDB is not just a simple rocksdb but a stacked DB
  // 1. ::OpenAndLoad
  // 2. Open Base DB with the changed_options
  // 3. ::LinkToBaseDB
  static Status OpenAndLoad(const Options& options,
                            const BlobDBOptions& bdb_options,
                            const std::string& dbname, BlobDB** blob_db,
                            Options* changed_options);

  // This is another way to open BLOB DB which do not have other
  // Stackable DB's in play
  // Steps.
  // 1. ::Open
  static Status Open(const Options& options, const BlobDBOptions& bdb_options,
                     const std::string& dbname, BlobDB** blob_db);

  static Status Open(const DBOptions& db_options,
                     const BlobDBOptions& bdb_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     BlobDB** blob_db, bool no_base_db = false);

  virtual ~BlobDB() {}

  virtual Status LinkToBaseDB(DB* db_base) = 0;

 protected:
  explicit BlobDB(DB* db);
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
