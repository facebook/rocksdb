//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#include <string>
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/stackable_db.h"

namespace rocksdb {


// A wrapped database which puts values of KV pairs in a separate log
// and store location to the log in the underlying DB.
// It lacks lots of importatant functionalities, e.g. DB restarts,
// garbage collection, iterators, etc.
//
// The factory needs to be moved to include/rocksdb/utilities to allow
// users to use blob DB.

struct BlobDBOptions {

  // name of the directory under main db, where blobs will be stored.
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
  uint32_t ttl_range;

  // at what size will the blobs be stored in separate log rather than
  // inline
  uint64_t min_blob_size;

  // at what bytes will the blob files be synced to blob log.
  uint64_t bytes_per_sync;

  uint64_t blob_file_size;

  // how many files to use for simple blobs at one time
  uint32_t num_concurrent_simple_blobs;

  // deletions check period
  uint32_t deletion_check_period;

  // gc percentage each check period
  uint32_t gc_file_pct;

  // gc period
  uint32_t gc_check_period;

  // sanity check task
  uint32_t sanity_check_period;

  // how many random access open files can we tolerate
  uint32_t open_files_trigger;

  // how many periods of stats do we keep.
  uint32_t wa_num_stats_periods;

  // what is the length of any period
  uint32_t wa_stats_period;

  // we will garbage collect blob files in
  // which entire files have expired. However if the
  // ttl_range of files is very large say a day, we
  // would have to wait for the entire day, before we
  // recover most of the space.
  uint32_t partial_expiration_gc_range;

  // this should be based on allowed Write Amplification
  // if 50% of the space of a blob file has been deleted/expired,
  uint32_t partial_expiration_pct;

  // how often should we schedule a job to fsync open files
  uint32_t fsync_files_period;

  // default constructor
  BlobDBOptions();
};

class BlobDB : public StackableDB {
 public:
  using rocksdb::StackableDB::Put;

  virtual Status Put(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& value) override  = 0;

  using rocksdb::StackableDB::Delete;
  virtual Status Delete(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key)
    override = 0;

  virtual Status PutWithTTL(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& value, uint32_t ttl) = 0;

  virtual Status PutUntil(const WriteOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    const Slice& value, uint32_t expiration) = 0;

  using rocksdb::StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
    ColumnFamilyHandle* column_family, const Slice& key,
    std::string* value) override = 0;

  static Status Open(const Options& options, const BlobDBOptions& bdb_options,
      const std::string& dbname, BlobDB** blob_db);

  virtual ~BlobDB() { }

protected:

  explicit BlobDB(DB* db);

};

}  // namespace rocksdb
