//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db.h"
#include "db/write_batch_internal.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {

namespace blob_db {
port::Mutex listener_mutex;
typedef std::shared_ptr<BlobDBFlushBeginListener> FlushBeginListener_t;
typedef std::shared_ptr<BlobReconcileWalFilter> ReconcileWalFilter_t;
typedef std::shared_ptr<EvictAllVersionsCompactionListener>
    CompactionListener_t;

// to ensure the lifetime of the listeners
std::vector<std::shared_ptr<EventListener>> all_blobdb_listeners;
std::vector<ReconcileWalFilter_t> all_wal_filters;

Status BlobDB::OpenAndLoad(const Options& options,
                           const BlobDBOptions& bdb_options,
                           const std::string& dbname, BlobDB** blob_db,
                           Options* changed_options) {
  *changed_options = options;
  *blob_db = nullptr;

  FlushBeginListener_t fblistener =
      std::make_shared<BlobDBFlushBeginListener>();
  ReconcileWalFilter_t rw_filter = std::make_shared<BlobReconcileWalFilter>();
  CompactionListener_t ce_listener =
      std::make_shared<EvictAllVersionsCompactionListener>();

  {
    MutexLock l(&listener_mutex);
    all_blobdb_listeners.push_back(fblistener);
    all_blobdb_listeners.push_back(ce_listener);
    all_wal_filters.push_back(rw_filter);
  }

  changed_options->listeners.emplace_back(fblistener);
  changed_options->listeners.emplace_back(ce_listener);
  changed_options->wal_filter = rw_filter.get();

  DBOptions db_options(*changed_options);

  // we need to open blob db first so that recovery can happen
  BlobDBImpl* bdb = new BlobDBImpl(dbname, bdb_options, db_options);

  fblistener->SetImplPtr(bdb);
  ce_listener->SetImplPtr(bdb);
  rw_filter->SetImplPtr(bdb);

  Status s = bdb->OpenPhase1();
  if (!s.ok()) return s;

  *blob_db = bdb;
  return s;
}

Status BlobDB::Open(const Options& options, const BlobDBOptions& bdb_options,
                    const std::string& dbname, BlobDB** blob_db) {
  *blob_db = nullptr;
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = BlobDB::Open(db_options, bdb_options, dbname, column_families,
                          &handles, blob_db);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status BlobDB::Open(const DBOptions& db_options,
                    const BlobDBOptions& bdb_options, const std::string& dbname,
                    const std::vector<ColumnFamilyDescriptor>& column_families,
                    std::vector<ColumnFamilyHandle*>* handles, BlobDB** blob_db,
                    bool no_base_db) {
  *blob_db = nullptr;

  DBOptions my_db_options(db_options);
  FlushBeginListener_t fblistener =
      std::make_shared<BlobDBFlushBeginListener>();
  CompactionListener_t ce_listener =
      std::make_shared<EvictAllVersionsCompactionListener>();
  ReconcileWalFilter_t rw_filter = std::make_shared<BlobReconcileWalFilter>();

  my_db_options.listeners.emplace_back(fblistener);
  my_db_options.listeners.emplace_back(ce_listener);
  my_db_options.wal_filter = rw_filter.get();

  {
    MutexLock l(&listener_mutex);
    all_blobdb_listeners.push_back(fblistener);
    all_blobdb_listeners.push_back(ce_listener);
    all_wal_filters.push_back(rw_filter);
  }

  // we need to open blob db first so that recovery can happen
  BlobDBImpl* bdb = new BlobDBImpl(dbname, bdb_options, my_db_options);
  fblistener->SetImplPtr(bdb);
  ce_listener->SetImplPtr(bdb);
  rw_filter->SetImplPtr(bdb);

  Status s = bdb->OpenPhase1();
  if (!s.ok()) return s;

  if (no_base_db) return s;

  DB* db = nullptr;
  s = DB::Open(my_db_options, dbname, column_families, handles, &db);
  if (!s.ok()) return s;

  // set the implementation pointer
  s = bdb->LinkToBaseDB(db);
  if (!s.ok()) {
    delete bdb;
    bdb = nullptr;
  }
  *blob_db = bdb;
  return s;
}

BlobDB::BlobDB(DB* db) : StackableDB(db) {}

////////////////////////////////////////////////////////////////////////////////
//
//
// std::function<int(double)> fnCaller =
//     std::bind(&A::fn, &anInstance, std::placeholders::_1);
////////////////////////////////////////////////////////////////////////////////
BlobDBOptions::BlobDBOptions()
    : blob_dir("blob_dir"),
      path_relative(true),
      is_fifo(false),
      blob_dir_size(1000ULL * 1024ULL * 1024ULL * 1024ULL),
      ttl_range_secs(3600),
      min_blob_size(512),
      bytes_per_sync(0),
      blob_file_size(256 * 1024 * 1024),
      num_concurrent_simple_blobs(4),
      default_ttl_extractor(false),
      compression(kNoCompression) {}

}  // namespace blob_db
}  // namespace rocksdb
#endif
