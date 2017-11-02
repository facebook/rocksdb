//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/blob_db/blob_db.h"

#include <inttypes.h>

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
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "utilities/blob_db/blob_compaction_filter.h"
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
  if (options.compaction_filter != nullptr ||
      options.compaction_filter_factory != nullptr) {
    return Status::NotSupported("Blob DB doesn't support compaction filter.");
  }

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
    if (bdb_options.enable_garbage_collection) {
      all_blobdb_listeners.push_back(ce_listener);
    }
    all_wal_filters.push_back(rw_filter);
  }

  changed_options->compaction_filter_factory.reset(
      new BlobIndexCompactionFilterFactory(options.env));
  changed_options->listeners.emplace_back(fblistener);
  if (bdb_options.enable_garbage_collection) {
    changed_options->listeners.emplace_back(ce_listener);
  }
  changed_options->wal_filter = rw_filter.get();

  DBOptions db_options(*changed_options);

  // we need to open blob db first so that recovery can happen
  BlobDBImpl* bdb = new BlobDBImpl(dbname, bdb_options, db_options);

  fblistener->SetImplPtr(bdb);
  if (bdb_options.enable_garbage_collection) {
    ce_listener->SetImplPtr(bdb);
  }
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

Status BlobDB::Open(const DBOptions& db_options_input,
                    const BlobDBOptions& bdb_options, const std::string& dbname,
                    const std::vector<ColumnFamilyDescriptor>& column_families,
                    std::vector<ColumnFamilyHandle*>* handles, BlobDB** blob_db,
                    bool no_base_db) {
  if (column_families.size() != 1 ||
      column_families[0].name != kDefaultColumnFamilyName) {
    return Status::NotSupported(
        "Blob DB doesn't support non-default column family.");
  }
  *blob_db = nullptr;
  Status s;

  DBOptions db_options(db_options_input);
  if (db_options.info_log == nullptr) {
    s = CreateLoggerFromOptions(dbname, db_options, &db_options.info_log);
    if (!s.ok()) {
      return s;
    }
  }

  FlushBeginListener_t fblistener =
      std::make_shared<BlobDBFlushBeginListener>();
  CompactionListener_t ce_listener =
      std::make_shared<EvictAllVersionsCompactionListener>();
  ReconcileWalFilter_t rw_filter = std::make_shared<BlobReconcileWalFilter>();

  db_options.listeners.emplace_back(fblistener);
  if (bdb_options.enable_garbage_collection) {
    db_options.listeners.emplace_back(ce_listener);
  }
  db_options.wal_filter = rw_filter.get();

  {
    MutexLock l(&listener_mutex);
    all_blobdb_listeners.push_back(fblistener);
    if (bdb_options.enable_garbage_collection) {
      all_blobdb_listeners.push_back(ce_listener);
    }
    all_wal_filters.push_back(rw_filter);
  }

  ColumnFamilyOptions cf_options(column_families[0].options);
  if (cf_options.compaction_filter != nullptr ||
      cf_options.compaction_filter_factory != nullptr) {
    return Status::NotSupported("Blob DB doesn't support compaction filter.");
  }
  cf_options.compaction_filter_factory.reset(
      new BlobIndexCompactionFilterFactory(db_options.env));
  ColumnFamilyDescriptor cf_descriptor(kDefaultColumnFamilyName, cf_options);

  // we need to open blob db first so that recovery can happen
  BlobDBImpl* bdb = new BlobDBImpl(dbname, bdb_options, db_options);
  fblistener->SetImplPtr(bdb);
  if (bdb_options.enable_garbage_collection) {
    ce_listener->SetImplPtr(bdb);
  }
  rw_filter->SetImplPtr(bdb);

  s = bdb->OpenPhase1();
  if (!s.ok()) {
    delete bdb;
    return s;
  }

  if (no_base_db) {
    *blob_db = bdb;
    return s;
  }

  DB* db = nullptr;
  s = DB::Open(db_options, dbname, {cf_descriptor}, handles, &db);
  if (!s.ok()) {
    delete bdb;
    return s;
  }

  // set the implementation pointer
  s = bdb->LinkToBaseDB(db);
  if (!s.ok()) {
    delete bdb;
    bdb = nullptr;
  }
  *blob_db = bdb;
  bdb_options.Dump(db_options.info_log.get());
  return s;
}

BlobDB::BlobDB(DB* db) : StackableDB(db) {}

void BlobDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "                 blob_db_options.blob_dir: %s",
                   blob_dir.c_str());
  ROCKS_LOG_HEADER(log, "            blob_db_options.path_relative: %d",
                   path_relative);
  ROCKS_LOG_HEADER(log, "                  blob_db_options.is_fifo: %d",
                   is_fifo);
  ROCKS_LOG_HEADER(log, "            blob_db_options.blob_dir_size: %" PRIu64,
                   blob_dir_size);
  ROCKS_LOG_HEADER(log, "           blob_db_options.ttl_range_secs: %" PRIu32,
                   ttl_range_secs);
  ROCKS_LOG_HEADER(log, "           blob_db_options.bytes_per_sync: %" PRIu64,
                   bytes_per_sync);
  ROCKS_LOG_HEADER(log, "           blob_db_options.blob_file_size: %" PRIu64,
                   blob_file_size);
  ROCKS_LOG_HEADER(log, "            blob_db_options.ttl_extractor: %p",
                   ttl_extractor.get());
  ROCKS_LOG_HEADER(log, "              blob_db_options.compression: %d",
                   static_cast<int>(compression));
  ROCKS_LOG_HEADER(log, "blob_db_options.enable_garbage_collection: %d",
                   enable_garbage_collection);
  ROCKS_LOG_HEADER(log, " blob_db_options.disable_background_tasks: %d",
                   disable_background_tasks);
}

}  // namespace blob_db
}  // namespace rocksdb
#endif
