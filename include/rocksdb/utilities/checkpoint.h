// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A checkpoint is an openable snapshot of a database at a point in time.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DB;
class ColumnFamilyHandle;
struct LiveFileMetaData;
struct ExportImportFilesMetaData;

class Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable snapshots
  static Status Create(DB* db, Checkpoint** checkpoint_ptr);

  // Builds an openable snapshot of RocksDB. checkpoint_dir should contain an
  // absolute path. The specified directory should not exist, since it will be
  // created by the API.
  // When a checkpoint is created,
  // (1) SST and blob files are hard linked if the output directory is on the
  // same filesystem as the database, and copied otherwise.
  // (2) other required files (like MANIFEST) are always copied.
  // log_size_for_flush: if the total log file size is equal or larger than
  // this value, then a flush is triggered for all the column families. The
  // default value is 0, which means flush is always triggered. If you move
  // away from the default, the checkpoint may not contain up-to-date data
  // if WAL writing is not always enabled.
  // Flush will always trigger if it is 2PC.
  // sequence_number_ptr: if it is not nullptr, the value it points to will be
  // set to a sequence number guaranteed to be part of the DB, not necessarily
  // the latest. The default value of this parameter is nullptr.
  // NOTE: db_paths and cf_paths are not supported for creating checkpoints
  // and NotSupported will be returned when the DB (without WALs) uses more
  // than one directory.
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir,
                                  uint64_t log_size_for_flush = 0,
                                  uint64_t* sequence_number_ptr = nullptr);

  // Exports all live SST files of a specified Column Family onto export_dir,
  // returning SST files information in metadata.
  // - SST files will be created as hard links when the directory specified
  //   is in the same partition as the db directory, copied otherwise.
  // - export_dir should not already exist and will be created by this API.
  // - Always triggers a flush.
  virtual Status ExportColumnFamily(ColumnFamilyHandle* handle,
                                    const std::string& export_dir,
                                    ExportImportFilesMetaData** metadata);

  virtual ~Checkpoint() {}
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
