//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/checkpoint.h"

#include <string>
#include "rocksdb/db.h"
#include "util/filename.h"

namespace rocksdb {

class CheckpointImpl : public Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable snapshots
  explicit CheckpointImpl(DB* db) : db_(db) {}

  // Builds an openable snapshot of RocksDB on the same disk, which
  // accepts an output directory on the same disk, and under the directory
  // (1) hard-linked SST files pointing to existing live SST files
  // SST files will be copied if output directory is on a different filesystem
  // (2) a copied manifest files and other files
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path
  using Checkpoint::CreateCheckpoint;
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir,
                                  uint64_t log_size_for_flush) override;

  // Gets the live set of SST files for a particular Column Family in
  // export_tables_dir and metadata about the files in metadata_vec.
  // The SST files will be created through hard links when the directory is in
  // the same partition as the db, copied otherwise.
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path. Triggers a flush.
  using Checkpoint::ExportColumnFamilyFiles;
  virtual Status ExportColumnFamilyFiles(
      ColumnFamilyHandle* column_family,
      std::vector<LiveFileMetaData>* metadata_vec,
      const std::string& export_tables_dir) override;

  // Checkpoint logic can be customized by providing callbacks for link, copy,
  // or create.
  Status CreateCustomCheckpoint(
      const DBOptions& db_options,
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname, FileType type)>
          link_file_cb,
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname, uint64_t size_limit_bytes,
                           FileType type)>
          copy_file_cb,
      std::function<Status(const std::string& fname,
                           const std::string& contents, FileType type)>
          create_file_cb,
      uint64_t* sequence_number, uint64_t log_size_for_flush);

 private:
  // Export logic can be customized by providing callbacks for link or copy.
  Status ExportFilesInMetaData(
      const DBOptions& db_options, const ColumnFamilyMetaData* metadata,
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname)>
          link_file_cb,
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname)>
          copy_file_cb);

 private:
  DB* db_;
};

}  //  namespace rocksdb

#endif  // ROCKSDB_LITE
