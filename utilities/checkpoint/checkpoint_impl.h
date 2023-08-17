//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <string>

#include "file/filename.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"

namespace ROCKSDB_NAMESPACE {

class CheckpointImpl : public Checkpoint {
 public:
  explicit CheckpointImpl(DB* db) : db_(db) {}

  Status CreateCheckpoint(const std::string& checkpoint_dir,
                          uint64_t log_size_for_flush,
                          uint64_t* sequence_number_ptr) override;

  Status ExportColumnFamily(ColumnFamilyHandle* handle,
                            const std::string& export_dir,
                            ExportImportFilesMetaData** metadata) override;

  // Checkpoint logic can be customized by providing callbacks for link, copy,
  // or create.
  Status CreateCustomCheckpoint(
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname, FileType type)>
          link_file_cb,
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname, uint64_t size_limit_bytes,
                           FileType type, const std::string& checksum_func_name,
                           const std::string& checksum_val,
                           const Temperature src_temperature)>
          copy_file_cb,
      std::function<Status(const std::string& fname,
                           const std::string& contents, FileType type)>
          create_file_cb,
      uint64_t* sequence_number, uint64_t log_size_for_flush,
      bool get_live_table_checksum = false);

 private:
  void CleanStagingDirectory(const std::string& path, Logger* info_log);

  // Export logic customization by providing callbacks for link or copy.
  Status ExportFilesInMetaData(
      const DBOptions& db_options, const ColumnFamilyMetaData& metadata,
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname)>
          link_file_cb,
      std::function<Status(const std::string& src_dirname,
                           const std::string& fname)>
          copy_file_cb);

 private:
  DB* db_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
