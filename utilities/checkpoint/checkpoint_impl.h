//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

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
  DB* db_;
};

}  //  namespace rocksdb

#endif  // ROCKSDB_LITE
