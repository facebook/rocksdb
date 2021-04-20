//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/backupable_db.h"

namespace ROCKSDB_NAMESPACE {

struct TEST_FutureSchemaVersion2Options {
  std::string version = "2";
  bool crc32c_checksums = false;
  bool file_sizes = true;
  std::map<std::string, std::string> meta_fields;
  std::map<std::string, std::string> file_fields;
  std::map<std::string, std::string> footer_fields;
};

// Modifies the BackupEngine(Impl) to write backup meta files using the
// unpublished schema version 2, for the life of this object (not backup_dir).
// TEST_FutureSchemaVersion2Options offers some customization for testing.
void TEST_EnableWriteFutureSchemaVersion2(
    BackupEngine *engine, const TEST_FutureSchemaVersion2Options &options);

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
