//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

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

void TEST_EnableWriteFutureSchemaVersion2(BackupEngine *engine, TEST_FutureSchemaVersion2Options options);

}  // namespace ROCKSDB_NAMESPACE
