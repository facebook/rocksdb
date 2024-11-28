//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

Status GetCurrentManifestPathUtil(const std::string& dbname,
                                  FileSystem* fs, bool is_retry,
                                  std::string* manifest_path,
                                  uint64_t* manifest_file_number);

}
