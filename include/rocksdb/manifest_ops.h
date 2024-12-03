//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>

#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/file_checksum.h"

namespace ROCKSDB_NAMESPACE {

Status GetCurrentManifestPath(const std::string& abs_path,
                              FileSystem* fs, bool is_retry,
                              std::string* manifest_path,
                              uint64_t* manifest_file_number);

// Requires absolute path to CURRENT file directory as a parameter.
// Returns the FileChecksumList constructed purely off of the MANIFEST file
// that was considered 'current' at the time of invoking this API.
//
// NOTE: We're NOT locking the referenced MANIFEST-* file.
// It might become stale in case of intertwining DB operations.
Status GetFileChecksumsFromCurrentManifest(Env* src_env,
                                           const std::string &abs_path,
                                           uint64_t manifest_file_size,
                                           FileChecksumList *checksum_list);

}  // namespace ROCKSDB_NAMESPACE
