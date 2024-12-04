//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cassert>

#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Parameter is_retry=true sets the verify_and_reconstruct_read flag.
// It comes handy when caller intends to re-read the data with much stronger
// data integrity checking - e.g. in case of a perceived file corruption.
Status GetCurrentManifestPath(const std::string& dbname, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number);

// This function is intended to be called with the DB closed and does not write
// to the DB path. It will usually work on an open DB but may fail spuriously
// in that case, and results may be out of date on return.
Status GetFileChecksumsFromCurrentManifest(FileSystem* fs,
                                           const std::string& dbname,
                                           FileChecksumList* checksum_list);

}  // namespace ROCKSDB_NAMESPACE
