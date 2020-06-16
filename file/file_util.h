//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <string>

#include "file/filename.h"
#include "options/db_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
// use_fsync maps to options.use_fsync, which determines the way that
// the file is synced after copying.
extern IOStatus CopyFile(FileSystem* fs, const std::string& source,
                         const std::string& destination, uint64_t size,
                         bool use_fsync);

extern IOStatus CreateFile(FileSystem* fs, const std::string& destination,
                           const std::string& contents, bool use_fsync);

extern Status DeleteDBFile(const ImmutableDBOptions* db_options,
                           const std::string& fname,
                           const std::string& path_to_sync, const bool force_bg,
                           const bool force_fg);

extern bool IsWalDirSameAsDBPath(const ImmutableDBOptions* db_options);

extern IOStatus GenerateOneFileChecksum(
    FileSystem* fs, const std::string& file_path,
    FileChecksumGenFactory* checksum_factory, std::string* file_checksum,
    std::string* file_checksum_func_name,
    size_t verify_checksums_readahead_size, bool allow_mmap_reads);

inline IOStatus PrepareIOFromReadOptions(const ReadOptions& ro, Env* env,
                                         IOOptions& opts) {
  if (!env) {
    env = Env::Default();
  }

  if (ro.deadline.count()) {
    std::chrono::microseconds now = std::chrono::microseconds(env->NowMicros());
    if (now > ro.deadline) {
      return IOStatus::TimedOut("Deadline exceeded");
    }
    opts.timeout = ro.deadline - now;
  }
  return IOStatus::OK();
}

}  // namespace ROCKSDB_NAMESPACE
