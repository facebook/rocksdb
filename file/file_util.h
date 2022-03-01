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
#include "rocksdb/system_clock.h"
#include "rocksdb/types.h"
#include "trace_replay/io_tracer.h"

namespace ROCKSDB_NAMESPACE {
// use_fsync maps to options.use_fsync, which determines the way that
// the file is synced after copying.
extern IOStatus CopyFile(FileSystem* fs, const std::string& source,
                         const std::string& destination, uint64_t size,
                         bool use_fsync,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         const Temperature temperature);
inline IOStatus CopyFile(const std::shared_ptr<FileSystem>& fs,
                         const std::string& source,
                         const std::string& destination, uint64_t size,
                         bool use_fsync,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         const Temperature temperature) {
  return CopyFile(fs.get(), source, destination, size, use_fsync, io_tracer,
                  temperature);
}
extern IOStatus CreateFile(FileSystem* fs, const std::string& destination,
                           const std::string& contents, bool use_fsync);

inline IOStatus CreateFile(const std::shared_ptr<FileSystem>& fs,
                           const std::string& destination,
                           const std::string& contents, bool use_fsync) {
  return CreateFile(fs.get(), destination, contents, use_fsync);
}

extern Status DeleteDBFile(const ImmutableDBOptions* db_options,
                           const std::string& fname,
                           const std::string& path_to_sync, const bool force_bg,
                           const bool force_fg);

extern IOStatus GenerateOneFileChecksum(
    FileSystem* fs, const std::string& file_path,
    FileChecksumGenFactory* checksum_factory,
    const std::string& requested_checksum_func_name, std::string* file_checksum,
    std::string* file_checksum_func_name,
    size_t verify_checksums_readahead_size, bool allow_mmap_reads,
    std::shared_ptr<IOTracer>& io_tracer, RateLimiter* rate_limiter,
    Env::IOPriority rate_limiter_priority);

inline IOStatus PrepareIOFromReadOptions(const ReadOptions& ro,
                                         SystemClock* clock, IOOptions& opts) {
  if (ro.deadline.count()) {
    std::chrono::microseconds now =
        std::chrono::microseconds(clock->NowMicros());
    // Ensure there is atleast 1us available. We don't want to pass a value of
    // 0 as that means no timeout
    if (now >= ro.deadline) {
      return IOStatus::TimedOut("Deadline exceeded");
    }
    opts.timeout = ro.deadline - now;
  }

  if (ro.io_timeout.count() &&
      (!opts.timeout.count() || ro.io_timeout < opts.timeout)) {
    opts.timeout = ro.io_timeout;
  }
  return IOStatus::OK();
}

// Test method to delete the input directory and all of its contents.
// This method is destructive and is meant for use only in tests!!!
Status DestroyDir(Env* env, const std::string& dir);
}  // namespace ROCKSDB_NAMESPACE
