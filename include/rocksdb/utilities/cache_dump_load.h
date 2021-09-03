//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <set>

#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

class CacheDumpWriter {
 public:
  virtual ~CacheDumpWriter() = default;

  virtual IOStatus Write(const Slice& data) = 0;
  virtual IOStatus Close() = 0;
  virtual uint64_t GetFileSize() = 0;
};

class CacheDumpReader {
 public:
  virtual ~CacheDumpReader() = default;
  virtual IOStatus Read(size_t len, std::string* data) = 0;
  virtual size_t GetOffset() const = 0;
  virtual IOStatus Close() = 0;
};

// CacheDumpOptions is used to control DumpCache()
struct CacheDumpOptions {
  // The env pointer
  Env* env;
  // The prefix set, which filter out the cache keys that does not match
  std::set<std::string> prefix_set;
};

class CacheDumper {
 public:
  virtual ~CacheDumper() = default;
  virtual Status Prepare() = 0;
  virtual IOStatus Run() = 0;
};

class CacheDumpedLoader {
 public:
  virtual ~CacheDumpedLoader() = default;
  virtual Status Prepare() = 0;
  virtual IOStatus Run() = 0;
};

Status NewDefaultCacheDumpWriter(Env* env, const EnvOptions& env_options,
                                 const std::string& file_name,
                                 std::unique_ptr<CacheDumpWriter>* writer);

Status NewDefaultCacheDumpReader(Env* env, const EnvOptions& env_options,
                                 const std::string& file_name,
                                 std::unique_ptr<CacheDumpReader>* reader);

Status NewDefaultCacheDumper(const CacheDumpOptions& dump_options,
                             const std::shared_ptr<Cache>& cache,
                             std::unique_ptr<CacheDumpWriter>&& writer,
                             std::unique_ptr<CacheDumper>* cache_dumper);

Status NewDefaultCacheDumpedLoader(
    const CacheDumpOptions& dump_options,
    const BlockBasedTableOptions& toptions,
    const std::shared_ptr<SecondaryCache>& secondary_cache,
    std::unique_ptr<CacheDumpReader>&& reader,
    std::unique_ptr<CacheDumpedLoader>* cache_dump_loader);

}  // namespace ROCKSDB_NAMESPACE
