//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <set>

#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

static const unsigned int kDumpUnitMetaSize = 16;
static const int kCacheDumpMajorVersion = 0;
static const int kCacheDumpMinorVersion = 1;

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
  SystemClock* clock;
};

class CacheDumper {
 public:
  virtual ~CacheDumper() = default;
  virtual Status SetDumpFilter(std::vector<DB*> db_list) = 0;
  virtual IOStatus DumpCacheEntriesToWriter() = 0;
};

class CacheDumpedLoader {
 public:
  virtual ~CacheDumpedLoader() = default;
  virtual IOStatus RestoreCacheEntriesToSecondaryCache() = 0;
};

IOStatus NewDefaultCacheDumpWriter(const std::shared_ptr<FileSystem>& fs,
                                   const FileOptions& file_opts,
                                   const std::string& file_name,
                                   std::unique_ptr<CacheDumpWriter>* writer);

IOStatus NewDefaultCacheDumpReader(const std::shared_ptr<FileSystem>& fs,
                                   const FileOptions& file_opts,
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
