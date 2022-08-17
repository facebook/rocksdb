//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/cache_dump_load.h"

#include "file/writable_file_writer.h"
#include "port/lang.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "table/format.h"
#include "util/crc32c.h"
#include "utilities/cache_dump_load_impl.h"

namespace ROCKSDB_NAMESPACE {

IOStatus NewToFileCacheDumpWriter(const std::shared_ptr<FileSystem>& fs,
                                  const FileOptions& file_opts,
                                  const std::string& file_name,
                                  std::unique_ptr<CacheDumpWriter>* writer) {
  std::unique_ptr<WritableFileWriter> file_writer;
  IOStatus io_s = WritableFileWriter::Create(fs, file_name, file_opts,
                                             &file_writer, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }
  writer->reset(new ToFileCacheDumpWriter(std::move(file_writer)));
  return io_s;
}

IOStatus NewFromFileCacheDumpReader(const std::shared_ptr<FileSystem>& fs,
                                    const FileOptions& file_opts,
                                    const std::string& file_name,
                                    std::unique_ptr<CacheDumpReader>* reader) {
  std::unique_ptr<RandomAccessFileReader> file_reader;
  IOStatus io_s = RandomAccessFileReader::Create(fs, file_name, file_opts,
                                                 &file_reader, nullptr);
  if (!io_s.ok()) {
    return io_s;
  }
  reader->reset(new FromFileCacheDumpReader(std::move(file_reader)));
  return io_s;
}

Status NewDefaultCacheDumper(const CacheDumpOptions& dump_options,
                             const std::shared_ptr<Cache>& cache,
                             std::unique_ptr<CacheDumpWriter>&& writer,
                             std::unique_ptr<CacheDumper>* cache_dumper) {
  cache_dumper->reset(
      new CacheDumperImpl(dump_options, cache, std::move(writer)));
  return Status::OK();
}

Status NewDefaultCacheDumpedLoader(
    const CacheDumpOptions& dump_options,
    const BlockBasedTableOptions& toptions,
    const std::shared_ptr<SecondaryCache>& secondary_cache,
    std::unique_ptr<CacheDumpReader>&& reader,
    std::unique_ptr<CacheDumpedLoader>* cache_dump_loader) {
  cache_dump_loader->reset(new CacheDumpedLoaderImpl(
      dump_options, toptions, secondary_cache, std::move(reader)));
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
