// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/file_system.h"

#include "env/composite_env_wrapper.h"
#include "options/db_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {

FileSystem::FileSystem() {}

FileSystem::~FileSystem() {}

Status FileSystem::Load(const std::string& value,
                        std::shared_ptr<FileSystem>* result) {
  return CreateFromString(ConfigOptions(), value, result);
}

Status FileSystem::CreateFromString(const ConfigOptions& config_options,
                                    const std::string& value,
                                    std::shared_ptr<FileSystem>* result) {
  Status s;
#ifndef ROCKSDB_LITE
  (void)config_options;
  s = ObjectRegistry::NewInstance()->NewSharedObject<FileSystem>(value, result);
#else
  (void)config_options;
  (void)result;
  s = Status::NotSupported("Cannot load FileSystem in LITE mode", value);
#endif
  return s;
}

IOStatus FileSystem::ReuseWritableFile(const std::string& fname,
                                       const std::string& old_fname,
                                       const FileOptions& opts,
                                       std::unique_ptr<FSWritableFile>* result,
                                       IODebugContext* dbg) {
  IOStatus s = RenameFile(old_fname, fname, opts.io_options, dbg);
  if (!s.ok()) {
    return s;
  }
  return NewWritableFile(fname, opts, result, dbg);
}

FileOptions FileSystem::OptimizeForLogRead(
              const FileOptions& file_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.use_direct_reads = false;
  return optimized_file_options;
}

FileOptions FileSystem::OptimizeForManifestRead(
    const FileOptions& file_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.use_direct_reads = false;
  return optimized_file_options;
}

FileOptions FileSystem::OptimizeForLogWrite(const FileOptions& file_options,
                                           const DBOptions& db_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.bytes_per_sync = db_options.wal_bytes_per_sync;
  optimized_file_options.writable_file_max_buffer_size =
      db_options.writable_file_max_buffer_size;
  return optimized_file_options;
}

FileOptions FileSystem::OptimizeForManifestWrite(
    const FileOptions& file_options) const {
  return file_options;
}

FileOptions FileSystem::OptimizeForCompactionTableWrite(
    const FileOptions& file_options,
    const ImmutableDBOptions& db_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.use_direct_writes =
      db_options.use_direct_io_for_flush_and_compaction;
  return optimized_file_options;
}

FileOptions FileSystem::OptimizeForCompactionTableRead(
    const FileOptions& file_options,
    const ImmutableDBOptions& db_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.use_direct_reads = db_options.use_direct_reads;
  return optimized_file_options;
}

FileOptions FileSystem::OptimizeForBlobFileRead(
    const FileOptions& file_options,
    const ImmutableDBOptions& db_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.use_direct_reads = db_options.use_direct_reads;
  return optimized_file_options;
}

IOStatus WriteStringToFile(FileSystem* fs, const Slice& data,
                           const std::string& fname, bool should_sync) {
  std::unique_ptr<FSWritableFile> file;
  EnvOptions soptions;
  IOStatus s = fs->NewWritableFile(fname, soptions, &file, nullptr);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data, IOOptions(), nullptr);
  if (s.ok() && should_sync) {
    s = file->Sync(IOOptions(), nullptr);
  }
  if (!s.ok()) {
    fs->DeleteFile(fname, IOOptions(), nullptr);
  }
  return s;
}

IOStatus ReadFileToString(FileSystem* fs, const std::string& fname,
                          std::string* data) {
  FileOptions soptions;
  data->clear();
  std::unique_ptr<FSSequentialFile> file;
  IOStatus s = status_to_io_status(
      fs->NewSequentialFile(fname, soptions, &file, nullptr));
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, IOOptions(), &fragment, space,
                   nullptr);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
