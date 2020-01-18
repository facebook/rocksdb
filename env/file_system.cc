// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "env/composite_env_wrapper.h"
#include "rocksdb/file_system.h"
#include "options/db_options.h"
#include "rocksdb/utilities/object_registry.h"

namespace rocksdb {

FileSystem::FileSystem() {}

FileSystem::~FileSystem() {}

Status FileSystem::Load(const std::string& value,
                        std::shared_ptr<FileSystem>* result) {
  Status s;
#ifndef ROCKSDB_LITE
  s = ObjectRegistry::NewInstance()->NewSharedObject<FileSystem>(value, result);
#else
  (void)result;
  s = Status::NotSupported("Cannot load FileSystem in LITE mode: ", value);
#endif
  return s;
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

Status ReadFileToString(FileSystem* fs, const std::string& fname,
                        std::string* data) {
  FileOptions soptions;
  data->clear();
  std::unique_ptr<FSSequentialFile> file;
  Status s = fs->NewSequentialFile(fname, soptions, &file, nullptr);
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

#ifdef OS_WIN
std::shared_ptr<FileSystem> FileSystem::Default() {
  static LegacyFileSystemWrapper default_fs(Env::Default());
  static std::shared_ptr<LegacyFileSystemWrapper> default_fs_ptr(
      &default_fs, [](LegacyFileSystemWrapper*) {});
  return default_fs_ptr;
}
#endif

}  // namespace rocksdb
