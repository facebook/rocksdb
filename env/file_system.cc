// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/file_system.h"

#include "env/composite_env_wrapper.h"
#include "env/env_chroot.h"
#include "env/env_encryption_ctr.h"
#include "env/fs_readonly.h"
#include "env/mock_env.h"
#include "logging/env_logger.h"
#include "options/db_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"
#include "utilities/counted_fs.h"
#include "utilities/env_timed.h"

namespace ROCKSDB_NAMESPACE {

FileSystem::FileSystem() {}

FileSystem::~FileSystem() {}

Status FileSystem::Load(const std::string& value,
                        std::shared_ptr<FileSystem>* result) {
  return CreateFromString(ConfigOptions(), value, result);
}

#ifndef ROCKSDB_LITE
static int RegisterBuiltinFileSystems(ObjectLibrary& library,
                                      const std::string& /*arg*/) {
  library.AddFactory<FileSystem>(
      TimedFileSystem::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* /* errmsg */) {
        guard->reset(new TimedFileSystem(nullptr));
        return guard->get();
      });
  library.AddFactory<FileSystem>(
      ReadOnlyFileSystem::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* /* errmsg */) {
        guard->reset(new ReadOnlyFileSystem(nullptr));
        return guard->get();
      });
  library.AddFactory<FileSystem>(
      EncryptedFileSystem::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* errmsg) {
        Status s = NewEncryptedFileSystemImpl(nullptr, nullptr, guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  library.AddFactory<FileSystem>(
      CountedFileSystem::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new CountedFileSystem(FileSystem::Default()));
        return guard->get();
      });
  library.AddFactory<FileSystem>(
      MockFileSystem::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* /*errmsg*/) {
        guard->reset(new MockFileSystem(SystemClock::Default()));
        return guard->get();
      });
#ifndef OS_WIN
  library.AddFactory<FileSystem>(
      ChrootFileSystem::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* /* errmsg */) {
        guard->reset(new ChrootFileSystem(nullptr, ""));
        return guard->get();
      });
#endif  // OS_WIN
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
#endif  // ROCKSDB_LITE

Status FileSystem::CreateFromString(const ConfigOptions& config_options,
                                    const std::string& value,
                                    std::shared_ptr<FileSystem>* result) {
  auto default_fs = FileSystem::Default();
  if (default_fs->IsInstanceOf(value)) {
    *result = default_fs;
    return Status::OK();
  } else {
#ifndef ROCKSDB_LITE
    static std::once_flag once;
    std::call_once(once, [&]() {
      RegisterBuiltinFileSystems(*(ObjectLibrary::Default().get()), "");
    });
#endif  // ROCKSDB_LITE
    return LoadSharedObject<FileSystem>(config_options, value, nullptr, result);
  }
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

IOStatus FileSystem::NewLogger(const std::string& fname,
                               const IOOptions& io_opts,
                               std::shared_ptr<Logger>* result,
                               IODebugContext* dbg) {
  FileOptions options;
  options.io_options = io_opts;
  // TODO: Tune the buffer size.
  options.writable_file_max_buffer_size = 1024 * 1024;
  std::unique_ptr<FSWritableFile> writable_file;
  const IOStatus status = NewWritableFile(fname, options, &writable_file, dbg);
  if (!status.ok()) {
    return status;
  }

  *result = std::make_shared<EnvLogger>(std::move(writable_file), fname,
                                        options, Env::Default());
  return IOStatus::OK();
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
    s = file->Read(kBufferSize, IOOptions(), &fragment, space, nullptr);
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

namespace {
static std::unordered_map<std::string, OptionTypeInfo> fs_wrapper_type_info = {
#ifndef ROCKSDB_LITE
    {"target",
     OptionTypeInfo::AsCustomSharedPtr<FileSystem>(
         0, OptionVerificationType::kByName, OptionTypeFlags::kDontSerialize)},
#endif  // ROCKSDB_LITE
};
}  // namespace
FileSystemWrapper::FileSystemWrapper(const std::shared_ptr<FileSystem>& t)
    : target_(t) {
  RegisterOptions("", &target_, &fs_wrapper_type_info);
}

Status FileSystemWrapper::PrepareOptions(const ConfigOptions& options) {
  if (target_ == nullptr) {
    target_ = FileSystem::Default();
  }
  return FileSystem::PrepareOptions(options);
}

#ifndef ROCKSDB_LITE
std::string FileSystemWrapper::SerializeOptions(
    const ConfigOptions& config_options, const std::string& header) const {
  auto parent = FileSystem::SerializeOptions(config_options, "");
  if (config_options.IsShallow() || target_ == nullptr ||
      target_->IsInstanceOf(FileSystem::kDefaultName())) {
    return parent;
  } else {
    std::string result = header;
    if (!StartsWith(parent, OptionTypeInfo::kIdPropName())) {
      result.append(OptionTypeInfo::kIdPropName()).append("=");
    }
    result.append(parent);
    if (!EndsWith(result, config_options.delimiter)) {
      result.append(config_options.delimiter);
    }
    result.append("target=").append(target_->ToString(config_options));
    return result;
  }
}
#endif  // ROCKSDB_LITE

DirFsyncOptions::DirFsyncOptions() { reason = kDefault; }

DirFsyncOptions::DirFsyncOptions(std::string file_renamed_new_name) {
  reason = kFileRenamed;
  renamed_new_name = file_renamed_new_name;
}

DirFsyncOptions::DirFsyncOptions(FsyncReason fsync_reason) {
  assert(fsync_reason != kFileRenamed);
  reason = fsync_reason;
}
}  // namespace ROCKSDB_NAMESPACE
