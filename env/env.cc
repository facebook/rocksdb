//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/env.h"

#include <thread>
#include "env/composite_env_wrapper.h"
#include "logging/env_logger.h"
#include "memory/arena.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class LegacyFileSystemWrapper : public FileSystem {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit LegacyFileSystemWrapper(Env* t) : target_(t) {}
  ~LegacyFileSystemWrapper() override {}

  const char* Name() const override { return "Legacy File System"; }

  // Return the target to which this Env forwards all calls
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target()
  IOStatus NewSequentialFile(const std::string& f, const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* r,
                             IODebugContext* /*dbg*/) override {
    std::unique_ptr<SequentialFile> file;
    Status s = target_->NewSequentialFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacySequentialFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewRandomAccessFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* r,
                               IODebugContext* /*dbg*/) override {
    std::unique_ptr<RandomAccessFile> file;
    Status s = target_->NewRandomAccessFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyRandomAccessFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->NewWritableFile(f, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->ReopenWritableFile(fname, &file, file_opts);
    if (s.ok()) {
      result->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* r,
                             IODebugContext* /*dbg*/) override {
    std::unique_ptr<WritableFile> file;
    Status s = target_->ReuseWritableFile(fname, old_fname, &file, file_opts);
    if (s.ok()) {
      r->reset(new LegacyWritableFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* /*dbg*/) override {
    std::unique_ptr<RandomRWFile> file;
    Status s = target_->NewRandomRWFile(fname, &file, file_opts);
    if (s.ok()) {
      result->reset(new LegacyRandomRWFileWrapper(std::move(file)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return status_to_io_status(
        target_->NewMemoryMappedFileBuffer(fname, result));
  }
  IOStatus NewDirectory(const std::string& name, const IOOptions& /*io_opts*/,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* /*dbg*/) override {
    std::unique_ptr<Directory> dir;
    Status s = target_->NewDirectory(name, &dir);
    if (s.ok()) {
      result->reset(new LegacyDirectoryWrapper(std::move(dir)));
    }
    return status_to_io_status(std::move(s));
  }
  IOStatus FileExists(const std::string& f, const IOOptions& /*io_opts*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->FileExists(f));
  }
  IOStatus GetChildren(const std::string& dir, const IOOptions& /*io_opts*/,
                       std::vector<std::string>* r,
                       IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetChildren(dir, r));
  }
  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& /*options*/,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetChildrenFileAttributes(dir, result));
  }
  IOStatus DeleteFile(const std::string& f, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->DeleteFile(f));
  }
  IOStatus Truncate(const std::string& fname, size_t size,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->Truncate(fname, size));
  }
  IOStatus CreateDir(const std::string& d, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->CreateDir(d));
  }
  IOStatus CreateDirIfMissing(const std::string& d,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->CreateDirIfMissing(d));
  }
  IOStatus DeleteDir(const std::string& d, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->DeleteDir(d));
  }
  IOStatus GetFileSize(const std::string& f, const IOOptions& /*options*/,
                       uint64_t* s, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetFileSize(f, s));
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& /*options*/,
                                   uint64_t* file_mtime,
                                   IODebugContext* /*dbg*/) override {
    return status_to_io_status(
        target_->GetFileModificationTime(fname, file_mtime));
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*options*/,
                           std::string* output_path,
                           IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetAbsolutePath(db_path, output_path));
  }

  IOStatus RenameFile(const std::string& s, const std::string& t,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->RenameFile(s, t));
  }

  IOStatus LinkFile(const std::string& s, const std::string& t,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->LinkFile(s, t));
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& /*options*/,
                        uint64_t* count, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->NumFileLinks(fname, count));
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& /*options*/, bool* res,
                        IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->AreFilesSame(first, second, res));
  }

  IOStatus LockFile(const std::string& f, const IOOptions& /*options*/,
                    FileLock** l, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->LockFile(f, l));
  }

  IOStatus UnlockFile(FileLock* l, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->UnlockFile(l));
  }

  IOStatus GetTestDirectory(const IOOptions& /*options*/, std::string* path,
                            IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetTestDirectory(path));
  }
  IOStatus NewLogger(const std::string& fname, const IOOptions& /*options*/,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->NewLogger(fname, result));
  }

  void SanitizeFileOptions(FileOptions* opts) const override {
    target_->SanitizeEnvOptions(opts);
  }

  FileOptions OptimizeForLogRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForLogRead(file_options);
  }
  FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestRead(file_options);
  }
  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override {
    return target_->OptimizeForLogWrite(file_options, db_options);
  }
  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    return target_->OptimizeForManifestWrite(file_options);
  }
  FileOptions OptimizeForCompactionTableWrite(
      const FileOptions& file_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_->OptimizeForCompactionTableWrite(file_options,
                                                    immutable_ops);
  }
  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForCompactionTableRead(file_options, db_options);
  }

#ifdef GetFreeSpace
#undef GetFreeSpace
#endif
  IOStatus GetFreeSpace(const std::string& path, const IOOptions& /*options*/,
                        uint64_t* diskfree, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->GetFreeSpace(path, diskfree));
  }
  IOStatus IsDirectory(const std::string& path, const IOOptions& /*options*/,
                       bool* is_dir, IODebugContext* /*dbg*/) override {
    return status_to_io_status(target_->IsDirectory(path, is_dir));
  }

 private:
  Env* target_;
};
}  // end anonymous namespace

Env::Env() : thread_status_updater_(nullptr) {
  file_system_ = std::make_shared<LegacyFileSystemWrapper>(this);
}

Env::Env(std::shared_ptr<FileSystem> fs)
  : thread_status_updater_(nullptr),
    file_system_(fs) {}

Env::~Env() {
}

Status Env::NewLogger(const std::string& fname,
                      std::shared_ptr<Logger>* result) {
  return NewEnvLogger(fname, this, result);
}

Status Env::LoadEnv(const std::string& value, Env** result) {
  Env* env = *result;
  Status s;
#ifndef ROCKSDB_LITE
  s = ObjectRegistry::NewInstance()->NewStaticObject<Env>(value, &env);
#else
  s = Status::NotSupported("Cannot load environment in LITE mode", value);
#endif
  if (s.ok()) {
    *result = env;
  }
  return s;
}

Status Env::LoadEnv(const std::string& value, Env** result,
                    std::shared_ptr<Env>* guard) {
  assert(result);
  Status s;
#ifndef ROCKSDB_LITE
  Env* env = nullptr;
  std::unique_ptr<Env> uniq_guard;
  std::string err_msg;
  assert(guard != nullptr);
  env = ObjectRegistry::NewInstance()->NewObject<Env>(value, &uniq_guard,
                                                      &err_msg);
  if (!env) {
    s = Status::NotFound(std::string("Cannot load ") + Env::Type() + ": " +
                         value);
    env = Env::Default();
  }
  if (s.ok() && uniq_guard) {
    guard->reset(uniq_guard.release());
    *result = guard->get();
  } else {
    *result = env;
  }
#else
  (void)result;
  (void)guard;
  s = Status::NotSupported("Cannot load environment in LITE mode", value);
#endif
  return s;
}

std::string Env::PriorityToString(Env::Priority priority) {
  switch (priority) {
    case Env::Priority::BOTTOM:
      return "Bottom";
    case Env::Priority::LOW:
      return "Low";
    case Env::Priority::HIGH:
      return "High";
    case Env::Priority::USER:
      return "User";
    case Env::Priority::TOTAL:
      assert(false);
  }
  return "Invalid";
}

uint64_t Env::GetThreadID() const {
  std::hash<std::thread::id> hasher;
  return hasher(std::this_thread::get_id());
}

Status Env::ReuseWritableFile(const std::string& fname,
                              const std::string& old_fname,
                              std::unique_ptr<WritableFile>* result,
                              const EnvOptions& options) {
  Status s = RenameFile(old_fname, fname);
  if (!s.ok()) {
    return s;
  }
  return NewWritableFile(fname, result, options);
}

Status Env::GetChildrenFileAttributes(const std::string& dir,
                                      std::vector<FileAttributes>* result) {
  assert(result != nullptr);
  std::vector<std::string> child_fnames;
  Status s = GetChildren(dir, &child_fnames);
  if (!s.ok()) {
    return s;
  }
  result->resize(child_fnames.size());
  size_t result_size = 0;
  for (size_t i = 0; i < child_fnames.size(); ++i) {
    const std::string path = dir + "/" + child_fnames[i];
    if (!(s = GetFileSize(path, &(*result)[result_size].size_bytes)).ok()) {
      if (FileExists(path).IsNotFound()) {
        // The file may have been deleted since we listed the directory
        continue;
      }
      return s;
    }
    (*result)[result_size].name = std::move(child_fnames[i]);
    result_size++;
  }
  result->resize(result_size);
  return Status::OK();
}

Status Env::GetHostNameString(std::string* result) {
  std::array<char, kMaxHostNameLen> hostname_buf;
  Status s = GetHostName(hostname_buf.data(), hostname_buf.size());
  if (s.ok()) {
    hostname_buf[hostname_buf.size() - 1] = '\0';
    result->assign(hostname_buf.data());
  }
  return s;
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

MemoryMappedFileBuffer::~MemoryMappedFileBuffer() {}

Logger::~Logger() {}

Status Logger::Close() {
  if (!closed_) {
    closed_ = true;
    return CloseImpl();
  } else {
    return Status::OK();
  }
}

Status Logger::CloseImpl() { return Status::NotSupported(); }

FileLock::~FileLock() {
}

void LogFlush(Logger *info_log) {
  if (info_log) {
    info_log->Flush();
  }
}

static void Logv(Logger *info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Log(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log, format, ap);
  va_end(ap);
}

void Logger::Logv(const InfoLogLevel log_level, const char* format, va_list ap) {
  static const char* kInfoLogLevelNames[5] = { "DEBUG", "INFO", "WARN",
    "ERROR", "FATAL" };
  if (log_level < log_level_) {
    return;
  }

  if (log_level == InfoLogLevel::INFO_LEVEL) {
    // Doesn't print log level if it is INFO level.
    // This is to avoid unexpected performance regression after we add
    // the feature of log level. All the logs before we add the feature
    // are INFO level. We don't want to add extra costs to those existing
    // logging.
    Logv(format, ap);
  } else if (log_level == InfoLogLevel::HEADER_LEVEL) {
    LogHeader(format, ap);
  } else {
    char new_format[500];
    snprintf(new_format, sizeof(new_format) - 1, "[%s] %s",
      kInfoLogLevelNames[log_level], format);
    Logv(new_format, ap);
  }

  if (log_level >= InfoLogLevel::WARN_LEVEL &&
      log_level != InfoLogLevel::HEADER_LEVEL) {
    // Log messages with severity of warning or higher should be rare and are
    // sometimes followed by an unclean crash. We want to be sure important
    // messages are not lost in an application buffer when that happens.
    Flush();
  }
}

static void Logv(const InfoLogLevel log_level, Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= log_level) {
    if (log_level == InfoLogLevel::HEADER_LEVEL) {
      info_log->LogHeader(format, ap);
    } else {
      info_log->Logv(log_level, format, ap);
    }
  }
}

void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
         ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log, format, ap);
  va_end(ap);
}

static void Headerv(Logger *info_log, const char *format, va_list ap) {
  if (info_log) {
    info_log->LogHeader(format, ap);
  }
}

void Header(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log, format, ap);
  va_end(ap);
}

static void Debugv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    info_log->Logv(InfoLogLevel::DEBUG_LEVEL, format, ap);
  }
}

void Debug(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log, format, ap);
  va_end(ap);
}

static void Infov(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Info(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log, format, ap);
  va_end(ap);
}

static void Warnv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::WARN_LEVEL) {
    info_log->Logv(InfoLogLevel::WARN_LEVEL, format, ap);
  }
}

void Warn(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log, format, ap);
  va_end(ap);
}

static void Errorv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::ERROR_LEVEL) {
    info_log->Logv(InfoLogLevel::ERROR_LEVEL, format, ap);
  }
}

void Error(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log, format, ap);
  va_end(ap);
}

static void Fatalv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::FATAL_LEVEL) {
    info_log->Logv(InfoLogLevel::FATAL_LEVEL, format, ap);
  }
}

void Fatal(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log, format, ap);
  va_end(ap);
}

void LogFlush(const std::shared_ptr<Logger>& info_log) {
  LogFlush(info_log.get());
}

void Log(const InfoLogLevel log_level, const std::shared_ptr<Logger>& info_log,
         const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log.get(), format, ap);
  va_end(ap);
}

void Header(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log.get(), format, ap);
  va_end(ap);
}

void Debug(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log.get(), format, ap);
  va_end(ap);
}

void Info(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log.get(), format, ap);
  va_end(ap);
}

void Warn(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log.get(), format, ap);
  va_end(ap);
}

void Error(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log.get(), format, ap);
  va_end(ap);
}

void Fatal(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log.get(), format, ap);
  va_end(ap);
}

void Log(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log.get(), format, ap);
  va_end(ap);
}

Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname,
                         bool should_sync) {
  const auto& fs = env->GetFileSystem();
  return WriteStringToFile(fs.get(), data, fname, should_sync);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  const auto& fs = env->GetFileSystem();
  return ReadFileToString(fs.get(), fname, data);
}

EnvWrapper::~EnvWrapper() {
}

namespace {  // anonymous namespace

void AssignEnvOptions(EnvOptions* env_options, const DBOptions& options) {
  env_options->use_mmap_reads = options.allow_mmap_reads;
  env_options->use_mmap_writes = options.allow_mmap_writes;
  env_options->use_direct_reads = options.use_direct_reads;
  env_options->set_fd_cloexec = options.is_fd_close_on_exec;
  env_options->bytes_per_sync = options.bytes_per_sync;
  env_options->compaction_readahead_size = options.compaction_readahead_size;
  env_options->random_access_max_buffer_size =
      options.random_access_max_buffer_size;
  env_options->rate_limiter = options.rate_limiter.get();
  env_options->writable_file_max_buffer_size =
      options.writable_file_max_buffer_size;
  env_options->allow_fallocate = options.allow_fallocate;
  env_options->strict_bytes_per_sync = options.strict_bytes_per_sync;
  options.env->SanitizeEnvOptions(env_options);
}

}

EnvOptions Env::OptimizeForLogWrite(const EnvOptions& env_options,
                                    const DBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.bytes_per_sync = db_options.wal_bytes_per_sync;
  optimized_env_options.writable_file_max_buffer_size =
      db_options.writable_file_max_buffer_size;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForManifestWrite(const EnvOptions& env_options) const {
  return env_options;
}

EnvOptions Env::OptimizeForLogRead(const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForManifestRead(const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForCompactionTableWrite(
    const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_writes =
      db_options.use_direct_io_for_flush_and_compaction;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForCompactionTableRead(
    const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = db_options.use_direct_reads;
  return optimized_env_options;
}

EnvOptions::EnvOptions(const DBOptions& options) {
  AssignEnvOptions(this, options);
}

EnvOptions::EnvOptions() {
  DBOptions options;
  AssignEnvOptions(this, options);
}

Status NewEnvLogger(const std::string& fname, Env* env,
                    std::shared_ptr<Logger>* result) {
  EnvOptions options;
  // TODO: Tune the buffer size.
  options.writable_file_max_buffer_size = 1024 * 1024;
  std::unique_ptr<WritableFile> writable_file;
  const auto status = env->NewWritableFile(fname, &writable_file, options);
  if (!status.ok()) {
    return status;
  }

  *result = std::make_shared<EnvLogger>(
      NewLegacyWritableFileWrapper(std::move(writable_file)), fname, options,
      env);
  return Status::OK();
}

const std::shared_ptr<FileSystem>& Env::GetFileSystem() const {
  return file_system_;
}
}  // namespace ROCKSDB_NAMESPACE
