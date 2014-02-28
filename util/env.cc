//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace rocksdb {

Env::~Env() {
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

Logger::~Logger() {
}

FileLock::~FileLock() {
}

void LogFlush(Logger *info_log) {
  if (info_log) {
    info_log->Flush();
  }
}

void Log(Logger* info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
         ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(log_level, format, ap);
    va_end(ap);
  }
}

void Debug(Logger* info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::DEBUG, format, ap);
    va_end(ap);
  }
}

void Info(Logger* info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::INFO, format, ap);
    va_end(ap);
  }
}

void Warn(Logger* info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::WARN, format, ap);
    va_end(ap);
  }
}
void Error(Logger* info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::ERROR, format, ap);
    va_end(ap);
  }
}
void Fatal(Logger* info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::FATAL, format, ap);
    va_end(ap);
  }
}

void LogFlush(const shared_ptr<Logger>& info_log) {
  if (info_log) {
    info_log->Flush();
  }
}

void Log(const InfoLogLevel log_level, const shared_ptr<Logger>& info_log,
         const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(log_level, format, ap);
    va_end(ap);
  }
}

void Debug(const shared_ptr<Logger>& info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::DEBUG, format, ap);
    va_end(ap);
  }
}

void Info(const shared_ptr<Logger>& info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::INFO, format, ap);
    va_end(ap);
  }
}

void Warn(const shared_ptr<Logger>& info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::WARN, format, ap);
    va_end(ap);
  }
}

void Error(const shared_ptr<Logger>& info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::ERROR, format, ap);
    va_end(ap);
  }
}

void Fatal(const shared_ptr<Logger>& info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(InfoLogLevel::FATAL, format, ap);
    va_end(ap);
  }
}

void Log(const shared_ptr<Logger>& info_log, const char* format, ...) {
  if (info_log) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync) {
  unique_ptr<WritableFile> file;
  EnvOptions soptions;
  Status s = env->NewWritableFile(fname, &file, soptions);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (!s.ok()) {
    env->DeleteFile(fname);
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  EnvOptions soptions;
  data->clear();
  unique_ptr<SequentialFile> file;
  Status s = env->NewSequentialFile(fname, &file, soptions);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
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

EnvWrapper::~EnvWrapper() {
}

namespace {  // anonymous namespace

void AssignEnvOptions(EnvOptions* env_options, const Options& options) {
  env_options->use_os_buffer = options.allow_os_buffer;
  env_options->use_mmap_reads = options.allow_mmap_reads;
  env_options->use_mmap_writes = options.allow_mmap_writes;
  env_options->set_fd_cloexec = options.is_fd_close_on_exec;
  env_options->bytes_per_sync = options.bytes_per_sync;
}

}

EnvOptions EnvOptions::AdaptForLogWrite() const {
  EnvOptions adapted = *this;
  adapted.use_mmap_writes = false;
  return adapted;
}

EnvOptions::EnvOptions(const Options& options) {
  AssignEnvOptions(this, options);
}

EnvOptions::EnvOptions() {
  Options options;
  AssignEnvOptions(this, options);
}


}  // namespace rocksdb
