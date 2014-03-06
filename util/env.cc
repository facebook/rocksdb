//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/env.h"

#include <sys/time.h>
#include "rocksdb/options.h"
#include "util/arena.h"
#include "util/autovector.h"

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

// One log entry with its timestamp
struct BufferedLog {
  struct timeval now_tv;  // Timestamp of the log
  char message[1];        // Beginning of log message
};

struct LogBuffer::Rep {
  Arena arena_;
  autovector<BufferedLog*> logs_;
};

// Lazily initialize Rep to avoid allocations when new log is added.
LogBuffer::LogBuffer(const InfoLogLevel log_level,
                     const shared_ptr<Logger>& info_log)
    : rep_(nullptr), log_level_(log_level), info_log_(info_log) {}

LogBuffer::~LogBuffer() { delete rep_; }

void LogBuffer::AddLogToBuffer(const char* format, va_list ap) {
  if (!info_log_ || log_level_ < info_log_->GetInfoLogLevel()) {
    // Skip the level because of its level.
    return;
  }
  if (rep_ == nullptr) {
    rep_ = new Rep();
  }

  const size_t kLogSizeLimit = 512;
  char* alloc_mem = rep_->arena_.AllocateAligned(kLogSizeLimit);
  BufferedLog* buffered_log = new (alloc_mem) BufferedLog();
  char* p = buffered_log->message;
  char* limit = alloc_mem + kLogSizeLimit - 1;

  // store the time
  gettimeofday(&(buffered_log->now_tv), nullptr);

  // Print the message
  if (p < limit) {
    va_list backup_ap;
    va_copy(backup_ap, ap);
    p += vsnprintf(p, limit - p, format, backup_ap);
    va_end(backup_ap);
  }

  // Add '\0' to the end
  *p = '\0';

  rep_->logs_.push_back(buffered_log);
}

void LogBuffer::FlushBufferToLog() const {
  if (rep_ != nullptr) {
    for (BufferedLog* log : rep_->logs_) {
      const time_t seconds = log->now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      Log(log_level_, info_log_,
          "(Original Log Time %04d/%02d/%02d-%02d:%02d:%02d.%06d) %s",
          t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min,
          t.tm_sec, static_cast<int>(log->now_tv.tv_usec), log->message);
    }
  }
}

FileLock::~FileLock() {
}

void LogToBuffer(LogBuffer* log_buffer, const char* format, ...) {
  if (log_buffer != nullptr) {
    va_list ap;
    va_start(ap, format);
    log_buffer->AddLogToBuffer(format, ap);
    va_end(ap);
  }
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
    info_log->Logv(InfoLogLevel::INFO, format, ap);
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
    info_log->Logv(InfoLogLevel::INFO, format, ap);
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
