//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "logging/auto_roll_logger.h"

#include <algorithm>
#include "file/filename.h"
#include "logging/logging.h"
#include "util/mutexlock.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
// -- AutoRollLogger

AutoRollLogger::AutoRollLogger(Env* env, const std::string& dbname,
                               const std::string& db_log_dir,
                               size_t log_max_size,
                               size_t log_file_time_to_roll,
                               size_t keep_log_file_num,
                               const InfoLogLevel log_level)
    : Logger(log_level),
      dbname_(dbname),
      db_log_dir_(db_log_dir),
      env_(env),
      status_(Status::OK()),
      kMaxLogFileSize(log_max_size),
      kLogFileTimeToRoll(log_file_time_to_roll),
      kKeepLogFileNum(keep_log_file_num),
      cached_now(static_cast<uint64_t>(env_->NowMicros() * 1e-6)),
      ctime_(cached_now),
      cached_now_access_count(0),
      call_NowMicros_every_N_records_(100),
      mutex_() {
  Status s = env->GetAbsolutePath(dbname, &db_absolute_path_);
  if (s.IsNotSupported()) {
    db_absolute_path_ = dbname;
  } else {
    status_ = s;
  }
  log_fname_ = InfoLogFileName(dbname_, db_absolute_path_, db_log_dir_);
  if (env_->FileExists(log_fname_).ok()) {
    RollLogFile();
  }
  GetExistingFiles();
  ResetLogger();
  if (status_.ok()) {
    status_ = TrimOldLogFiles();
  }
}

Status AutoRollLogger::ResetLogger() {
  TEST_SYNC_POINT("AutoRollLogger::ResetLogger:BeforeNewLogger");
  status_ = env_->NewLogger(log_fname_, &logger_);
  TEST_SYNC_POINT("AutoRollLogger::ResetLogger:AfterNewLogger");

  if (!status_.ok()) {
    return status_;
  }

  if (logger_->GetLogFileSize() == Logger::kDoNotSupportGetLogFileSize) {
    status_ = Status::NotSupported(
        "The underlying logger doesn't support GetLogFileSize()");
  }
  if (status_.ok()) {
    cached_now = static_cast<uint64_t>(env_->NowMicros() * 1e-6);
    ctime_ = cached_now;
    cached_now_access_count = 0;
  }

  return status_;
}

void AutoRollLogger::RollLogFile() {
  // This function is called when log is rotating. Two rotations
  // can happen quickly (NowMicro returns same value). To not overwrite
  // previous log file we increment by one micro second and try again.
  uint64_t now = env_->NowMicros();
  std::string old_fname;
  do {
    old_fname = OldInfoLogFileName(
      dbname_, now, db_absolute_path_, db_log_dir_);
    now++;
  } while (env_->FileExists(old_fname).ok());
  env_->RenameFile(log_fname_, old_fname);
  old_log_files_.push(old_fname);
}

void AutoRollLogger::GetExistingFiles() {
  {
    // Empty the queue to avoid duplicated entries in the queue.
    std::queue<std::string> empty;
    std::swap(old_log_files_, empty);
  }

  std::string parent_dir;
  std::vector<std::string> info_log_files;
  Status s =
      GetInfoLogFiles(env_, db_log_dir_, dbname_, &parent_dir, &info_log_files);
  if (status_.ok()) {
    status_ = s;
  }
  // We need to sort the file before enqueing it so that when we
  // delete file from the front, it is the oldest file.
  std::sort(info_log_files.begin(), info_log_files.end());

  for (const std::string& f : info_log_files) {
    old_log_files_.push(parent_dir + "/" + f);
  }
}

Status AutoRollLogger::TrimOldLogFiles() {
  // Here we directly list info files and delete them through Env.
  // The deletion isn't going through DB, so there are shortcomes:
  // 1. the deletion is not rate limited by SstFileManager
  // 2. there is a chance that an I/O will be issued here
  // Since it's going to be complicated to pass DB object down to
  // here, we take a simple approach to keep the code easier to
  // maintain.

  // old_log_files_.empty() is helpful for the corner case that
  // kKeepLogFileNum == 0. We can instead check kKeepLogFileNum != 0 but
  // it's essentially the same thing, and checking empty before accessing
  // the queue feels safer.
  while (!old_log_files_.empty() && old_log_files_.size() >= kKeepLogFileNum) {
    Status s = env_->DeleteFile(old_log_files_.front());
    // Remove the file from the tracking anyway. It's possible that
    // DB cleaned up the old log file, or people cleaned it up manually.
    old_log_files_.pop();
    // To make the file really go away, we should sync parent directory.
    // Since there isn't any consistency issue involved here, skipping
    // this part to avoid one I/O here.
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

std::string AutoRollLogger::ValistToString(const char* format,
                                           va_list args) const {
  // Any log messages longer than 1024 will get truncated.
  // The user is responsible for chopping longer messages into multi line log
  static const int MAXBUFFERSIZE = 1024;
  char buffer[MAXBUFFERSIZE];

  int count = vsnprintf(buffer, MAXBUFFERSIZE, format, args);
  (void) count;
  assert(count >= 0);

  return buffer;
}

void AutoRollLogger::LogInternal(const char* format, ...) {
  mutex_.AssertHeld();

  if (!logger_) {
    return;
  }

  va_list args;
  va_start(args, format);
  logger_->Logv(format, args);
  va_end(args);
}

void AutoRollLogger::Logv(const char* format, va_list ap) {
  assert(GetStatus().ok());
  if (!logger_) {
    return;
  }
  
  std::shared_ptr<Logger> logger;
  {
    MutexLock l(&mutex_);
    if ((kLogFileTimeToRoll > 0 && LogExpired()) ||
        (kMaxLogFileSize > 0 && logger_->GetLogFileSize() >= kMaxLogFileSize)) {
      RollLogFile();
      Status s = ResetLogger();
      Status s2 = TrimOldLogFiles();

      if (!s.ok()) {
        // can't really log the error if creating a new LOG file failed
        return;
      }

      WriteHeaderInfo();

      if (!s2.ok()) {
        ROCKS_LOG_WARN(logger.get(), "Fail to trim old info log file: %s",
                       s2.ToString().c_str());
      }
    }

    // pin down the current logger_ instance before releasing the mutex.
    logger = logger_;
  }

  // Another thread could have put a new Logger instance into logger_ by now.
  // However, since logger is still hanging on to the previous instance
  // (reference count is not zero), we don't have to worry about it being
  // deleted while we are accessing it.
  // Note that logv itself is not mutex protected to allow maximum concurrency,
  // as thread safety should have been handled by the underlying logger.
  logger->Logv(format, ap);
}

void AutoRollLogger::WriteHeaderInfo() {
  mutex_.AssertHeld();
  for (auto& header : headers_) {
    LogInternal("%s", header.c_str());
  }
}

void AutoRollLogger::LogHeader(const char* format, va_list args) {
  if (!logger_) {
    return;
  }

  // header message are to be retained in memory. Since we cannot make any
  // assumptions about the data contained in va_list, we will retain them as
  // strings
  va_list tmp;
  va_copy(tmp, args);
  std::string data = ValistToString(format, tmp);
  va_end(tmp);

  MutexLock l(&mutex_);
  headers_.push_back(data);

  // Log the original message to the current log
  logger_->Logv(format, args);
}

bool AutoRollLogger::LogExpired() {
  if (cached_now_access_count >= call_NowMicros_every_N_records_) {
    cached_now = static_cast<uint64_t>(env_->NowMicros() * 1e-6);
    cached_now_access_count = 0;
  }

  ++cached_now_access_count;
  return cached_now >= ctime_ + kLogFileTimeToRoll;
}
#endif  // !ROCKSDB_LITE

Status CreateLoggerFromOptions(const std::string& dbname,
                               const DBOptions& options,
                               std::shared_ptr<Logger>* logger) {
  if (options.info_log) {
    *logger = options.info_log;
    return Status::OK();
  }

  Env* env = options.env;
  std::string db_absolute_path;
  env->GetAbsolutePath(dbname, &db_absolute_path);
  std::string fname =
      InfoLogFileName(dbname, db_absolute_path, options.db_log_dir);

  env->CreateDirIfMissing(dbname);  // In case it does not exist
  // Currently we only support roll by time-to-roll and log size
#ifndef ROCKSDB_LITE
  if (options.log_file_time_to_roll > 0 || options.max_log_file_size > 0) {
    AutoRollLogger* result = new AutoRollLogger(
        env, dbname, options.db_log_dir, options.max_log_file_size,
        options.log_file_time_to_roll, options.keep_log_file_num,
        options.info_log_level);
    Status s = result->GetStatus();
    if (!s.ok()) {
      delete result;
    } else {
      logger->reset(result);
    }
    return s;
  }
#endif  // !ROCKSDB_LITE
  // Open a log file in the same directory as the db
  env->RenameFile(fname,
                  OldInfoLogFileName(dbname, env->NowMicros(), db_absolute_path,
                                     options.db_log_dir));
  auto s = env->NewLogger(fname, logger);
  if (logger->get() != nullptr) {
    (*logger)->SetInfoLogLevel(options.info_log_level);
  }
  return s;
}

}  // namespace rocksdb
