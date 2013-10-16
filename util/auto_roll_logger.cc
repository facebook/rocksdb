//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "util/auto_roll_logger.h"
#include "util/mutexlock.h"

using namespace std;

namespace rocksdb {

// -- AutoRollLogger
Status AutoRollLogger::ResetLogger() {
  status_ = env_->NewLogger(log_fname_, &logger_);

  if (!status_.ok()) {
    return status_;
  }

  if (logger_->GetLogFileSize() ==
      (size_t)Logger::DO_NOT_SUPPORT_GET_LOG_FILE_SIZE) {
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
  std::string old_fname = OldInfoLogFileName(
      dbname_, env_->NowMicros(), db_absolute_path_, db_log_dir_);
  env_->RenameFile(log_fname_, old_fname);
}

void AutoRollLogger::Logv(const char* format, va_list ap) {
  assert(GetStatus().ok());

  std::shared_ptr<Logger> logger;
  {
    MutexLock l(&mutex_);
    if ((kLogFileTimeToRoll > 0 && LogExpired()) ||
        (kMaxLogFileSize > 0 && logger_->GetLogFileSize() >= kMaxLogFileSize)) {
      RollLogFile();
      ResetLogger();
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

bool AutoRollLogger::LogExpired() {
  if (cached_now_access_count >= call_NowMicros_every_N_records_) {
    cached_now = static_cast<uint64_t>(env_->NowMicros() * 1e-6);
    cached_now_access_count = 0;
  }

  ++cached_now_access_count;
  return cached_now >= ctime_ + kLogFileTimeToRoll;
}

Status CreateLoggerFromOptions(
    const std::string& dbname,
    const std::string& db_log_dir,
    Env* env,
    const Options& options,
    std::shared_ptr<Logger>* logger) {
  std::string db_absolute_path;
  env->GetAbsolutePath(dbname, &db_absolute_path);
  std::string fname = InfoLogFileName(dbname, db_absolute_path, db_log_dir);

  // Currently we only support roll by time-to-roll and log size
  if (options.log_file_time_to_roll > 0 || options.max_log_file_size > 0) {
    AutoRollLogger* result = new AutoRollLogger(
        env, dbname, db_log_dir,
        options.max_log_file_size,
        options.log_file_time_to_roll);
    Status s = result->GetStatus();
    if (!s.ok()) {
      delete result;
    } else {
      logger->reset(result);
    }
    return s;
  } else {
    // Open a log file in the same directory as the db
    env->CreateDir(dbname);  // In case it does not exist
    env->RenameFile(fname, OldInfoLogFileName(dbname, env->NowMicros(),
                                              db_absolute_path, db_log_dir));
    return env->NewLogger(fname, logger);
  }
}

}  // namespace rocksdb
