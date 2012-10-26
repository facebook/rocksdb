// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_SPLIT_LOGGER_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_SPLIT_LOGGER_LOGGER_H_

#include "util/posix_logger.h"
#include "db/filename.h"

namespace leveldb {

// AutoSplitLogger can automatically create a new log file
// if the file size exceeds the limit.
//
// The template parameter "UnderlyingLogger" can be any Logger class
// that has the method "GetLogFileSize()" and "ResetFile()"
template<class UnderlyingLogger>
class AutoSplitLogger : public Logger {
 private:
  std::string log_fname_; // Current active info log's file name.
  std::string dbname_;
  std::string db_log_dir_;
  std::string db_absolute_path_;
  Env* env_;
  UnderlyingLogger* logger_;
  const size_t MAX_LOG_FILE_SIZE;
  Status status_;

 public:
  AutoSplitLogger(Env* env, const std::string& dbname,
                  const std::string& db_log_dir, size_t log_max_size):
    env_(env), dbname_(dbname), db_log_dir_(db_log_dir),
    MAX_LOG_FILE_SIZE(log_max_size), status_(Status::OK()) {
      env->GetAbsolutePath(dbname, &db_absolute_path_);
      log_fname_ = InfoLogFileName(dbname_, db_absolute_path_, db_log_dir_);
      InitLogger();
    }
  ~AutoSplitLogger() { delete logger_; }

  virtual void Logv(const char* format, va_list ap) {
    assert(GetStatus().ok());

    logger_->Logv(format, ap);
    // Check if the log file should be splitted.
    if (logger_->GetLogFileSize() > MAX_LOG_FILE_SIZE) {
      delete logger_;
      std::string old_fname = OldInfoLogFileName(
          dbname_, env_->NowMicros(), db_absolute_path_, db_log_dir_);
      env_->RenameFile(log_fname_, old_fname);
      InitLogger();
    }
  }

  // check if the logger has any problem.
  Status GetStatus() {
    return status_;
  }

 private:
  Status InitLogger() {
    status_ = env_->NewLogger(log_fname_, &logger_);
    if (!status_.ok()) {
      logger_ = NULL;
    }
    if (logger_->GetLogFileSize() ==
        Logger::DO_NOT_SUPPORT_GET_LOG_FILE_SIZE) {
      status_ = Status::NotSupported(
          "The underlying logger doesn't support GetLogFileSize()");
    }
    return status_;
  }

}; // class AutoSplitLogger

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_SPLIT_LOGGER_LOGGER_H_
