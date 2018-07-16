//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/error_handler.h"
#include "db/event_helpers.h"

namespace rocksdb {

// Maps to help decide the severity of an error based on the
// BackgroundErrorReason, Code, SubCode and whether db_options.paranoid_checks
// is set or not. There are 3 maps, going from most specific to least specific
// (i.e from all 4 fields in a tuple to only the BackgroundErrorReason and
// paranoid_checks). The less specific map serves as a catch all in case we miss
// a specific error code or subcode.
std::map<std::tuple<BackgroundErrorReason, Status::Code, Status::SubCode, bool>,
         Status::Severity>
    ErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kSoftError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kSpaceLimit,
                         true),
         Status::Severity::kHardError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, true),
         Status::Severity::kSoftError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kSpaceLimit, true),
         Status::Severity::kHardError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kFatalError},
};

std::map<std::tuple<BackgroundErrorReason, Status::Code, bool>, Status::Severity>
    DefaultErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
};

std::map<std::tuple<BackgroundErrorReason, bool>, Status::Severity>
    DefaultReasonMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction, false),
          Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush, false),
          Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, false),
          Status::Severity::kFatalError},
        // Errors during Memtable update
        {std::make_tuple(BackgroundErrorReason::kMemTable, true),
          Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kMemTable, false),
          Status::Severity::kFatalError},
};

Status ErrorHandler::SetBGError(const Status& bg_err, BackgroundErrorReason reason) {
  db_mutex_->AssertHeld();

  if (bg_err.ok()) {
    return Status::OK();
  }

  bool paranoid = db_options_.paranoid_checks;
  Status::Severity sev = Status::Severity::kFatalError;
  Status new_bg_err;
  bool found = false;

  {
    auto entry = ErrorSeverityMap.find(std::make_tuple(reason, bg_err.code(),
          bg_err.subcode(), paranoid));
    if (entry != ErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultErrorSeverityMap.find(std::make_tuple(reason,
          bg_err.code(), paranoid));
    if (entry != DefaultErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultReasonMap.find(std::make_tuple(reason, paranoid));
    if (entry != DefaultReasonMap.end()) {
      sev = entry->second;
    }
  }

  new_bg_err = Status(bg_err, sev);
  if (!new_bg_err.ok()) {
    Status s = new_bg_err;
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason, &s, db_mutex_);
    if (!s.ok() && (s.severity() > bg_error_.severity())) {
      bg_error_ = s;
    }
  }

  return bg_error_;
}

}
