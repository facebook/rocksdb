#include <rocksdb/status.h>
#include <db/db_impl.h>

namespace rocksdb {

std::map<std::tuple<BackgroundErrorReason, Status::Code, Status::SubCode,bool>, Status::Severity> ErrorSeverityMap = {
  // Errors during BG compaction
  {std::make_tuple(BackgroundErrorReason::kCompaction, Status::Code::kIOError, Status::SubCode::kNoSpace, true), Status::Severity::kSoftError},
  {std::make_tuple(BackgroundErrorReason::kCompaction, Status::Code::kIOError, Status::SubCode::kNoSpace, false), Status::Severity::kNoError},
  {std::make_tuple(BackgroundErrorReason::kCompaction, Status::Code::kIOError, Status::SubCode::kSpaceLimit, true), Status::Severity::kHardError},
  // Errors during BG flush
  {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError, Status::SubCode::kNoSpace, true), Status::Severity::kSoftError},
  {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError, Status::SubCode::kNoSpace, false), Status::Severity::kNoError},
  {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError, Status::SubCode::kSpaceLimit, true), Status::Severity::kHardError},
  // Errors during Write
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, Status::Code::kIOError, Status::SubCode::kNoSpace, true), Status::Severity::kHardError},
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, Status::Code::kIOError, Status::SubCode::kNoSpace, false), Status::Severity::kHardError},
};

std::map<std::tuple<BackgroundErrorReason, Status::Code, bool>, Status::Severity> DefaultErrorSeverityMap = {
  // Errors during BG compaction
  {std::make_tuple(BackgroundErrorReason::kCompaction, Status::Code::kCorruption, true), Status::Severity::kUnrecoverableError},
  {std::make_tuple(BackgroundErrorReason::kCompaction, Status::Code::kCorruption, false), Status::Severity::kNoError},
  {std::make_tuple(BackgroundErrorReason::kCompaction, Status::Code::kIOError, true), Status::Severity::kFatalError},
  {std::make_tuple(BackgroundErrorReason::kCompaction, Status::Code::kIOError, false), Status::Severity::kNoError},
  // Errors during BG flush
  {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kCorruption, true), Status::Severity::kUnrecoverableError},
  {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kCorruption, false), Status::Severity::kNoError},
  {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError, true), Status::Severity::kFatalError},
  {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError, false), Status::Severity::kNoError},
  // Errors during Write
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, Status::Code::kCorruption, true), Status::Severity::kUnrecoverableError},
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, Status::Code::kCorruption, false), Status::Severity::kNoError},
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, Status::Code::kIOError, true), Status::Severity::kFatalError},
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, Status::Code::kIOError, false), Status::Severity::kNoError},
};

std::map<std::tuple<BackgroundErrorReason, bool>, Status::Severity> DefaultReasonMap = {
  // Errors during BG compaction
  {std::make_tuple(BackgroundErrorReason::kCompaction, true), Status::Severity::kFatalError},
  {std::make_tuple(BackgroundErrorReason::kCompaction, false), Status::Severity::kNoError},
  // Errors during BG flush
  {std::make_tuple(BackgroundErrorReason::kFlush, true), Status::Severity::kFatalError},
  {std::make_tuple(BackgroundErrorReason::kFlush, false), Status::Severity::kNoError},
  // Errors during Write
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, true), Status::Severity::kFatalError},
  {std::make_tuple(BackgroundErrorReason::kWriteCallback, false), Status::Severity::kFatalError},
  // Errors during Memtable update
  {std::make_tuple(BackgroundErrorReason::kMemTable, true), Status::Severity::kFatalError},
  {std::make_tuple(BackgroundErrorReason::kMemTable, false), Status::Severity::kFatalError},
};

Status::Severity ErrorHandler::GetErrorSeverity(BackgroundErrorReason , Status::Code , Status::SubCode ) {
#if 0
  auto entry = ErrorSeverityMap.find(std::make_tuple(reason, code, subcode));
  if (entry != ErrorSeverityMap.end()) {
    return entry->second;
  }
#endif
  return Status::Severity::kNoError;
}

Status ErrorHandler::SetBGError(const Status& bg_err, BackgroundErrorReason reason) {
  if (bg_err.ok()) {
    return Status::OK();
  }

  auto paranoid = db_options_.paranoid_checks;
  Status::Severity sev;
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
      found = true;
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
