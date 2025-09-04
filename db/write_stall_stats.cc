//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_stall_stats.h"

namespace ROCKSDB_NAMESPACE {
const std::string& InvalidWriteStallHyphenString() {
  static const std::string kInvalidWriteStallHyphenString = "invalid";
  return kInvalidWriteStallHyphenString;
}

const std::string& WriteStallCauseToHyphenString(WriteStallCause cause) {
  static const std::string kMemtableLimit = "memtable-limit";
  static const std::string kL0FileCountLimit = "l0-file-count-limit";
  static const std::string kPendingCompactionBytes = "pending-compaction-bytes";
  static const std::string kWriteBufferManagerLimit =
      "write-buffer-manager-limit";
  switch (cause) {
    case WriteStallCause::kMemtableLimit:
      return kMemtableLimit;
    case WriteStallCause::kL0FileCountLimit:
      return kL0FileCountLimit;
    case WriteStallCause::kPendingCompactionBytes:
      return kPendingCompactionBytes;
    case WriteStallCause::kWriteBufferManagerLimit:
      return kWriteBufferManagerLimit;
    default:
      break;
  }
  return InvalidWriteStallHyphenString();
}

const std::string& WriteStallConditionToHyphenString(
    WriteStallCondition condition) {
  static const std::string kDelayed = "delays";
  static const std::string kStopped = "stops";
  switch (condition) {
    case WriteStallCondition::kDelayed:
      return kDelayed;
    case WriteStallCondition::kStopped:
      return kStopped;
    default:
      break;
  }
  return InvalidWriteStallHyphenString();
}

InternalStats::InternalCFStatsType InternalCFStat(
    WriteStallCause cause, WriteStallCondition condition) {
  switch (cause) {
    case WriteStallCause::kMemtableLimit: {
      switch (condition) {
        case WriteStallCondition::kDelayed:
          return InternalStats::MEMTABLE_LIMIT_DELAYS;
        case WriteStallCondition::kStopped:
          return InternalStats::MEMTABLE_LIMIT_STOPS;
        case WriteStallCondition::kNormal:
          break;
      }
      break;
    }
    case WriteStallCause::kL0FileCountLimit: {
      switch (condition) {
        case WriteStallCondition::kDelayed:
          return InternalStats::L0_FILE_COUNT_LIMIT_DELAYS;
        case WriteStallCondition::kStopped:
          return InternalStats::L0_FILE_COUNT_LIMIT_STOPS;
        case WriteStallCondition::kNormal:
          break;
      }
      break;
    }
    case WriteStallCause::kPendingCompactionBytes: {
      switch (condition) {
        case WriteStallCondition::kDelayed:
          return InternalStats::PENDING_COMPACTION_BYTES_LIMIT_DELAYS;
        case WriteStallCondition::kStopped:
          return InternalStats::PENDING_COMPACTION_BYTES_LIMIT_STOPS;
        case WriteStallCondition::kNormal:
          break;
      }
      break;
    }
    default:
      break;
  }
  return InternalStats::INTERNAL_CF_STATS_ENUM_MAX;
}

InternalStats::InternalDBStatsType InternalDBStat(
    WriteStallCause cause, WriteStallCondition condition) {
  switch (cause) {
    case WriteStallCause::kWriteBufferManagerLimit: {
      switch (condition) {
        case WriteStallCondition::kStopped:
          return InternalStats::kIntStatsWriteBufferManagerLimitStopsCounts;
        default:
          break;
      }
      break;
    }
    default:
      break;
  }
  return InternalStats::kIntStatsNumMax;
}

bool isCFScopeWriteStallCause(WriteStallCause cause) {
  uint32_t int_cause = static_cast<uint32_t>(cause);
  uint32_t lower_bound =
      static_cast<uint32_t>(WriteStallCause::kCFScopeWriteStallCauseEnumMax) -
      kNumCFScopeWriteStallCauses;
  uint32_t upper_bound =
      static_cast<uint32_t>(WriteStallCause::kCFScopeWriteStallCauseEnumMax) -
      1;
  return lower_bound <= int_cause && int_cause <= upper_bound;
}

bool isDBScopeWriteStallCause(WriteStallCause cause) {
  uint32_t int_cause = static_cast<uint32_t>(cause);
  uint32_t lower_bound =
      static_cast<uint32_t>(WriteStallCause::kDBScopeWriteStallCauseEnumMax) -
      kNumDBScopeWriteStallCauses;
  uint32_t upper_bound =
      static_cast<uint32_t>(WriteStallCause::kDBScopeWriteStallCauseEnumMax) -
      1;
  return lower_bound <= int_cause && int_cause <= upper_bound;
}

const std::string& WriteStallStatsMapKeys::TotalStops() {
  static const std::string kTotalStops = "total-stops";
  return kTotalStops;
}

const std::string& WriteStallStatsMapKeys::TotalDelays() {
  static const std::string kTotalDelays = "total-delays";
  return kTotalDelays;
}

const std::string&
WriteStallStatsMapKeys::CFL0FileCountLimitDelaysWithOngoingCompaction() {
  static const std::string ret =
      "cf-l0-file-count-limit-delays-with-ongoing-compaction";
  return ret;
}

const std::string&
WriteStallStatsMapKeys::CFL0FileCountLimitStopsWithOngoingCompaction() {
  static const std::string ret =
      "cf-l0-file-count-limit-stops-with-ongoing-compaction";
  return ret;
}

Status WriteStallStatsMapKeys::CauseConditionCount(
    WriteStallCause cause, WriteStallCondition condition, std::string& key) {
  key.clear();
  bool is_cf_scope = isCFScopeWriteStallCause(cause);
  bool is_db_scope = isDBScopeWriteStallCause(cause);
  if (!is_cf_scope && !is_db_scope) {
    return Status::InvalidArgument("Invalid write stall cause");
  }
  if (is_cf_scope && InternalCFStat(cause, condition) ==
                         InternalStats::INTERNAL_CF_STATS_ENUM_MAX) {
    return Status::InvalidArgument(
        "Invalid combination of write stall cause and condition");
  }
  if (is_db_scope &&
      InternalDBStat(cause, condition) == InternalStats::kIntStatsNumMax) {
    return Status::InvalidArgument(
        "Invalid combination of write stall cause and condition");
  }

  const std::string& cause_name = WriteStallCauseToHyphenString(cause);
  const std::string& condition_name =
      WriteStallConditionToHyphenString(condition);

  key.reserve(cause_name.size() + 1 + condition_name.size());
  key.append(cause_name);
  key.append("-");
  key.append(condition_name);

  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
