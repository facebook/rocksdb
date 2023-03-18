//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_stall_stats.h"

namespace ROCKSDB_NAMESPACE {
const std::array<std::string, static_cast<uint32_t>(WriteStallCause::kNone)>
    kWriteStallCauseToHyphenString{{
        "memtable-limit",
        "l0-file-count-limit",
        "pending-compaction-bytes",
        "invalid",
        "write-buffer-manager-limit"
        "invalid",
    }};

std::array<std::string, static_cast<uint32_t>(WriteStallCondition::kNormal)>
    kWriteStallConditionToHyphenString{{
        "delayed",
        "stopped",
    }};

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
          return InternalStats::kIntStatsWriteBufferManagerLimitStopCounts;
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

const std::string& WriteStallStatsMapKeys::TotalStop() {
  static const std::string kTotalStop = "total-stop";
  return kTotalStop;
}

const std::string& WriteStallStatsMapKeys::TotalDelay() {
  static const std::string kTotalDelay = "total-delay";
  return kTotalDelay;
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

std::string WriteStallStatsMapKeys::CauseConditionCount(
    WriteStallCause cause, WriteStallCondition condition) {
  std::string cause_condition_count_name;

  std::string cause_name;
  if (isCFScopeWriteStallCause(cause) || isDBScopeWriteStallCause(cause)) {
    cause_name = kWriteStallCauseToHyphenString[static_cast<uint32_t>(cause)];
  } else {
    assert(false);
    return "";
  }

  std::string& condition_name =
      kWriteStallConditionToHyphenString[static_cast<uint32_t>(condition)];

  cause_condition_count_name.reserve(cause_name.size() + 1 +
                                     condition_name.size());
  cause_condition_count_name.append(cause_name);
  cause_condition_count_name.append("-");
  cause_condition_count_name.append(condition_name);

  return cause_condition_count_name;
}
}  // namespace ROCKSDB_NAMESPACE
