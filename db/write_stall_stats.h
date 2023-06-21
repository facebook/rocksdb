//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>

#include "db/internal_stats.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
extern const std::string& InvalidWriteStallHyphenString();

extern const std::string& WriteStallCauseToHyphenString(WriteStallCause cause);

extern const std::string& WriteStallConditionToHyphenString(
    WriteStallCondition condition);

// REQUIRES:
// cause` is CF-scope `WriteStallCause`, see `WriteStallCause` for more
//
// REQUIRES:
// `condition` != `WriteStallCondition::kNormal`
extern InternalStats::InternalCFStatsType InternalCFStat(
    WriteStallCause cause, WriteStallCondition condition);

// REQUIRES:
// cause` is DB-scope `WriteStallCause`, see `WriteStallCause` for more
//
// REQUIRES:
// `condition` != `WriteStallCondition::kNormal`
extern InternalStats::InternalDBStatsType InternalDBStat(
    WriteStallCause cause, WriteStallCondition condition);

extern bool isCFScopeWriteStallCause(WriteStallCause cause);
extern bool isDBScopeWriteStallCause(WriteStallCause cause);

constexpr uint32_t kNumCFScopeWriteStallCauses =
    static_cast<uint32_t>(WriteStallCause::kCFScopeWriteStallCauseEnumMax) -
    static_cast<uint32_t>(WriteStallCause::kMemtableLimit);

constexpr uint32_t kNumDBScopeWriteStallCauses =
    static_cast<uint32_t>(WriteStallCause::kDBScopeWriteStallCauseEnumMax) -
    static_cast<uint32_t>(WriteStallCause::kWriteBufferManagerLimit);
}  // namespace ROCKSDB_NAMESPACE
