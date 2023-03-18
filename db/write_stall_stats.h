//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>

#include "db/internal_stats.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
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

}  // namespace ROCKSDB_NAMESPACE
