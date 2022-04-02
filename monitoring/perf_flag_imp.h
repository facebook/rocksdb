//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <cstdint>

#include "rocksdb/perf_flag.h"

namespace ROCKSDB_NAMESPACE {
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
extern __thread PerfFlags perf_flags;
#else
extern PerfFlags perf_flags;
#endif
}  // namespace ROCKSDB_NAMESPACE
