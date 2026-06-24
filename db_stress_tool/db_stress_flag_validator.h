//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef GFLAGS

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

enum class OptionCompatibilityCheckLevel : char;

// Validates db_stress flag combinations after gflags parsing and after any
// direct value sanitization. Returns 0 on success and non-zero on validation
// failure. Some legacy helper validation still exits directly; keep callers on
// process-startup paths.
int ValidateDbStressFlags();

Status ParseDbStressOptionCompatibilityCheckLevel(
    OptionCompatibilityCheckLevel* level);

int ValidateDbStressCoreOptionCompatibility();

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
