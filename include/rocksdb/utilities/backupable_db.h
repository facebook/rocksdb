//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This is a DEPRECATED header for API backward compatibility. Please
// use backup_engine.h.

#pragma once
#ifndef ROCKSDB_LITE

// A legacy unnecessary include
#include <cinttypes>

#include "rocksdb/utilities/backup_engine.h"

// A legacy unnecessary include
#include "rocksdb/utilities/stackable_db.h"

namespace ROCKSDB_NAMESPACE {

using BackupableDBOptions = BackupEngineOptions;

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
