//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#pragma once

#include <memory>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

bool IsDbStressPublicIteratorTraceEnabled();
void InitDbStressPublicIteratorTrace(const std::string& path);
void DumpDbStressPublicIteratorTrace();
std::string GetDbStressPublicIteratorTracePath();

std::unique_ptr<Iterator> MaybeWrapDbStressTraceIterator(
    std::unique_ptr<Iterator> iter, const ReadOptions& read_opts,
    ColumnFamilyHandle* column_family = nullptr);

std::unique_ptr<Iterator> NewDbStressTraceIterator(
    DB* db, const ReadOptions& read_opts,
    ColumnFamilyHandle* column_family = nullptr);

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
