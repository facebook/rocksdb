//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "test_util/thread_io_activity.h"

#ifdef NDEBUG
namespace ROCKSDB_NAMESPACE {
Env::IOActivity GetThreadIOActivity() { return Env::IOActivity::kUnknown; }
}  // namespace ROCKSDB_NAMESPACE
#else
namespace ROCKSDB_NAMESPACE {
thread_local Env::IOActivity thread_io_activity = Env::IOActivity::kUnknown;

Env::IOActivity GetThreadIOActivity() { return thread_io_activity; }
}  // namespace ROCKSDB_NAMESPACE
#endif  // NDEBUG
