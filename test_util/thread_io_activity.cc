//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "test_util/thread_io_activity.h"

namespace ROCKSDB_NAMESPACE {
#ifdef NDEBUG
Env::IOActivity TEST_GetThreadIOActivity() { return Env::IOActivity::kUnknown; }
ThreadIOActivityGuardForTest::ThreadIOActivityGuardForTest(
    Env::IOActivity /* io_activity */) {}
ThreadIOActivityGuardForTest::~ThreadIOActivityGuardForTest() {}
#else
thread_local Env::IOActivity thread_io_activity = Env::IOActivity::kUnknown;
Env::IOActivity TEST_GetThreadIOActivity() { return thread_io_activity; }
ThreadIOActivityGuardForTest::ThreadIOActivityGuardForTest(
    Env::IOActivity io_activity) {
  thread_io_activity = io_activity;
}
ThreadIOActivityGuardForTest::~ThreadIOActivityGuardForTest() {
  thread_io_activity = Env::IOActivity::kUnknown;
}
#endif  // NDEBUG
}  // namespace ROCKSDB_NAMESPACE
