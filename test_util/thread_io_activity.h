//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "rocksdb/env.h"
namespace ROCKSDB_NAMESPACE {
extern thread_local Env::IOActivity thread_io_activity;

extern Env::IOActivity TEST_GetThreadIOActivity();

class ThreadIOActivityGuardForTest {
 public:
  ThreadIOActivityGuardForTest(Env::IOActivity io_activity);

  ~ThreadIOActivityGuardForTest();
};
}  // namespace ROCKSDB_NAMESPACE
