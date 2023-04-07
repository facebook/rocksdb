//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "rocksdb/env.h"

#ifdef NDEBUG
namespace ROCKSDB_NAMESPACE {
extern thread_local Env::IOActivity thread_io_activity;

extern Env::IOActivity GetThreadIOActivity();

class ThreadIOActivityGuard {
 public:
  ThreadIOActivityGuard(Env::IOActivity /* io_activity */) {}

  ~ThreadIOActivityGuard() {}
};
}  // namespace ROCKSDB_NAMESPACE

#else
namespace ROCKSDB_NAMESPACE {
extern thread_local Env::IOActivity thread_io_activity;

extern Env::IOActivity GetThreadIOActivity();

class ThreadIOActivityGuard {
 public:
  ThreadIOActivityGuard(Env::IOActivity io_activity) {
    thread_io_activity = io_activity;
  }

  ~ThreadIOActivityGuard() { thread_io_activity = Env::IOActivity::kUnknown; }
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // NDEBUG
