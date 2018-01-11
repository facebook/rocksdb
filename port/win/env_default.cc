//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <mutex>

#include <rocksdb/env.h>
#include "port/win/env_win.h"

namespace rocksdb {
namespace port {

// We choose to create this on the heap and using std::once for the following
// reasons
// 1) Currently available MS compiler does not implement atomic C++11
// initialization of
//    function local statics
// 2) We choose not to destroy the env because joining the threads from the
// system loader
//    which destroys the statics (same as from DLLMain) creates a system loader
//    dead-lock.
//    in this manner any remaining threads are terminated OK.
namespace {
  std::once_flag winenv_once_flag;
  Env* envptr;
};

}

Env* Env::Default() {
  using namespace port;
  std::call_once(winenv_once_flag, []() { envptr = new WinEnv(); });
  return envptr;
}

}

