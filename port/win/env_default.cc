//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
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

