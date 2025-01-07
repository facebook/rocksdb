//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

// Include the appropriate platform specific file below.  If you are
// porting to a new platform, see "port_example.h" for documentation
// of what the new port_<platform>.h file must provide.
#if defined(ROCKSDB_PLATFORM_POSIX)
#include "port/port_posix.h"
#elif defined(OS_WIN)
#include "port/win/port_win.h"
#endif

#ifdef OS_LINUX
// A temporary hook into long-running RocksDB threads to support modifying their
// priority etc. This should become a public API hook once the requirements
// are better understood.
// Returns true if query is aborted.
extern "C" bool RocksDbThreadYieldAndCheckAbort() __attribute__((__weak__));
#define ROCKSDB_THREAD_YIELD_CHECK_ABORT() \
  (RocksDbThreadYieldAndCheckAbort ? RocksDbThreadYieldAndCheckAbort() : false)
#else
#define ROCKSDB_THREAD_YIELD_CHECK_ABORT() (false)
#endif
