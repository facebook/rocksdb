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

#ifndef STORAGE_LEVELDB_PORT_UTIL_LOGGER_H_
#define STORAGE_LEVELDB_PORT_UTIL_LOGGER_H_

// Include the appropriate platform specific file below.  If you are
// porting to a new platform, see "port_example.h" for documentation
// of what the new port_<platform>.h file must provide.

#if defined(ROCKSDB_PLATFORM_POSIX)
#include "env/posix_logger.h"
#elif defined(OS_WIN)
#include "port/win/win_logger.h"
#endif

#endif  // STORAGE_LEVELDB_PORT_UTIL_LOGGER_H_
