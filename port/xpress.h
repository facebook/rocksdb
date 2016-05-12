//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

// Xpress on Windows is implemeted using Win API
#if defined(ROCKSDB_PLATFORM_POSIX)
#error "Xpress compression not implemented"
#elif defined(OS_WIN)
#include "port/win/xpress_win.h"
#endif
