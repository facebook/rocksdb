//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTILITIES_PRAGMA_ERROR_H_
#define STORAGE_LEVELDB_UTILITIES_PRAGMA_ERROR_H_

#define RDB_STR__(x) #x
#define RDB_STR(x) RDB_STR__(x)

#if defined(ROCKSDB_PLATFORM_POSIX)
// Wrap unportable warning macro

#define ROCKSDB_WARNING(x) _Pragma(RDB_STR(GCC warning(x)))

#elif defined(OS_WIN)

// Wrap unportable warning macro
#if defined(_MSC_VER)
// format it according to visual studio output (to get source lines and warnings
// in the IDE)
#define ROCKSDB_WARNING(x) \
  __pragma(message(__FILE__ "(" RDB_STR(__LINE__) ") : warning: " x))
#else
// make #warning into #pragma GCC warning gcc 4.7+ and clang 3.2+ supported
#define ROCKSDB_WARNING(x) _Pragma(RDB_STR(GCC warning(x)))
#endif

#endif

#endif  // STORAGE_LEVELDB_UTILITIES_PRAGMA_ERROR_H_
