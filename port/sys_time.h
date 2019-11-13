//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This file is a portable substitute for sys/time.h which does not exist on
// Windows

#pragma once

#include <time.h>
#if !defined(_MSC_VER)
#include <sys/time.h>
#endif

#if defined(OS_WIN)

namespace rocksdb {

namespace port {

#if defined(HAVE_GETTIMEOFDAY)
// Nothing needed
#elif defined(_MSC_VER)
// Avoid including winsock2.h for this definition
typedef struct timeval {
  long tv_sec;
  long tv_usec;
} timeval;

void gettimeofday(struct timeval* tv, struct timezone* tz);
#elif defined(__MINGW32__) || defined(__MINGW64__)
inline void gettimeofday(struct timeval* tv, struct timezone* tz) {
   mingw_gettimeofday(tv, tz);
}
#endif

#if defined(HAVE_LOCALTIME_R)
// Nothing needed
#else
inline struct tm* localtime_r(const time_t* timep, struct tm* result) {
#  if defined(HAVE_LOCALTIME_S)
  errno_t ret = localtime_s(result, timep);
  return (ret == 0) ? result : NULL;
}
#  elif defined(HAVE_LOCALTIME)
  // MINGW <time.h> provides neither localtime_r nor localtime_s, but uses
  // Windows' localtime(), which has a thread-local tm buffer.
  struct tm* tm_ptr = localtime(timep);
  if (tm_ptr != NULL)
    *result = *tm_ptr;
  return tm_ptr;
#  else
#    error "No polyfill for localtime_r"
#  endif
}
#endif

}

#if !defined(HAVE_GETTIMEOFDAY) && defined(_MSC_VER)
using port::timeval;
#endif
#if !defined(HAVE_GETTIMEOFDAY)
using port::gettimeofday;
#endif
#if !defined(HAVE_LOCALTIME_R)
using port::localtime_r;
#endif
}

#endif
