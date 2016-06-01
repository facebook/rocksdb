//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <assert.h>
#include <sstream>
#include "port/port.h"
#include "util/perf_level_imp.h"

namespace rocksdb {

#if defined(IOS_CROSS_COMPILE)
PerfLevel perf_level = kEnableCount;
#else
__thread PerfLevel perf_level = kEnableCount;
#endif

void SetPerfLevel(PerfLevel level) {
  assert(level > kUninitialized);
  assert(level < kOutOfBounds);
  perf_level = level;
}

PerfLevel GetPerfLevel() {
  return perf_level;
}

}  // namespace rocksdb
