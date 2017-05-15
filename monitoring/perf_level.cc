//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//

#include <assert.h>
#include "monitoring/perf_level_imp.h"

namespace rocksdb {

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread PerfLevel perf_level = kEnableCount;
#else
PerfLevel perf_level = kEnableCount;
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
