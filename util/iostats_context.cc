// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>
#include "rocksdb/env.h"
#include "util/iostats_context_imp.h"

namespace rocksdb {

#ifndef IOS_CROSS_COMPILE
__thread IOStatsContext iostats_context;
#endif  // IOS_CROSS_COMPILE

void IOStatsContext::Reset() {
  thread_pool_id = Env::Priority::TOTAL;
  bytes_read = 0;
  bytes_written = 0;
}

#define OUTPUT(counter) #counter << " = " << counter << ", "

std::string IOStatsContext::ToString() const {
  std::ostringstream ss;
  ss << OUTPUT(thread_pool_id)
     << OUTPUT(bytes_read)
     << OUTPUT(bytes_written);
  return ss.str();
}

}  // namespace rocksdb
