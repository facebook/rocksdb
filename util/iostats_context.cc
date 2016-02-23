// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>
#include "rocksdb/env.h"
#include "util/iostats_context_imp.h"

namespace rocksdb {

#ifndef IOS_CROSS_COMPILE
# ifdef _WIN32
__declspec(thread) IOStatsContext iostats_context;
# else
__thread IOStatsContext iostats_context;
# endif
#endif  // IOS_CROSS_COMPILE

void IOStatsContext::Reset() {
  thread_pool_id = Env::Priority::TOTAL;
  bytes_read = 0;
  bytes_written = 0;
  open_nanos = 0;
  allocate_nanos = 0;
  write_nanos = 0;
  read_nanos = 0;
  range_sync_nanos = 0;
  prepare_write_nanos = 0;
  fsync_nanos = 0;
  logger_nanos = 0;
}

#define IOSTATS_CONTEXT_OUTPUT(counter)         \
  if (!exclude_zero_counters || counter > 0) {  \
    ss << #counter << " = " << counter << ", "; \
  }

std::string IOStatsContext::ToString(bool exclude_zero_counters) const {
  std::ostringstream ss;
  IOSTATS_CONTEXT_OUTPUT(thread_pool_id);
  IOSTATS_CONTEXT_OUTPUT(bytes_read);
  IOSTATS_CONTEXT_OUTPUT(bytes_written);
  IOSTATS_CONTEXT_OUTPUT(open_nanos);
  IOSTATS_CONTEXT_OUTPUT(allocate_nanos);
  IOSTATS_CONTEXT_OUTPUT(write_nanos);
  IOSTATS_CONTEXT_OUTPUT(read_nanos);
  IOSTATS_CONTEXT_OUTPUT(range_sync_nanos);
  IOSTATS_CONTEXT_OUTPUT(fsync_nanos);
  IOSTATS_CONTEXT_OUTPUT(prepare_write_nanos);
  IOSTATS_CONTEXT_OUTPUT(logger_nanos);

  return ss.str();
}

}  // namespace rocksdb
