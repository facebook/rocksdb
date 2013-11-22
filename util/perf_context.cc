//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "util/perf_context_imp.h"

namespace rocksdb {

// by default, enable counts only
PerfLevel perf_level = kEnableCount;

void SetPerfLevel(PerfLevel level) { perf_level = level; }

void PerfContext::Reset() {
  user_key_comparison_count = 0;
  block_cache_hit_count = 0;
  block_read_count = 0;
  block_read_byte = 0;
  block_read_time = 0;
  block_checksum_time = 0;
  block_decompress_time = 0;
  internal_key_skipped_count = 0;
  internal_delete_skipped_count = 0;
  wal_write_time = 0;
}

__thread PerfContext perf_context;

}
