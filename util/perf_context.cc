//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <sstream>
#include "util/perf_context_imp.h"

namespace rocksdb {

#if defined(NPERF_CONTEXT) || defined(IOS_CROSS_COMPILE)
PerfLevel perf_level = kEnableCount;
// This is a dummy variable since some place references it
PerfContext perf_context;
#else
__thread PerfLevel perf_level = kEnableCount;
__thread PerfContext perf_context;
#endif

void SetPerfLevel(PerfLevel level) {
  perf_level = level;
}

PerfLevel GetPerfLevel() {
  return perf_level;
}

void PerfContext::Reset() {
#if !defined(NPERF_CONTEXT) && !defined(IOS_CROSS_COMPILE)
  user_key_comparison_count = 0;
  block_cache_hit_count = 0;
  block_read_count = 0;
  block_read_byte = 0;
  block_read_time = 0;
  block_checksum_time = 0;
  block_decompress_time = 0;
  internal_key_skipped_count = 0;
  internal_delete_skipped_count = 0;
  write_wal_time = 0;

  get_snapshot_time = 0;
  get_from_memtable_time = 0;
  get_from_memtable_count = 0;
  get_post_process_time = 0;
  get_from_output_files_time = 0;
  seek_on_memtable_time = 0;
  seek_on_memtable_count = 0;
  seek_child_seek_time = 0;
  seek_child_seek_count = 0;
  seek_min_heap_time = 0;
  seek_internal_seek_time = 0;
  find_next_user_entry_time = 0;
  write_pre_and_post_process_time = 0;
  write_memtable_time = 0;
  db_mutex_lock_nanos = 0;
  db_condition_wait_nanos = 0;
  merge_operator_time_nanos = 0;
#endif
}

#define OUTPUT(counter) #counter << " = " << counter << ", "

std::string PerfContext::ToString() const {
#if defined(NPERF_CONTEXT) || defined(IOS_CROSS_COMPILE)
  return "";
#else
  std::ostringstream ss;
  ss << OUTPUT(user_key_comparison_count) << OUTPUT(block_cache_hit_count)
     << OUTPUT(block_read_count) << OUTPUT(block_read_byte)
     << OUTPUT(block_read_time) << OUTPUT(block_checksum_time)
     << OUTPUT(block_decompress_time) << OUTPUT(internal_key_skipped_count)
     << OUTPUT(internal_delete_skipped_count) << OUTPUT(write_wal_time)
     << OUTPUT(get_snapshot_time) << OUTPUT(get_from_memtable_time)
     << OUTPUT(get_from_memtable_count) << OUTPUT(get_post_process_time)
     << OUTPUT(get_from_output_files_time) << OUTPUT(seek_on_memtable_time)
     << OUTPUT(seek_on_memtable_count) << OUTPUT(seek_child_seek_time)
     << OUTPUT(seek_child_seek_count) << OUTPUT(seek_min_heap_time)
     << OUTPUT(seek_internal_seek_time) << OUTPUT(find_next_user_entry_time)
     << OUTPUT(write_pre_and_post_process_time) << OUTPUT(write_memtable_time)
     << OUTPUT(db_mutex_lock_nanos) << OUTPUT(db_condition_wait_nanos)
     << OUTPUT(merge_operator_time_nanos);
  return ss.str();
#endif
}

}
