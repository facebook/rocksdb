// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_ROCKSDB_INCLUDE_PERF_CONTEXT_H
#define STORAGE_ROCKSDB_INCLUDE_PERF_CONTEXT_H

#include <stdint.h>
#include <string>

#include "rocksdb/perf_level.h"

namespace rocksdb {

// A thread local context for gathering performance counter efficiently
// and transparently.
// Use SetPerfLevel(PerfLevel::kEnableTime) to enable time stats.

struct PerfContext {

  void Reset(); // reset all performance counters to zero

  std::string ToString(bool exclude_zero_counters = false) const;

  uint64_t user_key_comparison_count; // total number of user key comparisons
  uint64_t block_cache_hit_count;     // total number of block cache hits
  uint64_t block_read_count;          // total number of block reads (with IO)
  uint64_t block_read_byte;           // total number of bytes from block reads
  uint64_t block_read_time;           // total nanos spent on block reads
  uint64_t block_checksum_time;       // total nanos spent on block checksum
  uint64_t block_decompress_time;  // total nanos spent on block decompression
  // total number of internal keys skipped over during iteration.
  // There are several reasons for it:
  // 1. when calling Next(), the iterator is in the position of the previous
  //    key, so that we'll need to skip it. It means this counter will always
  //    be incremented in Next().
  // 2. when calling Next(), we need to skip internal entries for the previous
  //    keys that are overwritten.
  // 3. when calling Next(), Seek() or SeekToFirst(), after previous key
  //    before calling Next(), the seek key in Seek() or the beginning for
  //    SeekToFirst(), there may be one or more deleted keys before the next
  //    valid key that the operation should place the iterator to. We need
  //    to skip both of the tombstone and updates hidden by the tombstones. The
  //    tombstones are not included in this counter, while previous updates
  //    hidden by the tombstones will be included here.
  // 4. symmetric cases for Prev() and SeekToLast()
  // internal_recent_skipped_count is not included in this counter.
  //
  uint64_t internal_key_skipped_count;
  // Total number of deletes and single deletes skipped over during iteration
  // When calling Next(), Seek() or SeekToFirst(), after previous position
  // before calling Next(), the seek key in Seek() or the beginning for
  // SeekToFirst(), there may be one or more deleted keys before the next valid
  // key. Every deleted key is counted once. We don't recount here if there are
  // still older updates invalidated by the tombstones.
  //
  uint64_t internal_delete_skipped_count;
  // How many times iterators skipped over internal keys that are more recent
  // than the snapshot that iterator is using.
  //
  uint64_t internal_recent_skipped_count;
  // How many values were fed into merge operator by iterators.
  //
  uint64_t internal_merge_count;

  uint64_t get_snapshot_time;       // total nanos spent on getting snapshot
  uint64_t get_from_memtable_time;  // total nanos spent on querying memtables
  uint64_t get_from_memtable_count;    // number of mem tables queried
  // total nanos spent after Get() finds a key
  uint64_t get_post_process_time;
  uint64_t get_from_output_files_time;  // total nanos reading from output files
  // total nanos spent on seeking memtable
  uint64_t seek_on_memtable_time;
  // number of seeks issued on memtable
  // (including SeekForPrev but not SeekToFirst and SeekToLast)
  uint64_t seek_on_memtable_count;
  // number of Next()s issued on memtable
  uint64_t next_on_memtable_count;
  // number of Prev()s issued on memtable
  uint64_t prev_on_memtable_count;
  // total nanos spent on seeking child iters
  uint64_t seek_child_seek_time;
  // number of seek issued in child iterators
  uint64_t seek_child_seek_count;
  uint64_t seek_min_heap_time;  // total nanos spent on the merge min heap
  uint64_t seek_max_heap_time;  // total nanos spent on the merge max heap
  // total nanos spent on seeking the internal entries
  uint64_t seek_internal_seek_time;
  // total nanos spent on iterating internal entries to find the next user entry
  uint64_t find_next_user_entry_time;

  // total nanos spent on writing to WAL
  uint64_t write_wal_time;
  // total nanos spent on writing to mem tables
  uint64_t write_memtable_time;
  // total nanos spent on delaying write
  uint64_t write_delay_time;
  // total nanos spent on writing a record, excluding the above three times
  uint64_t write_pre_and_post_process_time;

  uint64_t db_mutex_lock_nanos;      // time spent on acquiring DB mutex.
  // Time spent on waiting with a condition variable created with DB mutex.
  uint64_t db_condition_wait_nanos;
  // Time spent on merge operator.
  uint64_t merge_operator_time_nanos;

  // Time spent on reading index block from block cache or SST file
  uint64_t read_index_block_nanos;
  // Time spent on reading filter block from block cache or SST file
  uint64_t read_filter_block_nanos;
  // Time spent on creating data block iterator
  uint64_t new_table_block_iter_nanos;
  // Time spent on creating a iterator of an SST file.
  uint64_t new_table_iterator_nanos;
  // Time spent on seeking a key in data/index blocks
  uint64_t block_seek_nanos;
  // Time spent on finding or creating a table reader
  uint64_t find_table_nanos;
  // total number of mem table bloom hits
  uint64_t bloom_memtable_hit_count;
  // total number of mem table bloom misses
  uint64_t bloom_memtable_miss_count;
  // total number of SST table bloom hits
  uint64_t bloom_sst_hit_count;
  // total number of SST table bloom misses
  uint64_t bloom_sst_miss_count;
};

#if defined(NPERF_CONTEXT) || defined(IOS_CROSS_COMPILE)
extern PerfContext perf_context;
#elif _WIN32
extern __declspec(thread) PerfContext perf_context;
#else
extern __thread PerfContext perf_context;
#endif

}

#endif
