// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <map>
#include <string>

#include "rocksdb/perf_level.h"

namespace ROCKSDB_NAMESPACE {

/*
 * Please add new metrics to this macro and appropriate fields will be copied,
 * and/or emitted when converted to string.
 * When people need to add new metrics please add an optional multi-line C style
 * comment on what the metric semantics are and enclose the specific metric
 * within defCmd()
 */
#define DEF_PERF_CONTEXT_LEVEL_METRICS(defCmd)                                   \
  /* # of times bloom filter has avoided file reads, i.e., negatives. */         \
  defCmd(bloom_filter_useful) /* # of times bloom FullFilter has not avoided     \
                                 the reads. */                                   \
      defCmd(bloom_filter_full_positive)      /*                                 \
                                               * # of times bloom FullFilter has \
                                               * not avoided the reads and data  \
                                               * actually      exist.            \
                                               */                                \
      defCmd(bloom_filter_full_true_positive) /*                                 \
                                               * total number of user key        \
                                               * returned (only include keys     \
                                               * that are found, does not        \
                                               * include keys that are deleted   \
                                               * or merged without a final put   \
                                               */                                \
      defCmd(user_key_return_count) /* total nanos spent on reading data from    \
                                       SST files */                              \
      defCmd(get_from_table_nanos)                                               \
          defCmd(block_cache_hit_count) /* total number of block cache hits */   \
      defCmd(block_cache_miss_count)    /* total number of block cache misses */

// A thread local context for gathering performance counter efficiently
// and transparently.
// Use SetPerfLevel(PerfLevel::kEnableTime) to enable time stats.

// Break down performance counters by level and store per-level perf context in
// PerfContextByLevel
struct PerfContextByLevel {
#define EMIT_FIELDS(x) uint64_t x = 0;
  DEF_PERF_CONTEXT_LEVEL_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS
  void Reset();  // reset all performance counters to zero
};

/*
 * Please add new metrics to this macro and appropriate fields will be copied,
 * and/or emitted when converted to string.
 * When people need to add new metrics please add an optional multi-line C style
 * comment on what the metric semantics are and enclose the specific metric
 * within defCmd()
 */
#define DEF_PERF_CONTEXT_METRICS(defCmd)     \
  /* total number of user key comparisons */ \
  defCmd(user_key_comparison_count)                                             \
  /* total number of block cache hits */                                        \
  defCmd(block_cache_hit_count)                                                 \
  /* total number of block reads (with IO) */                                   \
  defCmd(block_read_count)                                                      \
  /* total number of bytes from block reads */                                  \
  defCmd(block_read_byte)                                                       \
  /* total nanos spent on block reads (wall-clock time) */                      \
  defCmd(block_read_time)                                                       \
  /* total CPU time in nanos spent on block reads */                            \
  defCmd(block_read_cpu_time)                                                   \
  /* total number of index block hits */                                        \
  defCmd(block_cache_index_hit_count)                                           \
  /* total number of standalone handles lookup from secondary cache */          \
  defCmd(block_cache_standalone_handle_count)                                   \
  /*
   * total number of real handles lookup from secondary cache that are inserted
   * into primary cache
   */                                                                           \
  defCmd(block_cache_real_handle_count)                                         \
  /* total number of index block reads */                                       \
  defCmd(index_block_read_count)                                                \
  /* total number of filter block hits */                                       \
  defCmd(block_cache_filter_hit_count)                                          \
  /* total number of filter block reads */                                      \
  defCmd(filter_block_read_count)                                               \
  /* total number of compression dictionary block reads */                      \
  defCmd(compression_dict_block_read_count)                                     \
  /* total number of secondary cache hits */                                    \
  defCmd(secondary_cache_hit_count)                                             \
  /* total number of real handles inserted into secondary cache */              \
  defCmd(compressed_sec_cache_insert_real_count)                                \
  /* total number of dummy handles inserted into secondary cache */             \
  defCmd(compressed_sec_cache_insert_dummy_count)                               \
  /* bytes for vals before compression in secondary cache */                    \
  defCmd(compressed_sec_cache_uncompressed_bytes)                               \
  /* bytes for vals after compression in secondary cache */                     \
  defCmd(compressed_sec_cache_compressed_bytes)                                 \
  /* total nanos spent on block checksum */                                     \
  defCmd(block_checksum_time)                                                   \
  /* total nanos spent on block decompression */                                \
  defCmd(block_decompress_time)                                                 \
  /* bytes for vals returned by Get */                                          \
  defCmd(get_read_bytes)                                                        \
  /* bytes for vals returned by MultiGet */                                     \
  defCmd(multiget_read_bytes)                                                   \
  /* bytes for keys/vals decoded by iterator */                                 \
  defCmd(iter_read_bytes)                                                       \
  /* total number of blob cache hits */                                         \
  defCmd(blob_cache_hit_count)                                                  \
  /* total number of blob reads (with IO) */                                    \
  defCmd(blob_read_count)                                                       \
  /* total number of bytes from blob reads */                                   \
  defCmd(blob_read_byte)                                                        \
  /* total nanos spent on blob reads */                                         \
  defCmd(blob_read_time)                                                        \
  /* total nanos spent on blob checksum */                                      \
  defCmd(blob_checksum_time)                                                    \
  /* total nanos spent on blob decompression */                                 \
  defCmd(blob_decompress_time)                                                  \
  /*
   * total number of internal keys skipped over during iteration.
   * There are several reasons for it:
   * 1. when calling Next(), the iterator is in the position of the previous
   *    key, so that we'll need to skip it. It means this counter will always
   *    be incremented in Next().
   * 2. when calling Next(), we need to skip internal entries for the previous
   *    keys that are overwritten.
   * 3. when calling Next(), Seek() or SeekToFirst(), after previous key
   *    before calling Next(), the seek key in Seek() or the beginning for
   *    SeekToFirst(), there may be one or more deleted keys before the next
   *    valid key that the operation should place the iterator to. We need
   *    to skip both of the tombstone and updates hidden by the tombstones. The
   *    tombstones are not included in this counter, while previous updates
   *    hidden by the tombstones will be included here.
   * 4. symmetric cases for Prev() and SeekToLast()
   * internal_recent_skipped_count is not included in this counter.
   */                                                                           \
  defCmd(internal_key_skipped_count)                                            \
  /*
   * Total number of deletes and single deletes skipped over during iteration
   * When calling Next(), Seek() or SeekToFirst(), after previous position
   * before calling Next(), the seek key in Seek() or the beginning for
   * SeekToFirst(), there may be one or more deleted keys before the next valid
   * key. Every deleted key is counted once. We don't recount here if there are
   * still older updates invalidated by the tombstones.
   */                                                                           \
  defCmd(internal_delete_skipped_count)                                         \
  /*
   * How many times iterators skipped over internal keys that are more recent
   * than the snapshot that iterator is using.
   */                                                                           \
  defCmd(internal_recent_skipped_count)                                         \
  /*
   * How many merge operands were fed into the merge operator by iterators.
   * Note: base values are not included in the count.
   */                                                                           \
  defCmd(internal_merge_count)                                                  \
  /*
   * How many merge operands were fed into the merge operator by point lookups.
   * Note: base values are not included in the count.
   */                                                                           \
  defCmd(internal_merge_point_lookup_count)                                     \
  /*
   * Number of times we reseeked inside a merging iterator, specifically to skip
   * after or before a range of keys covered by a range deletion in a newer LSM
   * component.
   */                                                                           \
  defCmd(internal_range_del_reseek_count)                                       \
  defCmd(get_snapshot_time)      /* total nanos spent on getting snapshot */    \
  defCmd(get_from_memtable_time)   /* total nanos spent on querying memtable */ \
  defCmd(get_from_memtable_count)  /* number of mem tables queried) */          \
  /* total nanos spent after Get() finds a key */                               \
  defCmd(get_post_process_time)                                                 \
  defCmd(get_from_output_files_time)  /* total nanos reading from output file */\
  /* total nanos spent on seeking memtable */                                   \
  defCmd(seek_on_memtable_time)                                                 \
  /*
   * number of seeks issued on memtable
   * (including SeekForPrev but not SeekToFirst and SeekToLast)
   */                                                                           \
  defCmd(seek_on_memtable_count)                                                \
  /* number of Next()s issued on memtable */                                    \
  defCmd(next_on_memtable_count)                                                \
  /* number of Prev()s issued on memtable */                                    \
  defCmd(prev_on_memtable_count)                                                \
  /* total nanos spent on seeking child iters */                                \
  defCmd(seek_child_seek_time)                                                  \
  /* number of seek issued in child iterators */                                \
  defCmd(seek_child_seek_count)                                                 \
  defCmd(seek_min_heap_time)  /* total nanos spent on the merge min heap */     \
  defCmd(seek_max_heap_time)  /* total nanos spent on the merge max heap */     \
  /* total nanos spent on seeking the internal entries */                       \
  defCmd(seek_internal_seek_time)                                               \
  /*
   * total nanos spent on iterating internal entries to find the next user
   * entry
   */                                                                           \
  defCmd(find_next_user_entry_time)                                             \
  /*
   * This group of stats provide a breakdown of time spent by Write().
   * May be inaccurate when 2PC, two_write_queues or enable_pipelined_write
   * are enabled.
   */                                                                           \
  /* total nanos spent on writing to WAL */                                     \
  defCmd(write_wal_time)                                                        \
  /* total nanos spent on writing to mem tables */                              \
  defCmd(write_memtable_time)                                                   \
  /* total nanos spent on delaying or throttling write */                       \
  defCmd(write_delay_time)                                                      \
  /*
   * total nanos spent on switching memtable/wal and scheduling
   * flushes/compactions.
   */                                                                           \
  defCmd(write_scheduling_flushes_compactions_time)                             \
  /* total nanos spent on writing a record, excluding the above four things */  \
  defCmd(write_pre_and_post_process_time)                                       \
  /* time spent waiting for other threads of the batch group */                 \
  defCmd(write_thread_wait_nanos)                                               \
  /* time spent on acquiring DB mutex. */                                       \
  defCmd(db_mutex_lock_nanos)                                                   \
  /* Time spent on waiting with a condition variable created with DB mutex.*/   \
  defCmd(db_condition_wait_nanos)                                               \
  /* Time spent on merge operator. */                                           \
  defCmd(merge_operator_time_nanos)                                             \
  /* Time spent on reading index block from block cache or SST file */          \
  defCmd(read_index_block_nanos)                                                \
  /* Time spent on reading filter block from block cache or SST file */         \
  defCmd(read_filter_block_nanos)                                               \
  /* Time spent on creating data block iterator */                              \
  defCmd(new_table_block_iter_nanos)                                            \
  /* Time spent on creating a iterator of an SST file. */                       \
  defCmd(new_table_iterator_nanos)                                              \
  /* Time spent on seeking a key in data/index blocks */                        \
  defCmd(block_seek_nanos)                                                      \
  /* Time spent on finding or creating a table reader */                        \
  defCmd(find_table_nanos)                                                      \
  /* total number of mem table bloom hits */                                    \
  defCmd(bloom_memtable_hit_count)                                              \
  /* total number of mem table bloom misses */                                  \
  defCmd(bloom_memtable_miss_count)                                             \
  /* total number of SST table bloom hits */                                    \
  defCmd(bloom_sst_hit_count)                                                   \
  /* total number of SST table bloom misses */                                  \
  defCmd(bloom_sst_miss_count)                                                  \
  /* Time spent waiting on key locks in transaction lock manager. */            \
  defCmd(key_lock_wait_time)                                                    \
  /* number of times acquiring a lock was blocked by another transaction. */    \
  defCmd(key_lock_wait_count)                                                   \
  /*
   * Total time spent in Env filesystem operations. These are only populated
   * when TimedEnv is used.
   */                                                                           \
  defCmd(env_new_sequential_file_nanos)                                         \
  defCmd(env_new_random_access_file_nanos)                                      \
  defCmd(env_new_writable_file_nanos)                                           \
  defCmd(env_reuse_writable_file_nanos)                                         \
  defCmd(env_new_random_rw_file_nanos)                                          \
  defCmd(env_new_directory_nanos)                                               \
  defCmd(env_file_exists_nanos)                                                 \
  defCmd(env_get_children_nanos)                                                \
  defCmd(env_get_children_file_attributes_nanos)                                \
  defCmd(env_delete_file_nanos)                                                 \
  defCmd(env_create_dir_nanos)                                                  \
  defCmd(env_create_dir_if_missing_nanos)                                       \
  defCmd(env_delete_dir_nanos)                                                  \
  defCmd(env_get_file_size_nanos)                                               \
  defCmd(env_get_file_modification_time_nanos)                                  \
  defCmd(env_rename_file_nanos)                                                 \
  defCmd(env_link_file_nanos)                                                   \
  defCmd(env_lock_file_nanos)                                                   \
  defCmd(env_unlock_file_nanos)                                                 \
  defCmd(env_new_logger_nanos)                                                  \
  defCmd(get_cpu_nanos)                                                         \
  defCmd(iter_next_cpu_nanos)                                                   \
  defCmd(iter_prev_cpu_nanos)                                                   \
  defCmd(iter_seek_cpu_nanos)                                                   \
  /*
   * EXPERIMENTAL
   * Total number of db iterator's Next(), Prev(), Seek-related APIs being
   * called.
   */                                                                           \
  defCmd(iter_next_count)                                                       \
  defCmd(iter_prev_count)                                                       \
  defCmd(iter_seek_count)                                                       \
  /* Time spent in encrypting data. Populated when EncryptedEnv is used. */     \
  defCmd(encrypt_data_nanos)                                                    \
  /* Time spent in decrypting data. Populated when EncryptedEnv is used. */     \
  defCmd(decrypt_data_nanos)                                                    \
  defCmd(number_async_seek)

struct PerfContext {
  ~PerfContext();

  PerfContext() {}

  PerfContext(const PerfContext&);
  PerfContext& operator=(const PerfContext&);
  PerfContext(PerfContext&&) noexcept;

  void Reset();  // reset all performance counters to zero

  std::string ToString(bool exclude_zero_counters = false) const;

  // enable per level perf context and allocate storage for PerfContextByLevel
  void EnablePerLevelPerfContext();

  // temporarily disable per level perf context by setting the flag to false
  void DisablePerLevelPerfContext();

  // free the space for PerfContextByLevel, also disable per level perf context
  void ClearPerLevelPerfContext();

#define EMIT_FIELDS(x) uint64_t x;
  DEF_PERF_CONTEXT_METRICS(EMIT_FIELDS)
#undef EMIT_FIELDS

  std::map<uint32_t, PerfContextByLevel>* level_to_perf_context = nullptr;
  bool per_level_perf_context_enabled = false;

  void copyMetrics(const PerfContext* other) noexcept;
};

// If RocksDB is compiled with -DNPERF_CONTEXT, then a pointer to a global,
// non-thread-local PerfContext object will be returned. Attempts to update
// this object will be ignored, and reading from it will also be no-op.
// Otherwise,
// a) if thread-local is supported on the platform, then a pointer to
//    a thread-local PerfContext object will be returned.
// b) if thread-local is NOT supported, then compilation will fail.
//
// This function never returns nullptr.
PerfContext* get_perf_context();

}  // namespace ROCKSDB_NAMESPACE
