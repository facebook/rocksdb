// complements to perf_level
#pragma once

#include <bitset>
#include <initializer_list>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

enum PerfFlag : uint32_t {
  user_key_comparison_count = 0,
  block_cache_hit_count,
  block_read_count,
  block_read_byte,
  block_read_time,
  block_cache_index_hit_count,
  index_block_read_count,
  block_cache_filter_hit_count,
  filter_block_read_count,
  compression_dict_block_read_count,
  secondary_cache_hit_count,
  block_checksum_time,
  block_decompress_time,
  get_read_bytes,
  multiget_read_bytes,
  iter_read_bytes,
  internal_key_skipped_count,
  internal_delete_skipped_count,
  internal_recent_skipped_count,
  internal_merge_count,
  get_snapshot_time,
  get_from_memtable_time,
  get_from_memtable_count,
  get_post_process_time,
  get_from_output_files_time,
  seek_on_memtable_time,
  seek_on_memtable_count,
  next_on_memtable_count,
  prev_on_memtable_count,
  seek_child_seek_time,
  seek_child_seek_count,
  seek_min_heap_time,
  seek_max_heap_time,
  seek_internal_seek_time,
  find_next_user_entry_time,
  write_wal_time,
  write_memtable_time,
  write_delay_time,
  write_scheduling_flushes_compactions_time,
  write_pre_and_post_process_time,
  write_thread_wait_nanos,
  db_mutex_lock_nanos,
  db_condition_wait_nanos,
  merge_operator_time_nanos,
  read_index_block_nanos,
  read_filter_block_nanos,
  new_table_block_iter_nanos,
  new_table_iterator_nanos,
  block_seek_nanos,
  find_table_nanos,
  bloom_memtable_hit_count,
  bloom_memtable_miss_count,
  bloom_sst_hit_count,
  bloom_sst_miss_count,
  key_lock_wait_time,
  key_lock_wait_count,
  env_new_sequential_file_nanos,
  env_new_random_access_file_nanos,
  env_new_writable_file_nanos,
  env_reuse_writable_file_nanos,
  env_new_random_rw_file_nanos,
  env_new_directory_nanos,
  env_file_exists_nanos,
  env_get_children_nanos,
  env_get_children_file_attributes_nanos,
  env_delete_file_nanos,
  env_create_dir_nanos,
  env_create_dir_if_missing_nanos,
  env_delete_dir_nanos,
  env_get_file_size_nanos,
  env_get_file_modification_time_nanos,
  env_rename_file_nanos,
  env_link_file_nanos,
  env_lock_file_nanos,
  env_unlock_file_nanos,
  env_new_logger_nanos,
  get_cpu_nanos,
  iter_next_cpu_nanos,
  iter_prev_cpu_nanos,
  iter_seek_cpu_nanos,
  encrypt_data_nanos,
  decrypt_data_nanos,

  get_from_table_nanos,
  user_key_return_count,
  block_cache_miss_count,
  bloom_filter_full_positive,
  bloom_filter_useful,
  bloom_filter_full_true_positive,

  bytes_read,
  bytes_written,
  open_nanos,
  allocate_nanos,
  write_nanos,
  read_nanos,
  range_sync_nanos,
  prepare_write_nanos,
  fsync_nanos,
  logger_nanos,
  cpu_read_nanos,
  cpu_write_nanos,
  // Should always be the last
  COUNT
};

using PerfFlags = std::bitset<PerfFlag::COUNT>;

PerfFlags NewPerfFlags(std::initializer_list<PerfFlag> l);
bool CheckPerfFlag(PerfFlag flag);
PerfFlags GetPerfFlags();
void SetPerfFlags(PerfFlags flags);

}  // namespace ROCKSDB_NAMESPACE
