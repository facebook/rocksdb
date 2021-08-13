// complements to perf_level
#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

enum {
  flag_user_key_comparison_count = 0,
  flag_block_cache_hit_count,
  flag_block_read_count,
  flag_block_read_byte,
  flag_block_read_time,
  flag_block_cache_index_hit_count,
  flag_index_block_read_count,
  flag_block_cache_filter_hit_count,
  flag_filter_block_read_count,
  flag_compression_dict_block_read_count,
  flag_secondary_cache_hit_count,
  flag_block_checksum_time,
  flag_block_decompress_time,
  flag_get_read_bytes,
  flag_multiget_read_bytes,
  flag_iter_read_bytes,
  flag_internal_key_skipped_count,
  flag_internal_delete_skipped_count,
  flag_internal_recent_skipped_count,
  flag_internal_merge_count,
  flag_get_snapshot_time,
  flag_get_from_memtable_time,
  flag_get_from_memtable_count,
  flag_get_post_process_time,
  flag_get_from_output_files_time,
  flag_seek_on_memtable_time,
  flag_seek_on_memtable_count,
  flag_next_on_memtable_count,
  flag_prev_on_memtable_count,
  flag_seek_child_seek_time,
  flag_seek_child_seek_count,
  flag_seek_min_heap_time,
  flag_seek_max_heap_time,
  flag_seek_internal_seek_time,
  flag_find_next_user_entry_time,
  flag_write_wal_time,
  flag_write_memtable_time,
  flag_write_delay_time,
  flag_write_scheduling_flushes_compactions_time,
  flag_write_pre_and_post_process_time,
  flag_write_thread_wait_nanos,
  flag_db_mutex_lock_nanos,
  flag_db_condition_wait_nanos,
  flag_merge_operator_time_nanos,
  flag_read_index_block_nanos,
  flag_read_filter_block_nanos,
  flag_new_table_block_iter_nanos,
  flag_new_table_iterator_nanos,
  flag_block_seek_nanos,
  flag_find_table_nanos,
  flag_bloom_memtable_hit_count,
  flag_bloom_memtable_miss_count,
  flag_bloom_sst_hit_count,
  flag_bloom_sst_miss_count,
  flag_key_lock_wait_time,
  flag_key_lock_wait_count,
  flag_env_new_sequential_file_nanos,
  flag_env_new_random_access_file_nanos,
  flag_env_new_writable_file_nanos,
  flag_env_reuse_writable_file_nanos,
  flag_env_new_random_rw_file_nanos,
  flag_env_new_directory_nanos,
  flag_env_file_exists_nanos,
  flag_env_get_children_nanos,
  flag_env_get_children_file_attributes_nanos,
  flag_env_delete_file_nanos,
  flag_env_create_dir_nanos,
  flag_env_create_dir_if_missing_nanos,
  flag_env_delete_dir_nanos,
  flag_env_get_file_size_nanos,
  flag_env_get_file_modification_time_nanos,
  flag_env_rename_file_nanos,
  flag_env_link_file_nanos,
  flag_env_lock_file_nanos,
  flag_env_unlock_file_nanos,
  flag_env_new_logger_nanos,
  flag_get_cpu_nanos,
  flag_iter_next_cpu_nanos,
  flag_iter_prev_cpu_nanos,
  flag_iter_seek_cpu_nanos,
  flag_encrypt_data_nanos,
  flag_decrypt_data_nanos,

  flag_get_from_table_nanos,
  flag_user_key_return_count,
  flag_block_cache_miss_count,
  flag_bloom_filter_full_positive,
  flag_bloom_filter_useful,
  flag_bloom_filter_full_true_positive,

  flag_bytes_read,
  flag_bytes_written,
  flag_open_nanos,
  flag_allocate_nanos,
  flag_write_nanos,
  flag_read_nanos,
  flag_range_sync_nanos,
  flag_prepare_write_nanos,
  flag_fsync_nanos,
  flag_logger_nanos,
  flag_cpu_read_nanos,
  flag_cpu_write_nanos,
  // Should always be the last
  FLAG_END
};

// FLAGS_LEN = ceiling(FLAG_END / bits(uint8_t))
#define FLAGS_LEN                              \
  (((uint64_t)FLAG_END & (uint64_t)0b111) == 0 \
       ? ((uint64_t)FLAG_END >> 3)             \
       : ((uint64_t)FLAG_END >> 3) + 1)

void EnablePerfFlag(uint64_t flag);
void DisablePerfFlag(uint64_t flag);
bool CheckPerfFlag(uint64_t flag);

}  // namespace ROCKSDB_NAMESPACE
