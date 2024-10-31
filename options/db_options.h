// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;

struct ImmutableDBOptions {
  static const char* kName() { return "ImmutableDBOptions"; }
  ImmutableDBOptions();
  explicit ImmutableDBOptions(const DBOptions& options);

  void Dump(Logger* log) const;

  bool create_if_missing;
  bool create_missing_column_families;
  bool error_if_exists;
  bool paranoid_checks;
  bool flush_verify_memtable_count;
  bool compaction_verify_record_count;
  bool track_and_verify_wals_in_manifest;
  bool verify_sst_unique_id_in_manifest;
  Env* env;
  std::shared_ptr<RateLimiter> rate_limiter;
  std::shared_ptr<SstFileManager> sst_file_manager;
  std::shared_ptr<Logger> info_log;
  InfoLogLevel info_log_level;
  int max_file_opening_threads;
  std::shared_ptr<Statistics> statistics;
  bool use_fsync;
  std::vector<DbPath> db_paths;
  std::string db_log_dir;
  // The wal_dir option from the file.  To determine the
  // directory in use, the GetWalDir or IsWalDirSameAsDBPath
  // methods should be used instead of accessing this variable directly.
  std::string wal_dir;
  size_t max_log_file_size;
  size_t log_file_time_to_roll;
  size_t keep_log_file_num;
  size_t recycle_log_file_num;
  uint64_t max_manifest_file_size;
  int table_cache_numshardbits;
  uint64_t WAL_ttl_seconds;
  uint64_t WAL_size_limit_MB;
  uint64_t max_write_batch_group_size_bytes;
  size_t manifest_preallocation_size;
  bool allow_mmap_reads;
  bool allow_mmap_writes;
  bool use_direct_reads;
  bool use_direct_io_for_flush_and_compaction;
  bool allow_fallocate;
  bool is_fd_close_on_exec;
  bool advise_random_on_open;
  size_t db_write_buffer_size;
  std::shared_ptr<WriteBufferManager> write_buffer_manager;
  size_t random_access_max_buffer_size;
  bool use_adaptive_mutex;
  std::vector<std::shared_ptr<EventListener>> listeners;
  bool enable_thread_tracking;
  bool enable_pipelined_write;
  bool unordered_write;
  bool allow_concurrent_memtable_write;
  bool enable_write_thread_adaptive_yield;
  uint64_t write_thread_max_yield_usec;
  uint64_t write_thread_slow_yield_usec;
  bool skip_stats_update_on_db_open;
  bool skip_checking_sst_file_sizes_on_db_open;
  WALRecoveryMode wal_recovery_mode;
  bool allow_2pc;
  std::shared_ptr<Cache> row_cache;
  WalFilter* wal_filter;
  bool fail_if_options_file_error;
  bool dump_malloc_stats;
  bool avoid_flush_during_recovery;
  bool allow_ingest_behind;
  bool two_write_queues;
  bool manual_wal_flush;
  CompressionType wal_compression;
  bool background_close_inactive_wals;
  bool atomic_flush;
  bool avoid_unnecessary_blocking_io;
  bool prefix_seek_opt_in_only;
  bool persist_stats_to_disk;
  bool write_dbid_to_manifest;
  bool write_identity_file;
  size_t log_readahead_size;
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory;
  bool best_efforts_recovery;
  int max_bgerror_resume_count;
  uint64_t bgerror_resume_retry_interval;
  bool allow_data_in_errors;
  std::string db_host_id;
  FileTypeSet checksum_handoff_file_types;
  CacheTier lowest_used_cache_tier;
  std::shared_ptr<CompactionService> compaction_service;
  bool enforce_single_del_contracts;
  uint64_t follower_refresh_catchup_period_ms;
  uint64_t follower_catchup_retry_count;
  uint64_t follower_catchup_retry_wait_ms;
  Temperature metadata_write_temperature;
  Temperature wal_write_temperature;

  // Beginning convenience/helper objects that are not part of the base
  // DBOptions
  std::shared_ptr<FileSystem> fs;
  SystemClock* clock;
  Statistics* stats;
  Logger* logger;
  // End of convenience/helper objects.

  bool IsWalDirSameAsDBPath() const;
  bool IsWalDirSameAsDBPath(const std::string& path) const;
  const std::string& GetWalDir() const;
  const std::string& GetWalDir(const std::string& path) const;
};

struct MutableDBOptions {
  static const char* kName() { return "MutableDBOptions"; }
  MutableDBOptions();
  explicit MutableDBOptions(const DBOptions& options);

  void Dump(Logger* log) const;

  int max_background_jobs;
  int max_background_compactions;
  uint32_t max_subcompactions;
  bool avoid_flush_during_shutdown;
  size_t writable_file_max_buffer_size;
  uint64_t delayed_write_rate;
  uint64_t max_total_wal_size;
  uint64_t delete_obsolete_files_period_micros;
  unsigned int stats_dump_period_sec;
  unsigned int stats_persist_period_sec;
  size_t stats_history_buffer_size;
  int max_open_files;
  uint64_t bytes_per_sync;
  uint64_t wal_bytes_per_sync;
  bool strict_bytes_per_sync;
  size_t compaction_readahead_size;
  int max_background_flushes;
  std::string daily_offpeak_time_utc;
};

Status GetStringFromMutableDBOptions(const ConfigOptions& config_options,
                                     const MutableDBOptions& mutable_opts,
                                     std::string* opt_string);

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options);

bool MutableDBOptionsAreEqual(const MutableDBOptions& this_options,
                              const MutableDBOptions& that_options);

}  // namespace ROCKSDB_NAMESPACE
