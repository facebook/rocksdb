// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/db_options.h"

#include <cinttypes>

#include "logging/logging.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/wal_filter.h"

namespace ROCKSDB_NAMESPACE {
#ifndef ROCKSDB_LITE
static std::unordered_map<std::string, WALRecoveryMode>
    wal_recovery_mode_string_map = {
        {"kTolerateCorruptedTailRecords",
         WALRecoveryMode::kTolerateCorruptedTailRecords},
        {"kAbsoluteConsistency", WALRecoveryMode::kAbsoluteConsistency},
        {"kPointInTimeRecovery", WALRecoveryMode::kPointInTimeRecovery},
        {"kSkipAnyCorruptedRecords",
         WALRecoveryMode::kSkipAnyCorruptedRecords}};

static std::unordered_map<std::string, DBOptions::AccessHint>
    access_hint_string_map = {{"NONE", DBOptions::AccessHint::NONE},
                              {"NORMAL", DBOptions::AccessHint::NORMAL},
                              {"SEQUENTIAL", DBOptions::AccessHint::SEQUENTIAL},
                              {"WILLNEED", DBOptions::AccessHint::WILLNEED}};

static std::unordered_map<std::string, InfoLogLevel> info_log_level_string_map =
    {{"DEBUG_LEVEL", InfoLogLevel::DEBUG_LEVEL},
     {"INFO_LEVEL", InfoLogLevel::INFO_LEVEL},
     {"WARN_LEVEL", InfoLogLevel::WARN_LEVEL},
     {"ERROR_LEVEL", InfoLogLevel::ERROR_LEVEL},
     {"FATAL_LEVEL", InfoLogLevel::FATAL_LEVEL},
     {"HEADER_LEVEL", InfoLogLevel::HEADER_LEVEL}};

std::unordered_map<std::string, OptionTypeInfo>
    OptionsHelper::db_options_type_info = {
        /*
         // not yet supported
          std::shared_ptr<Cache> row_cache;
          std::shared_ptr<DeleteScheduler> delete_scheduler;
          std::shared_ptr<Logger> info_log;
          std::shared_ptr<RateLimiter> rate_limiter;
          std::shared_ptr<Statistics> statistics;
          std::vector<DbPath> db_paths;
          std::vector<std::shared_ptr<EventListener>> listeners;
         */
        {"advise_random_on_open",
         {offsetof(struct DBOptions, advise_random_on_open),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"allow_mmap_reads",
         {offsetof(struct DBOptions, allow_mmap_reads), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"allow_fallocate",
         {offsetof(struct DBOptions, allow_fallocate), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"allow_mmap_writes",
         {offsetof(struct DBOptions, allow_mmap_writes), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"use_direct_reads",
         {offsetof(struct DBOptions, use_direct_reads), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"use_direct_writes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"use_direct_io_for_flush_and_compaction",
         {offsetof(struct DBOptions, use_direct_io_for_flush_and_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"allow_2pc",
         {offsetof(struct DBOptions, allow_2pc), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"allow_os_buffer",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"create_if_missing",
         {offsetof(struct DBOptions, create_if_missing), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"create_missing_column_families",
         {offsetof(struct DBOptions, create_missing_column_families),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"disableDataSync",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"disable_data_sync",  // for compatibility
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"enable_thread_tracking",
         {offsetof(struct DBOptions, enable_thread_tracking),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"error_if_exists",
         {offsetof(struct DBOptions, error_if_exists), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"is_fd_close_on_exec",
         {offsetof(struct DBOptions, is_fd_close_on_exec), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"paranoid_checks",
         {offsetof(struct DBOptions, paranoid_checks), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"skip_log_error_on_recovery",
         {offsetof(struct DBOptions, skip_log_error_on_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"skip_stats_update_on_db_open",
         {offsetof(struct DBOptions, skip_stats_update_on_db_open),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"skip_checking_sst_file_sizes_on_db_open",
         {offsetof(struct DBOptions, skip_checking_sst_file_sizes_on_db_open),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"new_table_reader_for_compaction_inputs",
         {offsetof(struct DBOptions, new_table_reader_for_compaction_inputs),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"compaction_readahead_size",
         {offsetof(struct DBOptions, compaction_readahead_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, compaction_readahead_size)}},
        {"random_access_max_buffer_size",
         {offsetof(struct DBOptions, random_access_max_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"use_adaptive_mutex",
         {offsetof(struct DBOptions, use_adaptive_mutex), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"use_fsync",
         {offsetof(struct DBOptions, use_fsync), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"max_background_jobs",
         {offsetof(struct DBOptions, max_background_jobs), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, max_background_jobs)}},
        {"max_background_compactions",
         {offsetof(struct DBOptions, max_background_compactions),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, max_background_compactions)}},
        {"max_subcompactions",
         {offsetof(struct DBOptions, max_subcompactions), OptionType::kUInt32T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, max_subcompactions)}},
        {"base_background_compactions",
         {offsetof(struct DBOptions, base_background_compactions),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, base_background_compactions)}},
        {"max_background_flushes",
         {offsetof(struct DBOptions, max_background_flushes), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, max_background_flushes)}},
        {"max_file_opening_threads",
         {offsetof(struct DBOptions, max_file_opening_threads),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_open_files",
         {offsetof(struct DBOptions, max_open_files), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, max_open_files)}},
        {"table_cache_numshardbits",
         {offsetof(struct DBOptions, table_cache_numshardbits),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"db_write_buffer_size",
         {offsetof(struct DBOptions, db_write_buffer_size), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"keep_log_file_num",
         {offsetof(struct DBOptions, keep_log_file_num), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"recycle_log_file_num",
         {offsetof(struct DBOptions, recycle_log_file_num), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"log_file_time_to_roll",
         {offsetof(struct DBOptions, log_file_time_to_roll), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"manifest_preallocation_size",
         {offsetof(struct DBOptions, manifest_preallocation_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_log_file_size",
         {offsetof(struct DBOptions, max_log_file_size), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"db_log_dir",
         {offsetof(struct DBOptions, db_log_dir), OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"wal_dir",
         {offsetof(struct DBOptions, wal_dir), OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"WAL_size_limit_MB",
         {offsetof(struct DBOptions, WAL_size_limit_MB), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"WAL_ttl_seconds",
         {offsetof(struct DBOptions, WAL_ttl_seconds), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"bytes_per_sync",
         {offsetof(struct DBOptions, bytes_per_sync), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, bytes_per_sync)}},
        {"delayed_write_rate",
         {offsetof(struct DBOptions, delayed_write_rate), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, delayed_write_rate)}},
        {"delete_obsolete_files_period_micros",
         {offsetof(struct DBOptions, delete_obsolete_files_period_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions,
                   delete_obsolete_files_period_micros)}},
        {"max_manifest_file_size",
         {offsetof(struct DBOptions, max_manifest_file_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_total_wal_size",
         {offsetof(struct DBOptions, max_total_wal_size), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, max_total_wal_size)}},
        {"wal_bytes_per_sync",
         {offsetof(struct DBOptions, wal_bytes_per_sync), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, wal_bytes_per_sync)}},
        {"strict_bytes_per_sync",
         {offsetof(struct DBOptions, strict_bytes_per_sync),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, strict_bytes_per_sync)}},
        {"stats_dump_period_sec",
         {offsetof(struct DBOptions, stats_dump_period_sec), OptionType::kUInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, stats_dump_period_sec)}},
        {"stats_persist_period_sec",
         {offsetof(struct DBOptions, stats_persist_period_sec),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, stats_persist_period_sec)}},
        {"persist_stats_to_disk",
         {offsetof(struct DBOptions, persist_stats_to_disk),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          offsetof(struct ImmutableDBOptions, persist_stats_to_disk)}},
        {"stats_history_buffer_size",
         {offsetof(struct DBOptions, stats_history_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, stats_history_buffer_size)}},
        {"fail_if_options_file_error",
         {offsetof(struct DBOptions, fail_if_options_file_error),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"enable_pipelined_write",
         {offsetof(struct DBOptions, enable_pipelined_write),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"unordered_write",
         {offsetof(struct DBOptions, unordered_write), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"allow_concurrent_memtable_write",
         {offsetof(struct DBOptions, allow_concurrent_memtable_write),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"wal_recovery_mode", OptionTypeInfo::Enum<WALRecoveryMode>(
                                  offsetof(struct DBOptions, wal_recovery_mode),
                                  &wal_recovery_mode_string_map)},
        {"enable_write_thread_adaptive_yield",
         {offsetof(struct DBOptions, enable_write_thread_adaptive_yield),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"write_thread_slow_yield_usec",
         {offsetof(struct DBOptions, write_thread_slow_yield_usec),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_write_batch_group_size_bytes",
         {offsetof(struct DBOptions, max_write_batch_group_size_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"write_thread_max_yield_usec",
         {offsetof(struct DBOptions, write_thread_max_yield_usec),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"access_hint_on_compaction_start",
         OptionTypeInfo::Enum<DBOptions::AccessHint>(
             offsetof(struct DBOptions, access_hint_on_compaction_start),
             &access_hint_string_map)},
        {"info_log_level", OptionTypeInfo::Enum<InfoLogLevel>(
                               offsetof(struct DBOptions, info_log_level),
                               &info_log_level_string_map)},
        {"dump_malloc_stats",
         {offsetof(struct DBOptions, dump_malloc_stats), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"avoid_flush_during_recovery",
         {offsetof(struct DBOptions, avoid_flush_during_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"avoid_flush_during_shutdown",
         {offsetof(struct DBOptions, avoid_flush_during_shutdown),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, avoid_flush_during_shutdown)}},
        {"writable_file_max_buffer_size",
         {offsetof(struct DBOptions, writable_file_max_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableDBOptions, writable_file_max_buffer_size)}},
        {"allow_ingest_behind",
         {offsetof(struct DBOptions, allow_ingest_behind), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          offsetof(struct ImmutableDBOptions, allow_ingest_behind)}},
        {"preserve_deletes",
         {offsetof(struct DBOptions, preserve_deletes), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          offsetof(struct ImmutableDBOptions, preserve_deletes)}},
        {"concurrent_prepare",  // Deprecated by two_write_queues
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"two_write_queues",
         {offsetof(struct DBOptions, two_write_queues), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          offsetof(struct ImmutableDBOptions, two_write_queues)}},
        {"manual_wal_flush",
         {offsetof(struct DBOptions, manual_wal_flush), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          offsetof(struct ImmutableDBOptions, manual_wal_flush)}},
        {"seq_per_batch",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"atomic_flush",
         {offsetof(struct DBOptions, atomic_flush), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          offsetof(struct ImmutableDBOptions, atomic_flush)}},
        {"avoid_unnecessary_blocking_io",
         {offsetof(struct DBOptions, avoid_unnecessary_blocking_io),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          offsetof(struct ImmutableDBOptions, avoid_unnecessary_blocking_io)}},
        {"write_dbid_to_manifest",
         {offsetof(struct DBOptions, write_dbid_to_manifest),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"log_readahead_size",
         {offsetof(struct DBOptions, log_readahead_size), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"best_efforts_recovery",
         {offsetof(struct DBOptions, best_efforts_recovery),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_bgerror_resume_count",
         {offsetof(struct DBOptions, max_bgerror_resume_count),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"bgerror_resume_retry_interval",
         {offsetof(struct DBOptions, bgerror_resume_retry_interval),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        // The following properties were handled as special cases in ParseOption
        // This means that the properties could be read from the options file
        // but never written to the file or compared to each other.
        {"rate_limiter_bytes_per_sec",
         {offsetof(struct DBOptions, rate_limiter), OptionType::kUnknown,
          OptionVerificationType::kNormal,
          (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever), 0,
          // Parse the input value as a RateLimiter
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto limiter =
                reinterpret_cast<std::shared_ptr<RateLimiter>*>(addr);
            limiter->reset(NewGenericRateLimiter(
                static_cast<int64_t>(ParseUint64(value))));
            return Status::OK();
          }}},
        {"env",
         {offsetof(struct DBOptions, env), OptionType::kUnknown,
          OptionVerificationType::kNormal,
          (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever), 0,
          // Parse the input value as an Env
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto old_env = reinterpret_cast<Env**>(addr);  // Get the old value
            Env* new_env = *old_env;                       // Set new to old
            Status s = Env::LoadEnv(value, &new_env);      // Update new value
            if (s.ok()) {                                  // It worked
              *old_env = new_env;                          // Update the old one
            }
            return s;
          }}},
};
#endif  // ROCKSDB_LITE

ImmutableDBOptions::ImmutableDBOptions() : ImmutableDBOptions(Options()) {}

ImmutableDBOptions::ImmutableDBOptions(const DBOptions& options)
    : create_if_missing(options.create_if_missing),
      create_missing_column_families(options.create_missing_column_families),
      error_if_exists(options.error_if_exists),
      paranoid_checks(options.paranoid_checks),
      env(options.env),
      fs(options.env->GetFileSystem()),
      rate_limiter(options.rate_limiter),
      sst_file_manager(options.sst_file_manager),
      info_log(options.info_log),
      info_log_level(options.info_log_level),
      max_file_opening_threads(options.max_file_opening_threads),
      statistics(options.statistics),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      max_manifest_file_size(options.max_manifest_file_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      wal_ttl_seconds(options.WAL_ttl_seconds),
      wal_size_limit_mb(options.WAL_size_limit_MB),
      max_write_batch_group_size_bytes(
          options.max_write_batch_group_size_bytes),
      manifest_preallocation_size(options.manifest_preallocation_size),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      use_direct_reads(options.use_direct_reads),
      use_direct_io_for_flush_and_compaction(
          options.use_direct_io_for_flush_and_compaction),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
      advise_random_on_open(options.advise_random_on_open),
      db_write_buffer_size(options.db_write_buffer_size),
      write_buffer_manager(options.write_buffer_manager),
      access_hint_on_compaction_start(options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          options.new_table_reader_for_compaction_inputs),
      random_access_max_buffer_size(options.random_access_max_buffer_size),
      use_adaptive_mutex(options.use_adaptive_mutex),
      listeners(options.listeners),
      enable_thread_tracking(options.enable_thread_tracking),
      enable_pipelined_write(options.enable_pipelined_write),
      unordered_write(options.unordered_write),
      allow_concurrent_memtable_write(options.allow_concurrent_memtable_write),
      enable_write_thread_adaptive_yield(
          options.enable_write_thread_adaptive_yield),
      write_thread_max_yield_usec(options.write_thread_max_yield_usec),
      write_thread_slow_yield_usec(options.write_thread_slow_yield_usec),
      skip_stats_update_on_db_open(options.skip_stats_update_on_db_open),
      skip_checking_sst_file_sizes_on_db_open(
          options.skip_checking_sst_file_sizes_on_db_open),
      wal_recovery_mode(options.wal_recovery_mode),
      allow_2pc(options.allow_2pc),
      row_cache(options.row_cache),
#ifndef ROCKSDB_LITE
      wal_filter(options.wal_filter),
#endif  // ROCKSDB_LITE
      fail_if_options_file_error(options.fail_if_options_file_error),
      dump_malloc_stats(options.dump_malloc_stats),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery),
      allow_ingest_behind(options.allow_ingest_behind),
      preserve_deletes(options.preserve_deletes),
      two_write_queues(options.two_write_queues),
      manual_wal_flush(options.manual_wal_flush),
      atomic_flush(options.atomic_flush),
      avoid_unnecessary_blocking_io(options.avoid_unnecessary_blocking_io),
      persist_stats_to_disk(options.persist_stats_to_disk),
      write_dbid_to_manifest(options.write_dbid_to_manifest),
      log_readahead_size(options.log_readahead_size),
      file_checksum_gen_factory(options.file_checksum_gen_factory),
      best_efforts_recovery(options.best_efforts_recovery),
      max_bgerror_resume_count(options.max_bgerror_resume_count),
      bgerror_resume_retry_interval(options.bgerror_resume_retry_interval) {
}

void ImmutableDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "                        Options.error_if_exists: %d",
                   error_if_exists);
  ROCKS_LOG_HEADER(log, "                      Options.create_if_missing: %d",
                   create_if_missing);
  ROCKS_LOG_HEADER(log, "                        Options.paranoid_checks: %d",
                   paranoid_checks);
  ROCKS_LOG_HEADER(log, "                                    Options.env: %p",
                   env);
  ROCKS_LOG_HEADER(log, "                                     Options.fs: %s",
                   fs->Name());
  ROCKS_LOG_HEADER(log, "                               Options.info_log: %p",
                   info_log.get());
  ROCKS_LOG_HEADER(log, "               Options.max_file_opening_threads: %d",
                   max_file_opening_threads);
  ROCKS_LOG_HEADER(log, "                             Options.statistics: %p",
                   statistics.get());
  ROCKS_LOG_HEADER(log, "                              Options.use_fsync: %d",
                   use_fsync);
  ROCKS_LOG_HEADER(
      log, "                      Options.max_log_file_size: %" ROCKSDB_PRIszt,
      max_log_file_size);
  ROCKS_LOG_HEADER(log,
                   "                 Options.max_manifest_file_size: %" PRIu64,
                   max_manifest_file_size);
  ROCKS_LOG_HEADER(
      log, "                  Options.log_file_time_to_roll: %" ROCKSDB_PRIszt,
      log_file_time_to_roll);
  ROCKS_LOG_HEADER(
      log, "                      Options.keep_log_file_num: %" ROCKSDB_PRIszt,
      keep_log_file_num);
  ROCKS_LOG_HEADER(
      log, "                   Options.recycle_log_file_num: %" ROCKSDB_PRIszt,
      recycle_log_file_num);
  ROCKS_LOG_HEADER(log, "                        Options.allow_fallocate: %d",
                   allow_fallocate);
  ROCKS_LOG_HEADER(log, "                       Options.allow_mmap_reads: %d",
                   allow_mmap_reads);
  ROCKS_LOG_HEADER(log, "                      Options.allow_mmap_writes: %d",
                   allow_mmap_writes);
  ROCKS_LOG_HEADER(log, "                       Options.use_direct_reads: %d",
                   use_direct_reads);
  ROCKS_LOG_HEADER(log,
                   "                       "
                   "Options.use_direct_io_for_flush_and_compaction: %d",
                   use_direct_io_for_flush_and_compaction);
  ROCKS_LOG_HEADER(log, "         Options.create_missing_column_families: %d",
                   create_missing_column_families);
  ROCKS_LOG_HEADER(log, "                             Options.db_log_dir: %s",
                   db_log_dir.c_str());
  ROCKS_LOG_HEADER(log, "                                Options.wal_dir: %s",
                   wal_dir.c_str());
  ROCKS_LOG_HEADER(log, "               Options.table_cache_numshardbits: %d",
                   table_cache_numshardbits);
  ROCKS_LOG_HEADER(log,
                   "                        Options.WAL_ttl_seconds: %" PRIu64,
                   wal_ttl_seconds);
  ROCKS_LOG_HEADER(log,
                   "                      Options.WAL_size_limit_MB: %" PRIu64,
                   wal_size_limit_mb);
  ROCKS_LOG_HEADER(log,
                   "                       "
                   "Options.max_write_batch_group_size_bytes: %" PRIu64,
                   max_write_batch_group_size_bytes);
  ROCKS_LOG_HEADER(
      log, "            Options.manifest_preallocation_size: %" ROCKSDB_PRIszt,
      manifest_preallocation_size);
  ROCKS_LOG_HEADER(log, "                    Options.is_fd_close_on_exec: %d",
                   is_fd_close_on_exec);
  ROCKS_LOG_HEADER(log, "                  Options.advise_random_on_open: %d",
                   advise_random_on_open);
  ROCKS_LOG_HEADER(
      log, "                   Options.db_write_buffer_size: %" ROCKSDB_PRIszt,
      db_write_buffer_size);
  ROCKS_LOG_HEADER(log, "                   Options.write_buffer_manager: %p",
                   write_buffer_manager.get());
  ROCKS_LOG_HEADER(log, "        Options.access_hint_on_compaction_start: %d",
                   static_cast<int>(access_hint_on_compaction_start));
  ROCKS_LOG_HEADER(log, " Options.new_table_reader_for_compaction_inputs: %d",
                   new_table_reader_for_compaction_inputs);
  ROCKS_LOG_HEADER(
      log, "          Options.random_access_max_buffer_size: %" ROCKSDB_PRIszt,
      random_access_max_buffer_size);
  ROCKS_LOG_HEADER(log, "                     Options.use_adaptive_mutex: %d",
                   use_adaptive_mutex);
  ROCKS_LOG_HEADER(log, "                           Options.rate_limiter: %p",
                   rate_limiter.get());
  Header(
      log, "    Options.sst_file_manager.rate_bytes_per_sec: %" PRIi64,
      sst_file_manager ? sst_file_manager->GetDeleteRateBytesPerSecond() : 0);
  ROCKS_LOG_HEADER(log, "                      Options.wal_recovery_mode: %d",
                   static_cast<int>(wal_recovery_mode));
  ROCKS_LOG_HEADER(log, "                 Options.enable_thread_tracking: %d",
                   enable_thread_tracking);
  ROCKS_LOG_HEADER(log, "                 Options.enable_pipelined_write: %d",
                   enable_pipelined_write);
  ROCKS_LOG_HEADER(log, "                 Options.unordered_write: %d",
                   unordered_write);
  ROCKS_LOG_HEADER(log, "        Options.allow_concurrent_memtable_write: %d",
                   allow_concurrent_memtable_write);
  ROCKS_LOG_HEADER(log, "     Options.enable_write_thread_adaptive_yield: %d",
                   enable_write_thread_adaptive_yield);
  ROCKS_LOG_HEADER(log,
                   "            Options.write_thread_max_yield_usec: %" PRIu64,
                   write_thread_max_yield_usec);
  ROCKS_LOG_HEADER(log,
                   "           Options.write_thread_slow_yield_usec: %" PRIu64,
                   write_thread_slow_yield_usec);
  if (row_cache) {
    ROCKS_LOG_HEADER(
        log,
        "                              Options.row_cache: %" ROCKSDB_PRIszt,
        row_cache->GetCapacity());
  } else {
    ROCKS_LOG_HEADER(log,
                     "                              Options.row_cache: None");
  }
#ifndef ROCKSDB_LITE
  ROCKS_LOG_HEADER(log, "                             Options.wal_filter: %s",
                   wal_filter ? wal_filter->Name() : "None");
#endif  // ROCKDB_LITE

  ROCKS_LOG_HEADER(log, "            Options.avoid_flush_during_recovery: %d",
                   avoid_flush_during_recovery);
  ROCKS_LOG_HEADER(log, "            Options.allow_ingest_behind: %d",
                   allow_ingest_behind);
  ROCKS_LOG_HEADER(log, "            Options.preserve_deletes: %d",
                   preserve_deletes);
  ROCKS_LOG_HEADER(log, "            Options.two_write_queues: %d",
                   two_write_queues);
  ROCKS_LOG_HEADER(log, "            Options.manual_wal_flush: %d",
                   manual_wal_flush);
  ROCKS_LOG_HEADER(log, "            Options.atomic_flush: %d", atomic_flush);
  ROCKS_LOG_HEADER(log,
                   "            Options.avoid_unnecessary_blocking_io: %d",
                   avoid_unnecessary_blocking_io);
  ROCKS_LOG_HEADER(log, "                Options.persist_stats_to_disk: %u",
                   persist_stats_to_disk);
  ROCKS_LOG_HEADER(log, "                Options.write_dbid_to_manifest: %d",
                   write_dbid_to_manifest);
  ROCKS_LOG_HEADER(
      log, "                Options.log_readahead_size: %" ROCKSDB_PRIszt,
      log_readahead_size);
  ROCKS_LOG_HEADER(log, "                Options.file_checksum_gen_factory: %s",
                   file_checksum_gen_factory ? file_checksum_gen_factory->Name()
                                             : kUnknownFileChecksumFuncName);
  ROCKS_LOG_HEADER(log, "                Options.best_efforts_recovery: %d",
                   static_cast<int>(best_efforts_recovery));
  ROCKS_LOG_HEADER(log, "               Options.max_bgerror_resume_count: %d",
                   max_bgerror_resume_count);
  ROCKS_LOG_HEADER(log,
                   "           Options.bgerror_resume_retry_interval: %" PRIu64,
                   bgerror_resume_retry_interval);
}

MutableDBOptions::MutableDBOptions()
    : max_background_jobs(2),
      base_background_compactions(-1),
      max_background_compactions(-1),
      max_subcompactions(0),
      avoid_flush_during_shutdown(false),
      writable_file_max_buffer_size(1024 * 1024),
      delayed_write_rate(2 * 1024U * 1024U),
      max_total_wal_size(0),
      delete_obsolete_files_period_micros(6ULL * 60 * 60 * 1000000),
      stats_dump_period_sec(600),
      stats_persist_period_sec(600),
      stats_history_buffer_size(1024 * 1024),
      max_open_files(-1),
      bytes_per_sync(0),
      wal_bytes_per_sync(0),
      strict_bytes_per_sync(false),
      compaction_readahead_size(0),
      max_background_flushes(-1) {}

MutableDBOptions::MutableDBOptions(const DBOptions& options)
    : max_background_jobs(options.max_background_jobs),
      base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      max_subcompactions(options.max_subcompactions),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown),
      writable_file_max_buffer_size(options.writable_file_max_buffer_size),
      delayed_write_rate(options.delayed_write_rate),
      max_total_wal_size(options.max_total_wal_size),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      stats_dump_period_sec(options.stats_dump_period_sec),
      stats_persist_period_sec(options.stats_persist_period_sec),
      stats_history_buffer_size(options.stats_history_buffer_size),
      max_open_files(options.max_open_files),
      bytes_per_sync(options.bytes_per_sync),
      wal_bytes_per_sync(options.wal_bytes_per_sync),
      strict_bytes_per_sync(options.strict_bytes_per_sync),
      compaction_readahead_size(options.compaction_readahead_size),
      max_background_flushes(options.max_background_flushes) {}

void MutableDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "            Options.max_background_jobs: %d",
                   max_background_jobs);
  ROCKS_LOG_HEADER(log, "            Options.max_background_compactions: %d",
                   max_background_compactions);
  ROCKS_LOG_HEADER(log, "            Options.max_subcompactions: %" PRIu32,
                   max_subcompactions);
  ROCKS_LOG_HEADER(log, "            Options.avoid_flush_during_shutdown: %d",
                   avoid_flush_during_shutdown);
  ROCKS_LOG_HEADER(
      log, "          Options.writable_file_max_buffer_size: %" ROCKSDB_PRIszt,
      writable_file_max_buffer_size);
  ROCKS_LOG_HEADER(log, "            Options.delayed_write_rate : %" PRIu64,
                   delayed_write_rate);
  ROCKS_LOG_HEADER(log, "            Options.max_total_wal_size: %" PRIu64,
                   max_total_wal_size);
  ROCKS_LOG_HEADER(
      log, "            Options.delete_obsolete_files_period_micros: %" PRIu64,
      delete_obsolete_files_period_micros);
  ROCKS_LOG_HEADER(log, "                  Options.stats_dump_period_sec: %u",
                   stats_dump_period_sec);
  ROCKS_LOG_HEADER(log, "                Options.stats_persist_period_sec: %d",
                   stats_persist_period_sec);
  ROCKS_LOG_HEADER(
      log,
      "                Options.stats_history_buffer_size: %" ROCKSDB_PRIszt,
      stats_history_buffer_size);
  ROCKS_LOG_HEADER(log, "                         Options.max_open_files: %d",
                   max_open_files);
  ROCKS_LOG_HEADER(log,
                   "                         Options.bytes_per_sync: %" PRIu64,
                   bytes_per_sync);
  ROCKS_LOG_HEADER(log,
                   "                     Options.wal_bytes_per_sync: %" PRIu64,
                   wal_bytes_per_sync);
  ROCKS_LOG_HEADER(log,
                   "                  Options.strict_bytes_per_sync: %d",
                   strict_bytes_per_sync);
  ROCKS_LOG_HEADER(log,
                   "      Options.compaction_readahead_size: %" ROCKSDB_PRIszt,
                   compaction_readahead_size);
  ROCKS_LOG_HEADER(log, "                 Options.max_background_flushes: %d",
                          max_background_flushes);
}

}  // namespace ROCKSDB_NAMESPACE
