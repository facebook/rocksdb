#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import argparse
import glob
import math
import os
import random
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

per_iteration_random_seed_override = 0
remain_argv = None
is_remote_db = False


def get_random_seed(override):
    if override == 0:
        return random.randint(1, 2**64)
    else:
        return override


def quote_arg_for_display(arg):
    """
    Quote only the value after '=' for shell display.
    This makes the printed command safe to copy/paste into a Unix shell.
    Note: shlex is Unix-focused; Non-Unix shell users may need to adjust quoting after copying.
    """
    if "=" not in arg:
        return arg
    flag, value = arg.split("=", 1)
    return f"{flag}={shlex.quote(value)}"


def early_argument_parsing_before_main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--initial_random_seed_override",
        default=0,
        type=int,
        help="Random seed used for initialize the test parameters at the beginning of stress test run",
    )
    # sometimes the failure appeared after a few iteration, to reproduce the error, we have to wait for the test to run
    # multiple iterations to reach the iteration that fails the test. By overriding the seed used within each iteration,
    # we could skip all the previous iterations.
    parser.add_argument(
        "--per_iteration_random_seed_override",
        default=0,
        type=int,
        help="Random seed used for initialize the test parameters in each iteration of the stress test run",
    )

    global remain_args
    args, remain_args = parser.parse_known_args()
    init_random_seed = get_random_seed(args.initial_random_seed_override)
    global per_iteration_random_seed_override
    per_iteration_random_seed_override = args.per_iteration_random_seed_override
    global is_remote_db
    # Set is_remote_db if remain_args has a non-empty --env_uri= or --fs_uri= argument
    for arg in remain_args:
        parts = arg.split("=", 1)
        if parts[0] in ["--env_uri", "--fs_uri"] and len(parts) > 1 and parts[1]:
            is_remote_db = True
            break

    print(f"Start with random seed {init_random_seed}")
    random.seed(init_random_seed)


def apply_random_seed_per_iteration():
    per_iteration_random_seed = get_random_seed(per_iteration_random_seed_override)
    print(f"Use random seed for iteration {per_iteration_random_seed}")
    random.seed(per_iteration_random_seed)


# Random seed has to be setup before the rest of the script, so that the random
# value selected in the global variable uses the random seed specified. More
# arguments can also be parsed early.
early_argument_parsing_before_main()

# params overwrite priority:
#   for default:
#       default_params < {blackbox,whitebox}_default_params < args
#   for simple:
#       default_params < {blackbox,whitebox}_default_params <
#       simple_default_params <
#       {blackbox,whitebox}_simple_default_params < args
#   for cf_consistency:
#       default_params < {blackbox,whitebox}_default_params <
#       cf_consistency_params < args
#   for txn:
#       default_params < {blackbox,whitebox}_default_params < txn_params < args
#   for ts:
#       default_params < {blackbox,whitebox}_default_params < ts_params < args
#   for multiops_txn:
#       default_params < {blackbox,whitebox}_default_params < multiops_txn_params < args


default_params = {
    "acquire_snapshot_one_in": lambda: random.choice([100, 10000]),
    "backup_max_size": 100 * 1024 * 1024,
    # Consider larger number when backups considered more stable
    "backup_one_in": lambda: random.choice([1000, 100000]),
    "batch_protection_bytes_per_key": lambda: random.choice([0, 8]),
    "memtable_protection_bytes_per_key": lambda: random.choice([0, 1, 2, 4, 8]),
    "block_protection_bytes_per_key": lambda: random.choice([0, 1, 2, 4, 8]),
    "block_size": 16384,
    "bloom_bits": lambda: random.choice(
        [random.randint(0, 19), random.lognormvariate(2.3, 1.3)]
    ),
    "cache_index_and_filter_blocks": lambda: random.randint(0, 1),
    "cache_size": lambda: random.choice([8388608, 33554432]),
    "charge_compression_dictionary_building_buffer": lambda: random.choice([0, 1]),
    "charge_filter_construction": lambda: random.choice([0, 1]),
    "charge_table_reader": lambda: random.choice([0, 1]),
    "charge_file_metadata": lambda: random.choice([0, 1]),
    "checkpoint_one_in": lambda: random.choice([0, 0, 10000, 1000000]),
    "compression_type": lambda: random.choice(
        ["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
    ),
    "bottommost_compression_type": lambda: (
        "disable"
        if random.randint(0, 1) == 0
        else random.choice(["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"])
    ),
    "checksum_type": lambda: random.choice(
        ["kCRC32c", "kxxHash", "kxxHash64", "kXXH3"]
    ),
    "compression_max_dict_bytes": lambda: 16384 * random.randint(0, 1),
    "compression_zstd_max_train_bytes": lambda: 65536 * random.randint(0, 1),
    "compression_parallel_threads": lambda: random.choice([1, 1, 2, 3, 4, 5, 8, 9, 16]),
    "compression_max_dict_buffer_bytes": lambda: (1 << random.randint(0, 40)) - 1,
    "compression_use_zstd_dict_trainer": lambda: random.randint(0, 1),
    "compression_checksum": lambda: random.randint(0, 1),
    "clear_column_family_one_in": 0,
    "compact_files_one_in": lambda: random.choice([1000, 1000000]),
    "compact_range_one_in": lambda: random.choice([1000, 1000000]),
    # Disabled because of various likely related failures with
    # "Cannot delete table file #N from level 0 since it is on level X"
    "promote_l0_one_in": 0,
    "compaction_pri": random.randint(0, 4),
    "key_may_exist_one_in": lambda: random.choice([100, 100000]),
    "data_block_index_type": lambda: random.choice([0, 1]),
    "decouple_partitioned_filters": lambda: random.choice([0, 1, 1]),
    "delpercent": 4,
    "delrangepercent": 1,
    "destroy_db_initially": 0,
    "enable_pipelined_write": lambda: random.randint(0, 1),
    "enable_compaction_filter": lambda: random.choice([0, 0, 0, 1]),
    "enable_compaction_on_deletion_trigger": lambda: random.choice([0, 0, 0, 1]),
    # `inplace_update_support` is incompatible with DB that has delete
    # range data in memtables.
    # Such data can result from any of the previous db stress runs
    # using delete range.
    # Since there is no easy way to keep track of whether delete range
    # is used in any of the previous runs,
    # to simpify our testing, we set `inplace_update_support` across
    # runs and to disable delete range accordingly
    # (see below `finalize_and_sanitize`).
    "inplace_update_support": random.choice([0] * 9 + [1]),
    "expected_values_dir": lambda: setup_expected_values_dir(),
    "flush_one_in": lambda: random.choice([1000, 1000000]),
    "manual_wal_flush_one_in": lambda: random.choice([0, 1000]),
    "sync_wal_one_in": 0,
    "file_checksum_impl": lambda: random.choice(["none", "crc32c", "xxh64", "big"]),
    "get_live_files_apis_one_in": lambda: random.choice([10000, 1000000]),
    "checkpoint_atomic_flush": lambda: random.choice([0, 1]),
    "get_all_column_family_metadata_one_in": lambda: random.choice([10000, 1000000]),
    # Note: the following two are intentionally disabled as the corresponding
    # APIs are not guaranteed to succeed.
    "get_sorted_wal_files_one_in": 0,
    "get_current_wal_file_one_in": 0,
    # Temporarily disable hash index
    "index_type": lambda: random.choice([0, 0, 0, 2, 2, 3]),
    "index_block_search_type": lambda: random.choice([0, 1, 2]),
    "uniform_cv_threshold": lambda: random.choice([-1, 0.2, 1000]),
    "ingest_external_file_one_in": lambda: random.choice([1000, 1000000]),
    "test_ingest_standalone_range_deletion_one_in": lambda: random.choice([0, 5, 10]),
    "iterpercent": 10,
    "lock_wal_one_in": lambda: random.choice([10000, 1000000]),
    "mark_for_compaction_one_file_in": lambda: 10 * random.randint(0, 1),
    "max_background_compactions": lambda: random.choice([2, 20]),
    "num_bottom_pri_threads": lambda: random.choice([0, 1, 20]),
    "max_bytes_for_level_base": 10485760,
    # max_key has to be the same across invocations for verification to work, hence no lambda
    "max_key": random.choice([100000, 25000000]),
    "max_sequential_skip_in_iterations": lambda: random.choice([1, 2, 8, 16]),
    "max_write_buffer_number": 3,
    "mmap_read": lambda: random.choice([0, 0, 1]),
    # Setting `nooverwritepercent > 0` is only possible because we do not vary
    # the random seed, so the same keys are chosen by every run for disallowing
    # overwrites.
    "nooverwritepercent": 1,
    "open_files": lambda: random.choice([-1, -1, 100, 500000]),
    "open_files_async": lambda: random.choice([0, 1]),
    "optimize_filters_for_memory": lambda: random.randint(0, 1),
    "partition_filters": lambda: random.randint(0, 1),
    "partition_pinning": lambda: random.randint(0, 3),
    "rate_limit_auto_wal_flush": 0,
    "reset_stats_one_in": lambda: random.choice([10000, 1000000]),
    "pause_background_one_in": lambda: random.choice([10000, 1000000]),
    "disable_file_deletions_one_in": lambda: random.choice([10000, 1000000]),
    "disable_manual_compaction_one_in": lambda: random.choice([10000, 1000000]),
    "abort_and_resume_compactions_one_in": lambda: random.choice([10000, 1000000]),
    "prefix_size": lambda: random.choice([-1, 1, 5, 7, 8]),
    "prefixpercent": 5,
    "progress_reports": 0,
    "readpercent": 45,
    # See disabled DBWALTest.RecycleMultipleWalsCrash
    "recycle_log_file_num": 0,
    "snapshot_hold_ops": 100000,
    "sqfc_name": lambda: random.choice(["foo", "bar"]),
    # 0 = disable writing SstQueryFilters
    "sqfc_version": lambda: random.choice([0, 1, 1, 2, 2]),
    "sst_file_manager_bytes_per_sec": lambda: random.choice([0, 104857600]),
    "sst_file_manager_bytes_per_truncate": lambda: random.choice([0, 1048576]),
    "long_running_snapshots": lambda: random.randint(0, 1),
    "subcompactions": lambda: random.randint(1, 4),
    "target_file_size_base": lambda: random.choice([512 * 1024, 2048 * 1024]),
    "target_file_size_multiplier": 2,
    "test_batches_snapshots": random.randint(0, 1),
    "top_level_index_pinning": lambda: random.randint(0, 3),
    "unpartitioned_pinning": lambda: random.randint(0, 3),
    "use_direct_reads": lambda: random.randint(0, 1),
    "use_direct_io_for_flush_and_compaction": lambda: random.randint(0, 1),
    "use_sqfc_for_range_queries": lambda: random.choice([0, 1, 1, 1]),
    "mock_direct_io": False,
    "cache_type": lambda: random.choice(
        [
            "lru_cache",
            "fixed_hyper_clock_cache",
            "auto_hyper_clock_cache",
            "auto_hyper_clock_cache",
            "tiered_lru_cache",
            "tiered_fixed_hyper_clock_cache",
            "tiered_auto_hyper_clock_cache",
            "tiered_auto_hyper_clock_cache",
        ]
    ),
    "uncache_aggressiveness": lambda: int(math.pow(10, 4.0 * random.random()) - 1.0),
    "use_full_merge_v1": lambda: random.randint(0, 1),
    "use_merge": lambda: random.randint(0, 1),
    # use_trie_index must be the same across invocations so that all SSTs
    # in a DB are opened with matching table options.
    "use_trie_index": random.choice([0] * 15 + [1]),
    # use_udi_as_primary_index must be the same across invocations (like
    # use_trie_index) so that SSTs written in primary mode can be read on
    # reopen.
    "use_udi_as_primary_index": random.choice([0, 0, 0, 1]),
    # use_put_entity_one_in has to be the same across invocations for verification to work, hence no lambda
    "use_put_entity_one_in": random.choice([0] * 7 + [1, 5, 10]),
    "use_attribute_group": lambda: random.randint(0, 1),
    "use_multi_cf_iterator": lambda: random.randint(0, 1),
    # 999 -> use Bloom API
    "bloom_before_level": lambda: random.choice(
        [random.randint(-1, 2), random.randint(-1, 10), 0x7FFFFFFF - 1, 0x7FFFFFFF]
    ),
    "value_size_mult": 32,
    "verification_only": 0,
    "verify_checksum": 1,
    "write_buffer_size": lambda: random.choice([1024 * 1024, 4 * 1024 * 1024]),
    "writepercent": 35,
    "format_version": lambda: random.choice([2, 3, 4, 5, 6, 7, 7]),
    "separate_key_value_in_data_block": lambda: random.choice([0, 1, 1]),
    "index_block_restart_interval": lambda: random.choice(range(1, 16)),
    "use_multiget": lambda: random.randint(0, 1),
    "use_get_entity": lambda: random.choice([0] * 7 + [1]),
    "use_multi_get_entity": lambda: random.choice([0] * 7 + [1]),
    "periodic_compaction_seconds": lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    "max_compaction_trigger_wakeup_seconds": lambda: random.choice([43200, 600, 30]),
    "read_triggered_compaction_threshold": lambda: random.choice([0.0, 0.001, 0.01]),
    "daily_offpeak_time_utc": lambda: random.choice(
        ["", "", "00:00-23:59", "04:00-08:00", "23:30-03:15"]
    ),
    # 0 = never (used by some), 10 = often (for threading bugs), 600 = default
    "stats_dump_period_sec": lambda: random.choice([0, 10, 600]),
    "compaction_ttl": lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    "fifo_allow_compaction": lambda: random.randint(0, 1),
    # TODO(T260692223): FIFO compaction drops old SST files when total size
    # exceeds the limit, but the stress test expected state can't track these
    # drops. max_table_files_size is always active (default 1GB) so set it
    # very high. max_data_files_size when non-zero overrides max_table_files_size;
    # randomize between 0 (fallback to max_table_files_size) and very high.
    # Long-term, handle drops via OnCompactionBegin + SetPendingDel() as
    # concurrent deletes.
    "fifo_compaction_max_data_files_size_mb": lambda: random.choice(
        [0, 100 * 1024]  # 0 = disabled (defers to max_table_files_size), 100GB
    ),
    "fifo_compaction_max_table_files_size_mb": 100 * 1024,  # 100GB, always high
    "fifo_compaction_use_kv_ratio_compaction": lambda: random.randint(0, 1),
    # Test small max_manifest_file_size in a smaller chance, as most of the
    # time we wnat manifest history to be preserved to help debug
    "max_manifest_file_size": lambda: random.choice(
        [t * 2048 if t < 5 else 1024 * 1024 * 1024 for t in range(1, 30)]
    ),
    "verify_manifest_content_on_close": lambda: random.randint(0, 1),
    "max_manifest_space_amp_pct": lambda: random.choice([0, 10, 100, 1000]),
    # Sync mode might make test runs slower so running it in a smaller chance
    "sync": lambda: random.choice([1 if t == 0 else 0 for t in range(0, 20)]),
    "bytes_per_sync": lambda: random.choice([0, 262144]),
    # TODO(hx235): Enable `wal_bytes_per_sync` after fixing the DB recovery such
    # that it won't recover past the WAL data hole created by this option
    "wal_bytes_per_sync": 0,
    "compaction_readahead_size": lambda: random.choice([0, 0, 1024 * 1024]),
    "db_write_buffer_size": lambda: random.choice(
        [0, 0, 0, 1024 * 1024, 8 * 1024 * 1024, 128 * 1024 * 1024]
    ),
    "use_write_buffer_manager": lambda: random.randint(0, 1),
    "avoid_unnecessary_blocking_io": random.randint(0, 1),
    "write_dbid_to_manifest": random.randint(0, 1),
    "write_identity_file": random.randint(0, 1),
    "avoid_flush_during_recovery": lambda: random.choice(
        [1 if t == 0 else 0 for t in range(0, 8)]
    ),
    "enforce_write_buffer_manager_during_recovery": random.randint(0, 1),
    "max_write_batch_group_size_bytes": lambda: random.choice(
        [16, 64, 1024 * 1024, 16 * 1024 * 1024]
    ),
    "level_compaction_dynamic_level_bytes": lambda: random.randint(0, 1),
    "verify_checksum_one_in": lambda: random.choice([1000, 1000000]),
    "verify_file_checksums_one_in": lambda: random.choice([1000, 1000000]),
    "verify_db_one_in": lambda: random.choice([10000, 100000]),
    "continuous_verification_interval": 0,
    "max_key_len": 3,
    "key_len_percent_dist": "1,30,69",
    "error_recovery_with_no_fault_injection": lambda: random.randint(0, 1),
    "metadata_read_fault_one_in": lambda: random.choice([0, 32, 1000]),
    "metadata_write_fault_one_in": lambda: random.choice([0, 128, 1000]),
    "read_fault_one_in": lambda: random.choice([0, 32, 1000]),
    "write_fault_one_in": lambda: random.choice([0, 128, 1000]),
    "exclude_wal_from_write_fault_injection": 0,
    "open_metadata_write_fault_one_in": lambda: random.choice([0, 0, 8]),
    "open_metadata_read_fault_one_in": lambda: random.choice([0, 0, 8]),
    "open_write_fault_one_in": lambda: random.choice([0, 0, 16]),
    "open_read_fault_one_in": lambda: random.choice([0, 0, 32]),
    "sync_fault_injection": lambda: random.randint(0, 1),
    "get_property_one_in": lambda: random.choice([100000, 1000000]),
    "get_properties_of_all_tables_one_in": lambda: random.choice([100000, 1000000]),
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
    "max_write_buffer_size_to_maintain": lambda: random.choice(
        [0, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024]
    ),
    "user_timestamp_size": 0,
    "secondary_cache_fault_one_in": lambda: random.choice([0, 0, 32]),
    "compressed_secondary_cache_size": lambda: random.choice([8388608, 16777216]),
    "prepopulate_block_cache": lambda: random.choice([0, 1, 2]),
    "memtable_prefix_bloom_size_ratio": lambda: random.choice([0.001, 0.01, 0.1, 0.5]),
    "memtable_whole_key_filtering": lambda: random.randint(0, 1),
    "detect_filter_construct_corruption": lambda: random.choice([0, 1]),
    "adaptive_readahead": lambda: random.choice([0, 1]),
    "async_io": lambda: random.choice([0, 1]),
    "wal_compression": lambda: random.choice(["none", "zstd"]),
    "verify_sst_unique_id_in_manifest": 1,  # always do unique_id verification
    "secondary_cache_uri": lambda: random.choice(
        [
            "",
            "",
            "",
            "compressed_secondary_cache://capacity=8388608;enable_custom_split_merge=true",
        ]
    ),
    "allow_data_in_errors": True,
    "enable_thread_tracking": lambda: random.choice([0, 1]),
    "readahead_size": lambda: random.choice([0, 16384, 524288]),
    "initial_auto_readahead_size": lambda: random.choice([0, 16384, 524288]),
    "max_auto_readahead_size": lambda: random.choice([0, 16384, 524288]),
    "num_file_reads_for_auto_readahead": lambda: random.choice([0, 1, 2]),
    "min_write_buffer_number_to_merge": lambda: random.choice([1, 2]),
    "preserve_internal_time_seconds": lambda: random.choice([0, 60, 3600, 36000]),
    "memtable_max_range_deletions": lambda: random.choice([0] * 6 + [100, 1000]),
    # 0 (disable) is the default and more commonly used value.
    "bottommost_file_compaction_delay": lambda: random.choice(
        [0, 0, 0, 600, 3600, 86400]
    ),
    "auto_readahead_size": lambda: random.choice([0, 1]),
    "verify_iterator_with_expected_state_one_in": 5,
    "allow_fallocate": lambda: random.choice([0, 1]),
    "table_cache_numshardbits": lambda: random.choice([6] * 3 + [-1] * 2 + [0]),
    "enable_write_thread_adaptive_yield": lambda: random.choice([0, 1]),
    "log_readahead_size": lambda: random.choice([0, 16 * 1024 * 1024]),
    "bgerror_resume_retry_interval": lambda: random.choice([100, 1000000]),
    "delete_obsolete_files_period_micros": lambda: random.choice(
        [6 * 60 * 60 * 1000000, 30 * 1000000]
    ),
    "max_log_file_size": lambda: random.choice([0, 1024 * 1024]),
    "log_file_time_to_roll": lambda: random.choice([0, 60]),
    "use_adaptive_mutex": lambda: random.choice([0, 1]),
    "advise_random_on_open": lambda: random.choice([0] + [1] * 3),
    "WAL_ttl_seconds": lambda: random.choice([0, 60]),
    "WAL_size_limit_MB": lambda: random.choice([0, 1]),
    "strict_bytes_per_sync": lambda: random.choice([0, 1]),
    "avoid_flush_during_shutdown": lambda: random.choice([0, 1]),
    "fill_cache": lambda: random.choice([0, 1]),
    "optimize_multiget_for_io": lambda: random.choice([0, 1]),
    "memtable_insert_hint_per_batch": lambda: random.choice([0, 1]),
    "dump_malloc_stats": lambda: random.choice([0, 1]),
    "stats_history_buffer_size": lambda: random.choice([0, 1024 * 1024]),
    "skip_stats_update_on_db_open": lambda: random.choice([0, 1]),
    "optimize_filters_for_hits": lambda: random.choice([0, 1]),
    "sample_for_compression": lambda: random.choice([0, 5]),
    "report_bg_io_stats": lambda: random.choice([0, 1]),
    "cache_index_and_filter_blocks_with_high_priority": lambda: random.choice([0, 1]),
    "use_delta_encoding": lambda: random.choice([0, 1]),
    "verify_compression": lambda: random.choice([0, 1]),
    "read_amp_bytes_per_bit": lambda: random.choice([0, 32]),
    "enable_index_compression": lambda: random.choice([0, 1]),
    "index_shortening": lambda: random.choice([0, 1, 2]),
    "metadata_charge_policy": lambda: random.choice([0, 1]),
    "use_adaptive_mutex_lru": lambda: random.choice([0, 1]),
    "manifest_preallocation_size": lambda: random.choice([0, 5 * 1024]),
    "enable_checksum_handoff": lambda: random.choice([0, 1]),
    "max_total_wal_size": lambda: random.choice([0] * 4 + [64 * 1024 * 1024]),
    "high_pri_pool_ratio": lambda: random.choice([0, 0.5]),
    "low_pri_pool_ratio": lambda: random.choice([0, 0.5]),
    "soft_pending_compaction_bytes_limit": lambda: random.choice(
        [1024 * 1024] + [64 * 1073741824] * 4
    ),
    "hard_pending_compaction_bytes_limit": lambda: random.choice(
        [2 * 1024 * 1024] + [256 * 1073741824] * 4
    ),
    "enable_sst_partitioner_factory": lambda: random.choice([0, 1]),
    "enable_do_not_compress_roles": lambda: random.choice([0, 1]),
    "block_align": lambda: random.choice([0, 1]),
    "super_block_alignment_size": lambda: random.choice(
        [0, 128 * 1024, 512 * 1024, 2 * 1024 * 1024]
    ),
    "super_block_alignment_space_overhead_ratio": lambda: random.choice([0, 32, 4096]),
    "lowest_used_cache_tier": lambda: random.choice([0, 1, 2]),
    "enable_custom_split_merge": lambda: random.choice([0, 1]),
    "adm_policy": lambda: random.choice([0, 1, 2, 3]),
    "last_level_temperature": lambda: random.choice(
        ["kUnknown", "kHot", "kWarm", "kCool", "kCold", "kIce"]
    ),
    "default_write_temperature": lambda: random.choice(
        ["kUnknown", "kHot", "kWarm", "kCool", "kCold", "kIce"]
    ),
    "default_temperature": lambda: random.choice(
        ["kUnknown", "kHot", "kWarm", "kCool", "kCold", "kIce"]
    ),
    # TODO(hx235): enable `enable_memtable_insert_with_hint_prefix_extractor`
    # after fixing the surfaced issue with delete range
    "enable_memtable_insert_with_hint_prefix_extractor": 0,
    "check_multiget_consistency": lambda: random.choice([0, 0, 0, 1]),
    "check_multiget_entity_consistency": lambda: random.choice([0, 0, 0, 1]),
    "use_timed_put_one_in": lambda: random.choice([0] * 7 + [1, 5, 10]),
    "universal_max_read_amp": lambda: random.choice([-1] * 3 + [0, 4, 10]),
    # verify_output_flags is a bitmask: bits 0-2 are verification types
    # (block checksum, iteration, file checksum), bits 10-11 are when to
    # enable (local compaction, remote compaction). 0x407 = all types +
    # local, 0xC07 = all types + local + remote, 0xFFFFFFFF = all.
    "verify_output_flags": lambda: random.choice([0] * 3 + [0x407, 0xC07, 0xFFFFFFFF]),
    "paranoid_memory_checks": lambda: random.choice([0] * 7 + [1]),
    "memtable_veirfy_per_key_checksum_on_seek": lambda: random.choice([0] * 7 + [1]),
    "memtable_batch_lookup_optimization": lambda: random.randint(0, 1),
    "allow_unprepared_value": lambda: random.choice([0, 1]),
    # TODO(hx235): enable `track_and_verify_wals` after stabalizing the stress test
    "track_and_verify_wals": lambda: random.choice([0]),
    "remote_compaction_worker_threads": lambda: random.choice([0, 8]),
    "allow_resumption_one_in": lambda: random.choice([0, 1, 2, 20]),
    # TODO(jaykorean): Change to lambda: random.choice([0, 1]) after addressing all remote compaction failures
    "remote_compaction_failure_fall_back_to_local": 1,
    "auto_refresh_iterator_with_snapshot": lambda: random.choice([0, 1]),
    "memtable_op_scan_flush_trigger": lambda: random.choice([0, 10, 100, 1000]),
    "memtable_avg_op_scan_flush_trigger": lambda: random.choice([0, 2, 20, 200]),
    "min_tombstones_for_range_conversion": lambda: random.choice([0, 2, 2, 4, 16]),
    "ingest_wbwi_one_in": lambda: random.choice([0, 0, 100, 500]),
    "universal_reduce_file_locking": lambda: random.randint(0, 1),
    "compression_manager": lambda: random.choice(
        ["mixed"] * 1
        + ["none"] * 2
        + ["autoskip"] * 2
        + ["randommixed"] * 2
        + ["custom"] * 3
    ),
    # fixed within a run for easier debugging
    # actual frequency is lower after option sanitization
    "use_multiscan": random.choice([1] + [0] * 3),
    # By default, `statistics` use kExceptDetailedTimers level
    "statistics": random.choice([0, 1]),
    # TODO: re-enable after resolving "Req failed: Unknown error -14" errors
    "multiscan_use_async_io": 0,  # random.randint(0, 1),
}

_TEST_DIR_ENV_VAR = "TEST_TMPDIR"
# If TEST_TMPDIR_EXPECTED is not specified, default value will be TEST_TMPDIR
# except on remote filesystem
_TEST_EXPECTED_DIR_ENV_VAR = "TEST_TMPDIR_EXPECTED"
_DEBUG_LEVEL_ENV_VAR = "DEBUG_LEVEL"

stress_cmd = "./db_stress"


def is_release_mode():
    return os.environ.get(_DEBUG_LEVEL_ENV_VAR) == "0"


def get_dbname(test_name):
    test_dir_name = "rocksdb_crashtest_" + test_name
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        dbname = tempfile.mkdtemp(prefix=test_dir_name)
    else:
        dbname = test_tmpdir + "/" + test_dir_name
        if not is_remote_db:
            os.makedirs(dbname, exist_ok=True)
    return dbname


expected_values_dir = None


def setup_expected_values_dir():
    global expected_values_dir
    if expected_values_dir is not None:
        return expected_values_dir
    expected_dir_prefix = "rocksdb_crashtest_expected_"
    test_exp_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)

    if not is_remote_db and (test_exp_tmpdir is None or test_exp_tmpdir == ""):
        test_exp_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)

    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        expected_values_dir = tempfile.mkdtemp(prefix=expected_dir_prefix)
    else:
        # if tmpdir is specified, store the expected_values_dir under that dir
        expected_values_dir = test_exp_tmpdir + "/rocksdb_crashtest_expected"
        os.makedirs(expected_values_dir, exist_ok=True)
    return expected_values_dir


def prepare_expected_values_dir(expected_dir, destroy_db_initially):
    if expected_dir is None or expected_dir == "":
        return

    if destroy_db_initially and os.path.exists(expected_dir):
        shutil.rmtree(expected_dir, True)

    os.makedirs(expected_dir, exist_ok=True)


multiops_txn_key_spaces_file = None


def setup_multiops_txn_key_spaces_file():
    global multiops_txn_key_spaces_file
    if multiops_txn_key_spaces_file is not None:
        return multiops_txn_key_spaces_file
    key_spaces_file_prefix = "rocksdb_crashtest_multiops_txn_key_spaces"
    test_exp_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)

    if not is_remote_db and (test_exp_tmpdir is None or test_exp_tmpdir == ""):
        test_exp_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)

    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        multiops_txn_key_spaces_file = tempfile.mkstemp(prefix=key_spaces_file_prefix)[
            1
        ]
    else:
        if not os.path.exists(test_exp_tmpdir):
            os.mkdir(test_exp_tmpdir)
        multiops_txn_key_spaces_file = tempfile.mkstemp(
            prefix=key_spaces_file_prefix, dir=test_exp_tmpdir
        )[1]
    return multiops_txn_key_spaces_file


def is_direct_io_supported(dbname):
    if is_remote_db:
        return False
    else:
        # Note: db dir might be removed on check_mode change. Re-create it
        os.makedirs(dbname, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=dbname) as f:
            try:
                os.open(f.name, os.O_DIRECT)
            except BaseException:
                return False
            return True


blackbox_default_params = {
    "disable_wal": lambda: random.choice([0, 0, 0, 1]),
    # total time for this script to test db_stress
    "duration": 6000,
    # time for one db_stress instance to run
    "interval": 120,
    # time for the final verification step
    "verify_timeout": 1200,
    # since we will be killing anyway, use large value for ops_per_thread
    "ops_per_thread": 100000000,
    "reopen": 0,
    "set_options_one_in": 1000,
}

whitebox_default_params = {
    # TODO: enable this at random once we figure out two things. First, we need
    # to ensure the kill odds in WAL-disabled runs result in regular crashing
    # before the fifteen minute timeout. When WAL is disabled there are very few
    # calls to write functions since writes to SST files are buffered and other
    # writes (e.g., MANIFEST) are infrequent. Crashing in reasonable time might
    # currently assume killpoints in write functions are reached frequently.
    #
    # Second, we need to make sure disabling WAL works with `-reopen > 0`.
    "disable_wal": 0,
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
    "duration": 10000,
    "log2_keys_per_lock": 10,
    "ops_per_thread": 200000,
    "random_kill_odd": 888887,
    "reopen": 20,
}

simple_default_params = {
    "allow_concurrent_memtable_write": lambda: random.randint(0, 1),
    "column_families": 1,
    # TODO: re-enable once internal task T124324915 is fixed.
    # "experimental_mempurge_threshold": lambda: 10.0*random.random(),
    "max_background_compactions": 1,
    "max_bytes_for_level_base": 67108864,
    "memtablerep": "skip_list",
    "target_file_size_base": 16777216,
    "target_file_size_multiplier": 1,
    "test_batches_snapshots": 0,
    "write_buffer_size": 32 * 1024 * 1024,
    "level_compaction_dynamic_level_bytes": lambda: random.randint(0, 1),
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
    "test_secondary": lambda: random.choice([0, 1]),
}

blackbox_simple_default_params = {
    "open_files": -1,
    "set_options_one_in": 0,
}

whitebox_simple_default_params = {}

cf_consistency_params = {
    "disable_wal": lambda: random.randint(0, 1),
    "reopen": 0,
    "test_cf_consistency": 1,
    # use small value for write_buffer_size so that RocksDB triggers flush
    # more frequently
    "write_buffer_size": 1024 * 1024,
    "enable_pipelined_write": lambda: random.randint(0, 1),
    # Snapshots are used heavily in this test mode, while they are incompatible
    # with compaction filter, inplace_update_support
    "enable_compaction_filter": 0,
    "inplace_update_support": 0,
    # `CfConsistencyStressTest::TestIngestExternalFile()` is not implemented.
    "ingest_external_file_one_in": 0,
    # `CfConsistencyStressTest::TestIterateAgainstExpected()` is not implemented.
    "verify_iterator_with_expected_state_one_in": 0,
    "memtablerep": random.choice(["skip_list"] * 9 + ["vector"]),
}

# For pessimistic transaction db
txn_params = {
    "use_txn": 1,
    "use_optimistic_txn": 0,
    # Avoid lambda to set it once for the entire test
    # NOTE: often passed in from command line overriding this
    "txn_write_policy": random.randint(0, 2),
    "unordered_write": random.randint(0, 1),
    "use_per_key_point_lock_mgr": lambda: random.choice([0, 1]),
    # TODO: there is such a thing as transactions with WAL disabled. We should
    # cover that case.
    "disable_wal": 0,
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
    # OpenReadOnly after checkpoint is not currnetly compatible with WritePrepared txns
    "checkpoint_one_in": 0,
    # pipeline write is not currnetly compatible with WritePrepared txns
    "enable_pipelined_write": 0,
    "create_timestamped_snapshot_one_in": random.choice([0, 20]),
    # Should not be used with TransactionDB which uses snapshot.
    "inplace_update_support": 0,
    # TimedPut is not supported in transaction
    "use_timed_put_one_in": 0,
    # txn commit with this option will create a new memtable, keep the
    # frequency low to reduce stalls
    "commit_bypass_memtable_one_in": random.choice([0] * 2 + [500, 1000]),
    "two_write_queues": lambda: random.choice([0, 1]),
}

# For optimistic transaction db
optimistic_txn_params = {
    "use_txn": 1,
    "use_optimistic_txn": 1,
    "occ_validation_policy": random.randint(0, 1),
    "share_occ_lock_buckets": random.randint(0, 1),
    "occ_lock_bucket_count": lambda: random.choice([10, 100, 500]),
    # Should not be used with OptimisticTransactionDB which uses snapshot.
    "inplace_update_support": 0,
    # TimedPut is not supported in transaction
    "use_timed_put_one_in": 0,
}

best_efforts_recovery_params = {
    "best_efforts_recovery": 1,
    "disable_wal": 1,
    "column_families": 1,
    "skip_verifydb": 1,
    "verify_db_one_in": 0,
}

blob_params = {
    "allow_setting_blob_options_dynamically": 1,
    # Enable blob files and GC with a 75% chance initially; note that they might still be
    # enabled/disabled during the test via SetOptions
    "enable_blob_files": lambda: random.choice([0] + [1] * 3),
    "min_blob_size": lambda: random.choice([0, 8, 16]),
    "blob_file_size": lambda: random.choice([1048576, 16777216, 268435456, 1073741824]),
    "blob_compression_type": lambda: random.choice(["none", "snappy", "lz4", "zstd"]),
    "enable_blob_garbage_collection": lambda: random.choice([0] + [1] * 3),
    "blob_garbage_collection_age_cutoff": lambda: random.choice(
        [0.0, 0.25, 0.5, 0.75, 1.0]
    ),
    "blob_garbage_collection_force_threshold": lambda: random.choice([0.5, 0.75, 1.0]),
    "blob_compaction_readahead_size": lambda: random.choice([0, 1048576, 4194304]),
    "blob_file_starting_level": lambda: random.choice(
        [0] * 4 + [1] * 3 + [2] * 2 + [3]
    ),
    "use_blob_cache": lambda: random.randint(0, 1),
    "use_shared_block_and_blob_cache": lambda: random.randint(0, 1),
    "blob_cache_size": lambda: random.choice([1048576, 2097152, 4194304, 8388608]),
    "prepopulate_blob_cache": lambda: random.randint(0, 1),
    # TODO Fix races when both Remote Compaction + BlobDB enabled
    "remote_compaction_worker_threads": 0,
}

blob_direct_write_params = {
    "enable_blob_files": 1,
    "enable_blob_direct_write": 1,
    "blob_direct_write_partitions": lambda: random.choice([1, 2, 4, 8]),
    "allow_setting_blob_options_dynamically": 0,
    # Keep the fixed-across-runs write mode within the reduced WAL-disabled
    # direct-write profile.
    "inplace_update_support": 0,
    "min_blob_size": lambda: random.choice([8, 16, 64]),
    "blob_file_size": lambda: random.choice([1048576, 16777216, 268435456]),
    "blob_compression_type": lambda: random.choice(["none", "snappy", "lz4", "zstd"]),
    "enable_blob_garbage_collection": 0,
    "blob_garbage_collection_age_cutoff": 0.0,
    "blob_garbage_collection_force_threshold": 1.0,
    "blob_compaction_readahead_size": 0,
    "blob_file_starting_level": 0,
    "use_blob_cache": lambda: random.randint(0, 1),
    "use_shared_block_and_blob_cache": lambda: random.randint(0, 1),
    "blob_cache_size": lambda: random.choice([1048576, 2097152, 4194304, 8388608]),
    "prepopulate_blob_cache": lambda: random.randint(0, 1),
    "remote_compaction_worker_threads": 0,
}

# Wide-column entity stress needs `use_put_entity_one_in` fixed across the
# repeated db_stress invocations in one crash-test run series, so keep it as a
# once-per-process random choice rather than a lambda.
blob_direct_write_get_entity_params = dict(blob_direct_write_params)
blob_direct_write_get_entity_params.update(
    {
        "use_put_entity_one_in": random.choice([1, 5, 10]),
        "use_get_entity": 1,
        "use_multi_get_entity": 0,
        "use_attribute_group": 0,
    }
)

blob_direct_write_multi_get_entity_params = dict(blob_direct_write_params)
blob_direct_write_multi_get_entity_params.update(
    {
        "use_put_entity_one_in": random.choice([1, 5, 10]),
        "use_get_entity": 0,
        "use_multi_get_entity": 1,
        "use_attribute_group": 0,
    }
)

ts_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "user_timestamp_size": 8,
    # Below flag is randomly picked once and kept consistent in following runs.
    "persist_user_defined_timestamps": random.choice([0, 1, 1]),
    "use_merge": 0,
    # Causing failures and not yet compatible
    "use_multiscan": 0,
    "use_full_merge_v1": 0,
    "use_txn": 0,
    "ingest_external_file_one_in": 0,
    # PutEntity with timestamps is not yet implemented
    "use_put_entity_one_in": 0,
    # TimedPut is not compatible with user-defined timestamps yet.
    "use_timed_put_one_in": 0,
    # TrieIndexFactory requires plain BytewiseComparator, but timestamps use
    # BytewiseComparator.u64ts.
    "use_trie_index": 0,
    # when test_best_efforts_recovery == true, disable_wal becomes 0.
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
}

tiered_params = {
    # For Leveled/Universal compaction (ignored for FIFO)
    # Bias toward times that can elapse during a crash test run series
    # NOTE: -1 means starting disabled but dynamically changing
    "preclude_last_level_data_seconds": lambda: random.choice(
        [-1, -1, 10, 60, 1200, 86400]
    ),
    "last_level_temperature": lambda: random.choice(["kCold", "kIce"]),
    # For FIFO compaction (ignored otherwise)
    "file_temperature_age_thresholds": lambda: random.choice(
        [
            "{{temperature=kWarm;age=10}:{temperature=kCool;age=30}:{temperature=kCold;age=100}:{temperature=kIce;age=300}}",
            "{{temperature=kWarm;age=30}:{temperature=kCold;age=300}}",
            "{{temperature=kCold;age=100}}",
        ]
    ),
    "allow_trivial_copy_when_change_temperature": lambda: random.choice([0, 1]),
    # tiered storage doesn't support blob db yet
    "enable_blob_files": 0,
    "use_blob_db": 0,
    "default_write_temperature": lambda: random.choice(["kUnknown", "kHot", "kWarm"]),
}

multiops_txn_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "test_multi_ops_txns": 1,
    "use_txn": 1,
    # Avoid lambda to set it once for the entire test
    # NOTE: often passed in from command line overriding this
    "txn_write_policy": random.randint(0, 2),
    "two_write_queues": lambda: random.choice([0, 1]),
    # TODO: enable write-prepared
    "disable_wal": 0,
    # TODO: Re-enable this once we fix WAL + Remote Compaction in Stress Test
    "remote_compaction_worker_threads": 0,
    "use_only_the_last_commit_time_batch_for_recovery": lambda: random.choice([0, 1]),
    "clear_column_family_one_in": 0,
    "column_families": 1,
    # TODO re-enable pipelined write (lambda: random.choice([0, 1]))
    "enable_pipelined_write": 0,
    # This test already acquires snapshots in reads
    "acquire_snapshot_one_in": 0,
    "backup_one_in": 0,
    "writepercent": 0,
    "delpercent": 0,
    "delrangepercent": 0,
    "customopspercent": 80,
    "readpercent": 5,
    "iterpercent": 15,
    "prefixpercent": 0,
    "verify_db_one_in": 1000,
    "continuous_verification_interval": 1000,
    "delay_snapshot_read_one_in": 3,
    # 65536 is the smallest possible value for write_buffer_size. Smaller
    # values will be sanitized to 65536 during db open. SetOptions currently
    # does not sanitize options, but very small write_buffer_size may cause
    # assertion failure in
    # https://github.com/facebook/rocksdb/blob/7.0.fb/db/memtable.cc#L117.
    "write_buffer_size": 65536,
    # flush more frequently to generate more files, thus trigger more
    # compactions.
    "flush_one_in": 1000,
    "key_spaces_path": setup_multiops_txn_key_spaces_file(),
    "rollback_one_in": 4,
    # Re-enable once we have a compaction for MultiOpsTxnStressTest
    "enable_compaction_filter": 0,
    "create_timestamped_snapshot_one_in": 50,
    "sync_fault_injection": 0,
    "metadata_write_fault_one_in": 0,
    "manual_wal_flush_one_in": 0,
    # This test has aggressive flush frequency and small write buffer size.
    # Disabling write fault to avoid writes being stopped.
    "write_fault_one_in": 0,
    "metadata_write_fault_one_in": 0,
    # PutEntity in transactions is not yet implemented
    "use_put_entity_one_in": 0,
    "use_get_entity": 0,
    "use_multi_get_entity": 0,
    # `MultiOpsTxnsStressTest::TestIterateAgainstExpected()` is not implemented.
    "verify_iterator_with_expected_state_one_in": 0,
    # This test uses snapshot heavily which is incompatible with this option.
    "inplace_update_support": 0,
    # TimedPut not supported in transaction
    "use_timed_put_one_in": 0,
    # AttributeGroup not yet supported
    "use_attribute_group": 0,
    "commit_bypass_memtable_one_in": random.choice([0] * 4 + [100]),
}


# =============================================================================
# Feature Requirements System
# =============================================================================
#
# Each feature declares what parameter values it *needs* to function correctly.
# The engine detects conflicts automatically by finding features whose
# requirements contradict each other on the same parameter.
#
# When a conflict is detected, resolution depends on provenance:
#   - Two explicit flags that demand incompatible states: exit(1) with error
#   - Explicit vs random/default: explicit flag wins; random feature/rule yields
#   - Both random: deterministic per-pair tiebreak disables one feature
#
# Adding a new incompatibility:
#   1. Add the new feature's requirements to FEATURE_REQUIREMENTS
#   2. Conflicts with all existing features are detected automatically
#
# Cross-reference: C++ hard checks in db_stress_tool.cc and
# multi_ops_txns_stress.cc enforce these at runtime with exit(1).
# The Python rules here prevent those assertions from ever firing.
# =============================================================================


def _is_udt_memtable_only(params):
    return (
        params.get("user_timestamp_size", 0) > 0
        and params.get("persist_user_defined_timestamps") == 0
    )


# FEATURE_REQUIREMENTS maps feature names to their required parameter values.
#
# Format:
#   "feature_name": {
#       "active_when": lambda p: <condition that means this feature is on>,
#       "disable_self": lambda p: <dict of params that turn this feature off>,
#       "requires": {
#           "param_name": value,          # static requirement
#           "param_name": lambda p: value # dynamic requirement (depends on other params)
#       },
#       "comment": "why these requirements exist",
#   }
#
# "active_when" must be the feature's activation condition.
# "disable_self" is called when the engine needs to turn this feature off
#   (either because it lost a conflict or has prerequisites unmet).
# "requires" is the set of params this feature needs.
#
# ---- Maintenance Guide: How to add a new feature requirement ----
#
# Use FEATURE_REQUIREMENTS when two features are *mutually incompatible* on a
# shared parameter (feature A requires param=X, feature B requires param=Y).
# The engine auto-detects these conflicts — you just declare requirements.
#
# Use CONSEQUENCE_RULES (below) instead when:
#   - One feature forces a downstream consequence (one-way, not mutual)
#   - The constraint involves numeric ranges or non-binary relationships
#   - A condition affects params that no single feature "owns"
#
# Conflict resolution uses explicit_keys (params set via --extra_flags):
#   - explicit + random   -> explicit wins, random feature/rule disabled
#   - explicit + explicit -> exit(1), user must fix their flags
#   - random + random    -> deterministic per-pair tiebreak disables one feature
#
# Worked example — adding a hypothetical "my_feature":
#
#   "my_feature": {
#       # Active when the param is turned on (adjust to your activation logic)
#       "active_when": lambda p: p.get("my_feature_enabled") == 1,
#       # How to turn this feature off when it loses a conflict
#       "disable_self": lambda p: {"my_feature_enabled": 0},
#       # What this feature needs from other params to work correctly
#       "requires": {
#           "foo": 1,   # my_feature needs foo=1
#           "bar": 0,   # my_feature needs bar=0
#       },
#       "comment": "my_feature relies on foo and is incompatible with bar. "
#                  "C++ check: db_stress_tool.cc asserts ...",
#   }
#
# That's it. If any existing feature also requires "foo" or "bar" with a
# different value, the engine will detect and resolve the conflict
# automatically. No need to touch engine code.
#
# Tests: run `python3 tools/test_db_crashtest.py` to verify your new rule.
# ---- End maintenance guide ----

FEATURE_REQUIREMENTS = {
    "best_efforts_recovery": {
        "active_when": lambda p: p.get("best_efforts_recovery") == 1,
        "disable_self": lambda p: {"best_efforts_recovery": 0},
        "requires": {
            "disable_wal": 1,
            "inplace_update_support": 0,
            "enable_blob_files": 0,
            "enable_blob_garbage_collection": 0,
            "allow_setting_blob_options_dynamically": 0,
            "enable_blob_direct_write": 0,
            "enable_compaction_filter": 0,
            "sync": 0,
            "write_fault_one_in": 0,
            "skip_verifydb": 1,
            "verify_db_one_in": 0,
        },
        "comment": "BER disables WAL and tests unsynced data loss. BlobDB and "
                   "blob direct write are also incompatible. Atomic flush "
                   "normally follows from disable_wal, except for the "
                   "UDT memtable-only BER test profile. "
                   "C++ check: db_stress_tool.cc asserts skip_verifydb && disable_wal",
    },

    "inplace_update_support": {
        "active_when": lambda p: p.get("inplace_update_support") == 1,
        "disable_self": lambda p: {"inplace_update_support": 0},
        "requires": {
            "disable_wal": 0,
            "allow_concurrent_memtable_write": 0,
            "sync_fault_injection": 0,
            "manual_wal_flush_one_in": 0,
            "memtable_veirfy_per_key_checksum_on_seek": 0,
        },
        "comment": "inplace_update_support does not update sequence numbers, "
                   "so WAL recovery logic breaks. Requires exclusive memtable access.",
    },

    "udt_memtable_only": {
        "active_when": _is_udt_memtable_only,
        # Disabling means re-enabling persistence
        "disable_self": lambda p: {"persist_user_defined_timestamps": 1},
        "requires": {
            "atomic_flush": 0,
            "allow_concurrent_memtable_write": 0,
            "enable_blob_files": 0,
            "allow_setting_blob_options_dynamically": 0,
            "block_protection_bytes_per_key": 0,
            "use_multiget": 0,
            "use_multi_get_entity": 0,
        },
        "comment": "UDT in memtable only mode: timestamps can be collapsed, "
                   "incompatible with concurrent writes. It also forces a "
                   "special WAL/atomic_flush profile outside BER tests.",
    },

    "remote_compaction": {
        "active_when": lambda p: p.get("remote_compaction_worker_threads", 0) > 0,
        "disable_self": lambda p: {"remote_compaction_worker_threads": 0},
        "requires": {
            "enable_blob_files": 0,
            "enable_blob_garbage_collection": 0,
            "allow_setting_blob_options_dynamically": 0,
            "inplace_update_support": 0,
            "checkpoint_one_in": 0,
            "use_timed_put_one_in": 0,
            "test_secondary": 0,
            "mmap_read": 0,
            "open_metadata_write_fault_one_in": 0,
            "open_metadata_read_fault_one_in": 0,
            "open_write_fault_one_in": 0,
            "open_read_fault_one_in": 0,
            "sync_fault_injection": 0,
        },
        "comment": "Remote compaction incompatible with BlobDB, mmap, fault injection during open. "
                   "C++ check: db_stress_test_base.cc asserts no blob + remote compaction",
    },

    "blob_direct_write": {
        "active_when": lambda p: p.get("enable_blob_direct_write", 0) == 1,
        "disable_self": lambda p: {"enable_blob_direct_write": 0},
        "requires": {
            "enable_blob_files": 1,
            "blob_direct_write_partitions": lambda p: max(
                1, p.get("blob_direct_write_partitions", 1)
            ),
            "allow_concurrent_memtable_write": 0,
            "enable_pipelined_write": 0,
            "two_write_queues": 0,
            "unordered_write": 0,
            "inplace_update_support": 0,
            "use_blob_db": 0,
            "allow_setting_blob_options_dynamically": 0,
            "enable_blob_garbage_collection": 0,
            "blob_garbage_collection_age_cutoff": 0.0,
            "blob_garbage_collection_force_threshold": 1.0,
            "blob_compaction_readahead_size": 0,
            "blob_file_starting_level": 0,
            "use_merge": 0,
            "use_full_merge_v1": 0,
            "use_timed_put_one_in": 0,
            "use_attribute_group": 0,
            "user_timestamp_size": 0,
            "persist_user_defined_timestamps": 0,
            "create_timestamped_snapshot_one_in": 0,
            "use_txn": 0,
            "txn_write_policy": 0,
            "use_optimistic_txn": 0,
            "test_multi_ops_txns": 0,
            "commit_bypass_memtable_one_in": 0,
            "disable_wal": 1,
            "best_efforts_recovery": 0,
            "reopen": 0,
            "manual_wal_flush_one_in": 0,
            "sync_wal_one_in": 0,
            "lock_wal_one_in": 0,
            "get_sorted_wal_files_one_in": 0,
            "get_current_wal_file_one_in": 0,
            "track_and_verify_wals": 0,
            "rate_limit_auto_wal_flush": 0,
            "recycle_log_file_num": 0,
            "sync_fault_injection": 0,
            "write_fault_one_in": 0,
            "metadata_write_fault_one_in": 0,
            "read_fault_one_in": 0,
            "metadata_read_fault_one_in": 0,
            "open_metadata_write_fault_one_in": 0,
            "open_metadata_read_fault_one_in": 0,
            "open_write_fault_one_in": 0,
            "open_read_fault_one_in": 0,
            "remote_compaction_worker_threads": 0,
            "test_secondary": 0,
            "backup_one_in": 0,
            "checkpoint_one_in": 0,
            "get_live_files_apis_one_in": 0,
            "ingest_external_file_one_in": 0,
            "ingest_wbwi_one_in": 0,
        },
        "comment": "Blob direct write keeps a reduced WAL-disabled crash-test "
                   "profile. PutEntity/GetEntity/MultiGetEntity are allowed, "
                   "but AttributeGroup and broader WAL/recovery modes stay off.",
    },

    "mmap_read": {
        "active_when": lambda p: p.get("mmap_read") == 1,
        "disable_self": lambda p: {"mmap_read": 0},
        "requires": {
            "use_direct_io_for_flush_and_compaction": 0,
            "use_direct_reads": 0,
            "multiscan_use_async_io": 0,
        },
        "comment": "mmap and direct IO are mutually exclusive",
    },

    "txn_non_write_committed": {
        "active_when": lambda p: (
            p.get("use_txn") == 1 and p.get("txn_write_policy", 0) != 0
        ),
        # Disabling means switching back to write-committed
        "disable_self": lambda p: {"txn_write_policy": 0},
        "requires": {
            "sync_fault_injection": 0,
            "disable_wal": 0,
            "manual_wal_flush_one_in": 0,
            "use_put_entity_one_in": 0,
            "use_multi_cf_iterator": 0,
            "commit_bypass_memtable_one_in": 0,
            "remote_compaction_worker_threads": 0,
        },
        "comment": "Non-write-committed txns incompatible with WAL disruption and many features. "
                   "C++ check: db_stress_tool.cc exits if sync_fault + non-WC txn",
    },

    "unordered_write": {
        "active_when": lambda p: (
            p.get("unordered_write", 0) == 1 and p.get("txn_write_policy", 0) == 1
        ),
        "disable_self": lambda p: {"unordered_write": 0},
        "requires": {
            # Needs concurrent memtable writes
            "allow_concurrent_memtable_write": 1,
        },
        "comment": "Unordered write only stays enabled for WritePrepared txns. "
                   "When active, it requires concurrent memtable writes.",
    },

    "commit_bypass_memtable": {
        "active_when": lambda p: (
            p.get("use_txn", 0) == 1
            and p.get("commit_bypass_memtable_one_in", 0) > 0
        ),
        "disable_self": lambda p: {"commit_bypass_memtable_one_in": 0},
        "requires": {
            "enable_blob_files": 0,
            "allow_setting_blob_options_dynamically": 0,
            "allow_concurrent_memtable_write": 0,
            "use_put_entity_one_in": 0,
            "use_get_entity": 0,
            "use_multi_get_entity": 0,
            "enable_pipelined_write": 0,
            "use_attribute_group": 0,
        },
        "comment": "commit_bypass_memtable incompatible with blob and entity features.",
    },

    "multiscan": {
        "active_when": lambda p: p.get("use_multiscan") == 1,
        "disable_self": lambda p: {"use_multiscan": 0},
        "requires": {
            "async_io": 0,
            "prefix_size": -1,
            "read_fault_one_in": 0,
            "memtable_prefix_bloom_size_ratio": 0,
            "max_sequential_skip_in_iterations": sys.maxsize,
            "test_ingest_standalone_range_deletion_one_in": 0,
            "skip_stats_update_on_db_open": 0,
        },
        "comment": "Multiscan incompatible with async IO, prefix, and fault injection.",
    },
}


def _check_and_resolve_feature_conflicts(dest_params, explicit_keys):
    """Detect and resolve conflicts between active features.

    For each pair of active features with contradicting requirements:
    - Both explicit: exit(1) with error message
    - One explicit: explicit wins, other feature disabled
    - Both random: deterministic per-pair tiebreaker picks which to disable

    Returns (changed, conflict_params) where conflict_params is a dict of
    params set by disable_self calls during conflict resolution. These params
    are "protected" — later phases must not undo them.
    """
    changed = False
    conflict_params = {}

    # Find all active features
    active = [
        (name, spec)
        for name, spec in FEATURE_REQUIREMENTS.items()
        if spec["active_when"](dest_params)
    ]

    # For each active feature, check if its requirements are satisfiable
    # given other active features' requirements
    for i, (name_a, spec_a) in enumerate(active):
        for name_b, spec_b in active[i + 1:]:
            # Find contradicting requirements between a and b
            conflicts = []
            for key, val_a in spec_a["requires"].items():
                if callable(val_a):
                    val_a = val_a(dest_params)
                if key in spec_b["requires"]:
                    val_b = spec_b["requires"][key]
                    if callable(val_b):
                        val_b = val_b(dest_params)
                    if val_a != val_b:
                        conflicts.append((key, val_a, val_b))

            if not conflicts:
                continue

            # We have a conflict between name_a and name_b
            disable_a = spec_a["disable_self"](dest_params)
            disable_b = spec_b["disable_self"](dest_params)
            a_explicit = any(k in explicit_keys for k in disable_a)
            b_explicit = any(k in explicit_keys for k in disable_b)

            if a_explicit and b_explicit:
                # Both forced — hard error
                conflict_desc = ", ".join(
                    f"{k}: {name_a} needs {va}, {name_b} needs {vb}"
                    for k, va, vb in conflicts
                )
                _trigger_keys_a = _feature_trigger_keys(name_a, spec_a, dest_params)
                _trigger_keys_b = _feature_trigger_keys(name_b, spec_b, dest_params)
                print(
                    f"\nError: conflicting features specified via extra flags:\n"
                    f"  --{list(_trigger_keys_a)[0]} activates feature '{name_a}'\n"
                    f"  --{list(_trigger_keys_b)[0]} activates feature '{name_b}'\n"
                    f"  Conflict on: {conflict_desc}\n"
                    f"  These features cannot be active at the same time.\n"
                    f"  db_crashtest now sanitizes explicit db_stress flags before\n"
                    f"  launching db_stress, so conflicting explicit overrides fail\n"
                    f"  fast instead of relying on last-one-wins behavior.",
                    file=sys.stderr,
                )
                sys.exit(1)
            elif a_explicit:
                # a wins, disable b
                updates = disable_b
            elif b_explicit:
                # b wins, disable a
                updates = disable_a
            else:
                # Both random: deterministic per-pair resolution.
                loser_name = _pick_random_conflict_loser(name_a, name_b)
                updates = disable_a if loser_name == name_a else disable_b

            for k, v in updates.items():
                if dest_params.get(k) != v:
                    dest_params[k] = v
                    changed = True
                conflict_params[k] = v

    return changed, conflict_params


def _pick_random_conflict_loser(name_a, name_b):
    """Pick a stable loser for a conflict between two randomized features."""
    sorted_pair = sorted([name_a, name_b])
    h = 0
    for ch in sorted_pair[0] + ":" + sorted_pair[1]:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return sorted_pair[1 - h % 2]


def _feature_trigger_keys(name, spec, params):
    """Return the set of parameter keys that activate this feature."""
    # Map feature names to their primary activation keys
    # (used for error messages)
    TRIGGER_KEYS = {
        "best_efforts_recovery": {"best_efforts_recovery"},
        "inplace_update_support": {"inplace_update_support"},
        "udt_memtable_only": {"user_timestamp_size", "persist_user_defined_timestamps"},
        "remote_compaction": {"remote_compaction_worker_threads"},
        "blob_direct_write": {"enable_blob_direct_write"},
        "mmap_read": {"mmap_read"},
        "txn_non_write_committed": {"use_txn", "txn_write_policy"},
        "unordered_write": {"unordered_write"},
        "commit_bypass_memtable": {"commit_bypass_memtable_one_in"},
        "multiscan": {"use_multiscan"},
    }
    return TRIGGER_KEYS.get(name, {name})


def _merge_protected_params(protected_params, updates, context):
    for key, value in updates.items():
        existing = protected_params.get(key)
        if existing is not None and existing != value:
            print(
                f"ERROR: incompatible protected updates while resolving {context}: "
                f"{key} needs both {existing} and {value}.",
                file=sys.stderr,
            )
            sys.exit(1)
        protected_params[key] = value


def _resolve_consequence_explicit_conflict(rule, dest_params, explicit_conflicts, explicit_keys):
    resolver = CONSEQUENCE_RULE_EXPLICIT_RESOLUTION.get(rule["name"])
    if resolver is None:
        conflict_desc = ", ".join(
            f"{key}={dest_params.get(key)} vs required {value}"
            for key, value in explicit_conflicts.items()
        )
        print(
            f"ERROR: explicit passthrough flag conflicts with consequence rule "
            f"'{rule['name']}': {conflict_desc}. "
            f"Add supporting extra flags or extend the declarative resolver.",
            file=sys.stderr,
        )
        sys.exit(1)

    alternative_updates = resolver(dest_params, explicit_conflicts)
    conflicting_alternatives = {
        key: value
        for key, value in alternative_updates.items()
        if key in explicit_keys and dest_params.get(key) != value
    }
    if conflicting_alternatives:
        conflict_desc = ", ".join(
            f"--{key}={dest_params.get(key)} vs alternate {value}"
            for key, value in conflicting_alternatives.items()
        )
        print(
            f"ERROR: explicit passthrough flags conflict while resolving "
            f"consequence rule '{rule['name']}': {conflict_desc}.",
            file=sys.stderr,
        )
        sys.exit(1)
    return alternative_updates


def _apply_feature_requirements(dest_params, explicit_keys, max_iterations=30):
    """Apply all sanitize rules in a fixed-point loop.

    Phase 0 (special rules): normalize params that depend on external
    state or derived computation. These rerun every iteration so later
    phases cannot permanently overwrite them.

    Phase 1 (conflict resolution): detect and resolve conflicts between
    active features based on provenance (explicit vs random).

    Phase 2 (requirement enforcement): for each active feature, apply its
    required parameter values.

    Phase 3 (one-way rules): apply consequences of param values (e.g.
    disable_wal=1 requires atomic_flush=1).

    Loops until stable.
    """
    explicit_protected_params = {}
    for _ in range(max_iterations):
        changed = _apply_special_rules(dest_params)

        # Phase 1: resolve conflicts between active features
        changed_p1, conflict_params = _check_and_resolve_feature_conflicts(
            dest_params, explicit_keys
        )
        if changed_p1:
            changed = True

        protected_params = dict(conflict_params)
        _merge_protected_params(
            protected_params,
            explicit_protected_params,
            "explicit passthrough precedence",
        )

        # Phase 2: enforce requirements of each active feature.
        # If a feature's requirements would undo conflict resolution
        # (change a param that was set by disable_self in Phase 1),
        # disable the feature instead — it has a transitive conflict.
        for name, spec in FEATURE_REQUIREMENTS.items():
            if spec["active_when"](dest_params):
                requirement_conflicts_with_explicit = False
                requirement_conflicts_with_protection = False
                for key, value in spec["requires"].items():
                    if callable(value):
                        value = value(dest_params)
                    if key in explicit_keys and dest_params.get(key) != value:
                        requirement_conflicts_with_explicit = True
                        break
                    if key in protected_params and value != protected_params[key]:
                        requirement_conflicts_with_protection = True
                        break

                if (
                    requirement_conflicts_with_explicit
                    or requirement_conflicts_with_protection
                ):
                    updates = spec["disable_self"](dest_params)
                    conflicting_disable_updates = {
                        key: value
                        for key, value in updates.items()
                        if key in explicit_keys and dest_params.get(key) != value
                    }
                    if conflicting_disable_updates:
                        conflict_desc = ", ".join(
                            f"--{key}={dest_params.get(key)} vs disable_self {value}"
                            for key, value in conflicting_disable_updates.items()
                        )
                        print(
                            f"ERROR: explicit passthrough flags conflict with feature "
                            f"'{name}': {conflict_desc}.",
                            file=sys.stderr,
                        )
                        sys.exit(1)
                    for k, v in updates.items():
                        if dest_params.get(k) != v:
                            dest_params[k] = v
                            changed = True
                else:
                    for key, value in spec["requires"].items():
                        if callable(value):
                            value = value(dest_params)
                        if dest_params.get(key) != value:
                            dest_params[key] = value
                            changed = True

        # Phase 3: apply one-way consequence rules
        new_explicit_protected_params = {}
        for rule in CONSEQUENCE_RULES:
            if rule["when"](dest_params):
                then = rule["then"]
                updates = then(dest_params) if callable(then) else then
                explicit_conflicts = {
                    key: value
                    for key, value in updates.items()
                    if key in explicit_keys and dest_params.get(key) != value
                }
                if explicit_conflicts:
                    updates = _resolve_consequence_explicit_conflict(
                        rule,
                        dest_params,
                        explicit_conflicts,
                        explicit_keys,
                    )
                    _merge_protected_params(
                        new_explicit_protected_params,
                        updates,
                        f"consequence rule '{rule['name']}'",
                    )

                for key, value in updates.items():
                    if dest_params.get(key) != value:
                        dest_params[key] = value
                        changed = True

        if new_explicit_protected_params != explicit_protected_params:
            changed = True
        explicit_protected_params = new_explicit_protected_params

        if not changed:
            break
    else:
        print(
            f"ERROR: finalize_and_sanitize did not converge after"
            f" {max_iterations} iterations in _apply_feature_requirements()."
            f" Check FEATURE_REQUIREMENTS / CONSEQUENCE_RULES for cycles.",
            file=sys.stderr,
        )
        sys.exit(1)

    return dest_params



def _apply_special_rules(dest_params):
    """Apply rules that depend on external functions or complex computation."""
    changed = False

    def set_param(key, value):
        nonlocal changed
        if dest_params.get(key) != value:
            dest_params[key] = value
            changed = True

    # 1. Release mode: disable read fault injection
    if is_release_mode():
        set_param("read_fault_one_in", 0)

    # 2. Direct IO support check (depends on filesystem)
    if (
        dest_params.get("use_direct_io_for_flush_and_compaction") == 1
        or dest_params.get("use_direct_reads") == 1
    ) and not is_direct_io_supported(dest_params["db"]):
        if is_release_mode():
            print(
                "{} does not support direct IO. Disabling use_direct_reads and "
                "use_direct_io_for_flush_and_compaction.\n".format(dest_params["db"])
            )
            set_param("use_direct_reads", 0)
            set_param("use_direct_io_for_flush_and_compaction", 0)
        else:
            set_param("mock_direct_io", True)

    # 3. Remote DBs do not guarantee active blob-file visibility across file
    # handles, so blob direct write must stay disabled there.
    if is_remote_db:
        set_param("enable_blob_direct_write", 0)

    # 4. Secondary cache / tiered cache computation
    if dest_params.get("secondary_cache_uri", "").find("compressed_secondary_cache") >= 0:
        set_param("compressed_secondary_cache_size", 0)
        set_param("compressed_secondary_cache_ratio", 0.0)
    if dest_params.get("cache_type", "").find("tiered_") >= 0:
        if dest_params.get("compressed_secondary_cache_size", 0) > 0:
            total = (
                dest_params.get("cache_size", 0)
                + dest_params.get("compressed_secondary_cache_size", 0)
            )
            if total > 0:
                set_param(
                    "compressed_secondary_cache_ratio",
                    float(
                        dest_params["compressed_secondary_cache_size"] / total
                    ),
                )
            set_param("compressed_secondary_cache_size", 0)
        else:
            set_param("compressed_secondary_cache_ratio", 0.0)
            set_param("cache_type", dest_params["cache_type"].replace("tiered_", ""))
    else:
        if dest_params.get("secondary_cache_uri"):
            set_param("compressed_secondary_cache_size", 0)
            set_param("compressed_secondary_cache_ratio", 0.0)

    # 5. Compression manager (involves random.choice)
    cm = dest_params.get("compression_manager")
    if cm == "custom":
        if dest_params.get("block_align") == 1:
            set_param("block_align", 0)
        if dest_params.get("format_version", 0) < 7:
            set_param("format_version", 7)
    elif cm in ("mixed", "randommixed"):
        set_param("block_align", 0)
    elif cm == "autoskip":
        if dest_params.get("compression_type") == "none":
            set_param(
                "compression_type",
                random.choice(["snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]),
            )
        if dest_params.get("bottommost_compression_type") == "none":
            set_param(
                "bottommost_compression_type",
                random.choice(["snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]),
            )
        set_param("block_align", 0)
    else:
        # Default: block_align requires no compression
        if dest_params.get("block_align") == 1:
            set_param("compression_type", "none")
            set_param("bottommost_compression_type", "none")

    return changed


def finalize_and_sanitize(src_params, explicit_keys=None):
    """Resolve feature incompatibilities.

    Args:
        src_params: raw params dict (may contain lambdas for random values)
        explicit_keys: set of param names that were explicitly forced via
            passthrough db_stress flags / --extra_flags. These take precedence
            over randomized/default params. If explicit flags conflict with
            each other or with a hard runtime constraint, sanitization exits
            with an error instead of silently rewriting them.
    """
    if explicit_keys is None:
        explicit_keys = set()

    dest_params = {k: v() if callable(v) else v for (k, v) in src_params.items()}

    _apply_feature_requirements(dest_params, explicit_keys)

    return dest_params


CONSEQUENCE_RULES = [
    # One-way consequence rules: these propagate the effects of a condition
    # being true. They do NOT resolve conflicts between features —
    # that is handled by FEATURE_REQUIREMENTS above.
    #
    # These stay separate from FEATURE_REQUIREMENTS because they often do not
    # belong to a single feature with a meaningful disable_self(), or they
    # normalize derived/numeric params instead of choosing between features.
    #
    # ---- Maintenance Guide: How to add a new consequence rule ----
    #
    # Use CONSEQUENCE_RULES when a condition forces downstream param
    # changes (one-way), NOT when two features are mutually incompatible
    # (use FEATURE_REQUIREMENTS for that).
    #
    # Good fit for CONSEQUENCE_RULES:
    #   - "if compression dict is 0, then training bytes must be 0"
    #   - "if WAL is disabled, then sync must be 0 and atomic_flush must be 1"
    #   - numeric constraints, conditional defaults, downstream side-effects
    #
    # Format:
    #   {
    #       "name": "unique_rule_name",
    #       "when": lambda p: <condition>,
    #       "then": {"param": value, ...}       # static dict
    #           OR: lambda p: {"param": value}  # callable (can read current params)
    #       "comment": "why this rule exists",
    #   }
    #
    # Rules run in a fixed-point loop until no rule changes any param.
    # Order in the list does NOT matter — all rules re-evaluate each iteration.
    # The loop exits with an error after the fixed-point iteration limit
    # (cycle detection).
    #
    # Worked example — a hypothetical rule:
    #
    #   {
    #       "name": "my_rule",
    #       "when": lambda p: p.get("enable_foo") == 1,
    #       "then": {"bar_timeout": 0, "baz_mode": 1},
    #       "comment": "When foo is on, bar timeout is meaningless and baz required",
    #   }
    #
    # Tests: run `python3 tools/test_db_crashtest.py` to verify your new rule.
    # ---- End maintenance guide ----
    {
            "name": "compression_dict_zero",
            "when": lambda p: p.get("compression_max_dict_bytes") == 0,
            "then": {
                "compression_zstd_max_train_bytes": 0,
                "compression_max_dict_buffer_bytes": 0,
            },
            "comment": "No dictionary means no training or buffer needed",
        },
    {
            "name": "compression_non_zstd",
            "when": lambda p: p.get("compression_type") != "zstd",
            "then": {"compression_zstd_max_train_bytes": 0},
            "comment": "ZSTD training only applies to ZSTD compression",
        },
    {
            "name": "vector_memtable",
            "when": lambda p: p.get("memtablerep") == "vector",
            "then": {"inplace_update_support": 0},
            "comment": "Vector memtable doesn't support inplace updates",
        },
    {
            "name": "non_skiplist_memtable",
            "when": lambda p: p.get("memtablerep") != "skip_list",
            "then": {
                "paranoid_memory_checks": 0,
                "memtable_veirfy_per_key_checksum_on_seek": 0,
            },
            "comment": "Only skip_list supports paranoid memory checks and per-key checksum on seek",
        },
    {
            "name": "test_batches_snapshots",
            "when": lambda p: p.get("test_batches_snapshots") == 1,
            "then": lambda p: {
                "enable_compaction_filter": 0,
                "inplace_update_support": 0,
                "write_fault_one_in": 0,
                "metadata_write_fault_one_in": 0,
                "read_fault_one_in": 0,
                "metadata_read_fault_one_in": 0,
                "use_multiscan": 0,
                # prefix_size must be positive for batched ops
                **({"prefix_size": 1} if p.get("prefix_size", 0) < 0 else {}),
            },
            "comment": "Batched ops incompatible with many features; "
                       "TODO(hx235): enable with fault injection after CI stabilization. "
                       "C++ check: db_stress_tool.cc requires prefix_size > 0",
        },
    {
            "name": "trie_index",
            "when": lambda p: p.get("use_trie_index") == 1,
            "then": {
                "mmap_read": 0,
                "compression_parallel_threads": 1,
            },
            "comment": "Trie UDI uses zero-copy pointers incompatible with mmap; "
                       "parallel compression incompatible with UDI. "
                       "C++ check: db_stress_tool.cc exits if use_trie_index && mmap_read",
        },
    {
            "name": "udi_primary_index_requires_trie",
            "when": lambda p: (
                p.get("use_udi_as_primary_index", 0) == 1
                and p.get("use_trie_index", 0) != 1
            ),
            "then": {"use_udi_as_primary_index": 0},
            "comment": "Primary UDI mode requires trie index support on every reopen.",
        },
    {
            "name": "udi_primary_index_constraints",
            "when": lambda p: p.get("use_udi_as_primary_index", 0) == 1,
            "then": lambda p: {
                **(
                    {"index_type": random.choice([0, 0, 3])}
                    if p.get("index_type") not in {0, 3}
                    else {}
                ),
                "partition_filters": 0,
                "backup_one_in": 0,
                "test_secondary": 0,
            },
            "comment": "Primary UDI mode requires trie-compatible index layouts and no "
                       "secondary/backup paths that reopen without the UDI factory.",
        },
    {
            "name": "multi_key_ops_ingest",
            "when": lambda p: (
                p.get("test_batches_snapshots") == 1
                or p.get("use_txn") == 1
                or p.get("user_timestamp_size", 0) > 0
            ),
            "then": {"ingest_external_file_one_in": 0},
            "comment": "Multi-key operations incompatible with external file ingestion in txn/ts mode",
        },
    {
            "name": "multi_key_ops_delrange",
            "when": lambda p: (
                p.get("test_batches_snapshots") == 1
                or p.get("use_txn") == 1
            ),
            "then": lambda p: (
                {
                    "delpercent": p.get("delpercent", 0) + p.get("delrangepercent", 0),
                    "delrangepercent": 0,
                }
                if p.get("delrangepercent", 0) > 0
                else {}
            ),
            "comment": "Delete range unsupported with batched ops or transactions. "
                       "C++ check: db_stress_tool.cc exits if test_batches_snapshots && delrangepercent > 0",
        },
    {
            "name": "udt_memtable_only_wal_profile",
            "when": lambda p: (
                _is_udt_memtable_only(p)
                and p.get("best_efforts_recovery", 0) == 0
                and p.get("test_best_efforts_recovery", 0) == 0
            ),
            "then": {"disable_wal": 0},
            "comment": "UDT memtable-only requires WAL outside BER mode.",
        },
    {
            "name": "udt_memtable_only_iteration_shape",
            "when": _is_udt_memtable_only,
            "then": lambda p: (
                {
                    "readpercent": p.get("readpercent", 0) + p.get("iterpercent", 10),
                    "iterpercent": 0,
                }
                if p.get("iterpercent", 0) > 0
                else {"iterpercent": 0}
            ),
            "comment": "UDT memtable-only disables iterator shapes that rely on stable SuperVersions.",
        },
    {
            "name": "inplace_update_fixed_across_runs",
            "when": lambda p: p.get("inplace_update_support", 0) == 1,
            "then": lambda p: {
                **(
                    {
                        "delpercent": p.get("delpercent", 0) + p.get("delrangepercent", 0),
                        "delrangepercent": 0,
                    }
                    if p.get("delrangepercent", 0) > 0
                    else {"delrangepercent": 0}
                ),
                **(
                    {
                        "readpercent": p.get("readpercent", 0) + p.get("prefixpercent", 0),
                        "prefixpercent": 0,
                    }
                    if p.get("prefixpercent", 0) > 0
                    else {"prefixpercent": 0}
                ),
            },
            "comment": "inplace_update_support must stay fixed across runs, so disable "
                       "other incompatible random read/delete shapes instead.",
        },
    {
            "name": "multiscan_shape_adjustments",
            "when": lambda p: p.get("use_multiscan") == 1,
            "then": lambda p: {
                **(
                    {
                        "delpercent": p.get("delpercent", 0) + p.get("delrangepercent", 0),
                        "delrangepercent": 0,
                    }
                    if p.get("delrangepercent", 0) > 0
                    else {"delrangepercent": 0}
                ),
                **(
                    {
                        "iterpercent": p.get("iterpercent", 0) + p.get("prefixpercent", 0),
                        "prefixpercent": 0,
                    }
                    if p.get("prefixpercent", 0) > 0
                    else {"prefixpercent": 0}
                ),
                **(
                    {
                        "multiscan_max_prefetch_memory_bytes": random.choice(
                            [0, 0, 64 * 1024, 256 * 1024]
                        ),
                    }
                    if p.get("multiscan_max_prefetch_memory_bytes")
                    not in {0, 64 * 1024, 256 * 1024}
                    else {}
                ),
            },
            "comment": "Multiscan reuses iter/delete-share percentages and limits prefetch to tested values.",
        },
    {
            "name": "wal_disruption_ingest",
            "when": lambda p: (
                p.get("sync_fault_injection") == 1
                or p.get("disable_wal") == 1
                or p.get("manual_wal_flush_one_in", 0) > 0
            ),
            "then": {
                "ingest_external_file_one_in": 0,
                "enable_compaction_filter": 0,
            },
            "comment": "File ingestion syncs data newer than unsynced memtable data. "
                       "Compaction filter can apply memtable updates to SSTs, problematic with data loss.",
        },
    {
            "name": "unordered_write_requires_write_prepared",
            "when": lambda p: (
                p.get("unordered_write", 0) == 1 and p.get("txn_write_policy", 0) != 1
            ),
            "then": {"unordered_write": 0},
            "comment": "Unordered write is only valid with WritePrepared and must not rewrite txn_write_policy.",
        },
    {
            "name": "timestamped_snapshot_unordered",
            "when": lambda p: p.get("create_timestamped_snapshot_one_in", 0) > 0,
            "then": lambda p: {
                "unordered_write": 0,
                **(
                    {"create_timestamped_snapshot_one_in": 0}
                    if p.get("txn_write_policy", 0) != 0
                    else {}
                ),
            },
            "comment": "Timestamped snapshots require write-committed policy. "
                       "C++ check: db_stress_tool.cc exits if non-write-committed + timestamped snapshots",
        },
    {
            "name": "disable_wal_consequences",
            "when": lambda p: p.get("disable_wal", 0) == 1,
            "then": lambda p: {
                "atomic_flush": (
                    0
                    if (
                        _is_udt_memtable_only(p)
                        and (
                            p.get("best_efforts_recovery", 0) == 1
                            or p.get("test_best_efforts_recovery", 0) == 1
                        )
                    )
                    else 1
                ),
                "sync": 0,
                "write_fault_one_in": 0,
                "test_batches_snapshots": 0,
                "reopen": 0,
                "manual_wal_flush_one_in": 0,
                "recycle_log_file_num": 0,
            },
            "comment": "Consequences of WAL being disabled: usually atomic flush, no sync, "
                       "no batched ops, and no reopen. UDT memtable-only BER mode keeps "
                       "atomic_flush off. "
                       "C++ check: db_stress_tool.cc exits if disable_wal && reopen > 0",
        },
    {
            "name": "open_files_limited",
            "when": lambda p: p.get("open_files", 1) != -1,
            "then": {
                "compaction_ttl": 0,
                "periodic_compaction_seconds": 0,
            },
            "comment": "Compaction TTL and periodic compaction only work with open_files = -1",
        },
    {
            "name": "fifo_compaction",
            "when": lambda p: p.get("compaction_style", 0) == 2,
            "then": {
                "compaction_ttl": 0,
                "periodic_compaction_seconds": 0,
                "open_files_async": 0,
                "preclude_last_level_data_seconds": 0,
                "last_level_temperature": "kUnknown",
            },
            "comment": "FIFO compaction incompatible with TTL, async open, tiering",
        },
    {
            "name": "non_fifo_compaction",
            "when": lambda p: p.get("compaction_style", 0) != 2,
            "then": {
                "file_temperature_age_thresholds": "",
                "fifo_compaction_max_data_files_size_mb": 0,
                "fifo_compaction_max_table_files_size_mb": 0,
                "fifo_compaction_use_kv_ratio_compaction": 0,
            },
            "comment": "FIFO-specific options irrelevant for non-FIFO compaction",
        },
    {
            "name": "partition_filters_index",
            "when": lambda p: p.get("partition_filters") == 1 and p.get("index_type") != 2,
            "then": {"partition_filters": 0},
            "comment": "Partition filters require kTwoLevelIndexSearch (index_type=2)",
        },
    {
            "name": "atomic_flush_pipelined",
            "when": lambda p: p.get("atomic_flush", 0) == 1,
            "then": {"enable_pipelined_write": 0},
            "comment": "Pipelined write disabled when atomic flush is used",
        },
    {
            "name": "sst_file_manager_truncate",
            "when": lambda p: (
                p.get("sst_file_manager_bytes_per_sec", 0) == 0
                or p.get("test_secondary") == 1
            ),
            "then": {"sst_file_manager_bytes_per_truncate": 0},
            "comment": "SST truncation incompatible with secondary DB or no rate limiter",
        },
    {
            "name": "prefix_size_negative",
            "when": lambda p: p.get("prefix_size") == -1 and p.get("use_multiscan") != 1,
            "then": lambda p: (
                {
                    "readpercent": p.get("readpercent", 0) + p.get("prefixpercent", 20),
                    "prefixpercent": 0,
                }
                if p.get("prefixpercent", 0) > 0
                else {"prefixpercent": 0}
            ),
            "comment": "Cannot do prefix scans without a prefix",
        },
    {
            "name": "simple_blackbox_prefix_iteration",
            "when": lambda p: (
                p.get("prefix_size") != -1
                and p.get("simple")
                and p.get("test_type") == "blackbox"
            ),
            "then": lambda p: (
                {
                    "readpercent": p.get("readpercent", 0) + p.get("iterpercent", 10),
                    "iterpercent": 0,
                }
                if p.get("iterpercent", 0) > 0
                else {"iterpercent": 0}
            ),
            "comment": "Simple blackbox mode with prefix extraction disables random iterator "
                       "operations to avoid same-prefix bound violations.",
        },
    {
            "name": "prefix_bloom_no_prefix",
            "when": lambda p: (
                p.get("prefix_size") == -1
                and p.get("memtable_whole_key_filtering") == 0
            ),
            "then": {"memtable_prefix_bloom_size_ratio": 0},
            "comment": "Prefix bloom useless without prefix or whole-key filtering",
        },
    {
            "name": "two_write_queues",
            "when": lambda p: p.get("two_write_queues") == 1,
            "then": {"enable_pipelined_write": 0},
            "comment": "Two write queues incompatible with pipelined write",
        },
    {
            "name": "multi_ops_txns_faults",
            "when": lambda p: p.get("test_multi_ops_txns") == 1,
            "then": {
                "write_fault_one_in": 0,
                "metadata_write_fault_one_in": 0,
                "read_fault_one_in": 0,
                "metadata_read_fault_one_in": 0,
            },
            "comment": "TODO(hx235): enable multi_ops_txns with fault injection after CI stabilization",
        },
    {
            "name": "multi_ops_txns_write_prepared",
            "when": lambda p: (
                p.get("test_multi_ops_txns") == 1
                and p.get("txn_write_policy", 0) != 0
            ),
            "then": {
                "wp_snapshot_cache_bits": 1,
                "wp_commit_cache_bits": 10,
                "enable_pipelined_write": 0,
                "checkpoint_one_in": 0,
                "use_only_the_last_commit_time_batch_for_recovery": 1,
                "clear_wp_commit_cache_one_in": 10,
                "lock_wal_one_in": 0,
            },
            "comment": "WritePrepared/WriteUnprepared multi-ops txn settings",
        },
    {
            "name": "put_entity_merge",
            "when": lambda p: p.get("use_put_entity_one_in", 0) != 0,
            "then": {"use_full_merge_v1": 0},
            "comment": "Wide column stress tests require FullMergeV3",
        },
    {
            "name": "file_checksum_none",
            "when": lambda p: p.get("file_checksum_impl") == "none",
            "then": {"verify_file_checksums_one_in": 0},
            "comment": "Cannot verify checksums when no checksum impl configured",
        },
    {
            "name": "write_fault_buffer",
            "when": lambda p: p.get("write_fault_one_in", 0) > 0,
            "then": lambda p: {
                "max_write_buffer_number": max(p.get("max_write_buffer_number", 4), 10),
            },
            "comment": "Need extra write buffers when background work may be disabled during recovery",
        },
    {
            "name": "write_buffer_manager_prereqs",
            "when": lambda p: (
                p.get("use_write_buffer_manager")
                and (p.get("cache_size", 0) <= 0 or p.get("db_write_buffer_size", 0) <= 0)
            ),
            "then": {"use_write_buffer_manager": 0},
            "comment": "Write buffer manager requires positive cache_size and db_write_buffer_size",
        },
    {
            "name": "user_timestamps",
            "when": lambda p: p.get("user_timestamp_size", 0) > 0,
            "then": {
                "index_block_search_type": 0,
                "use_trie_index": 0,
                "use_udi_as_primary_index": 0,
            },
            "comment": "Timestamps use BytewiseComparator.u64ts, incompatible with interpolation "
                       "search and TrieIndexFactory. "
                       "C++ check: db_stress_tool.cc exits if use_trie_index && user_timestamp_size > 0",
        },
    {
            "name": "compaction_filter_or_inplace_snapshots",
            "when": lambda p: (
                p.get("enable_compaction_filter", 0) == 1
                or p.get("inplace_update_support", 0) == 1
            ),
            "then": lambda p: {
                "acquire_snapshot_one_in": 0,
                "compact_range_one_in": 0,
                **(
                    {
                        "readpercent": (
                            p.get("readpercent", 0)
                            + p.get("iterpercent", 10)
                            + p.get("prefixpercent", 20)
                        ),
                        "iterpercent": 0,
                        "prefixpercent": 0,
                    }
                    if p.get("iterpercent", 0) > 0 or p.get("prefixpercent", 0) > 0
                    else {}
                ),
                "check_multiget_consistency": 0,
                "check_multiget_entity_consistency": 0,
            },
            "comment": "Compaction filter and inplace update are incompatible with snapshots. "
                       "C++ check: db_stress_tool.cc has extensive assertion for these",
        },
    {
            "name": "wal_write_error_injection",
            "when": lambda p: (
                p.get("disable_wal") == 0
                and (
                    p.get("reopen", 0) > 0
                    or (
                        p.get("manual_wal_flush_one_in")
                        and p.get("column_families") != 1
                    )
                    or (
                        p.get("use_txn") != 0
                        and p.get("use_optimistic_txn") == 0
                    )
                )
            ),
            "then": {
                "exclude_wal_from_write_fault_injection": 1,
                "metadata_write_fault_one_in": 0,
            },
            "comment": "WAL write errors can cause inconsistency with reopen, multi-CF manual flush, "
                       "or pessimistic transactions (2PC). "
                       "TODO(hx235): support WAL write error injection with reopen",
        },
    {
            "name": "periodic_compaction_offpeak",
            "when": lambda p: p.get("periodic_compaction_seconds") == 0,
            "then": {"daily_offpeak_time_utc": ""},
            "comment": "Offpeak time only relevant with periodic compaction enabled",
        },
    {
            "name": "read_triggered_compaction_wakeup",
            "when": lambda p: p.get("read_triggered_compaction_threshold", 0) > 0,
            "then": {"max_compaction_trigger_wakeup_seconds": 20},
            "comment": "Use a short periodic wakeup so read-triggered compaction is "
                       "exercised on quiet DBs.",
        },
    {
            "name": "put_entity_timed_put_exclusive",
            "when": lambda p: p.get("use_put_entity_one_in") == 1,
            "then": {"use_timed_put_one_in": 0},
            "comment": "use_put_entity_one_in=1 and timed_put can't coexist",
        },
    {
            "name": "put_entity_timed_put_coexist",
            "when": lambda p: (
                p.get("use_put_entity_one_in", 0) > 1
                and p.get("use_timed_put_one_in") == 1
            ),
            "then": {"use_timed_put_one_in": 3},
            "comment": "Adjust timed_put frequency when put_entity is enabled at > 1",
        },
    {
            "name": "identity_file",
            "when": lambda p: (
                p.get("write_dbid_to_manifest") == 0
                and p.get("write_identity_file") == 0
            ),
            "then": {"write_dbid_to_manifest": 1},
            "comment": "At least one identity mechanism must be enabled",
        },
    {
            "name": "checkpoint_lock_wal",
            "when": lambda p: p.get("checkpoint_one_in", 0) != 0,
            "then": {"lock_wal_one_in": 0},
            "comment": "Checkpoint skips flush if WAL is locked, causing verification failure",
        },
    {
            "name": "ingest_range_deletion",
            "when": lambda p: (
                p.get("ingest_external_file_one_in") == 0
                or p.get("delrangepercent") == 0
            ),
            "then": {"test_ingest_standalone_range_deletion_one_in": 0},
            "comment": "Standalone range deletion ingestion needs both ingestion and delrange enabled",
        },
    {
            "name": "ingest_wbwi",
            "when": lambda p: (
                p.get("enable_pipelined_write", 0)
                or p.get("unordered_write", 0)
                or p.get("disable_wal", 0) == 0
                or p.get("user_timestamp_size", 0)
            ),
            "then": {"ingest_wbwi_one_in": 0},
            "comment": "WBWI ingestion incompatible with pipelined write, unordered write, "
                       "WAL enabled, or user timestamps",
        },
    {
            "name": "test_secondary_continuous_verification",
            "when": lambda p: p.get("test_secondary") == 1,
            "then": {"continuous_verification_interval": 0},
            "comment": "Continuous verification fails with secondaries in NonBatchedOpsStressTest",
        },
    {
            "name": "skip_stats_open_files_async",
            "when": lambda p: p.get("skip_stats_update_on_db_open", 0) == 0,
            "then": {"open_files_async": 0},
            "comment": "open_files_async requires skip_stats_update_on_db_open "
                       "to avoid sync I/O in UpdateAccumulatedStats",
        },
    {
            "name": "allow_resumption_requires_remote",
            "when": lambda p: p.get("remote_compaction_worker_threads", 0) == 0,
            "then": {"allow_resumption_one_in": 0},
            "comment": "allow_resumption requires remote compaction",
        },
    {
            "name": "optimistic_txn_write_buffer_maintain",
            "when": lambda p: (
                p.get("use_optimistic_txn") == 1
                and p.get("max_write_buffer_size_to_maintain", 0)
                < p.get("write_buffer_size", 0)
            ),
            "then": lambda p: {
                "max_write_buffer_size_to_maintain": p.get("write_buffer_size", 0),
            },
            "comment": "OptimisticTransactionDB requires "
                       "max_write_buffer_size_to_maintain >= write_buffer_size "
                       "for OCC conflict detection against old memtable data",
        },
]


CONSEQUENCE_RULE_EXPLICIT_RESOLUTION = {
    "compression_dict_zero": lambda p, conflicts: {
        "compression_max_dict_bytes": 1,
    },
    "compression_non_zstd": lambda p, conflicts: {
        "compression_type": "zstd",
    },
    "vector_memtable": lambda p, conflicts: {
        "memtablerep": "skip_list",
    },
    "non_skiplist_memtable": lambda p, conflicts: {
        "memtablerep": "skip_list",
    },
    "test_batches_snapshots": lambda p, conflicts: {
        "test_batches_snapshots": 0,
    },
    "trie_index": lambda p, conflicts: {
        "use_trie_index": 0,
    },
    "udi_primary_index_requires_trie": lambda p, conflicts: {
        "use_trie_index": 1,
    },
    "udi_primary_index_constraints": lambda p, conflicts: {
        "use_udi_as_primary_index": 0,
    },
    "multi_key_ops_ingest": lambda p, conflicts: {
        "test_batches_snapshots": 0,
        "use_txn": 0,
        "use_optimistic_txn": 0,
        "user_timestamp_size": 0,
        "persist_user_defined_timestamps": 1,
    },
    "multi_key_ops_delrange": lambda p, conflicts: {
        "test_batches_snapshots": 0,
        "use_txn": 0,
        "use_optimistic_txn": 0,
    },
    "udt_memtable_only_wal_profile": lambda p, conflicts: {
        "persist_user_defined_timestamps": 1,
    },
    "udt_memtable_only_iteration_shape": lambda p, conflicts: {
        "persist_user_defined_timestamps": 1,
    },
    "inplace_update_fixed_across_runs": lambda p, conflicts: {
        "inplace_update_support": 0,
    },
    "multiscan_shape_adjustments": lambda p, conflicts: {
        "use_multiscan": 0,
    },
    "wal_disruption_ingest": lambda p, conflicts: {
        "sync_fault_injection": 0,
        "disable_wal": 0,
        "manual_wal_flush_one_in": 0,
    },
    "unordered_write_requires_write_prepared": lambda p, conflicts: {
        "txn_write_policy": 1,
        "use_txn": 1,
    },
    "timestamped_snapshot_unordered": lambda p, conflicts: (
        {"txn_write_policy": 0}
        if "create_timestamped_snapshot_one_in" in conflicts
        else {"create_timestamped_snapshot_one_in": 0}
    ),
    "disable_wal_consequences": lambda p, conflicts: {
        "disable_wal": 0,
    },
    "open_files_limited": lambda p, conflicts: {
        "open_files": -1,
    },
    "fifo_compaction": lambda p, conflicts: {
        "compaction_style": 0,
    },
    "non_fifo_compaction": lambda p, conflicts: {
        "compaction_style": 2,
    },
    "partition_filters_index": lambda p, conflicts: {
        "index_type": 2,
    },
    "atomic_flush_pipelined": lambda p, conflicts: {
        "atomic_flush": 0,
    },
    "sst_file_manager_truncate": lambda p, conflicts: {
        "sst_file_manager_bytes_per_sec": max(
            1, p.get("sst_file_manager_bytes_per_sec", 0)
        ),
        "test_secondary": 0,
    },
    "prefix_size_negative": lambda p, conflicts: {
        "prefix_size": 1,
    },
    "simple_blackbox_prefix_iteration": lambda p, conflicts: {
        "prefix_size": -1,
    },
    "prefix_bloom_no_prefix": lambda p, conflicts: {
        "memtable_whole_key_filtering": 1,
    },
    "two_write_queues": lambda p, conflicts: {
        "two_write_queues": 0,
    },
    "multi_ops_txns_faults": lambda p, conflicts: {
        "test_multi_ops_txns": 0,
    },
    "multi_ops_txns_write_prepared": lambda p, conflicts: {
        "test_multi_ops_txns": 0,
    },
    "put_entity_merge": lambda p, conflicts: {
        "use_put_entity_one_in": 0,
    },
    "file_checksum_none": lambda p, conflicts: {
        "file_checksum_impl": "crc32c",
    },
    "write_fault_buffer": lambda p, conflicts: {
        "write_fault_one_in": 0,
    },
    "write_buffer_manager_prereqs": lambda p, conflicts: {
        "cache_size": max(p.get("cache_size", 0), 8388608),
        "db_write_buffer_size": max(p.get("db_write_buffer_size", 0), 1),
    },
    "user_timestamps": lambda p, conflicts: {
        "user_timestamp_size": 0,
        "persist_user_defined_timestamps": 1,
    },
    "compaction_filter_or_inplace_snapshots": lambda p, conflicts: {
        "enable_compaction_filter": 0,
        "inplace_update_support": 0,
    },
    "wal_write_error_injection": lambda p, conflicts: {
        "reopen": 0,
        "manual_wal_flush_one_in": 0,
        "use_txn": 0,
        "use_optimistic_txn": 0,
    },
    "periodic_compaction_offpeak": lambda p, conflicts: {
        "periodic_compaction_seconds": 1,
    },
    "read_triggered_compaction_wakeup": lambda p, conflicts: {
        "read_triggered_compaction_threshold": 0,
    },
    "put_entity_timed_put_exclusive": lambda p, conflicts: {
        "use_put_entity_one_in": 0,
    },
    "put_entity_timed_put_coexist": lambda p, conflicts: {
        "use_put_entity_one_in": 0,
    },
    "identity_file": lambda p, conflicts: (
        {"write_identity_file": 1}
        if "write_dbid_to_manifest" in conflicts
        else {"write_dbid_to_manifest": 1}
    ),
    "checkpoint_lock_wal": lambda p, conflicts: {
        "checkpoint_one_in": 0,
    },
    "ingest_range_deletion": lambda p, conflicts: {
        "ingest_external_file_one_in": max(
            1, p.get("ingest_external_file_one_in", 0)
        ),
        "delrangepercent": max(1, p.get("delrangepercent", 0)),
    },
    "ingest_wbwi": lambda p, conflicts: {
        "enable_pipelined_write": 0,
        "unordered_write": 0,
        "disable_wal": 1,
        "user_timestamp_size": 0,
        "persist_user_defined_timestamps": 1,
    },
    "test_secondary_continuous_verification": lambda p, conflicts: {
        "test_secondary": 0,
    },
    "skip_stats_open_files_async": lambda p, conflicts: {
        "skip_stats_update_on_db_open": 1,
    },
    "allow_resumption_requires_remote": lambda p, conflicts: {
        "remote_compaction_worker_threads": 1,
    },
    "optimistic_txn_write_buffer_maintain": lambda p, conflicts: {
        "write_buffer_size": p.get("max_write_buffer_size_to_maintain", 0),
    },
}


def gen_cmd_params(args):
    params = {}

    params.update(default_params)
    if args.test_type == "blackbox":
        params.update(blackbox_default_params)
    if args.test_type == "whitebox":
        params.update(whitebox_default_params)
    if args.simple:
        params.update(simple_default_params)
        if args.test_type == "blackbox":
            params.update(blackbox_simple_default_params)
        if args.test_type == "whitebox":
            params.update(whitebox_simple_default_params)
    if args.cf_consistency:
        params.update(cf_consistency_params)
    if args.txn:
        params.update(txn_params)
    if args.optimistic_txn:
        params.update(optimistic_txn_params)
    if args.test_best_efforts_recovery:
        params.update(best_efforts_recovery_params)
    if args.enable_ts:
        params.update(ts_params)
    if args.test_multiops_txn:
        params.update(multiops_txn_params)
    if args.test_tiered_storage:
        params.update(tiered_params)

    # Best-effort recovery, tiered storage are currently incompatible with
    # BlobDB and blob direct write. Test BE recovery if specified on the
    # command line; otherwise, apply one of the blob feature overrides with a
    # 10% chance.
    if (
        not args.test_best_efforts_recovery
        and not args.test_tiered_storage
        and params.get("test_secondary", 0) == 0
        and random.choice([0] * 9 + [1]) == 1
    ):
        params.update(
            random.choice(
                [
                    blob_params,
                    blob_direct_write_params,
                    blob_direct_write_get_entity_params,
                    blob_direct_write_multi_get_entity_params,
                ]
            )
        )

    if "compaction_style" not in params:
        # Default to leveled compaction
        # TODO: Fix "Unsafe to store Seq later" with tiered+leveled and
        # enable that combination rather than falling back to universal.
        # TODO: There is also an alleged bug with leveled compaction
        # infinite looping but that likely would not fail the crash test.
        params["compaction_style"] = 0 if not args.test_tiered_storage else 1

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
    return params


DBCRASHTEST_ONLY_PARAMS = {
    "test_type",
    "simple",
    "duration",
    "interval",
    "random_kill_odd",
    "cf_consistency",
    "txn",
    "optimistic_txn",
    "test_best_efforts_recovery",
    "enable_ts",
    "test_multiops_txn",
    "stress_cmd",
    "test_tiered_storage",
    "cleanup_cmd",
    "print_stderr_separately",
    "verify_timeout",
}


def _parse_extra_flag_value(raw_value):
    lowered = raw_value.lower()
    if lowered in {"true", "yes", "on"}:
        return 1
    if lowered in {"false", "no", "off"}:
        return 0
    if raw_value == "":
        return raw_value
    try:
        return int(raw_value)
    except ValueError:
        try:
            return float(raw_value)
        except ValueError:
            return raw_value


def _format_flag_assignment(key, value):
    return quote_arg_for_display(f"--{key}={value}")


def gen_cmd(params, unknown_params):
    # Merge passthrough db_stress flags into the params dict BEFORE
    # sanitization. This ensures forced flags participate in constraint
    # resolution and win over randomized/default params instead of being
    # silently rewritten by later sanitize passes.
    #
    # Previously, unknown_params were appended to the CLI after sanitization,
    # which meant gflags last-one-wins would override incompatibility guards.
    # For example, passing --enable_blob_direct_write=1 via extra flags would
    # re-enable BDW even after finalize_and_sanitize disabled it due to
    # conflicts with transactions. db_crashtest-only options (e.g. --duration)
    # are not db_stress flags, so ignore them here if they leak into the
    # passthrough list.
    merged_params = dict(params)
    explicit_keys = set()
    explicit_overrides = {}
    ignored_overrides = []
    remaining_unknown = []
    for arg in unknown_params:
        if arg.startswith("--") and "=" in arg:
            key_val = arg[2:]  # strip --
            key, val = key_val.split("=", 1)
            if key in DBCRASHTEST_ONLY_PARAMS:
                ignored_overrides.append(arg)
                continue
            val = _parse_extra_flag_value(val)
            merged_params[key] = val
            explicit_keys.add(key)
            explicit_overrides[key] = val
        else:
            remaining_unknown.append(arg)

    finalized_params = finalize_and_sanitize(merged_params, explicit_keys=explicit_keys)

    for arg in ignored_overrides:
        print(
            "WARNING: ignoring passthrough override "
            f"{quote_arg_for_display(arg)} because it is handled by "
            "db_crashtest.py, not forwarded to db_stress.",
            file=sys.stderr,
        )

    for key in sorted(explicit_overrides):
        explicit_value = explicit_overrides[key]
        finalized_value = finalized_params.get(key)
        if explicit_value != finalized_value:
            print(
                "ERROR: explicit db_stress flag did not win after sanitization: "
                f"{_format_flag_assignment(key, explicit_value)} vs "
                f"{_format_flag_assignment(key, finalized_value)}. "
                "Add supporting explicit flags or extend the rule resolver.",
                file=sys.stderr,
            )
            sys.exit(1)

    prepare_expected_values_dir(
        finalized_params.get("expected_values_dir"),
        finalized_params.get("destroy_db_initially", 0),
    )
    cmd = (
        [stress_cmd]
        + [
            f"--{k}={v}"
            for k, v in [(k, finalized_params[k]) for k in sorted(finalized_params)]
            if k not in DBCRASHTEST_ONLY_PARAMS and v is not None
        ]
        + remaining_unknown
    )
    return cmd


def execute_cmd(cmd, timeout=None, timeout_pstack=False):
    child = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print(
        "Running db_stress with pid=%d: %s\n\n"
        % (child.pid, " ".join(quote_arg_for_display(arg) for arg in cmd))
    )
    pid = child.pid

    try:
        outs, errs = child.communicate(timeout=timeout)
        hit_timeout = False
        print("WARNING: db_stress ended before kill: exitcode=%d\n" % child.returncode)
    except subprocess.TimeoutExpired:
        hit_timeout = True
        if timeout_pstack:
            os.system("pstack %d" % pid)
        child.terminate()  # SIGTERM — triggers TerminationHandler
        try:
            outs, errs = child.communicate(timeout=3)
            print("TERMINATED %d\n" % child.pid)
        except subprocess.TimeoutExpired:
            child.kill()
            print("KILLED %d (SIGTERM did not work)\n" % child.pid)
            outs, errs = child.communicate()

    return (
        hit_timeout,
        child.returncode,
        outs.decode("utf-8"),
        errs.decode("utf-8"),
        pid,
    )


def print_output_and_exit_on_error(stdout, stderr, print_stderr_separately=False):
    print("stdout:\n", stdout)
    if len(stderr) == 0:
        return

    if print_stderr_separately:
        print("stderr:\n", stderr, file=sys.stderr)
    else:
        print("stderr:\n", stderr)

    sys.exit(2)


def cleanup_after_success(dbname):
    # Use db_stress --destroy_db_and_exit, which simplifies remote DB cleanup
    cleanup_cmd_parts = [stress_cmd, "--destroy_db_and_exit=1", "--db=" + dbname]
    # Pass through relevant arguments for remote DB access
    for arg in remain_args:
        parts = arg.split("=", 1)
        if parts[0] in ["--env_uri", "--fs_uri"]:
            cleanup_cmd_parts.append(arg)
    print("Running DB cleanup command - %s\n" % " ".join(cleanup_cmd_parts))
    ret = subprocess.call(cleanup_cmd_parts)
    if ret != 0:
        print("ERROR: DB cleanup returned error %d\n" % ret)
        sys.exit(2)


def print_and_cleanup_fault_injection_log(pid):
    # Fault injection logs are stored in TEST_TMPDIR (or /tmp) to survive
    # DB reopen cleanup, and to be included in sandcastle's db.tar.gz artifact.
    # Filter by pid to only print the log from the current run.
    max_tail_entries = 32
    log_dir = os.environ.get(_TEST_DIR_ENV_VAR) or "/tmp"
    pattern = os.path.join(log_dir, "fault_injection_%d_*.log" % pid)
    for log in glob.glob(pattern):
        print("=== Fault injection log: %s ===" % log)
        try:
            with open(log) as f:
                lines = f.readlines()
            # Log format: header line(s), entry lines, footer line.
            # The footer starts with "=== End of".
            # Print header and footer always, truncate entries in the middle.
            header = []
            footer = []
            entries = []
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("=== End of"):
                    footer.append(line)
                elif stripped.startswith("===") or stripped == "(none)":
                    header.append(line)
                else:
                    entries.append(line)
            total_entries = len(entries)
            print("".join(header), end="")
            if total_entries <= max_tail_entries:
                print("".join(entries), end="")
                print("".join(footer), end="")
            else:
                skipped = total_entries - max_tail_entries
                print(
                    "... (%d entries omitted, showing last %d. "
                    "Full log: %s)\n" % (skipped, max_tail_entries, log),
                    end="",
                )
                print("".join(entries[-max_tail_entries:]), end="")
                print(
                    "=== Showed %d of %d injected error entries ===\n"
                    % (max_tail_entries, total_entries),
                    end="",
                )
        except OSError:
            pass


# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in RocksDB.
def blackbox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname("blackbox")
    exit_time = time.time() + cmd_params["duration"]

    print(
        "Running blackbox-crash-test with \n"
        + "interval_between_crash="
        + str(cmd_params["interval"])
        + "\n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )

    while time.time() < exit_time:
        apply_random_seed_per_iteration()
        cmd = gen_cmd(
            dict(list(cmd_params.items()) + list({"db": dbname}.items())), unknown_args
        )

        hit_timeout, retcode, outs, errs, pid = execute_cmd(cmd, cmd_params["interval"])

        print_and_cleanup_fault_injection_log(pid)

        # Reset destroy_db_initially after each run (it may have been set by
        # command line for first run only)
        cmd_params["destroy_db_initially"] = 0

        if not hit_timeout:
            print("Exit Before Killing")
            print_output_and_exit_on_error(outs, errs, args.print_stderr_separately)
            sys.exit(2)

        print_output_and_exit_on_error(outs, errs, args.print_stderr_separately)

        time.sleep(1)  # time to stabilize before the next run

        time.sleep(1)  # time to stabilize before the next run

    # We should run the test one more time with VerifyOnly setup and no-timeout
    # Only do this if the tests are not failed for total-duration
    print("Running final time for verification")
    cmd_params.update({"verification_only": 1})
    cmd_params.update({"skip_verifydb": 0})

    cmd = gen_cmd(
        dict(list(cmd_params.items()) + list({"db": dbname}.items())), unknown_args
    )
    hit_timeout, retcode, outs, errs, pid = execute_cmd(
        cmd, cmd_params["verify_timeout"], True
    )

    print_and_cleanup_fault_injection_log(pid)

    # For the final run
    print_output_and_exit_on_error(outs, errs, args.print_stderr_separately)

    # we need to clean up after ourselves -- only do this on test success
    cleanup_after_success(dbname)


# This python script runs db_stress multiple times. Some runs with
# kill_random_test that causes rocksdb to crash at various points in code.
def whitebox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname("whitebox")

    cur_time = time.time()
    exit_time = cur_time + cmd_params["duration"]
    half_time = cur_time + cmd_params["duration"] // 2

    print(
        "Running whitebox-crash-test with \n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )

    total_check_mode = 4
    check_mode = 0
    kill_random_test = cmd_params["random_kill_odd"]
    kill_mode = 0
    prev_compaction_style = -1
    succeeded = True
    hit_timeout = False
    while time.time() < exit_time:
        apply_random_seed_per_iteration()
        if check_mode == 0:
            additional_opts = {
                # use large ops per thread since we will kill it anyway
                "ops_per_thread": 100
                * cmd_params["ops_per_thread"],
            }
            # run with kill_random_test, with three modes.
            # Mode 0 covers all kill points. Mode 1 covers less kill points but
            # increases change of triggering them. Mode 2 covers even less
            # frequent kill points and further increases triggering change.
            if kill_mode == 0:
                additional_opts.update(
                    {
                        "kill_random_test": kill_random_test,
                    }
                )
            elif kill_mode == 1:
                if cmd_params.get("disable_wal", 0) == 1:
                    my_kill_odd = kill_random_test // 50 + 1
                else:
                    my_kill_odd = kill_random_test // 10 + 1
                additional_opts.update(
                    {
                        "kill_random_test": my_kill_odd,
                        "kill_exclude_prefixes": "WritableFileWriter::Append,"
                        + "WritableFileWriter::WriteBuffered",
                    }
                )
            elif kill_mode == 2:
                # TODO: May need to adjust random odds if kill_random_test
                # is too small.
                additional_opts.update(
                    {
                        "kill_random_test": (kill_random_test // 5000 + 1),
                        "kill_exclude_prefixes": "WritableFileWriter::Append,"
                        "WritableFileWriter::WriteBuffered,"
                        "PosixMmapFile::Allocate,WritableFileWriter::Flush",
                    }
                )
            # Run kill mode 0, 1 and 2 by turn.
            kill_mode = (kill_mode + 1) % 3
        elif check_mode == 1:
            # normal run with universal compaction mode
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"],
                "compaction_style": 1,
            }
            # Single level universal has a lot of special logic. Ensure we cover
            # it sometimes.
            if not args.test_tiered_storage and random.randint(0, 1) == 1:
                additional_opts["num_levels"] = 1
        elif check_mode == 2:
            # normal run with FIFO compaction mode
            # ops_per_thread is divided by 5 because FIFO compaction
            # style is quite a bit slower on reads with lot of files
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"] // 5,
                "compaction_style": 2,
            }
            # TODO: test transition from non-FIFO to FIFO with num_levels > 1.
            # See https://github.com/facebook/rocksdb/pull/10348
            # For now, tiered storage FIFO (file_temperature_age_thresholds)
            # requires num_levels == 1 and non-tiered operates that way.
            if args.test_tiered_storage:
                additional_opts["num_levels"] = 1
        else:
            # normal run
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"],
            }

        cur_compaction_style = additional_opts.get(
            "compaction_style", cmd_params.get("compaction_style", 0)
        )
        if (
            prev_compaction_style != -1
            and prev_compaction_style != cur_compaction_style
        ):
            print(
                "`compaction_style` is changed in current run so `destroy_db_initially` is set to 1 as a short-term solution to avoid cycling through previous db of different compaction style."
                + "\n"
            )
            cmd_params["destroy_db_initially"] = 1
        prev_compaction_style = cur_compaction_style

        cmd = gen_cmd(
            dict(
                list(cmd_params.items())
                + list(additional_opts.items())
                + list({"db": dbname}.items())
            ),
            unknown_args,
        )

        print(
            "Running:" + " ".join(cmd) + "\n"
        )  # noqa: E999 T25377293 Grandfathered in

        # If the running time is 15 minutes over the run time, explicit kill and
        # exit even if white box kill didn't hit. This is to guarantee run time
        # limit, as if it runs as a job, running too long will create problems
        # for job scheduling or execution.
        # TODO detect a hanging condition. The job might run too long as RocksDB
        # hits a hanging bug.
        hit_timeout, retncode, stdoutdata, stderrdata, pid = execute_cmd(
            cmd, exit_time - time.time() + 900
        )

        # Reset destroy_db_initially after each run (it may have been set by
        # command line for first run, or set for various reasons for a step)
        cmd_params["destroy_db_initially"] = 0

        msg = "check_mode={}, kill option={}, exitcode={}\n".format(
            check_mode, additional_opts["kill_random_test"], retncode
        )

        print(msg)
        print_output_and_exit_on_error(
            stdoutdata, stderrdata, args.print_stderr_separately
        )

        if hit_timeout:
            print("Killing the run for running too long")
            break

        succeeded = False
        if additional_opts["kill_random_test"] is None and (retncode == 0):
            # we expect zero retncode if no kill option
            succeeded = True
        elif additional_opts["kill_random_test"] is not None and retncode <= 0:
            # When kill option is given, the test MIGHT kill itself.
            # If it does, negative retncode is expected. Otherwise 0.
            succeeded = True

        if not succeeded:
            print("TEST FAILED. See kill option and exit code above!!!\n")
            sys.exit(1)

        # First half of the duration, keep doing kill test. For the next half,
        # try different modes.
        if time.time() > half_time:
            # Set next iteration to destroy DB (works for remote DB)
            cmd_params["destroy_db_initially"] = 1
            check_mode = (check_mode + 1) % total_check_mode

        time.sleep(1)  # time to stabilize after a kill

    # If successfully finished or timed out (we currently treat timed out test as passing)
    # Clean up after ourselves
    if succeeded or hit_timeout:
        cleanup_after_success(dbname)


def main():
    global stress_cmd

    parser = argparse.ArgumentParser(
        description="This script runs and kills \
        db_stress multiple times"
    )
    parser.add_argument("test_type", choices=["blackbox", "whitebox"])
    parser.add_argument("--simple", action="store_true")
    parser.add_argument("--cf_consistency", action="store_true")
    parser.add_argument("--txn", action="store_true")
    parser.add_argument("--optimistic_txn", action="store_true")
    parser.add_argument("--test_best_efforts_recovery", action="store_true")
    parser.add_argument("--enable_ts", action="store_true")
    parser.add_argument("--test_multiops_txn", action="store_true")
    parser.add_argument("--stress_cmd")
    parser.add_argument("--test_tiered_storage", action="store_true")
    parser.add_argument("--cleanup_cmd")  # ignore old option for now
    parser.add_argument("--print_stderr_separately", action="store_true", default=False)

    all_params = dict(
        list(default_params.items())
        + list(blackbox_default_params.items())
        + list(whitebox_default_params.items())
        + list(simple_default_params.items())
        + list(blackbox_simple_default_params.items())
        + list(whitebox_simple_default_params.items())
        + list(blob_params.items())
        + list(blob_direct_write_params.items())
        + list(blob_direct_write_get_entity_params.items())
        + list(blob_direct_write_multi_get_entity_params.items())
        + list(ts_params.items())
        + list(multiops_txn_params.items())
        + list(best_efforts_recovery_params.items())
        + list(cf_consistency_params.items())
        + list(tiered_params.items())
        + list(txn_params.items())
        + list(optimistic_txn_params.items())
    )

    for k, v in all_params.items():
        parser.add_argument("--" + k, type=type(v() if callable(v) else v))
    # unknown_args are passed directly to db_stress

    args, unknown_args = parser.parse_known_args(remain_args)
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is not None and not is_remote_db:
        isdir = False
        try:
            isdir = os.path.isdir(test_tmpdir)
            if not isdir:
                print(
                    "ERROR: %s env var is set to a non-existent directory: %s. Update it to correct directory path."
                    % (_TEST_DIR_ENV_VAR, test_tmpdir)
                )
                sys.exit(1)
        except OSError:
            pass

    if args.stress_cmd:
        stress_cmd = args.stress_cmd
    if args.test_type == "blackbox":
        blackbox_crash_main(args, unknown_args)
    if args.test_type == "whitebox":
        whitebox_crash_main(args, unknown_args)
    # Only delete the `expected_values_dir` if test passes
    if expected_values_dir is not None:
        shutil.rmtree(expected_values_dir)
    if multiops_txn_key_spaces_file is not None:
        os.remove(multiops_txn_key_spaces_file)


if __name__ == "__main__":
    main()
