#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
from __future__ import absolute_import, division, print_function, unicode_literals

import argparse

import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

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
    "acquire_snapshot_one_in": 10000,
    "backup_max_size": 100 * 1024 * 1024,
    # Consider larger number when backups considered more stable
    "backup_one_in": 100000,
    "batch_protection_bytes_per_key": lambda: random.choice([0, 8]),
    "memtable_protection_bytes_per_key": lambda: random.choice([0, 1, 2, 4, 8]),
    "block_size": 16384,
    "bloom_bits": lambda: random.choice(
        [random.randint(0, 19), random.lognormvariate(2.3, 1.3)]
    ),
    "cache_index_and_filter_blocks": lambda: random.randint(0, 1),
    "cache_size": 8388608,
    "charge_compression_dictionary_building_buffer": lambda: random.choice([0, 1]),
    "charge_filter_construction": lambda: random.choice([0, 1]),
    "charge_table_reader": lambda: random.choice([0, 1]),
    "charge_file_metadata": lambda: random.choice([0, 1]),
    "checkpoint_one_in": 1000000,
    "compression_type": lambda: random.choice(
        ["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
    ),
    "bottommost_compression_type": lambda: "disable"
    if random.randint(0, 1) == 0
    else random.choice(["none", "snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]),
    "checksum_type": lambda: random.choice(
        ["kCRC32c", "kxxHash", "kxxHash64", "kXXH3"]
    ),
    "compression_max_dict_bytes": lambda: 16384 * random.randint(0, 1),
    "compression_zstd_max_train_bytes": lambda: 65536 * random.randint(0, 1),
    # Disabled compression_parallel_threads as the feature is not stable
    # lambda: random.choice([1] * 9 + [4])
    "compression_parallel_threads": 1,
    "compression_max_dict_buffer_bytes": lambda: (1 << random.randint(0, 40)) - 1,
    "compression_use_zstd_dict_trainer": lambda: random.randint(0, 1),
    "clear_column_family_one_in": 0,
    "compact_files_one_in": 1000000,
    "compact_range_one_in": 1000000,
    "compaction_pri": random.randint(0, 4),
    "data_block_index_type": lambda: random.choice([0, 1]),
    "delpercent": 4,
    "delrangepercent": 1,
    "destroy_db_initially": 0,
    "enable_pipelined_write": lambda: random.randint(0, 1),
    "enable_compaction_filter": lambda: random.choice([0, 0, 0, 1]),
    "expected_values_dir": lambda: setup_expected_values_dir(),
    "fail_if_options_file_error": lambda: random.randint(0, 1),
    "flush_one_in": 1000000,
    "manual_wal_flush_one_in": lambda: random.choice([0, 0, 1000, 1000000]),
    "file_checksum_impl": lambda: random.choice(["none", "crc32c", "xxh64", "big"]),
    "get_live_files_one_in": 1000000,
    # Note: the following two are intentionally disabled as the corresponding
    # APIs are not guaranteed to succeed.
    "get_sorted_wal_files_one_in": 0,
    "get_current_wal_file_one_in": 0,
    # Temporarily disable hash index
    "index_type": lambda: random.choice([0, 0, 0, 2, 2, 3]),
    "ingest_external_file_one_in": 1000000,
    "iterpercent": 10,
    "mark_for_compaction_one_file_in": lambda: 10 * random.randint(0, 1),
    "max_background_compactions": 20,
    "max_bytes_for_level_base": 10485760,
    "max_key": 25000000,
    "max_write_buffer_number": 3,
    "mmap_read": lambda: random.randint(0, 1),
    # Setting `nooverwritepercent > 0` is only possible because we do not vary
    # the random seed, so the same keys are chosen by every run for disallowing
    # overwrites.
    "nooverwritepercent": 1,
    "open_files": lambda: random.choice([-1, -1, 100, 500000]),
    "optimize_filters_for_memory": lambda: random.randint(0, 1),
    "partition_filters": lambda: random.randint(0, 1),
    "partition_pinning": lambda: random.randint(0, 3),
    "pause_background_one_in": 1000000,
    "prefix_size": lambda: random.choice([-1, 1, 5, 7, 8]),
    "prefixpercent": 5,
    "progress_reports": 0,
    "readpercent": 45,
    "recycle_log_file_num": lambda: random.randint(0, 1),
    "snapshot_hold_ops": 100000,
    "sst_file_manager_bytes_per_sec": lambda: random.choice([0, 104857600]),
    "sst_file_manager_bytes_per_truncate": lambda: random.choice([0, 1048576]),
    "long_running_snapshots": lambda: random.randint(0, 1),
    "subcompactions": lambda: random.randint(1, 4),
    "target_file_size_base": 2097152,
    "target_file_size_multiplier": 2,
    "test_batches_snapshots": random.randint(0, 1),
    "top_level_index_pinning": lambda: random.randint(0, 3),
    "unpartitioned_pinning": lambda: random.randint(0, 3),
    "use_direct_reads": lambda: random.randint(0, 1),
    "use_direct_io_for_flush_and_compaction": lambda: random.randint(0, 1),
    "mock_direct_io": False,
    "cache_type": lambda: random.choice(["lru_cache", "hyper_clock_cache"]),
    "use_full_merge_v1": lambda: random.randint(0, 1),
    "use_merge": lambda: random.randint(0, 1),
    # use_put_entity_one_in has to be the same across invocations for verification to work, hence no lambda
    "use_put_entity_one_in": random.choice([0] * 7 + [1, 5, 10]),
    # 999 -> use Bloom API
    "ribbon_starting_level": lambda: random.choice([random.randint(-1, 10), 999]),
    "value_size_mult": 32,
    "verify_checksum": 1,
    "write_buffer_size": 4 * 1024 * 1024,
    "writepercent": 35,
    "format_version": lambda: random.choice([2, 3, 4, 5, 5]),
    "index_block_restart_interval": lambda: random.choice(range(1, 16)),
    "use_multiget": lambda: random.randint(0, 1),
    "periodic_compaction_seconds": lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    # 0 = never (used by some), 10 = often (for threading bugs), 600 = default
    "stats_dump_period_sec": lambda: random.choice([0, 10, 600]),
    "compaction_ttl": lambda: random.choice([0, 0, 1, 2, 10, 100, 1000]),
    "fifo_allow_compaction": lambda: random.randint(0, 1),
    # Test small max_manifest_file_size in a smaller chance, as most of the
    # time we wnat manifest history to be preserved to help debug
    "max_manifest_file_size": lambda: random.choice(
        [t * 16384 if t < 3 else 1024 * 1024 * 1024 for t in range(1, 30)]
    ),
    # Sync mode might make test runs slower so running it in a smaller chance
    "sync": lambda: random.choice([1 if t == 0 else 0 for t in range(0, 20)]),
    "bytes_per_sync": lambda: random.choice([0, 262144]),
    "wal_bytes_per_sync": lambda: random.choice([0, 524288]),
    # Disable compaction_readahead_size because the test is not passing.
    # "compaction_readahead_size" : lambda : random.choice(
    #    [0, 0, 1024 * 1024]),
    "db_write_buffer_size": lambda: random.choice(
        [0, 0, 0, 1024 * 1024, 8 * 1024 * 1024, 128 * 1024 * 1024]
    ),
    "avoid_unnecessary_blocking_io": random.randint(0, 1),
    "write_dbid_to_manifest": random.randint(0, 1),
    "avoid_flush_during_recovery": lambda: random.choice(
        [1 if t == 0 else 0 for t in range(0, 8)]
    ),
    "max_write_batch_group_size_bytes": lambda: random.choice(
        [16, 64, 1024 * 1024, 16 * 1024 * 1024]
    ),
    "level_compaction_dynamic_level_bytes": True,
    "verify_checksum_one_in": 1000000,
    "verify_db_one_in": 100000,
    "continuous_verification_interval": 0,
    "max_key_len": 3,
    "key_len_percent_dist": "1,30,69",
    "read_fault_one_in": lambda: random.choice([0, 32, 1000]),
    "open_metadata_write_fault_one_in": lambda: random.choice([0, 0, 8]),
    "open_write_fault_one_in": lambda: random.choice([0, 0, 16]),
    "open_read_fault_one_in": lambda: random.choice([0, 0, 32]),
    "sync_fault_injection": lambda: random.randint(0, 1),
    "get_property_one_in": 1000000,
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
    "max_write_buffer_size_to_maintain": lambda: random.choice(
        [0, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024]
    ),
    "user_timestamp_size": 0,
    "secondary_cache_fault_one_in": lambda: random.choice([0, 0, 32]),
    "prepopulate_block_cache": lambda: random.choice([0, 1]),
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
            "compressed_secondary_cache://capacity=8388608",
            "compressed_secondary_cache://capacity=8388608;enable_custom_split_merge=true",
        ]
    ),
    "allow_data_in_errors": True,
    "readahead_size": lambda: random.choice([0, 16384, 524288]),
    "initial_auto_readahead_size": lambda: random.choice([0, 16384, 524288]),
    "max_auto_readahead_size": lambda: random.choice([0, 16384, 524288]),
    "num_file_reads_for_auto_readahead": lambda: random.choice([0, 1, 2]),
    "min_write_buffer_number_to_merge": lambda: random.choice([1, 2]),
    "preserve_internal_time_seconds": lambda: random.choice([0, 60, 3600, 36000]),
}

_TEST_DIR_ENV_VAR = "TEST_TMPDIR"
_DEBUG_LEVEL_ENV_VAR = "DEBUG_LEVEL"

stress_cmd = "./db_stress"
cleanup_cmd = None


def is_release_mode():
    return os.environ.get(_DEBUG_LEVEL_ENV_VAR) == "0"


def get_dbname(test_name):
    test_dir_name = "rocksdb_crashtest_" + test_name
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        dbname = tempfile.mkdtemp(prefix=test_dir_name)
    else:
        dbname = test_tmpdir + "/" + test_dir_name
        shutil.rmtree(dbname, True)
        if cleanup_cmd is not None:
            print("Running DB cleanup command - %s\n" % cleanup_cmd)
            # Ignore failure
            os.system(cleanup_cmd)
        os.mkdir(dbname)
    return dbname


expected_values_dir = None


def setup_expected_values_dir():
    global expected_values_dir
    if expected_values_dir is not None:
        return expected_values_dir
    expected_dir_prefix = "rocksdb_crashtest_expected_"
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        expected_values_dir = tempfile.mkdtemp(prefix=expected_dir_prefix)
    else:
        # if tmpdir is specified, store the expected_values_dir under that dir
        expected_values_dir = test_tmpdir + "/rocksdb_crashtest_expected"
        if os.path.exists(expected_values_dir):
            shutil.rmtree(expected_values_dir)
        os.mkdir(expected_values_dir)
    return expected_values_dir


multiops_txn_key_spaces_file = None


def setup_multiops_txn_key_spaces_file():
    global multiops_txn_key_spaces_file
    if multiops_txn_key_spaces_file is not None:
        return multiops_txn_key_spaces_file
    key_spaces_file_prefix = "rocksdb_crashtest_multiops_txn_key_spaces"
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        multiops_txn_key_spaces_file = tempfile.mkstemp(prefix=key_spaces_file_prefix)[
            1
        ]
    else:
        if not os.path.exists(test_tmpdir):
            os.mkdir(test_tmpdir)
        multiops_txn_key_spaces_file = tempfile.mkstemp(
            prefix=key_spaces_file_prefix, dir=test_tmpdir
        )[1]
    return multiops_txn_key_spaces_file


def is_direct_io_supported(dbname):
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
    # since we will be killing anyway, use large value for ops_per_thread
    "ops_per_thread": 100000000,
    "reopen": 0,
    "set_options_one_in": 10000,
}

whitebox_default_params = {
    # TODO: enable this once we figure out how to adjust kill odds for WAL-
    # disabled runs, and either (1) separate full `db_stress` runs out of
    # whitebox crash or (2) support verification at end of `db_stress` runs
    # that ran with WAL disabled.
    "disable_wal": 0,
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
    "level_compaction_dynamic_level_bytes": False,
    "paranoid_file_checks": lambda: random.choice([0, 1, 1, 1]),
    "verify_iterator_with_expected_state_one_in": 5,  # this locks a range of keys
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
    # with compaction filter.
    "enable_compaction_filter": 0,
    # `CfConsistencyStressTest::TestIngestExternalFile()` is not implemented.
    "ingest_external_file_one_in": 0,
}

txn_params = {
    "use_txn": 1,
    # Avoid lambda to set it once for the entire test
    "txn_write_policy": random.randint(0, 2),
    "unordered_write": random.randint(0, 1),
    # TODO: there is such a thing as transactions with WAL disabled. We should
    # cover that case.
    "disable_wal": 0,
    # OpenReadOnly after checkpoint is not currnetly compatible with WritePrepared txns
    "checkpoint_one_in": 0,
    # pipeline write is not currnetly compatible with WritePrepared txns
    "enable_pipelined_write": 0,
    "create_timestamped_snapshot_one_in": random.choice([0, 20]),
    # PutEntity in transactions is not yet implemented
    "use_put_entity_one_in" : 0,
}

best_efforts_recovery_params = {
    "best_efforts_recovery": 1,
    "atomic_flush": 0,
    "disable_wal": 1,
    "column_families": 1,
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
}

ts_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "user_timestamp_size": 8,
    "use_merge": 0,
    "use_full_merge_v1": 0,
    "use_txn": 0,
    "enable_blob_files": 0,
    "use_blob_db": 0,
    "ingest_external_file_one_in": 0,
    # PutEntity with timestamps is not yet implemented
    "use_put_entity_one_in" : 0,
}

tiered_params = {
    "enable_tiered_storage": 1,
    # Set tiered compaction hot data time as: 1 minute, 1 hour, 10 hour
    "preclude_last_level_data_seconds": lambda: random.choice([60, 3600, 36000]),
    # only test universal compaction for now, level has known issue of
    # endless compaction
    "compaction_style": 1,
    # tiered storage doesn't support blob db yet
    "enable_blob_files": 0,
    "use_blob_db": 0,
}

multiops_txn_default_params = {
    "test_cf_consistency": 0,
    "test_batches_snapshots": 0,
    "test_multi_ops_txns": 1,
    "use_txn": 1,
    "two_write_queues": lambda: random.choice([0, 1]),
    # TODO: enable write-prepared
    "disable_wal": 0,
    "use_only_the_last_commit_time_batch_for_recovery": lambda: random.choice([0, 1]),
    "clear_column_family_one_in": 0,
    "column_families": 1,
    "enable_pipelined_write": lambda: random.choice([0, 1]),
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
    # PutEntity in transactions is not yet implemented
    "use_put_entity_one_in" : 0,
}

multiops_wc_txn_params = {
    "txn_write_policy": 0,
    # TODO re-enable pipelined write. Not well tested atm
    "enable_pipelined_write": 0,
}

multiops_wp_txn_params = {
    "txn_write_policy": 1,
    "wp_snapshot_cache_bits": 1,
    # try small wp_commit_cache_bits, e.g. 0 once we explore storing full
    # commit sequence numbers in commit cache
    "wp_commit_cache_bits": 10,
    # pipeline write is not currnetly compatible with WritePrepared txns
    "enable_pipelined_write": 0,
    # OpenReadOnly after checkpoint is not currnetly compatible with WritePrepared txns
    "checkpoint_one_in": 0,
    # Required to be 1 in order to use commit-time-batch
    "use_only_the_last_commit_time_batch_for_recovery": 1,
    "clear_wp_commit_cache_one_in": 10,
    "create_timestamped_snapshot_one_in": 0,
}


def finalize_and_sanitize(src_params):
    dest_params = {k: v() if callable(v) else v for (k, v) in src_params.items()}
    if is_release_mode():
        dest_params["read_fault_one_in"] = 0
    if dest_params.get("compression_max_dict_bytes") == 0:
        dest_params["compression_zstd_max_train_bytes"] = 0
        dest_params["compression_max_dict_buffer_bytes"] = 0
    if dest_params.get("compression_type") != "zstd":
        dest_params["compression_zstd_max_train_bytes"] = 0
    if dest_params.get("allow_concurrent_memtable_write", 1) == 1:
        dest_params["memtablerep"] = "skip_list"
    if dest_params["mmap_read"] == 1:
        dest_params["use_direct_io_for_flush_and_compaction"] = 0
        dest_params["use_direct_reads"] = 0
        if dest_params["file_checksum_impl"] != "none":
            # TODO(T109283569): there is a bug in `GenerateOneFileChecksum()`,
            # used by `IngestExternalFile()`, causing it to fail with mmap
            # reads. Remove this once it is fixed.
            dest_params["ingest_external_file_one_in"] = 0
    if (
        dest_params["use_direct_io_for_flush_and_compaction"] == 1
        or dest_params["use_direct_reads"] == 1
    ) and not is_direct_io_supported(dest_params["db"]):
        if is_release_mode():
            print(
                "{} does not support direct IO. Disabling use_direct_reads and "
                "use_direct_io_for_flush_and_compaction.\n".format(dest_params["db"])
            )
            dest_params["use_direct_reads"] = 0
            dest_params["use_direct_io_for_flush_and_compaction"] = 0
        else:
            dest_params["mock_direct_io"] = True

    if dest_params["test_batches_snapshots"] == 1:
        dest_params["enable_compaction_filter"] = 0
        if dest_params["prefix_size"] < 0:
            dest_params["prefix_size"] = 1

    # Multi-key operations are not currently compatible with transactions or
    # timestamp.
    if (dest_params.get("test_batches_snapshots") == 1 or
        dest_params.get("use_txn") == 1 or
        dest_params.get("user_timestamp_size") > 0):
        dest_params["ingest_external_file_one_in"] = 0
    if (dest_params.get("test_batches_snapshots") == 1 or
        dest_params.get("use_txn") == 1):
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
    if (
        dest_params.get("disable_wal") == 1
        or dest_params.get("sync_fault_injection") == 1
        or dest_params.get("manual_wal_flush_one_in") > 0
    ):
        # File ingestion does not guarantee prefix-recoverability when unsynced
        # data can be lost. Ingesting a file syncs data immediately that is
        # newer than unsynced memtable data that can be lost on restart.
        #
        # Even if the above issue is fixed or worked around, our
        # trace-and-replay does not trace file ingestion, so in its current form
        # it would not recover the expected state to the correct point in time.
        dest_params["ingest_external_file_one_in"] = 0
        # The `DbStressCompactionFilter` can apply memtable updates to SST
        # files, which would be problematic when unsynced data can be lost in
        # crash recoveries.
        dest_params["enable_compaction_filter"] = 0
    # Only under WritePrepared txns, unordered_write would provide the same guarnatees as vanilla rocksdb
    if dest_params.get("unordered_write", 0) == 1:
        dest_params["txn_write_policy"] = 1
        dest_params["allow_concurrent_memtable_write"] = 1
    if dest_params.get("disable_wal", 0) == 1:
        dest_params["atomic_flush"] = 1
        dest_params["sync"] = 0
        dest_params["write_fault_one_in"] = 0
    if dest_params.get("open_files", 1) != -1:
        # Compaction TTL and periodic compactions are only compatible
        # with open_files = -1
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
    if dest_params.get("compaction_style", 0) == 2:
        # Disable compaction TTL in FIFO compaction, because right
        # now assertion failures are triggered.
        dest_params["compaction_ttl"] = 0
        dest_params["periodic_compaction_seconds"] = 0
    if dest_params["partition_filters"] == 1:
        if dest_params["index_type"] != 2:
            dest_params["partition_filters"] = 0
    if dest_params.get("atomic_flush", 0) == 1:
        # disable pipelined write when atomic flush is used.
        dest_params["enable_pipelined_write"] = 0
    if dest_params.get("sst_file_manager_bytes_per_sec", 0) == 0:
        dest_params["sst_file_manager_bytes_per_truncate"] = 0
    if dest_params.get("enable_compaction_filter", 0) == 1:
        # Compaction filter is incompatible with snapshots. Need to avoid taking
        # snapshots, as well as avoid operations that use snapshots for
        # verification.
        dest_params["acquire_snapshot_one_in"] = 0
        dest_params["compact_range_one_in"] = 0
        # Give the iterator ops away to reads.
        dest_params["readpercent"] += dest_params.get("iterpercent", 10)
        dest_params["iterpercent"] = 0
    if dest_params.get("prefix_size") == -1:
        dest_params["readpercent"] += dest_params.get("prefixpercent", 20)
        dest_params["prefixpercent"] = 0
    if (
        dest_params.get("prefix_size") == -1
        and dest_params.get("memtable_whole_key_filtering") == 0
    ):
        dest_params["memtable_prefix_bloom_size_ratio"] = 0
    if dest_params.get("two_write_queues") == 1:
        dest_params["enable_pipelined_write"] = 0
    if dest_params.get("best_efforts_recovery") == 1:
        dest_params["disable_wal"] = 1
        dest_params["atomic_flush"] = 0
        dest_params["enable_compaction_filter"] = 0
        dest_params["sync"] = 0
        dest_params["write_fault_one_in"] = 0
    if dest_params["secondary_cache_uri"] != "":
        # Currently the only cache type compatible with a secondary cache is LRUCache
        dest_params["cache_type"] = "lru_cache"
    # Remove the following once write-prepared/write-unprepared with/without
    # unordered write supports timestamped snapshots
    if dest_params.get("create_timestamped_snapshot_one_in", 0) > 0:
        dest_params["txn_write_policy"] = 0
        dest_params["unordered_write"] = 0
    # For TransactionDB, correctness testing with unsync data loss is currently
    # compatible with only write committed policy
    if (dest_params.get("use_txn") == 1 and dest_params.get("txn_write_policy") != 0):
        dest_params["sync_fault_injection"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
    # PutEntity is currently not supported by SstFileWriter or in conjunction with Merge
    if dest_params["use_put_entity_one_in"] != 0:
        dest_params["ingest_external_file_one_in"] = 0
        dest_params["use_merge"] = 0
        dest_params["use_full_merge_v1"] = 0

    return dest_params


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
    if args.test_best_efforts_recovery:
        params.update(best_efforts_recovery_params)
    if args.enable_ts:
        params.update(ts_params)
    if args.test_multiops_txn:
        params.update(multiops_txn_default_params)
        if args.write_policy == "write_committed":
            params.update(multiops_wc_txn_params)
        elif args.write_policy == "write_prepared":
            params.update(multiops_wp_txn_params)
    if args.test_tiered_storage:
        params.update(tiered_params)

    # Best-effort recovery, user defined timestamp, tiered storage are currently
    # incompatible with BlobDB. Test BE recovery if specified on the command
    # line; otherwise, apply BlobDB related overrides with a 10% chance.
    if (
        not args.test_best_efforts_recovery
        and not args.enable_ts
        and not args.test_tiered_storage
        and random.choice([0] * 9 + [1]) == 1
    ):
        params.update(blob_params)

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
    return params


def gen_cmd(params, unknown_params):
    finalzied_params = finalize_and_sanitize(params)
    cmd = (
        [stress_cmd]
        + [
            "--{0}={1}".format(k, v)
            for k, v in [(k, finalzied_params[k]) for k in sorted(finalzied_params)]
            if k
            not in {
                "test_type",
                "simple",
                "duration",
                "interval",
                "random_kill_odd",
                "cf_consistency",
                "txn",
                "test_best_efforts_recovery",
                "enable_ts",
                "test_multiops_txn",
                "write_policy",
                "stress_cmd",
                "test_tiered_storage",
                "cleanup_cmd",
            }
            and v is not None
        ]
        + unknown_params
    )
    return cmd


def execute_cmd(cmd, timeout):
    child = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print("Running db_stress with pid=%d: %s\n\n" % (child.pid, " ".join(cmd)))

    try:
        outs, errs = child.communicate(timeout=timeout)
        hit_timeout = False
        print("WARNING: db_stress ended before kill: exitcode=%d\n" % child.returncode)
    except subprocess.TimeoutExpired:
        hit_timeout = True
        child.kill()
        print("KILLED %d\n" % child.pid)
        outs, errs = child.communicate()

    return hit_timeout, child.returncode, outs.decode("utf-8"), errs.decode("utf-8")


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
        cmd = gen_cmd(
            dict(list(cmd_params.items()) + list({"db": dbname}.items())), unknown_args
        )

        hit_timeout, retcode, outs, errs = execute_cmd(cmd, cmd_params["interval"])

        if not hit_timeout:
            print("Exit Before Killing")
            print("stdout:")
            print(outs)
            print("stderr:")
            print(errs)
            sys.exit(2)

        for line in errs.split("\n"):
            if line != "" and not line.startswith("WARNING"):
                print("stderr has error message:")
                print("***" + line + "***")

        time.sleep(1)  # time to stabilize before the next run

        time.sleep(1)  # time to stabilize before the next run

    # we need to clean up after ourselves -- only do this on test success
    shutil.rmtree(dbname, True)


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
    while time.time() < exit_time:
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
            if random.randint(0, 1) == 1:
                additional_opts.update(
                    {
                        "num_levels": 1,
                    }
                )
        elif check_mode == 2:
            # normal run with FIFO compaction mode
            # ops_per_thread is divided by 5 because FIFO compaction
            # style is quite a bit slower on reads with lot of files
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"] // 5,
                "compaction_style": 2,
            }
        else:
            # normal run
            additional_opts = {
                "kill_random_test": None,
                "ops_per_thread": cmd_params["ops_per_thread"],
            }

        cur_compaction_style = additional_opts.get("compaction_style", cmd_params.get("compaction_style", 0))
        if prev_compaction_style != -1 and prev_compaction_style != cur_compaction_style:
            print("`compaction_style` is changed in current run so `destroy_db_initially` is set to 1 as a short-term solution to avoid cycling through previous db of different compaction style." + "\n")
            additional_opts["destroy_db_initially"] = 1
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
        hit_timeout, retncode, stdoutdata, stderrdata = execute_cmd(
            cmd, exit_time - time.time() + 900
        )
        msg = "check_mode={0}, kill option={1}, exitcode={2}\n".format(
            check_mode, additional_opts["kill_random_test"], retncode
        )

        print(msg)
        print(stdoutdata)
        print(stderrdata)

        if hit_timeout:
            print("Killing the run for running too long")
            break

        expected = False
        if additional_opts["kill_random_test"] is None and (retncode == 0):
            # we expect zero retncode if no kill option
            expected = True
        elif additional_opts["kill_random_test"] is not None and retncode <= 0:
            # When kill option is given, the test MIGHT kill itself.
            # If it does, negative retncode is expected. Otherwise 0.
            expected = True

        if not expected:
            print("TEST FAILED. See kill option and exit code above!!!\n")
            sys.exit(1)

        stderrdata = stderrdata.lower()
        errorcount = stderrdata.count("error") - stderrdata.count("got errors 0 times")
        print("#times error occurred in output is " + str(errorcount) + "\n")

        if errorcount > 0:
            print("TEST FAILED. Output has 'error'!!!\n")
            sys.exit(2)
        if stderrdata.find("fail") >= 0:
            print("TEST FAILED. Output has 'fail'!!!\n")
            sys.exit(2)

        # First half of the duration, keep doing kill test. For the next half,
        # try different modes.
        if time.time() > half_time:
            # we need to clean up after ourselves -- only do this on test
            # success
            shutil.rmtree(dbname, True)
            if cleanup_cmd is not None:
                print("Running DB cleanup command - %s\n" % cleanup_cmd)
                ret = os.system(cleanup_cmd)
                if ret != 0:
                    print("TEST FAILED. DB cleanup returned error %d\n" % ret)
                    sys.exit(1)
            os.mkdir(dbname)
            if (expected_values_dir is not None):
                shutil.rmtree(expected_values_dir, True)
                os.mkdir(expected_values_dir)

            check_mode = (check_mode + 1) % total_check_mode

        time.sleep(1)  # time to stabilize after a kill


def main():
    global stress_cmd
    global cleanup_cmd

    parser = argparse.ArgumentParser(
        description="This script runs and kills \
        db_stress multiple times"
    )
    parser.add_argument("test_type", choices=["blackbox", "whitebox"])
    parser.add_argument("--simple", action="store_true")
    parser.add_argument("--cf_consistency", action="store_true")
    parser.add_argument("--txn", action="store_true")
    parser.add_argument("--test_best_efforts_recovery", action="store_true")
    parser.add_argument("--enable_ts", action="store_true")
    parser.add_argument("--test_multiops_txn", action="store_true")
    parser.add_argument("--write_policy", choices=["write_committed", "write_prepared"])
    parser.add_argument("--stress_cmd")
    parser.add_argument("--test_tiered_storage", action="store_true")
    parser.add_argument("--cleanup_cmd")

    all_params = dict(
        list(default_params.items())
        + list(blackbox_default_params.items())
        + list(whitebox_default_params.items())
        + list(simple_default_params.items())
        + list(blackbox_simple_default_params.items())
        + list(whitebox_simple_default_params.items())
        + list(blob_params.items())
        + list(ts_params.items())
        + list(multiops_txn_default_params.items())
        + list(multiops_wc_txn_params.items())
        + list(multiops_wp_txn_params.items())
        + list(best_efforts_recovery_params.items())
        + list(cf_consistency_params.items())
        + list(tiered_params.items())
        + list(txn_params.items())
    )

    for k, v in all_params.items():
        parser.add_argument("--" + k, type=type(v() if callable(v) else v))
    # unknown_args are passed directly to db_stress
    args, unknown_args = parser.parse_known_args()

    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is not None and not os.path.isdir(test_tmpdir):
        print(
            "%s env var is set to a non-existent directory: %s"
            % (_TEST_DIR_ENV_VAR, test_tmpdir)
        )
        sys.exit(1)

    if args.stress_cmd:
        stress_cmd = args.stress_cmd
    if args.cleanup_cmd:
        cleanup_cmd = args.cleanup_cmd
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
