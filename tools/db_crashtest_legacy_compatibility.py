#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the COPYING file in the root directory)
#  and the Apache 2.0 License (found in the LICENSE.Apache file in the root directory).

#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
"""Legacy db_crashtest feature compatibility sanitizer.

This module intentionally preserves the old in-place sanitizer so stress-test
production can switch back during dependency-solver rollout without reverting
all solver code.
"""

import random
import sys


def run_legacy_compatibility_sanitizer(
    dest_params,
    *,
    is_release_mode,
    is_direct_io_supported,
    is_remote_db,
):
    """Apply the old compatibility sanitizer in-place."""
    if is_release_mode():
        dest_params["read_fault_one_in"] = 0
    if dest_params.get("compression_max_dict_bytes") == 0:
        dest_params["compression_zstd_max_train_bytes"] = 0
        dest_params["compression_max_dict_buffer_bytes"] = 0
    if dest_params.get("compression_type") != "zstd":
        dest_params["compression_zstd_max_train_bytes"] = 0
    if dest_params["mmap_read"] == 1:
        dest_params["use_direct_io_for_flush_and_compaction"] = 0
        dest_params["use_direct_reads"] = 0
        dest_params["use_direct_io_for_compaction_reads"] = 0
        dest_params["multiscan_use_async_io"] = 0
    if dest_params.get("min_tombstones_for_range_conversion", 0) > 0:
        # SQFC range-query filtering installs ReadOptions::table_filter on
        # iterators. Read-write iterators reject table_filter when read-path
        # range tombstone conversion is enabled, because conversion must see
        # the full relevant SST set before synthesizing a memtable tombstone.
        dest_params["use_sqfc_for_range_queries"] = 0
        # Delete range not compatible with inplace_update_support
        dest_params["inplace_update_support"] = 0
    if (
        dest_params["use_direct_io_for_flush_and_compaction"] == 1
        or dest_params["use_direct_reads"] == 1
        or dest_params["use_direct_io_for_compaction_reads"] == 1
    ) and not is_direct_io_supported(dest_params["db"]):
        if is_release_mode():
            print(
                "{} does not support direct IO. Disabling use_direct_reads, "
                "use_direct_io_for_compaction_reads and "
                "use_direct_io_for_flush_and_compaction.\n".format(dest_params["db"])
            )
            dest_params["use_direct_reads"] = 0
            dest_params["use_direct_io_for_compaction_reads"] = 0
            dest_params["use_direct_io_for_flush_and_compaction"] = 0
        else:
            dest_params["mock_direct_io"] = True

    # Blob direct write requires concurrent read visibility of files still open
    # for writing (BlobFileReader calls GetFileSize() on active partition files).
    # Remote file systems such as Warm Storage do not guarantee that writes from
    # a WritableFile are visible to a separate RandomAccessFile until the writer
    # is closed, causing "Malformed blob file" corruption.  Disable BDW when a
    # remote --env_uri / --fs_uri is in use.
    if is_remote_db:
        dest_params["enable_blob_direct_write"] = 0

    if dest_params.get("enable_blob_direct_write", 0) == 1:
        # Keep blob direct write in its reduced-scope crash-test profile.
        #
        # Supported recovery shape:
        #   * clean shutdown / flush
        #   * crash restart that only relies on SST + manifest-visible blob
        #     state, including wide-column entities stored through PutEntity
        #
        # Unsupported here:
        #   * WAL replay / best-efforts recovery
        #   * broad dynamic blob option changes and blob GC
        #   * merge / transaction variants and parallel write queue modes
        #   * secondary / backup / checkpoint / ingest style APIs that reason
        #     about active files directly
        dest_params["enable_blob_files"] = 1
        dest_params["blob_direct_write_partitions"] = max(
            1, dest_params.get("blob_direct_write_partitions", 1)
        )
        # BDW can still run with multiple application threads as long as writes
        # stay on the ordered write path. Disable the write modes that would
        # let memtable/WAL publication diverge from the transformed blob-file
        # write ordering.
        dest_params["allow_concurrent_memtable_write"] = 0
        dest_params["enable_pipelined_write"] = 0
        dest_params["two_write_queues"] = 0
        dest_params["unordered_write"] = 0
        # Keep inplace updates off. The later inplace_update_support
        # sanitization intentionally forces disable_wal=0 for its own recovery
        # assumptions, which would silently undo the BDW crash-test profile.
        dest_params["inplace_update_support"] = 0
        # Direct write is implemented only for integrated BlobDB, without
        # dynamic blob option changes or background blob GC.
        dest_params["use_blob_db"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["enable_blob_garbage_collection"] = 0
        dest_params["blob_garbage_collection_age_cutoff"] = 0.0
        dest_params["blob_garbage_collection_force_threshold"] = 1.0
        dest_params["blob_compaction_readahead_size"] = 0
        dest_params["blob_file_starting_level"] = 0
        dest_params["use_merge"] = 0
        dest_params["use_full_merge_v1"] = 0
        dest_params["use_timed_put_one_in"] = 0
        # Wide-column PutEntity/GetEntity/MultiGetEntity are now compatible
        # with this profile. AttributeGroup exercises a different path that
        # still stays disabled here.
        dest_params["use_attribute_group"] = 0
        # Direct write stress only supports the plain comparator / key encoding
        # path. User-defined timestamps and the TransactionDB-only timestamped
        # snapshot API are outside this feature envelope.
        dest_params["user_timestamp_size"] = 0
        dest_params["persist_user_defined_timestamps"] = 0
        dest_params["create_timestamped_snapshot_one_in"] = 0
        dest_params["use_txn"] = 0
        dest_params["txn_write_policy"] = 0
        dest_params["use_optimistic_txn"] = 0
        dest_params["test_multi_ops_txns"] = 0
        dest_params["commit_bypass_memtable_one_in"] = 0
        # Force the WAL-disabled crash-test profile. The generic disable_wal
        # sanitization below still applies, but keep the WAL-dependent stress
        # features explicitly off here so later sanitizers or explicit command
        # line overrides do not silently re-enable them.
        dest_params["disable_wal"] = 1
        dest_params["best_efforts_recovery"] = 0
        # Direct write v1 crash testing does not cover reopen with unlogged
        # data, manual/sync WAL persistence, or WAL metadata/locking APIs.
        dest_params["reopen"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
        dest_params["sync_wal_one_in"] = 0
        dest_params["lock_wal_one_in"] = 0
        dest_params["get_sorted_wal_files_one_in"] = 0
        dest_params["get_current_wal_file_one_in"] = 0
        dest_params["track_and_verify_wals"] = 0
        dest_params["rate_limit_auto_wal_flush"] = 0
        dest_params["recycle_log_file_num"] = 0
        # Write/open fault injection currently assumes WAL-based recovery or
        # error-retry behavior that direct write v1 does not provide.
        dest_params["sync_fault_injection"] = 0
        dest_params["write_fault_one_in"] = 0
        dest_params["metadata_write_fault_one_in"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["metadata_read_fault_one_in"] = 0
        dest_params["open_metadata_write_fault_one_in"] = 0
        dest_params["open_metadata_read_fault_one_in"] = 0
        dest_params["open_write_fault_one_in"] = 0
        dest_params["open_read_fault_one_in"] = 0
        # Remote compaction, secondary readers, file snapshot style APIs, and
        # ingest APIs are outside the initial direct-write feature envelope.
        dest_params["remote_compaction_worker_threads"] = 0
        dest_params["test_secondary"] = 0
        dest_params["backup_one_in"] = 0
        dest_params["checkpoint_one_in"] = 0
        dest_params["get_live_files_apis_one_in"] = 0
        dest_params["ingest_external_file_one_in"] = 0
        dest_params["ingest_wbwi_one_in"] = 0

    if dest_params.get("memtablerep") == "vector":
        dest_params["inplace_update_support"] = 0

    # only skip list memtable representation supports paranoid memory checks
    if dest_params.get("memtablerep") != "skip_list":
        dest_params["paranoid_memory_checks"] = 0
        dest_params["memtable_verify_per_key_checksum_on_seek"] = 0

    if dest_params["test_batches_snapshots"] == 1:
        dest_params["enable_compaction_filter"] = 0
        dest_params["inplace_update_support"] = 0
        # TODO(hx235): enable test_batches_snapshots with fault injection after stabilizing the CI
        dest_params["write_fault_one_in"] = 0
        dest_params["metadata_write_fault_one_in"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["metadata_read_fault_one_in"] = 0
        dest_params["use_multiscan"] = 0
        if dest_params["prefix_size"] < 0:
            dest_params["prefix_size"] = 1

    # BER disables WAL and tests unsynced data loss which
    # does not work with inplace_update_support. Integrated BlobDB is also
    # incompatible, so force blob-related toggles off even if they came from
    # command-line overrides or another preset.
    if dest_params.get("best_efforts_recovery") == 1:
        dest_params["inplace_update_support"] = 0
        dest_params["enable_blob_files"] = 0
        dest_params["enable_blob_garbage_collection"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["enable_blob_direct_write"] = 0

    # Remote Compaction Incompatible Tests and Features
    if dest_params.get("remote_compaction_worker_threads", 0) > 0:
        # TODO Fix races when both Remote Compaction + BlobDB enabled
        dest_params["enable_blob_files"] = 0
        dest_params["enable_blob_garbage_collection"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        # Disable Incompatible Ones
        dest_params["inplace_update_support"] = 0
        dest_params["checkpoint_one_in"] = 0
        dest_params["use_timed_put_one_in"] = 0
        dest_params["test_secondary"] = 0
        dest_params["mmap_read"] = 0
        # skip_stats_update_on_db_open leaves num_entries and
        # num_range_deletions at 0 on the remote worker, which breaks
        # standalone range deletion file filtering in compaction and causes
        # input key count mismatch. This can happen even if standalone range
        # deletion ingestion is off in the current run, because such files
        # may have been ingested in a previous run with different options.
        # TODO: remove after the real fix lands.
        dest_params["skip_stats_update_on_db_open"] = 0

        # Disable database open fault injection to prevent test inefficiency described below.
        # When fault injection occurs during DB open, the db will wait for compaction
        # to finish to clean up the database before retrying without injected error.
        # However remote compaction threads are not yet created at that point
        # so the db has to wait for the timeout (currently 30 seconds) to fall back to
        # local compaction in order for the compaction to finish.
        #
        # TODO: Consider moving compaction thread creation earlier in the startup sequence
        # to allow db open fault injection testing without this performance penalty
        dest_params["open_metadata_write_fault_one_in"] = 0
        dest_params["open_metadata_read_fault_one_in"] = 0
        dest_params["open_write_fault_one_in"] = 0
        dest_params["open_read_fault_one_in"] = 0
        dest_params["sync_fault_injection"] = 0

    # UDI now supports all operation types and all iteration directions.
    # Only parallel compression and mmap_read remain incompatible.
    if dest_params.get("use_trie_index") == 1:
        # Trie UDI uses zero-copy pointers into block data, which is
        # incompatible with mmap_read.
        dest_params["mmap_read"] = 0
        # Parallel compression is incompatible with UDI
        dest_params["compression_parallel_threads"] = 1
        if dest_params.get("use_udi_as_primary_index") == 1:
            # Primary UDI mode: the standard index is still fully populated,
            # but partitioned index (kTwoLevelIndexSearch) and partitioned
            # filters are not compatible with the UDI wrapper layout.
            dest_params["index_type"] = random.choice([0, 0, 3])
            dest_params["partition_filters"] = 0
            # Backup/restore serializes Options to strings, losing the
            # user_defined_index_factory (shared_ptr). The restored DB
            # opens without UDI support and cannot route reads through
            # the trie in primary mode.
            dest_params["backup_one_in"] = 0
            # Secondary DB opens SSTs with default Options (not a copy of
            # the primary's), losing the UDI factory. Without the factory,
            # reads cannot be routed through the trie.
            dest_params["test_secondary"] = 0
    else:
        # use_udi_as_primary_index requires use_trie_index
        dest_params["use_udi_as_primary_index"] = 0

    # Multi-key operations are not currently compatible with transactions or
    # timestamp.
    if (
        dest_params.get("test_batches_snapshots") == 1
        or dest_params.get("use_txn") == 1
        or dest_params.get("user_timestamp_size", 0) > 0
    ):
        dest_params["ingest_external_file_one_in"] = 0
    if (
        dest_params.get("test_batches_snapshots") == 1
        or dest_params.get("use_txn") == 1
    ):
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
    # Since the value of inplace_update_support needs to be fixed across runs,
    # we disable other incompatible options here instead of disabling
    # inplace_update_support based on other option values, which may change
    # across runs.
    if dest_params["inplace_update_support"] == 1:
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
        dest_params["readpercent"] += dest_params["prefixpercent"]
        dest_params["prefixpercent"] = 0
        dest_params["allow_concurrent_memtable_write"] = 0
        # inplace_update_support does not update sequence number. Our stress test recovery
        # logic for unsynced data loss relies on max sequence number stored
        # in MANIFEST, so they don't work together.
        dest_params["sync_fault_injection"] = 0
        dest_params["disable_wal"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
    if (
        dest_params.get("sync_fault_injection") == 1
        or dest_params.get("disable_wal") == 1
        or dest_params.get("manual_wal_flush_one_in", 0) > 0
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
    # Remove the following once write-prepared/write-unprepared with/without
    # unordered write supports timestamped snapshots
    if dest_params.get("create_timestamped_snapshot_one_in", 0) > 0:
        dest_params["unordered_write"] = 0
        if dest_params.get("txn_write_policy", 0) != 0:
            dest_params["create_timestamped_snapshot_one_in"] = 0
    # Only under WritePrepared txns, unordered_write would provide the same guarnatees as vanilla rocksdb
    # unordered_write is only enabled with --txn, and txn_params disables inplace_update_support, so
    # setting allow_concurrent_memtable_write=1 won't conflcit with inplace_update_support.
    # don't overwrite txn_write_policy
    if dest_params.get("unordered_write", 0) == 1:
        if dest_params.get("txn_write_policy", 0) == 1:
            dest_params["allow_concurrent_memtable_write"] = 1
        else:
            dest_params["unordered_write"] = 0
    if dest_params.get("disable_wal", 0) == 1:
        # WAL-disabled stress runs do not support in-process reopen. Blob
        # direct write v1 relies on this path so crash testing stays within its
        # SST/blob-manifest recovery envelope rather than WAL replay.
        dest_params["atomic_flush"] = 1
        dest_params["sync"] = 0
        dest_params["write_fault_one_in"] = 0
        dest_params["reopen"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
        # disableWAL and recycle_log_file_num options are not mutually
        # compatible at the moment
        dest_params["recycle_log_file_num"] = 0
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
        # FIFO compaction is not supported with open_files_async
        dest_params["open_files_async"] = 0
        # Disable irrelevant tiering options
        dest_params["preclude_last_level_data_seconds"] = 0
        dest_params["last_level_temperature"] = "kUnknown"
    else:
        # Disable irrelevant tiering options
        dest_params["file_temperature_age_thresholds"] = ""
        # Disable FIFO-specific options for non-FIFO compaction styles
        dest_params["fifo_compaction_max_data_files_size_mb"] = 0
        dest_params["fifo_compaction_max_table_files_size_mb"] = 0
        dest_params["fifo_compaction_use_kv_ratio_compaction"] = 0
    if dest_params["partition_filters"] == 1:
        if dest_params["index_type"] != 2:
            dest_params["partition_filters"] = 0
    if dest_params.get("atomic_flush", 0) == 1:
        # disable pipelined write when atomic flush is used.
        dest_params["enable_pipelined_write"] = 0
    # Truncating SST files in primary DB is incompatible
    # with secondary DB since the latter can't read the shared
    # and truncated SST file correctly
    if (
        dest_params.get("sst_file_manager_bytes_per_sec", 0) == 0
        or dest_params.get("test_secondary") == 1
    ):
        dest_params["sst_file_manager_bytes_per_truncate"] = 0
    if dest_params.get("prefix_size") == -1:
        dest_params["readpercent"] += dest_params.get("prefixpercent", 20)
        dest_params["prefixpercent"] = 0
    elif dest_params.get("simple") and dest_params.get("test_type") == "blackbox":
        # `db_stress` randomizes iterate_lower_bound independently of the seek
        # target. With a configured prefix extractor this can violate
        # ReadOptions' same-prefix requirement, so disable random iterator
        # operations in simple blackbox mode.
        dest_params["readpercent"] += dest_params.get("iterpercent", 10)
        dest_params["iterpercent"] = 0
    if (
        dest_params.get("prefix_size") == -1
        and dest_params.get("memtable_whole_key_filtering") == 0
    ):
        dest_params["memtable_prefix_bloom_size_ratio"] = 0
    if dest_params.get("two_write_queues") == 1:
        dest_params["enable_pipelined_write"] = 0
    if dest_params.get("best_efforts_recovery") == 1:
        dest_params["disable_wal"] = 1
        dest_params["enable_compaction_filter"] = 0
        dest_params["sync"] = 0
        dest_params["write_fault_one_in"] = 0
        dest_params["skip_verifydb"] = 1
        dest_params["verify_db_one_in"] = 0
    # For TransactionDB, correctness testing with unsync data loss is currently
    # compatible with only write committed policy
    if dest_params.get("use_txn") == 1 and dest_params.get("txn_write_policy", 0) != 0:
        dest_params["sync_fault_injection"] = 0
        dest_params["disable_wal"] = 0
        dest_params["manual_wal_flush_one_in"] = 0
        # Wide-column pessimistic transaction APIs are initially supported for
        # WriteCommitted only
        dest_params["use_put_entity_one_in"] = 0
        # MultiCfIterator is currently only compatible with write committed policy
        dest_params["use_multi_cf_iterator"] = 0
        # only works with write committed policy
        dest_params["commit_bypass_memtable_one_in"] = 0
        # not compatible with Remote Compaction yet
        dest_params["remote_compaction_worker_threads"] = 0
    # TODO(hx235): enable test_multi_ops_txns with fault injection after stabilizing the CI
    if dest_params.get("test_multi_ops_txns") == 1:
        dest_params["write_fault_one_in"] = 0
        dest_params["metadata_write_fault_one_in"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["metadata_read_fault_one_in"] = 0
        if dest_params.get("txn_write_policy", 0) != 0:
            # TODO: should any of this change for WUP (txn_write_policy==2)?
            dest_params["wp_snapshot_cache_bits"] = 1
            # try small wp_commit_cache_bits, e.g. 0 once we explore storing full
            # commit sequence numbers in commit cache
            dest_params["wp_commit_cache_bits"] = 10
            # pipeline write is not currnetly compatible with WritePrepared txns
            dest_params["enable_pipelined_write"] = 0
            # OpenReadOnly after checkpoint is not currently compatible with WritePrepared txns
            dest_params["checkpoint_one_in"] = 0
            # Required to be 1 in order to use commit-time-batch
            dest_params["use_only_the_last_commit_time_batch_for_recovery"] = 1
            dest_params["clear_wp_commit_cache_one_in"] = 10
            # sequence number can be advanced in SwitchMemtable::WriteRecoverableState() for WP.
            # disable it for now until we find another way to test LockWAL().
            dest_params["lock_wal_one_in"] = 0

    # Wide column stress tests require FullMergeV3
    if dest_params["use_put_entity_one_in"] != 0:
        dest_params["use_full_merge_v1"] = 0
    if dest_params["file_checksum_impl"] == "none":
        dest_params["verify_file_checksums_one_in"] = 0
    if dest_params["write_fault_one_in"] > 0:
        # background work may be disabled while DB is resuming after some error
        dest_params["max_write_buffer_number"] = max(
            dest_params["max_write_buffer_number"], 10
        )
    if dest_params["secondary_cache_uri"].find("compressed_secondary_cache") >= 0:
        dest_params["compressed_secondary_cache_size"] = 0
        dest_params["compressed_secondary_cache_ratio"] = 0.0
    if dest_params["cache_type"].find("tiered_") >= 0:
        if dest_params["compressed_secondary_cache_size"] > 0:
            dest_params["compressed_secondary_cache_ratio"] = float(
                dest_params["compressed_secondary_cache_size"]
                / (
                    dest_params["cache_size"]
                    + dest_params["compressed_secondary_cache_size"]
                )
            )
            dest_params["compressed_secondary_cache_size"] = 0
        else:
            dest_params["compressed_secondary_cache_ratio"] = 0.0
            dest_params["cache_type"] = dest_params["cache_type"].replace("tiered_", "")
    else:
        if dest_params["secondary_cache_uri"]:
            dest_params["compressed_secondary_cache_size"] = 0
            dest_params["compressed_secondary_cache_ratio"] = 0.0
    if dest_params["use_write_buffer_manager"]:
        if dest_params["cache_size"] <= 0 or dest_params["db_write_buffer_size"] <= 0:
            dest_params["use_write_buffer_manager"] = 0
    if (
        dest_params["user_timestamp_size"] > 0
        and dest_params["persist_user_defined_timestamps"] == 0
    ):
        # Features that are not compatible with UDT in memtable only feature.
        dest_params["enable_blob_files"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["atomic_flush"] = 0
        dest_params["allow_concurrent_memtable_write"] = 0
        dest_params["block_protection_bytes_per_key"] = 0
        # TODO(yuzhangyu): make stress test logic handle this and enable testing
        # these APIs.
        # These operations need to compare side to side one operation with another.
        # It's hard to guarantee their consistency because when timestamps can be
        # collapsed, only operations using the same SuperVersion can be consistent
        # with each other. There is no external APIs to ensure that.
        dest_params["use_multiget"] = 0
        dest_params["use_multi_get_entity"] = 0
        dest_params["readpercent"] += dest_params.get("iterpercent", 10)
        dest_params["iterpercent"] = 0
        # Only best efforts recovery test support disabling wal and
        # disable atomic flush.
        if dest_params["test_best_efforts_recovery"] == 0:
            dest_params["disable_wal"] = 0
    if dest_params.get("user_timestamp_size", 0) > 0:
        # Interpolation search requires BytewiseComparator but user-defined
        # timestamps use BytewiseComparatorWithU64TsWrapper.
        dest_params["index_block_search_type"] = 0
        # TrieIndexFactory requires BytewiseComparator.
        dest_params["use_trie_index"] = 0
    if (
        dest_params.get("enable_compaction_filter", 0) == 1
        or dest_params.get("inplace_update_support", 0) == 1
    ):
        # Compaction filter, inplace update support are incompatible with snapshots. Need to avoid taking
        # snapshots, as well as avoid operations that use snapshots for
        # verification.
        dest_params["acquire_snapshot_one_in"] = 0
        dest_params["compact_range_one_in"] = 0
        # Redistribute to maintain 100% total
        dest_params["readpercent"] += dest_params.get(
            "iterpercent", 10
        ) + dest_params.get("prefixpercent", 20)
        dest_params["iterpercent"] = 0
        dest_params["prefixpercent"] = 0
        dest_params["check_multiget_consistency"] = 0
        dest_params["check_multiget_entity_consistency"] = 0
    if dest_params.get("disable_wal") == 0:
        if (
            dest_params.get("reopen", 0) > 0
            or (
                dest_params.get("manual_wal_flush_one_in")
                and dest_params.get("column_families") != 1
            )
            or (
                dest_params.get("use_txn") != 0
                and dest_params.get("use_optimistic_txn") == 0
            )
        ):
            # 1. Reopen with WAL currently requires persisting WAL data before closing for reopen.
            # Previous injected WAL write errors may not be cleared by the time of closing and ready
            # for persisting WAL.
            # To simplify, we disable any WAL write error injection.
            # TODO(hx235): support WAL write error injection with reopen
            #
            # 2. WAL write failure can drop buffered WAL data. This can cause
            # inconsistency when one CF has a successful flush during auto
            # recovery. Disable the fault injection in this path for now until
            # we have a fix that allows auto recovery.
            #
            # 3. Pessimistic transactions use 2PC, which can't auto-recover from WAL write errors.
            # This is because RocksDB cannot easily discard the corrupted WAL without risking the
            # loss of uncommitted prepared data within the same WAL.
            # Therefore disabling WAL write error injection in stress tests to prevent crashing
            # since stress test does not support injecting errors that can' be auto-recovered.
            #
            # TODO(hx235): support excluding WAL from metadata write fault injection so we don't
            # have to disable metadata write fault injection to other file
            dest_params["exclude_wal_from_write_fault_injection"] = 1
            dest_params["metadata_write_fault_one_in"] = 0
    # Disabling block align if mixed manager is being used
    if dest_params.get("compression_manager") == "custom":
        if dest_params.get("block_align") == 1:
            dest_params["block_align"] = 0
        if dest_params["format_version"] < 7:
            dest_params["format_version"] = 7
    elif (
        dest_params.get("compression_manager") == "mixed"
        or dest_params.get("compression_manager") == "randommixed"
    ):
        dest_params["block_align"] = 0
    elif dest_params.get("compression_manager") == "autoskip":
        # ensuring the compression is being used
        if dest_params.get("compression_type") == "none":
            dest_params["compression_type"] = random.choice(
                ["snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
            )
        if dest_params.get("bottommost_compression_type") == "none":
            dest_params["bottommost_compression_type"] = random.choice(
                ["snappy", "zlib", "lz4", "lz4hc", "xpress", "zstd"]
            )
        dest_params["block_align"] = 0
    else:
        # Enabling block_align with compression is not supported
        if dest_params.get("block_align") == 1:
            dest_params["compression_type"] = "none"
            dest_params["bottommost_compression_type"] = "none"
    # If periodic_compaction_seconds is not set, daily_offpeak_time_utc doesn't do anything
    if dest_params.get("periodic_compaction_seconds") == 0:
        dest_params["daily_offpeak_time_utc"] = ""
    # `use_put_entity_one_in` cannot be enabled/disabled across runs, modify
    # `use_timed_put_one_in` option so that they make sense together.
    if dest_params.get("use_put_entity_one_in") == 1:
        dest_params["use_timed_put_one_in"] = 0
    elif (
        dest_params.get("use_put_entity_one_in", 0) > 1
        and dest_params.get("use_timed_put_one_in") == 1
    ):
        dest_params["use_timed_put_one_in"] = 3
    if (
        dest_params.get("write_dbid_to_manifest") == 0
        and dest_params.get("write_identity_file") == 0
    ):
        # At least one must be true
        dest_params["write_dbid_to_manifest"] = 1
    # Checkpoint creation skips flush if the WAL is locked, so enabling lock_wal_one_in
    # can cause checkpoint verification to fail. So make the two mutually exclusive.
    if dest_params.get("checkpoint_one_in") != 0:
        dest_params["lock_wal_one_in"] = 0
    if (
        dest_params.get("ingest_external_file_one_in") == 0
        or dest_params.get("delrangepercent") == 0
    ):
        dest_params["test_ingest_standalone_range_deletion_one_in"] = 0
    if (
        dest_params.get("use_txn", 0) == 1
        and dest_params.get("commit_bypass_memtable_one_in", 0) > 0
    ):
        dest_params["enable_blob_files"] = 0
        dest_params["allow_setting_blob_options_dynamically"] = 0
        dest_params["allow_concurrent_memtable_write"] = 0
        dest_params["use_put_entity_one_in"] = 0
        dest_params["use_get_entity"] = 0
        dest_params["use_multi_get_entity"] = 0
        dest_params["enable_pipelined_write"] = 0
        dest_params["use_attribute_group"] = 0
    if (
        dest_params.get("enable_pipelined_write", 0)
        or dest_params.get("unordered_write", 0)
        or dest_params.get("disable_wal", 0) == 0
        or dest_params.get("user_timestamp_size", 0)
    ):
        dest_params["ingest_wbwi_one_in"] = 0
    # Continuous verification fails with secondaries inside NonBatchedOpsStressTest
    if dest_params.get("test_secondary") == 1:
        dest_params["continuous_verification_interval"] = 0
    if dest_params.get("use_multiscan") == 1:
        dest_params["async_io"] = 0
        dest_params["delpercent"] += dest_params["delrangepercent"]
        dest_params["delrangepercent"] = 0
        dest_params["prefix_size"] = -1
        dest_params["iterpercent"] += dest_params["prefixpercent"]
        dest_params["prefixpercent"] = 0
        dest_params["read_fault_one_in"] = 0
        dest_params["memtable_prefix_bloom_size_ratio"] = 0
        dest_params["max_sequential_skip_in_iterations"] = sys.maxsize
        dest_params["min_tombstones_for_range_conversion"] = 0
        # This option ingests a delete range that might partially overlap with
        # existing key range, which will cause a reseek that's currently not
        # supported by multiscan
        dest_params["test_ingest_standalone_range_deletion_one_in"] = 0
        # LevelIterator multiscan currently relies on num_entries and num_range_deletions,
        # which are not updated if skip_stats_update_on_db_open is true
        dest_params["skip_stats_update_on_db_open"] = 0
        dest_params["multiscan_max_prefetch_memory_bytes"] = random.choice(
            [0, 0, 64 * 1024, 256 * 1024]
        )

    # open_files_async requires skip_stats_update_on_db_open to avoid
    # synchronous I/O in UpdateAccumulatedStats during DB open
    if dest_params.get("skip_stats_update_on_db_open", 0) == 0:
        dest_params["open_files_async"] = 0

    # inplace update and key checksum verification during seek would cause race condition
    # Therefore, when inplace_update_support is enabled, disable memtable_verify_per_key_checksum_on_seek
    if dest_params["inplace_update_support"] == 1:
        dest_params["memtable_verify_per_key_checksum_on_seek"] = 0

    # allow_resumption requires remote compaction
    if dest_params.get("remote_compaction_worker_threads", 0) == 0:
        dest_params["allow_resumption_one_in"] = 0

    # When read-triggered compaction is enabled, use a short periodic trigger
    # interval so that the feature gets exercised on a quiet DB.
    if dest_params.get("read_triggered_compaction_threshold", 0) > 0:
        dest_params["max_compaction_trigger_wakeup_seconds"] = 20

    # Batch/snapshot stress relies on WAL-backed recovery semantics. Keep it
    # off for every finalized WAL-disabled profile, including blob direct
    # write presets that force disable_wal=1 earlier in sanitization.
    if dest_params.get("disable_wal", 0) == 1:
        dest_params["test_batches_snapshots"] = 0

    if dest_params.get("num_dbs", 1) > 1:
        # These features assume a single DB instance.
        # See ValidateNumDbsFlags() in db_stress_flag_validator.cc for C++
        # guards.
        dest_params["clear_column_family_one_in"] = 0
        dest_params["test_multi_ops_txns"] = 0

    return dest_params
