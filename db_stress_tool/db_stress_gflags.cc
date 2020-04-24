//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"

static bool ValidateUint32Range(const char* flagname, uint64_t value) {
  if (value > std::numeric_limits<uint32_t>::max()) {
    fprintf(stderr, "Invalid value for --%s: %lu, overflow\n", flagname,
            (unsigned long)value);
    return false;
  }
  return true;
}

DEFINE_uint64(seed, 2341234, "Seed for PRNG");
static const bool FLAGS_seed_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_seed, &ValidateUint32Range);

DEFINE_bool(read_only, false, "True if open DB in read-only mode during tests");

DEFINE_int64(max_key, 1 * KB * KB,
             "Max number of key/values to place in database");

DEFINE_int32(max_key_len, 3, "Maximum length of a key in 8-byte units");

DEFINE_string(key_len_percent_dist, "",
              "Percentages of keys of various lengths. For example, 1,30,69 "
              "means 1% of keys are 8 bytes, 30% are 16 bytes, and 69% are "
              "24 bytes. If not specified, it will be evenly distributed");

DEFINE_int32(key_window_scale_factor, 10,
              "This value will be multiplied by 100 to come up with a window "
              "size for varying the key length");

DEFINE_int32(column_families, 10, "Number of column families");

DEFINE_double(
    hot_key_alpha, 0,
    "Use Zipfian distribution to generate the key "
    "distribution. If it is not specified, write path will use random "
    "distribution to generate the keys. The parameter is [0, double_max]). "
    "However, the larger alpha is, the more shewed will be. If alpha is "
    "larger than 2, it is likely that only 1 key will be accessed. The "
    "Recommended value is [0.8-1.5]. The distribution is also related to "
    "max_key and total iterations of generating the hot key. ");

DEFINE_string(
    options_file, "",
    "The path to a RocksDB options file.  If specified, then db_stress will "
    "run with the RocksDB options in the default column family of the "
    "specified options file. Note that, when an options file is provided, "
    "db_stress will ignore the flag values for all options that may be passed "
    "via options file.");

DEFINE_int64(
    active_width, 0,
    "Number of keys in active span of the key-range at any given time. The "
    "span begins with its left endpoint at key 0, gradually moves rightwards, "
    "and ends with its right endpoint at max_key. If set to 0, active_width "
    "will be sanitized to be equal to max_key.");

// TODO(noetzli) Add support for single deletes
DEFINE_bool(test_batches_snapshots, false,
            "If set, the test uses MultiGet(), MultiPut() and MultiDelete()"
            " which read/write/delete multiple keys in a batch. In this mode,"
            " we do not verify db content by comparing the content with the "
            "pre-allocated array. Instead, we do partial verification inside"
            " MultiGet() by checking various values in a batch. Benefit of"
            " this mode:\n"
            "\t(a) No need to acquire mutexes during writes (less cache "
            "flushes in multi-core leading to speed up)\n"
            "\t(b) No long validation at the end (more speed up)\n"
            "\t(c) Test snapshot and atomicity of batch writes");

DEFINE_bool(atomic_flush, false,
            "If set, enables atomic flush in the options.\n");

DEFINE_bool(test_cf_consistency, false,
            "If set, runs the stress test dedicated to verifying writes to "
            "multiple column families are consistent. Setting this implies "
            "`atomic_flush=true` is set true if `disable_wal=false`.\n");

DEFINE_int32(threads, 32, "Number of concurrent threads to run.");

DEFINE_int32(ttl, -1,
             "Opens the db with this ttl value if this is not -1. "
             "Carefully specify a large value such that verifications on "
             "deleted values don't fail");

DEFINE_int32(value_size_mult, 8,
             "Size of value will be this number times rand_int(1,3) bytes");

DEFINE_int32(compaction_readahead_size, 0, "Compaction readahead size");

DEFINE_bool(enable_pipelined_write, false, "Pipeline WAL/memtable writes");

DEFINE_bool(verify_before_write, false, "Verify before write");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

DEFINE_bool(destroy_db_initially, true,
            "Destroys the database dir before start if this is true");

DEFINE_bool(verbose, false, "Verbose");

DEFINE_bool(progress_reports, true,
            "If true, db_stress will report number of finished operations");

DEFINE_uint64(db_write_buffer_size,
              ROCKSDB_NAMESPACE::Options().db_write_buffer_size,
              "Number of bytes to buffer in all memtables before compacting");

DEFINE_int32(
    write_buffer_size,
    static_cast<int32_t>(ROCKSDB_NAMESPACE::Options().write_buffer_size),
    "Number of bytes to buffer in memtable before compacting");

DEFINE_int32(max_write_buffer_number,
             ROCKSDB_NAMESPACE::Options().max_write_buffer_number,
             "The number of in-memory memtables. "
             "Each memtable is of size FLAGS_write_buffer_size.");

DEFINE_int32(min_write_buffer_number_to_merge,
             ROCKSDB_NAMESPACE::Options().min_write_buffer_number_to_merge,
             "The minimum number of write buffers that will be merged together "
             "before writing to storage. This is cheap because it is an "
             "in-memory merge. If this feature is not enabled, then all these "
             "write buffers are flushed to L0 as separate files and this "
             "increases read amplification because a get request has to check "
             "in all of these files. Also, an in-memory merge may result in "
             "writing less data to storage if there are duplicate records in"
             " each of these individual write buffers.");

DEFINE_int32(max_write_buffer_number_to_maintain,
             ROCKSDB_NAMESPACE::Options().max_write_buffer_number_to_maintain,
             "The total maximum number of write buffers to maintain in memory "
             "including copies of buffers that have already been flushed. "
             "Unlike max_write_buffer_number, this parameter does not affect "
             "flushing. This controls the minimum amount of write history "
             "that will be available in memory for conflict checking when "
             "Transactions are used. If this value is too low, some "
             "transactions may fail at commit time due to not being able to "
             "determine whether there were any write conflicts. Setting this "
             "value to 0 will cause write buffers to be freed immediately "
             "after they are flushed.  If this value is set to -1, "
             "'max_write_buffer_number' will be used.");

DEFINE_int64(max_write_buffer_size_to_maintain,
             ROCKSDB_NAMESPACE::Options().max_write_buffer_size_to_maintain,
             "The total maximum size of write buffers to maintain in memory "
             "including copies of buffers that have already been flushed. "
             "Unlike max_write_buffer_number, this parameter does not affect "
             "flushing. This controls the minimum amount of write history "
             "that will be available in memory for conflict checking when "
             "Transactions are used. If this value is too low, some "
             "transactions may fail at commit time due to not being able to "
             "determine whether there were any write conflicts. Setting this "
             "value to 0 will cause write buffers to be freed immediately "
             "after they are flushed.  If this value is set to -1, "
             "'max_write_buffer_number' will be used.");

DEFINE_double(memtable_prefix_bloom_size_ratio,
              ROCKSDB_NAMESPACE::Options().memtable_prefix_bloom_size_ratio,
              "creates prefix blooms for memtables, each with size "
              "`write_buffer_size * memtable_prefix_bloom_size_ratio`.");

DEFINE_bool(memtable_whole_key_filtering,
            ROCKSDB_NAMESPACE::Options().memtable_whole_key_filtering,
            "Enable whole key filtering in memtables.");

DEFINE_int32(open_files, ROCKSDB_NAMESPACE::Options().max_open_files,
             "Maximum number of files to keep open at the same time "
             "(use default if == 0)");

DEFINE_int64(compressed_cache_size, -1,
             "Number of bytes to use as a cache of compressed data."
             " Negative means use default settings.");

DEFINE_int32(compaction_style, ROCKSDB_NAMESPACE::Options().compaction_style,
             "");

DEFINE_int32(level0_file_num_compaction_trigger,
             ROCKSDB_NAMESPACE::Options().level0_file_num_compaction_trigger,
             "Level0 compaction start trigger");

DEFINE_int32(level0_slowdown_writes_trigger,
             ROCKSDB_NAMESPACE::Options().level0_slowdown_writes_trigger,
             "Number of files in level-0 that will slow down writes");

DEFINE_int32(level0_stop_writes_trigger,
             ROCKSDB_NAMESPACE::Options().level0_stop_writes_trigger,
             "Number of files in level-0 that will trigger put stop.");

DEFINE_int32(block_size,
             static_cast<int32_t>(
                 ROCKSDB_NAMESPACE::BlockBasedTableOptions().block_size),
             "Number of bytes in a block.");

DEFINE_int32(format_version,
             static_cast<int32_t>(
                 ROCKSDB_NAMESPACE::BlockBasedTableOptions().format_version),
             "Format version of SST files.");

DEFINE_int32(
    index_block_restart_interval,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().index_block_restart_interval,
    "Number of keys between restart points "
    "for delta encoding of keys in index block.");

DEFINE_int32(max_background_compactions,
             ROCKSDB_NAMESPACE::Options().max_background_compactions,
             "The maximum number of concurrent background compactions "
             "that can occur in parallel.");

DEFINE_int32(num_bottom_pri_threads, 0,
             "The number of threads in the bottom-priority thread pool (used "
             "by universal compaction only).");

DEFINE_int32(compaction_thread_pool_adjust_interval, 0,
             "The interval (in milliseconds) to adjust compaction thread pool "
             "size. Don't change it periodically if the value is 0.");

DEFINE_int32(compaction_thread_pool_variations, 2,
             "Range of background thread pool size variations when adjusted "
             "periodically.");

DEFINE_int32(max_background_flushes,
             ROCKSDB_NAMESPACE::Options().max_background_flushes,
             "The maximum number of concurrent background flushes "
             "that can occur in parallel.");

DEFINE_int32(universal_size_ratio, 0,
             "The ratio of file sizes that trigger"
             " compaction in universal style");

DEFINE_int32(universal_min_merge_width, 0,
             "The minimum number of files to "
             "compact in universal style compaction");

DEFINE_int32(universal_max_merge_width, 0,
             "The max number of files to compact"
             " in universal style compaction");

DEFINE_int32(universal_max_size_amplification_percent, 0,
             "The max size amplification for universal style compaction");

DEFINE_int32(clear_column_family_one_in, 1000000,
             "With a chance of 1/N, delete a column family and then recreate "
             "it again. If N == 0, never drop/create column families. "
             "When test_batches_snapshots is true, this flag has no effect");

DEFINE_int32(get_live_files_one_in, 1000000,
             "With a chance of 1/N, call GetLiveFiles to verify if it returns "
             "correctly. If N == 0, do not call the interface.");

DEFINE_int32(
    get_sorted_wal_files_one_in, 1000000,
    "With a chance of 1/N, call GetSortedWalFiles to verify if it returns "
    "correctly. (Note that this API may legitimately return an error.) If N == "
    "0, do not call the interface.");

DEFINE_int32(
    get_current_wal_file_one_in, 1000000,
    "With a chance of 1/N, call GetCurrentWalFile to verify if it returns "
    "correctly. (Note that this API may legitimately return an error.) If N == "
    "0, do not call the interface.");

DEFINE_int32(set_options_one_in, 0,
             "With a chance of 1/N, change some random options");

DEFINE_int32(set_in_place_one_in, 0,
             "With a chance of 1/N, toggle in place support option");

DEFINE_int64(cache_size, 2LL * KB * KB * KB,
             "Number of bytes to use as a cache of uncompressed data.");

DEFINE_bool(cache_index_and_filter_blocks, false,
            "True if indexes/filters should be cached in block cache.");

DEFINE_bool(use_clock_cache, false,
            "Replace default LRU block cache with clock cache.");

DEFINE_uint64(subcompactions, 1,
              "Maximum number of subcompactions to divide L0-L1 compactions "
              "into.");

DEFINE_uint64(periodic_compaction_seconds, 1000,
              "Files older than this value will be picked up for compaction.");

DEFINE_uint64(compaction_ttl, 1000,
              "Files older than TTL will be compacted to the next level.");

DEFINE_bool(allow_concurrent_memtable_write, false,
            "Allow multi-writers to update mem tables in parallel.");

DEFINE_bool(enable_write_thread_adaptive_yield, true,
            "Use a yielding spin loop for brief writer thread waits.");

#ifndef ROCKSDB_LITE
// BlobDB Options
DEFINE_bool(use_blob_db, false, "Use BlobDB.");

DEFINE_uint64(blob_db_min_blob_size,
              ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().min_blob_size,
              "Smallest blob to store in a file. Blobs smaller than this "
              "will be inlined with the key in the LSM tree.");

DEFINE_uint64(blob_db_bytes_per_sync,
              ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().bytes_per_sync,
              "Sync blob files once per every N bytes written.");

DEFINE_uint64(blob_db_file_size,
              ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().blob_file_size,
              "Target size of each blob file.");

DEFINE_bool(
    blob_db_enable_gc,
    ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().enable_garbage_collection,
    "Enable BlobDB garbage collection.");

DEFINE_double(
    blob_db_gc_cutoff,
    ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().garbage_collection_cutoff,
    "Cutoff ratio for BlobDB garbage collection.");
#endif  // !ROCKSDB_LITE

static const bool FLAGS_subcompactions_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_subcompactions, &ValidateUint32Range);

static bool ValidateInt32Positive(const char* flagname, int32_t value) {
  if (value < 0) {
    fprintf(stderr, "Invalid value for --%s: %d, must be >=0\n", flagname,
            value);
    return false;
  }
  return true;
}
DEFINE_int32(reopen, 10, "Number of times database reopens");
static const bool FLAGS_reopen_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_reopen, &ValidateInt32Positive);

DEFINE_double(bloom_bits, 10,
              "Bloom filter bits per key. "
              "Negative means use default settings.");

DEFINE_bool(use_block_based_filter, false,
            "use block based filter"
            "instead of full filter for block based table");

DEFINE_bool(partition_filters, false,
            "use partitioned filters "
            "for block-based table");

DEFINE_int32(
    index_type,
    static_cast<int32_t>(
        ROCKSDB_NAMESPACE::BlockBasedTableOptions::kBinarySearch),
    "Type of block-based table index (see `enum IndexType` in table.h)");

DEFINE_string(db, "", "Use the db with the following name.");

DEFINE_string(secondaries_base, "",
              "Use this path as the base path for secondary instances.");

DEFINE_bool(test_secondary, false, "Test secondary instance.");

DEFINE_string(
    expected_values_path, "",
    "File where the array of expected uint32_t values will be stored. If "
    "provided and non-empty, the DB state will be verified against these "
    "values after recovery. --max_key and --column_family must be kept the "
    "same across invocations of this program that use the same "
    "--expected_values_path.");

DEFINE_bool(verify_checksum, false,
            "Verify checksum for every block read from storage");

DEFINE_bool(mmap_read, ROCKSDB_NAMESPACE::Options().allow_mmap_reads,
            "Allow reads to occur via mmap-ing files");

DEFINE_bool(mmap_write, ROCKSDB_NAMESPACE::Options().allow_mmap_writes,
            "Allow writes to occur via mmap-ing files");

DEFINE_bool(use_direct_reads, ROCKSDB_NAMESPACE::Options().use_direct_reads,
            "Use O_DIRECT for reading data");

DEFINE_bool(use_direct_io_for_flush_and_compaction,
            ROCKSDB_NAMESPACE::Options().use_direct_io_for_flush_and_compaction,
            "Use O_DIRECT for writing data");

DEFINE_bool(statistics, false, "Create database statistics");

DEFINE_bool(sync, false, "Sync all writes to disk");

DEFINE_bool(use_fsync, false, "If true, issue fsync instead of fdatasync");

DEFINE_int32(kill_random_test, 0,
             "If non-zero, kill at various points in source code with "
             "probability 1/this");
static const bool FLAGS_kill_random_test_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_kill_random_test, &ValidateInt32Positive);
extern int rocksdb_kill_odds;

DEFINE_string(kill_prefix_blacklist, "",
              "If non-empty, kill points with prefix in the list given will be"
              " skipped. Items are comma-separated.");
extern std::vector<std::string> rocksdb_kill_prefix_blacklist;

DEFINE_bool(disable_wal, false, "If true, do not write WAL for write.");

DEFINE_uint64(recycle_log_file_num,
              ROCKSDB_NAMESPACE::Options().recycle_log_file_num,
              "Number of old WAL files to keep around for later recycling");

DEFINE_int64(target_file_size_base,
             ROCKSDB_NAMESPACE::Options().target_file_size_base,
             "Target level-1 file size for compaction");

DEFINE_int32(target_file_size_multiplier, 1,
             "A multiplier to compute target level-N file size (N >= 2)");

DEFINE_uint64(max_bytes_for_level_base,
              ROCKSDB_NAMESPACE::Options().max_bytes_for_level_base,
              "Max bytes for level-1");

DEFINE_double(max_bytes_for_level_multiplier, 2,
              "A multiplier to compute max bytes for level-N (N >= 2)");

DEFINE_int32(range_deletion_width, 10,
             "The width of the range deletion intervals.");

DEFINE_uint64(rate_limiter_bytes_per_sec, 0, "Set options.rate_limiter value.");

DEFINE_bool(rate_limit_bg_reads, false,
            "Use options.rate_limiter on compaction reads");

DEFINE_uint64(sst_file_manager_bytes_per_sec, 0,
              "Set `Options::sst_file_manager` to delete at this rate. By "
              "default the deletion rate is unbounded.");

DEFINE_uint64(sst_file_manager_bytes_per_truncate, 0,
              "Set `Options::sst_file_manager` to delete in chunks of this "
              "many bytes. By default whole files will be deleted.");

DEFINE_bool(use_txn, false,
            "Use TransactionDB. Currently the default write policy is "
            "TxnDBWritePolicy::WRITE_PREPARED");

DEFINE_uint64(txn_write_policy, 0,
              "The transaction write policy. Default is "
              "TxnDBWritePolicy::WRITE_COMMITTED. Note that this should not be "
              "changed accross crashes.");

DEFINE_bool(unordered_write, false,
            "Turn on the unordered_write feature. This options is currently "
            "tested only in combination with use_txn=true and "
            "txn_write_policy=TxnDBWritePolicy::WRITE_PREPARED.");

DEFINE_int32(backup_one_in, 0,
             "If non-zero, then CreateNewBackup() will be called once for "
             "every N operations on average.  0 indicates CreateNewBackup() "
             "is disabled.");

DEFINE_int32(checkpoint_one_in, 0,
             "If non-zero, then CreateCheckpoint() will be called once for "
             "every N operations on average.  0 indicates CreateCheckpoint() "
             "is disabled.");

DEFINE_int32(ingest_external_file_one_in, 0,
             "If non-zero, then IngestExternalFile() will be called once for "
             "every N operations on average.  0 indicates IngestExternalFile() "
             "is disabled.");

DEFINE_int32(ingest_external_file_width, 1000,
             "The width of the ingested external files.");

DEFINE_int32(compact_files_one_in, 0,
             "If non-zero, then CompactFiles() will be called once for every N "
             "operations on average.  0 indicates CompactFiles() is disabled.");

DEFINE_int32(compact_range_one_in, 0,
             "If non-zero, then CompactRange() will be called once for every N "
             "operations on average.  0 indicates CompactRange() is disabled.");

DEFINE_int32(flush_one_in, 0,
             "If non-zero, then Flush() will be called once for every N ops "
             "on average.  0 indicates calls to Flush() are disabled.");

DEFINE_int32(pause_background_one_in, 0,
             "If non-zero, then PauseBackgroundWork()+Continue will be called "
             "once for every N ops on average.  0 disables.");

DEFINE_int32(compact_range_width, 10000,
             "The width of the ranges passed to CompactRange().");

DEFINE_int32(acquire_snapshot_one_in, 0,
             "If non-zero, then acquires a snapshot once every N operations on "
             "average.");

DEFINE_bool(compare_full_db_state_snapshot, false,
            "If set we compare state of entire db (in one of the threads) with"
            "each snapshot.");

DEFINE_uint64(snapshot_hold_ops, 0,
              "If non-zero, then releases snapshots N operations after they're "
              "acquired.");

DEFINE_bool(long_running_snapshots, false,
            "If set, hold on some some snapshots for much longer time.");

DEFINE_bool(use_multiget, false,
            "If set, use the batched MultiGet API for reads");

static bool ValidateInt32Percent(const char* flagname, int32_t value) {
  if (value < 0 || value > 100) {
    fprintf(stderr, "Invalid value for --%s: %d, 0<= pct <=100 \n", flagname,
            value);
    return false;
  }
  return true;
}

DEFINE_int32(readpercent, 10,
             "Ratio of reads to total workload (expressed as a percentage)");
static const bool FLAGS_readpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_readpercent, &ValidateInt32Percent);

DEFINE_int32(prefixpercent, 20,
             "Ratio of prefix iterators to total workload (expressed as a"
             " percentage)");
static const bool FLAGS_prefixpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_prefixpercent, &ValidateInt32Percent);

DEFINE_int32(writepercent, 45,
             "Ratio of writes to total workload (expressed as a percentage)");
static const bool FLAGS_writepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_writepercent, &ValidateInt32Percent);

DEFINE_int32(delpercent, 15,
             "Ratio of deletes to total workload (expressed as a percentage)");
static const bool FLAGS_delpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_delpercent, &ValidateInt32Percent);

DEFINE_int32(delrangepercent, 0,
             "Ratio of range deletions to total workload (expressed as a "
             "percentage). Cannot be used with test_batches_snapshots");
static const bool FLAGS_delrangepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_delrangepercent, &ValidateInt32Percent);

DEFINE_int32(nooverwritepercent, 60,
             "Ratio of keys without overwrite to total workload (expressed as "
             " a percentage)");
static const bool FLAGS_nooverwritepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_nooverwritepercent, &ValidateInt32Percent);

DEFINE_int32(iterpercent, 10,
             "Ratio of iterations to total workload"
             " (expressed as a percentage)");
static const bool FLAGS_iterpercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_iterpercent, &ValidateInt32Percent);

DEFINE_uint64(num_iterations, 10, "Number of iterations per MultiIterate run");
static const bool FLAGS_num_iterations_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_num_iterations, &ValidateUint32Range);

DEFINE_string(compression_type, "snappy",
              "Algorithm to use to compress the database");

DEFINE_int32(compression_max_dict_bytes, 0,
             "Maximum size of dictionary used to prime the compression "
             "library.");

DEFINE_int32(compression_zstd_max_train_bytes, 0,
             "Maximum size of training data passed to zstd's dictionary "
             "trainer.");

DEFINE_string(bottommost_compression_type, "disable",
              "Algorithm to use to compress bottommost level of the database. "
              "\"disable\" means disabling the feature");

DEFINE_string(checksum_type, "kCRC32c", "Algorithm to use to checksum blocks");

DEFINE_string(hdfs, "", "Name of hdfs environment");

DEFINE_string(env_uri, "",
              "URI for env lookup. Mutually exclusive with --hdfs");

DEFINE_uint64(ops_per_thread, 1200000, "Number of operations per thread.");
static const bool FLAGS_ops_per_thread_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_ops_per_thread, &ValidateUint32Range);

DEFINE_uint64(log2_keys_per_lock, 2, "Log2 of number of keys per lock");
static const bool FLAGS_log2_keys_per_lock_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_log2_keys_per_lock, &ValidateUint32Range);

DEFINE_uint64(max_manifest_file_size, 16384, "Maximum size of a MANIFEST file");

DEFINE_bool(in_place_update, false, "On true, does inplace update in memtable");

DEFINE_int32(secondary_catch_up_one_in, 0,
             "If non-zero, the secondaries attemp to catch up with the primary "
             "once for every N operations on average. 0 indicates the "
             "secondaries do not try to catch up after open.");

DEFINE_string(memtablerep, "skip_list", "");

inline static bool ValidatePrefixSize(const char* flagname, int32_t value) {
  if (value < -1 || value > 8) {
    fprintf(stderr, "Invalid value for --%s: %d. -1 <= PrefixSize <= 8\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(prefix_size, 7,
             "Control the prefix size for HashSkipListRep. "
             "-1 is disabled.");
static const bool FLAGS_prefix_size_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_prefix_size, &ValidatePrefixSize);

DEFINE_bool(use_merge, false,
            "On true, replaces all writes with a Merge "
            "that behaves like a Put");

DEFINE_bool(use_full_merge_v1, false,
            "On true, use a merge operator that implement the deprecated "
            "version of FullMerge");

DEFINE_int32(sync_wal_one_in, 0,
             "If non-zero, then SyncWAL() will be called once for every N ops "
             "on average. 0 indicates that calls to SyncWAL() are disabled.");

DEFINE_bool(avoid_unnecessary_blocking_io,
            ROCKSDB_NAMESPACE::Options().avoid_unnecessary_blocking_io,
            "If true, some expensive cleaning up operations will be moved from "
            "user reads to high-pri background threads.");

DEFINE_bool(write_dbid_to_manifest,
            ROCKSDB_NAMESPACE::Options().write_dbid_to_manifest,
            "Write DB_ID to manifest");

DEFINE_bool(avoid_flush_during_recovery,
            ROCKSDB_NAMESPACE::Options().avoid_flush_during_recovery,
            "Avoid flush during recovery");

DEFINE_uint64(max_write_batch_group_size_bytes,
              ROCKSDB_NAMESPACE::Options().max_write_batch_group_size_bytes,
              "Max write batch group size");

DEFINE_bool(level_compaction_dynamic_level_bytes,
            ROCKSDB_NAMESPACE::Options().level_compaction_dynamic_level_bytes,
            "Use dynamic level");

DEFINE_int32(verify_checksum_one_in, 0,
             "If non-zero, then DB::VerifyChecksum() will be called to do"
             " checksum verification of all the files in the database once for"
             " every N ops on average. 0 indicates that calls to"
             " VerifyChecksum() are disabled.");
DEFINE_int32(verify_db_one_in, 0,
             "If non-zero, call VerifyDb() once for every N ops. 0 indicates "
             "that VerifyDb() will not be called in OperateDb(). Note that "
             "enabling this can slow down tests.");

DEFINE_int32(continuous_verification_interval, 1000,
             "While test is running, verify db every N milliseconds. 0 "
             "disables continuous verification.");

DEFINE_int32(approximate_size_one_in, 64,
             "If non-zero, DB::GetApproximateSizes() will be called against"
             " random key ranges.");

DEFINE_int32(read_fault_one_in, 1000,
            "On non-zero, enables fault injection on read");

DEFINE_bool(sync_fault_injection, false,
            "If true, FaultInjectionTestFS will be used for write operations, "
            " and unsynced data in DB will lost after crash.");
#endif  // GFLAGS
