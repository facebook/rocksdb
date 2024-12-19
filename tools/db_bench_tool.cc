//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef GFLAGS
#ifdef NUMA
#include <numa.h>
#endif
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <fcntl.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>
#ifdef __APPLE__
#include <mach/host_info.h>
#include <mach/mach_host.h>
#include <sys/sysctl.h>
#endif
#ifdef __FreeBSD__
#include <sys/sysctl.h>
#endif
#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <cstddef>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "db/malloc_stats.h"
#include "db/version_set.h"
#include "monitoring/histogram.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/replayer.h"
#include "rocksdb/utilities/sim_cache.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"
#include "test_util/testutil.h"
#include "test_util/transaction_test_util.h"
#include "tools/simulated_hybrid_file_system.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_checksum_helper.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/xxhash.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/counted_fs.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/persistent_cache/block_cache_tier.h"

#ifdef MEMKIND
#include "memory/memkind_kmem_allocator.h"
#endif

#ifdef OS_WIN
#include <io.h>  // open/close
#endif

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;
using GFLAGS_NAMESPACE::SetVersionString;

DEFINE_string(
    benchmarks,
    "fillseq,"
    "fillseqdeterministic,"
    "fillsync,"
    "fillrandom,"
    "filluniquerandomdeterministic,"
    "overwrite,"
    "readrandom,"
    "newiterator,"
    "newiteratorwhilewriting,"
    "seekrandom,"
    "seekrandomwhilewriting,"
    "seekrandomwhilemerging,"
    "readseq,"
    "readreverse,"
    "compact,"
    "compactall,"
    "flush,"
    "compact0,"
    "compact1,"
    "waitforcompaction,"
    "multireadrandom,"
    "mixgraph,"
    "readseq,"
    "readtorowcache,"
    "readtocache,"
    "readreverse,"
    "readwhilewriting,"
    "readwhilemerging,"
    "readwhilescanning,"
    "readrandomwriterandom,"
    "updaterandom,"
    "xorupdaterandom,"
    "approximatesizerandom,"
    "randomwithverify,"
    "fill100K,"
    "crc32c,"
    "xxhash,"
    "xxhash64,"
    "xxh3,"
    "compress,"
    "uncompress,"
    "acquireload,"
    "fillseekseq,"
    "randomtransaction,"
    "randomreplacekeys,"
    "timeseries,"
    "getmergeoperands,"
    "readrandomoperands,"
    "backup,"
    "restore,"
    "approximatememtablestats",

    "Comma-separated list of operations to run in the specified"
    " order. Available benchmarks:\n"
    "\tfillseq       -- write N values in sequential key"
    " order in async mode\n"
    "\tfillseqdeterministic       -- write N values in the specified"
    " key order and keep the shape of the LSM tree\n"
    "\tfillrandom    -- write N values in random key order in async"
    " mode\n"
    "\tfilluniquerandomdeterministic       -- write N values in a random"
    " key order and keep the shape of the LSM tree\n"
    "\toverwrite     -- overwrite N values in random key order in "
    "async mode\n"
    "\tfillsync      -- write N/1000 values in random key order in "
    "sync mode\n"
    "\tfill100K      -- write N/1000 100K values in random order in"
    " async mode\n"
    "\tdeleteseq     -- delete N keys in sequential order\n"
    "\tdeleterandom  -- delete N keys in random order\n"
    "\treadseq       -- read N times sequentially\n"
    "\treadtocache   -- 1 thread reading database sequentially\n"
    "\treadreverse   -- read N times in reverse order\n"
    "\treadrandom    -- read N times in random order\n"
    "\treadmissing   -- read N missing keys in random order\n"
    "\treadwhilewriting      -- 1 writer, N threads doing random "
    "reads\n"
    "\treadwhilemerging      -- 1 merger, N threads doing random "
    "reads\n"
    "\treadwhilescanning     -- 1 thread doing full table scan, "
    "N threads doing random reads\n"
    "\treadrandomwriterandom -- N threads doing random-read, "
    "random-write\n"
    "\tupdaterandom  -- N threads doing read-modify-write for random "
    "keys\n"
    "\txorupdaterandom  -- N threads doing read-XOR-write for "
    "random keys\n"
    "\tappendrandom  -- N threads doing read-modify-write with "
    "growing values\n"
    "\tmergerandom   -- same as updaterandom/appendrandom using merge"
    " operator. "
    "Must be used with merge_operator\n"
    "\treadrandommergerandom -- perform N random read-or-merge "
    "operations. Must be used with merge_operator\n"
    "\tnewiterator   -- repeated iterator creation\n"
    "\tseekrandom    -- N random seeks, call Next seek_nexts times "
    "per seek\n"
    "\tseekrandomwhilewriting -- seekrandom and 1 thread doing "
    "overwrite\n"
    "\tseekrandomwhilemerging -- seekrandom and 1 thread doing "
    "merge\n"
    "\tcrc32c        -- repeated crc32c of <block size> data\n"
    "\txxhash        -- repeated xxHash of <block size> data\n"
    "\txxhash64      -- repeated xxHash64 of <block size> data\n"
    "\txxh3          -- repeated XXH3 of <block size> data\n"
    "\tacquireload   -- load N*1000 times\n"
    "\tfillseekseq   -- write N values in sequential key, then read "
    "them by seeking to each key\n"
    "\trandomtransaction     -- execute N random transactions and "
    "verify correctness\n"
    "\trandomreplacekeys     -- randomly replaces N keys by deleting "
    "the old version and putting the new version\n\n"
    "\ttimeseries            -- 1 writer generates time series data "
    "and multiple readers doing random reads on id\n\n"
    "Meta operations:\n"
    "\tcompact     -- Compact the entire DB; If multiple, randomly choose one\n"
    "\tcompactall  -- Compact the entire DB\n"
    "\tcompact0  -- compact L0 into L1\n"
    "\tcompact1  -- compact L1 into L2\n"
    "\twaitforcompaction - pause until compaction is (probably) done\n"
    "\tflush - flush the memtable\n"
    "\tstats       -- Print DB stats\n"
    "\tresetstats  -- Reset DB stats\n"
    "\tlevelstats  -- Print the number of files and bytes per level\n"
    "\tmemstats  -- Print memtable stats\n"
    "\tsstables    -- Print sstable info\n"
    "\theapprofile -- Dump a heap profile (if supported by this port)\n"
    "\treplay      -- replay the trace file specified with trace_file\n"
    "\tgetmergeoperands -- Insert lots of merge records which are a list of "
    "sorted ints for a key and then compare performance of lookup for another "
    "key by doing a Get followed by binary searching in the large sorted list "
    "vs doing a GetMergeOperands and binary searching in the operands which "
    "are sorted sub-lists. The MergeOperator used is sortlist.h\n"
    "\treadrandomoperands -- read random keys using `GetMergeOperands()`. An "
    "operation includes a rare but possible retry in case it got "
    "`Status::Incomplete()`. This happens upon encountering more keys than "
    "have ever been seen by the thread (or eight initially)\n"
    "\tbackup --  Create a backup of the current DB and verify that a new "
    "backup is corrected. "
    "Rate limit can be specified through --backup_rate_limit\n"
    "\trestore -- Restore the DB from the latest backup available, rate limit "
    "can be specified through --restore_rate_limit\n"
    "\tapproximatememtablestats -- Tests accuracy of "
    "GetApproximateMemTableStats, ideally\n"
    "after fillrandom, where actual answer is batch_size");

DEFINE_int64(num, 1000000, "Number of key/values to place in database");

DEFINE_int64(numdistinct, 1000,
             "Number of distinct keys to use. Used in RandomWithVerify to "
             "read/write on fewer keys so that gets are more likely to find the"
             " key and puts are more likely to update the same key");

DEFINE_int64(merge_keys, -1,
             "Number of distinct keys to use for MergeRandom and "
             "ReadRandomMergeRandom. "
             "If negative, there will be FLAGS_num keys.");
DEFINE_int32(num_column_families, 1, "Number of Column Families to use.");

DEFINE_int32(
    num_hot_column_families, 0,
    "Number of Hot Column Families. If more than 0, only write to this "
    "number of column families. After finishing all the writes to them, "
    "create new set of column families and insert to them. Only used "
    "when num_column_families > 1.");

DEFINE_string(column_family_distribution, "",
              "Comma-separated list of percentages, where the ith element "
              "indicates the probability of an op using the ith column family. "
              "The number of elements must be `num_hot_column_families` if "
              "specified; otherwise, it must be `num_column_families`. The "
              "sum of elements must be 100. E.g., if `num_column_families=4`, "
              "and `num_hot_column_families=0`, a valid list could be "
              "\"10,20,30,40\".");

DEFINE_int64(reads, -1,
             "Number of read operations to do.  "
             "If negative, do FLAGS_num reads.");

DEFINE_int64(deletes, -1,
             "Number of delete operations to do.  "
             "If negative, do FLAGS_num deletions.");

DEFINE_int32(bloom_locality, 0, "Control bloom filter probes locality");

DEFINE_int64(seed, 0,
             "Seed base for random number generators. "
             "When 0 it is derived from the current time.");
static std::optional<int64_t> seed_base;

DEFINE_int32(threads, 1, "Number of concurrent threads to run.");

DEFINE_int32(duration, 0,
             "Time in seconds for the random-ops tests to run."
             " When 0 then num & reads determine the test duration");

DEFINE_string(value_size_distribution_type, "fixed",
              "Value size distribution type: fixed, uniform, normal");

DEFINE_int32(value_size, 100, "Size of each value in fixed distribution");
static unsigned int value_size = 100;

DEFINE_int32(value_size_min, 100, "Min size of random value");

DEFINE_int32(value_size_max, 102400, "Max size of random value");

DEFINE_int32(seek_nexts, 0,
             "How many times to call Next() after Seek() in "
             "fillseekseq, seekrandom, seekrandomwhilewriting and "
             "seekrandomwhilemerging");

DEFINE_bool(reverse_iterator, false,
            "When true use Prev rather than Next for iterators that do "
            "Seek and then Next");

DEFINE_bool(auto_prefix_mode, false, "Set auto_prefix_mode for seek benchmark");

DEFINE_int64(max_scan_distance, 0,
             "Used to define iterate_upper_bound (or iterate_lower_bound "
             "if FLAGS_reverse_iterator is set to true) when value is nonzero");

DEFINE_bool(use_uint64_comparator, false, "use Uint64 user comparator");

DEFINE_int64(batch_size, 1, "Batch size");

static bool ValidateKeySize(const char* /*flagname*/, int32_t /*value*/) {
  return true;
}

static bool ValidateUint32Range(const char* flagname, uint64_t value) {
  if (value > std::numeric_limits<uint32_t>::max()) {
    fprintf(stderr, "Invalid value for --%s: %lu, overflow\n", flagname,
            (unsigned long)value);
    return false;
  }
  return true;
}

DEFINE_int32(key_size, 16, "size of each key");

DEFINE_int32(user_timestamp_size, 0,
             "number of bytes in a user-defined timestamp");

DEFINE_int32(num_multi_db, 0,
             "Number of DBs used in the benchmark. 0 means single DB.");

DEFINE_double(compression_ratio, 0.5,
              "Arrange to generate values that shrink to this fraction of "
              "their original size after compression");

DEFINE_double(
    overwrite_probability, 0.0,
    "Used in 'filluniquerandom' benchmark: for each write operation, "
    "we give a probability to perform an overwrite instead. The key used for "
    "the overwrite is randomly chosen from the last 'overwrite_window_size' "
    "keys previously inserted into the DB. "
    "Valid overwrite_probability values: [0.0, 1.0].");

DEFINE_uint32(overwrite_window_size, 1,
              "Used in 'filluniquerandom' benchmark. For each write operation,"
              " when the overwrite_probability flag is set by the user, the "
              "key used to perform an overwrite is randomly chosen from the "
              "last 'overwrite_window_size' keys previously inserted into DB. "
              "Warning: large values can affect throughput. "
              "Valid overwrite_window_size values: [1, kMaxUint32].");

DEFINE_uint64(
    disposable_entries_delete_delay, 0,
    "Minimum delay in microseconds for the series of Deletes "
    "to be issued. When 0 the insertion of the last disposable entry is "
    "immediately followed by the issuance of the Deletes. "
    "(only compatible with fillanddeleteuniquerandom benchmark).");

DEFINE_uint64(disposable_entries_batch_size, 0,
              "Number of consecutively inserted disposable KV entries "
              "that will be deleted after 'delete_delay' microseconds. "
              "A series of Deletes is always issued once all the "
              "disposable KV entries it targets have been inserted "
              "into the DB. When 0 no deletes are issued and a "
              "regular 'filluniquerandom' benchmark occurs. "
              "(only compatible with fillanddeleteuniquerandom benchmark)");

DEFINE_int32(disposable_entries_value_size, 64,
             "Size of the values (in bytes) of the entries targeted by "
             "selective deletes. "
             "(only compatible with fillanddeleteuniquerandom benchmark)");

DEFINE_uint64(
    persistent_entries_batch_size, 0,
    "Number of KV entries being inserted right before the deletes "
    "targeting the disposable KV entries are issued. These "
    "persistent keys are not targeted by the deletes, and will always "
    "remain valid in the DB. (only compatible with "
    "--benchmarks='fillanddeleteuniquerandom' "
    "and used when--disposable_entries_batch_size is > 0).");

DEFINE_int32(persistent_entries_value_size, 64,
             "Size of the values (in bytes) of the entries not targeted by "
             "deletes. (only compatible with "
             "--benchmarks='fillanddeleteuniquerandom' "
             "and used when--disposable_entries_batch_size is > 0).");

DEFINE_double(read_random_exp_range, 0.0,
              "Read random's key will be generated using distribution of "
              "num * exp(-r) where r is uniform number from 0 to this value. "
              "The larger the number is, the more skewed the reads are. "
              "Only used in readrandom and multireadrandom benchmarks.");

DEFINE_bool(histogram, false, "Print histogram of operation timings");

DEFINE_bool(confidence_interval_only, false,
            "Print 95% confidence interval upper and lower bounds only for "
            "aggregate stats.");

DEFINE_bool(enable_numa, false,
            "Make operations aware of NUMA architecture and bind memory "
            "and cpus corresponding to nodes together. In NUMA, memory "
            "in same node as CPUs are closer when compared to memory in "
            "other nodes. Reads can be faster when the process is bound to "
            "CPU and memory of same node. Use \"$numactl --hardware\" command "
            "to see NUMA memory architecture.");

DEFINE_int64(db_write_buffer_size,
             ROCKSDB_NAMESPACE::Options().db_write_buffer_size,
             "Number of bytes to buffer in all memtables before compacting");

DEFINE_bool(cost_write_buffer_to_cache, false,
            "The usage of memtable is costed to the block cache");

DEFINE_int64(arena_block_size, ROCKSDB_NAMESPACE::Options().arena_block_size,
             "The size, in bytes, of one block in arena memory allocation.");

DEFINE_int64(write_buffer_size, ROCKSDB_NAMESPACE::Options().write_buffer_size,
             "Number of bytes to buffer in memtable before compacting");

DEFINE_int32(max_write_buffer_number,
             ROCKSDB_NAMESPACE::Options().max_write_buffer_number,
             "The number of in-memory memtables. Each memtable is of size"
             " write_buffer_size bytes.");

DEFINE_int32(min_write_buffer_number_to_merge,
             ROCKSDB_NAMESPACE::Options().min_write_buffer_number_to_merge,
             "The minimum number of write buffers that will be merged together"
             "before writing to storage. This is cheap because it is an"
             "in-memory merge. If this feature is not enabled, then all these"
             "write buffers are flushed to L0 as separate files and this "
             "increases read amplification because a get request has to check"
             " in all of these files. Also, an in-memory merge may result in"
             " writing less data to storage if there are duplicate records "
             " in each of these individual write buffers.");

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

DEFINE_int32(max_background_jobs,
             ROCKSDB_NAMESPACE::Options().max_background_jobs,
             "The maximum number of concurrent background jobs that can occur "
             "in parallel.");

DEFINE_int32(num_bottom_pri_threads, 0,
             "The number of threads in the bottom-priority thread pool (used "
             "by universal compaction only).");

DEFINE_int32(num_high_pri_threads, 0,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(num_low_pri_threads, 0,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(max_background_compactions,
             ROCKSDB_NAMESPACE::Options().max_background_compactions,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_uint64(subcompactions, 1,
              "For CompactRange, set max_subcompactions for each compaction "
              "job in this CompactRange, for auto compactions, this is "
              "Maximum number of subcompactions to divide L0-L1 compactions "
              "into.");
static const bool FLAGS_subcompactions_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_subcompactions, &ValidateUint32Range);

DEFINE_int32(max_background_flushes,
             ROCKSDB_NAMESPACE::Options().max_background_flushes,
             "The maximum number of concurrent background flushes"
             " that can occur in parallel.");

static ROCKSDB_NAMESPACE::CompactionStyle FLAGS_compaction_style_e;
DEFINE_int32(compaction_style,
             (int32_t)ROCKSDB_NAMESPACE::Options().compaction_style,
             "style of compaction: level-based, universal and fifo");

static ROCKSDB_NAMESPACE::CompactionPri FLAGS_compaction_pri_e;
DEFINE_int32(compaction_pri,
             (int32_t)ROCKSDB_NAMESPACE::Options().compaction_pri,
             "priority of files to compaction: by size or by data age");

DEFINE_int32(universal_size_ratio, 0,
             "Percentage flexibility while comparing file size "
             "(for universal compaction only).");

DEFINE_int32(universal_min_merge_width, 0,
             "The minimum number of files in a single compaction run "
             "(for universal compaction only).");

DEFINE_int32(universal_max_merge_width, 0,
             "The max number of files to compact in universal style "
             "compaction");

DEFINE_int32(universal_max_size_amplification_percent, 0,
             "The max size amplification for universal style compaction");

DEFINE_int32(universal_compression_size_percent, -1,
             "The percentage of the database to compress for universal "
             "compaction. -1 means compress everything.");

DEFINE_int32(universal_max_read_amp, -1,
             "The limit on the number of sorted runs");

DEFINE_bool(universal_allow_trivial_move, false,
            "Allow trivial move in universal compaction.");

DEFINE_bool(universal_incremental, false,
            "Enable incremental compactions in universal compaction.");

DEFINE_int32(
    universal_stop_style,
    (int32_t)ROCKSDB_NAMESPACE::CompactionOptionsUniversal().stop_style,
    "Universal compaction stop style.");

DEFINE_int64(cache_size, 32 << 20,  // 32MB
             "Number of bytes to use as a cache of uncompressed data");

DEFINE_int32(cache_numshardbits, -1,
             "Number of shards for the block cache"
             " is 2 ** cache_numshardbits. Negative means use default settings."
             " This is applied only if FLAGS_cache_size is non-negative.");

DEFINE_double(cache_high_pri_pool_ratio, 0.0,
              "Ratio of block cache reserve for high pri blocks. "
              "If > 0.0, we also enable "
              "cache_index_and_filter_blocks_with_high_priority.");

DEFINE_double(cache_low_pri_pool_ratio, 0.0,
              "Ratio of block cache reserve for low pri blocks.");

DEFINE_string(cache_type, "lru_cache", "Type of block cache.");

DEFINE_bool(use_compressed_secondary_cache, false,
            "Use the CompressedSecondaryCache as the secondary cache.");

DEFINE_int64(compressed_secondary_cache_size, 32 << 20,  // 32MB
             "Number of bytes to use as a cache of data");

DEFINE_int32(compressed_secondary_cache_numshardbits, 6,
             "Number of shards for the block cache"
             " is 2 ** compressed_secondary_cache_numshardbits."
             " Negative means use default settings."
             " This is applied only if FLAGS_cache_size is non-negative.");

DEFINE_double(compressed_secondary_cache_high_pri_pool_ratio, 0.0,
              "Ratio of block cache reserve for high pri blocks. "
              "If > 0.0, we also enable "
              "cache_index_and_filter_blocks_with_high_priority.");

DEFINE_double(compressed_secondary_cache_low_pri_pool_ratio, 0.0,
              "Ratio of block cache reserve for low pri blocks.");

DEFINE_string(compressed_secondary_cache_compression_type, "lz4",
              "The compression algorithm to use for large "
              "values stored in CompressedSecondaryCache.");
static enum ROCKSDB_NAMESPACE::CompressionType
    FLAGS_compressed_secondary_cache_compression_type_e =
        ROCKSDB_NAMESPACE::kLZ4Compression;

DEFINE_int32(compressed_secondary_cache_compression_level,
             ROCKSDB_NAMESPACE::CompressionOptions().level,
             "Compression level. The meaning of this value is library-"
             "dependent. If unset, we try to use the default for the library "
             "specified in `--compressed_secondary_cache_compression_type`");

DEFINE_uint32(
    compressed_secondary_cache_compress_format_version, 2,
    "compress_format_version can have two values: "
    "compress_format_version == 1 -- decompressed size is not included"
    " in the block header."
    "compress_format_version == 2 -- decompressed size is included"
    " in the block header in varint32 format.");

DEFINE_bool(use_tiered_cache, false,
            "If use_compressed_secondary_cache is true and "
            "use_tiered_volatile_cache is true, then allocate a tiered cache "
            "that distributes cache reservations proportionally over both "
            "the caches.");

DEFINE_string(
    tiered_adm_policy, "auto",
    "Admission policy to use for the secondary cache(s) in the tiered cache. "
    "Allowed values are auto, placeholder, allow_cache_hits, and three_queue.");

DEFINE_int64(simcache_size, -1,
             "Number of bytes to use as a simcache of "
             "uncompressed data. Nagative value disables simcache.");

DEFINE_bool(cache_index_and_filter_blocks, false,
            "Cache index/filter blocks in block cache.");

DEFINE_bool(use_cache_jemalloc_no_dump_allocator, false,
            "Use JemallocNodumpAllocator for block/blob cache.");

DEFINE_bool(use_cache_memkind_kmem_allocator, false,
            "Use memkind kmem allocator for block/blob cache.");

DEFINE_bool(
    decouple_partitioned_filters,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().decouple_partitioned_filters,
    "Decouple filter partitioning from index partitioning.");

DEFINE_bool(partition_index_and_filters, false,
            "Partition index and filter blocks.");

DEFINE_bool(partition_index, false, "Partition index blocks");

DEFINE_bool(index_with_first_key, false, "Include first key in the index");

DEFINE_bool(
    optimize_filters_for_memory,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().optimize_filters_for_memory,
    "Minimize memory footprint of filters");

DEFINE_int64(
    index_shortening_mode, 2,
    "mode to shorten index: 0 for no shortening; 1 for only shortening "
    "separaters; 2 for shortening shortening and successor");

DEFINE_int64(metadata_block_size,
             ROCKSDB_NAMESPACE::BlockBasedTableOptions().metadata_block_size,
             "Max partition size when partitioning index/filters");

// The default reduces the overhead of reading time with flash. With HDD, which
// offers much less throughput, however, this number better to be set to 1.
DEFINE_int32(ops_between_duration_checks, 1000,
             "Check duration limit every x ops");

DEFINE_bool(pin_l0_filter_and_index_blocks_in_cache, false,
            "Pin index/filter blocks of L0 files in block cache.");

DEFINE_bool(
    pin_top_level_index_and_filter, false,
    "Pin top-level index of partitioned index/filter blocks in block cache.");

DEFINE_int32(block_size,
             static_cast<int32_t>(
                 ROCKSDB_NAMESPACE::BlockBasedTableOptions().block_size),
             "Number of bytes in a block.");

DEFINE_int32(format_version,
             static_cast<int32_t>(
                 ROCKSDB_NAMESPACE::BlockBasedTableOptions().format_version),
             "Format version of SST files.");

DEFINE_int32(block_restart_interval,
             ROCKSDB_NAMESPACE::BlockBasedTableOptions().block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys in data block.");

DEFINE_int32(
    index_block_restart_interval,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().index_block_restart_interval,
    "Number of keys between restart points "
    "for delta encoding of keys in index block.");

DEFINE_int32(read_amp_bytes_per_bit,
             ROCKSDB_NAMESPACE::BlockBasedTableOptions().read_amp_bytes_per_bit,
             "Number of bytes per bit to be used in block read-amp bitmap");

DEFINE_bool(
    enable_index_compression,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().enable_index_compression,
    "Compress the index block");

DEFINE_bool(block_align,
            ROCKSDB_NAMESPACE::BlockBasedTableOptions().block_align,
            "Align data blocks on page size");

DEFINE_int64(prepopulate_block_cache, 0,
             "Pre-populate hot/warm blocks in block cache. 0 to disable and 1 "
             "to insert during flush");

DEFINE_uint32(uncache_aggressiveness,
              ROCKSDB_NAMESPACE::ColumnFamilyOptions().uncache_aggressiveness,
              "Aggressiveness of erasing cache entries that are likely "
              "obsolete. 0 = disabled, 1 = minimum, 100 = moderate, 10000 = "
              "normal max");

DEFINE_bool(use_data_block_hash_index, false,
            "if use kDataBlockBinaryAndHash "
            "instead of kDataBlockBinarySearch. "
            "This is valid if only we use BlockTable");

DEFINE_double(data_block_hash_table_util_ratio, 0.75,
              "util ratio for data block hash index table. "
              "This is only valid if use_data_block_hash_index is "
              "set to true");

DEFINE_int64(compressed_cache_size, -1,
             "Number of bytes to use as a cache of compressed data.");

DEFINE_int64(row_cache_size, 0,
             "Number of bytes to use as a cache of individual rows"
             " (0 = disabled).");

DEFINE_int32(open_files, ROCKSDB_NAMESPACE::Options().max_open_files,
             "Maximum number of files to keep open at the same time"
             " (use default if == 0)");

DEFINE_int32(file_opening_threads,
             ROCKSDB_NAMESPACE::Options().max_file_opening_threads,
             "If open_files is set to -1, this option set the number of "
             "threads that will be used to open files during DB::Open()");

DEFINE_uint64(compaction_readahead_size,
              ROCKSDB_NAMESPACE::Options().compaction_readahead_size,
              "Compaction readahead size");

DEFINE_int32(log_readahead_size, 0, "WAL and manifest readahead size");

DEFINE_int32(random_access_max_buffer_size, 1024 * 1024,
             "Maximum windows randomaccess buffer size");

DEFINE_int32(writable_file_max_buffer_size, 1024 * 1024,
             "Maximum write buffer for Writable File");

DEFINE_int32(bloom_bits, -1,
             "Bloom filter bits per key. Negative means use default."
             "Zero disables.");

DEFINE_bool(use_ribbon_filter, false, "Use Ribbon instead of Bloom filter");

DEFINE_double(memtable_bloom_size_ratio, 0,
              "Ratio of memtable size used for bloom filter. 0 means no bloom "
              "filter.");
DEFINE_bool(memtable_whole_key_filtering, false,
            "Try to use whole key bloom filter in memtables.");
DEFINE_bool(memtable_use_huge_page, false,
            "Try to use huge page in memtables.");

DEFINE_bool(whole_key_filtering,
            ROCKSDB_NAMESPACE::BlockBasedTableOptions().whole_key_filtering,
            "Use whole keys (in addition to prefixes) in SST bloom filter.");

DEFINE_bool(use_existing_db, false,
            "If true, do not destroy the existing database.  If you set this "
            "flag and also specify a benchmark that wants a fresh database, "
            "that benchmark will fail.");

DEFINE_bool(use_existing_keys, false,
            "If true, uses existing keys in the DB, "
            "rather than generating new ones. This involves some startup "
            "latency to load all keys into memory. It is supported for the "
            "same read/overwrite benchmarks as `-use_existing_db=true`, which "
            "must also be set for this flag to be enabled. When this flag is "
            "set, the value for `-num` will be ignored.");

DEFINE_bool(show_table_properties, false,
            "If true, then per-level table"
            " properties will be printed on every stats-interval when"
            " stats_interval is set and stats_per_interval is on.");

DEFINE_string(db, "", "Use the db with the following name.");

DEFINE_bool(progress_reports, true,
            "If true, db_bench will report number of finished operations.");

// Read cache flags

DEFINE_string(read_cache_path, "",
              "If not empty string, a read cache will be used in this path");

DEFINE_int64(read_cache_size, 4LL * 1024 * 1024 * 1024,
             "Maximum size of the read cache");

DEFINE_bool(read_cache_direct_write, true,
            "Whether to use Direct IO for writing to the read cache");

DEFINE_bool(read_cache_direct_read, true,
            "Whether to use Direct IO for reading from read cache");

DEFINE_bool(use_keep_filter, false, "Whether to use a noop compaction filter");

static bool ValidateCacheNumshardbits(const char* flagname, int32_t value) {
  if (value >= 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be < 20\n", flagname,
            value);
    return false;
  }
  return true;
}

DEFINE_bool(verify_checksum, true,
            "Verify checksum for every block read from storage");

DEFINE_int32(checksum_type,
             ROCKSDB_NAMESPACE::BlockBasedTableOptions().checksum,
             "ChecksumType as an int");

DEFINE_bool(statistics, false, "Database statistics");
DEFINE_int32(stats_level, ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers,
             "stats level for statistics");
DEFINE_string(statistics_string, "", "Serialized statistics string");
static class std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> dbstats;

DEFINE_int64(writes, -1,
             "Number of write operations to do. If negative, do --num reads.");

DEFINE_bool(finish_after_writes, false,
            "Write thread terminates after all writes are finished");

DEFINE_bool(sync, false, "Sync all writes to disk");

DEFINE_bool(use_fsync, false, "If true, issue fsync instead of fdatasync");

DEFINE_bool(disable_wal, false, "If true, do not write WAL for write.");

DEFINE_bool(manual_wal_flush, false,
            "If true, buffer WAL until buffer is full or a manual FlushWAL().");

DEFINE_string(wal_compression, "none",
              "Algorithm to use for WAL compression. none to disable.");
static enum ROCKSDB_NAMESPACE::CompressionType FLAGS_wal_compression_e =
    ROCKSDB_NAMESPACE::kNoCompression;

DEFINE_string(wal_dir, "", "If not empty, use the given dir for WAL");

DEFINE_string(truth_db, "/dev/shm/truth_db/dbbench",
              "Truth key/values used when using verify");

DEFINE_int32(num_levels, 7, "The total number of levels");

DEFINE_int64(target_file_size_base,
             ROCKSDB_NAMESPACE::Options().target_file_size_base,
             "Target file size at level-1");

DEFINE_int32(target_file_size_multiplier,
             ROCKSDB_NAMESPACE::Options().target_file_size_multiplier,
             "A multiplier to compute target level-N file size (N >= 2)");

DEFINE_uint64(max_bytes_for_level_base,
              ROCKSDB_NAMESPACE::Options().max_bytes_for_level_base,
              "Max bytes for level-1");

DEFINE_bool(level_compaction_dynamic_level_bytes, false,
            "Whether level size base is dynamic");

DEFINE_double(max_bytes_for_level_multiplier, 10,
              "A multiplier to compute max bytes for level-N (N >= 2)");

static std::vector<int> FLAGS_max_bytes_for_level_multiplier_additional_v;
DEFINE_string(max_bytes_for_level_multiplier_additional, "",
              "A vector that specifies additional fanout per level");

DEFINE_int32(level0_stop_writes_trigger,
             ROCKSDB_NAMESPACE::Options().level0_stop_writes_trigger,
             "Number of files in level-0 that will trigger put stop.");

DEFINE_int32(level0_slowdown_writes_trigger,
             ROCKSDB_NAMESPACE::Options().level0_slowdown_writes_trigger,
             "Number of files in level-0 that will slow down writes.");

DEFINE_int32(level0_file_num_compaction_trigger,
             ROCKSDB_NAMESPACE::Options().level0_file_num_compaction_trigger,
             "Number of files in level-0 when compactions start.");

DEFINE_uint64(periodic_compaction_seconds,
              ROCKSDB_NAMESPACE::Options().periodic_compaction_seconds,
              "Files older than this will be picked up for compaction and"
              " rewritten to the same level");

DEFINE_uint64(ttl_seconds, ROCKSDB_NAMESPACE::Options().ttl, "Set options.ttl");

static bool ValidateInt32Percent(const char* flagname, int32_t value) {
  if (value <= 0 || value >= 100) {
    fprintf(stderr, "Invalid value for --%s: %d, 0< pct <100 \n", flagname,
            value);
    return false;
  }
  return true;
}
DEFINE_int32(readwritepercent, 90,
             "Ratio of reads to reads/writes (expressed as percentage) for "
             "the ReadRandomWriteRandom workload. The default value 90 means "
             "90% operations out of all reads and writes operations are "
             "reads. In other words, 9 gets for every 1 put.");

DEFINE_int32(mergereadpercent, 70,
             "Ratio of merges to merges&reads (expressed as percentage) for "
             "the ReadRandomMergeRandom workload. The default value 70 means "
             "70% out of all read and merge operations are merges. In other "
             "words, 7 merges for every 3 gets.");

DEFINE_int32(deletepercent, 2,
             "Percentage of deletes out of reads/writes/deletes (used in "
             "RandomWithVerify only). RandomWithVerify "
             "calculates writepercent as (100 - FLAGS_readwritepercent - "
             "deletepercent), so deletepercent must be smaller than (100 - "
             "FLAGS_readwritepercent)");

DEFINE_bool(optimize_filters_for_hits,
            ROCKSDB_NAMESPACE::Options().optimize_filters_for_hits,
            "Optimizes bloom filters for workloads for most lookups return "
            "a value. For now this doesn't create bloom filters for the max "
            "level of the LSM to reduce metadata that should fit in RAM. ");

DEFINE_bool(paranoid_checks, ROCKSDB_NAMESPACE::Options().paranoid_checks,
            "RocksDB will aggressively check consistency of the data.");

DEFINE_bool(force_consistency_checks,
            ROCKSDB_NAMESPACE::Options().force_consistency_checks,
            "Runs consistency checks on the LSM every time a change is "
            "applied.");

DEFINE_uint64(delete_obsolete_files_period_micros, 0,
              "Ignored. Left here for backward compatibility");

DEFINE_int64(writes_before_delete_range, 0,
             "Number of writes before DeleteRange is called regularly.");

DEFINE_int64(writes_per_range_tombstone, 0,
             "Number of writes between range tombstones");

DEFINE_int64(range_tombstone_width, 100, "Number of keys in tombstone's range");

DEFINE_int64(max_num_range_tombstones, 0,
             "Maximum number of range tombstones to insert.");

DEFINE_bool(expand_range_tombstones, false,
            "Expand range tombstone into sequential regular tombstones.");

// Transactions Options
DEFINE_bool(optimistic_transaction_db, false,
            "Open a OptimisticTransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_bool(transaction_db, false,
            "Open a TransactionDB instance. "
            "Required for randomtransaction benchmark.");

DEFINE_uint64(transaction_sets, 2,
              "Number of keys each transaction will "
              "modify (use in RandomTransaction only).  Max: 9999");

DEFINE_bool(transaction_set_snapshot, false,
            "Setting to true will have each transaction call SetSnapshot()"
            " upon creation.");

DEFINE_int32(transaction_sleep, 0,
             "Max microseconds to sleep in between "
             "reading and writing a value (used in RandomTransaction only). ");

DEFINE_uint64(transaction_lock_timeout, 100,
              "If using a transaction_db, specifies the lock wait timeout in"
              " milliseconds before failing a transaction waiting on a lock");
DEFINE_string(
    options_file, "",
    "The path to a RocksDB options file.  If specified, then db_bench will "
    "run with the RocksDB options in the default column family of the "
    "specified options file. "
    "Note that with this setting, db_bench will ONLY accept the following "
    "RocksDB options related command-line arguments, all other arguments "
    "that are related to RocksDB options will be ignored:\n"
    "\t--use_existing_db\n"
    "\t--use_existing_keys\n"
    "\t--statistics\n"
    "\t--row_cache_size\n"
    "\t--row_cache_numshardbits\n"
    "\t--enable_io_prio\n"
    "\t--dump_malloc_stats\n"
    "\t--num_multi_db\n");

// FIFO Compaction Options
DEFINE_uint64(fifo_compaction_max_table_files_size_mb, 0,
              "The limit of total table file sizes to trigger FIFO compaction");

DEFINE_bool(fifo_compaction_allow_compaction, true,
            "Allow compaction in FIFO compaction.");

DEFINE_uint64(fifo_compaction_ttl, 0, "TTL for the SST Files in seconds.");

DEFINE_uint64(fifo_age_for_warm, 0, "age_for_warm for FIFO compaction.");

// Stacked BlobDB Options
DEFINE_bool(use_blob_db, false, "[Stacked BlobDB] Open a BlobDB instance.");

DEFINE_bool(
    blob_db_enable_gc,
    ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().enable_garbage_collection,
    "[Stacked BlobDB] Enable BlobDB garbage collection.");

DEFINE_double(
    blob_db_gc_cutoff,
    ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().garbage_collection_cutoff,
    "[Stacked BlobDB] Cutoff ratio for BlobDB garbage collection.");

DEFINE_bool(blob_db_is_fifo,
            ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().is_fifo,
            "[Stacked BlobDB] Enable FIFO eviction strategy in BlobDB.");

DEFINE_uint64(blob_db_max_db_size,
              ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().max_db_size,
              "[Stacked BlobDB] Max size limit of the directory where blob "
              "files are stored.");

DEFINE_uint64(blob_db_max_ttl_range, 0,
              "[Stacked BlobDB] TTL range to generate BlobDB data (in "
              "seconds). 0 means no TTL.");

DEFINE_uint64(
    blob_db_ttl_range_secs,
    ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().ttl_range_secs,
    "[Stacked BlobDB] TTL bucket size to use when creating blob files.");

DEFINE_uint64(
    blob_db_min_blob_size,
    ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().min_blob_size,
    "[Stacked BlobDB] Smallest blob to store in a file. Blobs "
    "smaller than this will be inlined with the key in the LSM tree.");

DEFINE_uint64(blob_db_bytes_per_sync,
              ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().bytes_per_sync,
              "[Stacked BlobDB] Bytes to sync blob file at.");

DEFINE_uint64(blob_db_file_size,
              ROCKSDB_NAMESPACE::blob_db::BlobDBOptions().blob_file_size,
              "[Stacked BlobDB] Target size of each blob file.");

DEFINE_string(
    blob_db_compression_type, "snappy",
    "[Stacked BlobDB] Algorithm to use to compress blobs in blob files.");
static enum ROCKSDB_NAMESPACE::CompressionType
    FLAGS_blob_db_compression_type_e = ROCKSDB_NAMESPACE::kSnappyCompression;

// Integrated BlobDB options
DEFINE_bool(
    enable_blob_files,
    ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().enable_blob_files,
    "[Integrated BlobDB] Enable writing large values to separate blob files.");

DEFINE_uint64(min_blob_size,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().min_blob_size,
              "[Integrated BlobDB] The size of the smallest value to be stored "
              "separately in a blob file.");

DEFINE_uint64(blob_file_size,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().blob_file_size,
              "[Integrated BlobDB] The size limit for blob files.");

DEFINE_string(blob_compression_type, "none",
              "[Integrated BlobDB] The compression algorithm to use for large "
              "values stored in blob files.");

DEFINE_bool(enable_blob_garbage_collection,
            ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                .enable_blob_garbage_collection,
            "[Integrated BlobDB] Enable blob garbage collection.");

DEFINE_double(blob_garbage_collection_age_cutoff,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                  .blob_garbage_collection_age_cutoff,
              "[Integrated BlobDB] The cutoff in terms of blob file age for "
              "garbage collection.");

DEFINE_double(blob_garbage_collection_force_threshold,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                  .blob_garbage_collection_force_threshold,
              "[Integrated BlobDB] The threshold for the ratio of garbage in "
              "the eligible blob files for forcing garbage collection.");

DEFINE_uint64(blob_compaction_readahead_size,
              ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions()
                  .blob_compaction_readahead_size,
              "[Integrated BlobDB] Compaction readahead for blob files.");

DEFINE_int32(
    blob_file_starting_level,
    ROCKSDB_NAMESPACE::AdvancedColumnFamilyOptions().blob_file_starting_level,
    "[Integrated BlobDB] The starting level for blob files.");

DEFINE_bool(use_blob_cache, false, "[Integrated BlobDB] Enable blob cache.");

DEFINE_bool(
    use_shared_block_and_blob_cache, true,
    "[Integrated BlobDB] Use a shared backing cache for both block "
    "cache and blob cache. It only takes effect if use_blob_cache is enabled.");

DEFINE_uint64(
    blob_cache_size, 8 << 20,
    "[Integrated BlobDB] Number of bytes to use as a cache of blobs. It only "
    "takes effect if the block and blob caches are different "
    "(use_shared_block_and_blob_cache = false).");

DEFINE_int32(blob_cache_numshardbits, 6,
             "[Integrated BlobDB] Number of shards for the blob cache is 2 ** "
             "blob_cache_numshardbits. Negative means use default settings. "
             "It only takes effect if blob_cache_size is greater than 0, and "
             "the block and blob caches are different "
             "(use_shared_block_and_blob_cache = false).");

DEFINE_int32(prepopulate_blob_cache, 0,
             "[Integrated BlobDB] Pre-populate hot/warm blobs in blob cache. 0 "
             "to disable and 1 to insert during flush.");

// Secondary DB instance Options
DEFINE_bool(use_secondary_db, false,
            "Open a RocksDB secondary instance. A primary instance can be "
            "running in another db_bench process.");

DEFINE_string(secondary_path, "",
              "Path to a directory used by the secondary instance to store "
              "private files, e.g. info log.");

DEFINE_int32(secondary_update_interval, 5,
             "Secondary instance attempts to catch up with the primary every "
             "secondary_update_interval seconds.");

DEFINE_bool(open_as_follower, false,
            "Open a RocksDB DB as a follower. The leader instance can be "
            "running in another db_bench process.");

DEFINE_string(leader_path, "", "Path to the directory of the leader DB");

DEFINE_bool(report_bg_io_stats, false,
            "Measure times spents on I/Os while in compactions. ");

DEFINE_bool(use_stderr_info_logger, false,
            "Write info logs to stderr instead of to LOG file. ");

DEFINE_string(trace_file, "", "Trace workload to a file. ");

DEFINE_double(trace_replay_fast_forward, 1.0,
              "Fast forward trace replay, must > 0.0.");
DEFINE_int32(block_cache_trace_sampling_frequency, 1,
             "Block cache trace sampling frequency, termed s. It uses spatial "
             "downsampling and samples accesses to one out of s blocks.");
DEFINE_int64(
    block_cache_trace_max_trace_file_size_in_bytes,
    uint64_t{64} * 1024 * 1024 * 1024,
    "The maximum block cache trace file size in bytes. Block cache accesses "
    "will not be logged if the trace file size exceeds this threshold. Default "
    "is 64 GB.");
DEFINE_string(block_cache_trace_file, "", "Block cache trace file path.");
DEFINE_int32(trace_replay_threads, 1,
             "The number of threads to replay, must >=1.");

DEFINE_bool(io_uring_enabled, true,
            "If true, enable the use of IO uring if the platform supports it");
extern "C" bool RocksDbIOUringEnable() { return FLAGS_io_uring_enabled; }

DEFINE_bool(adaptive_readahead, false,
            "carry forward internal auto readahead size from one file to next "
            "file at each level during iteration");

DEFINE_bool(rate_limit_user_ops, false,
            "When true use Env::IO_USER priority level to charge internal rate "
            "limiter for reads associated with user operations.");

DEFINE_bool(file_checksum, false,
            "When true use FileChecksumGenCrc32cFactory for "
            "file_checksum_gen_factory.");

DEFINE_bool(rate_limit_auto_wal_flush, false,
            "When true use Env::IO_USER priority level to charge internal rate "
            "limiter for automatic WAL flush (`Options::manual_wal_flush` == "
            "false) after the user write operation.");

DEFINE_bool(async_io, false,
            "When set true, RocksDB does asynchronous reads for internal auto "
            "readahead prefetching.");

DEFINE_bool(optimize_multiget_for_io, true,
            "When set true, RocksDB does asynchronous reads for SST files in "
            "multiple levels for MultiGet.");

DEFINE_bool(charge_compression_dictionary_building_buffer, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kCompressionDictionaryBuildingBuffer");

DEFINE_bool(charge_filter_construction, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kFilterConstruction");

DEFINE_bool(charge_table_reader, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kBlockBasedTableReader");

DEFINE_bool(charge_file_metadata, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kFileMetadata");

DEFINE_bool(charge_blob_cache, false,
            "Setting for "
            "CacheEntryRoleOptions::charged of "
            "CacheEntryRole::kBlobCache");

DEFINE_uint64(backup_rate_limit, 0ull,
              "If non-zero, db_bench will rate limit reads and writes for DB "
              "backup. This "
              "is the global rate in ops/second.");

DEFINE_uint64(restore_rate_limit, 0ull,
              "If non-zero, db_bench will rate limit reads and writes for DB "
              "restore. This "
              "is the global rate in ops/second.");

DEFINE_string(backup_dir, "",
              "If not empty string, use the given dir for backup.");

DEFINE_string(restore_dir, "",
              "If not empty string, use the given dir for restore.");

DEFINE_uint64(
    initial_auto_readahead_size,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().initial_auto_readahead_size,
    "RocksDB does auto-readahead for iterators on noticing more than two reads "
    "for a table file if user doesn't provide readahead_size. The readahead "
    "size starts at initial_auto_readahead_size");

DEFINE_uint64(
    max_auto_readahead_size,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions().max_auto_readahead_size,
    "Rocksdb implicit readahead starts at "
    "BlockBasedTableOptions.initial_auto_readahead_size and doubles on every "
    "additional read upto max_auto_readahead_size");

DEFINE_uint64(
    num_file_reads_for_auto_readahead,
    ROCKSDB_NAMESPACE::BlockBasedTableOptions()
        .num_file_reads_for_auto_readahead,
    "Rocksdb implicit readahead is enabled if reads are sequential and "
    "num_file_reads_for_auto_readahead indicates after how many sequential "
    "reads into that file internal auto prefetching should be start.");

DEFINE_bool(
    auto_readahead_size, false,
    "When set true, RocksDB does auto tuning of readahead size during Scans");

DEFINE_bool(paranoid_memory_checks, false,
            "Sets CF option paranoid_memory_checks");

static enum ROCKSDB_NAMESPACE::CompressionType StringToCompressionType(
    const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "none")) {
    return ROCKSDB_NAMESPACE::kNoCompression;
  } else if (!strcasecmp(ctype, "snappy")) {
    return ROCKSDB_NAMESPACE::kSnappyCompression;
  } else if (!strcasecmp(ctype, "zlib")) {
    return ROCKSDB_NAMESPACE::kZlibCompression;
  } else if (!strcasecmp(ctype, "bzip2")) {
    return ROCKSDB_NAMESPACE::kBZip2Compression;
  } else if (!strcasecmp(ctype, "lz4")) {
    return ROCKSDB_NAMESPACE::kLZ4Compression;
  } else if (!strcasecmp(ctype, "lz4hc")) {
    return ROCKSDB_NAMESPACE::kLZ4HCCompression;
  } else if (!strcasecmp(ctype, "xpress")) {
    return ROCKSDB_NAMESPACE::kXpressCompression;
  } else if (!strcasecmp(ctype, "zstd")) {
    return ROCKSDB_NAMESPACE::kZSTD;
  } else {
    fprintf(stderr, "Cannot parse compression type '%s'\n", ctype);
    exit(1);
  }
}

static enum ROCKSDB_NAMESPACE::TieredAdmissionPolicy StringToAdmissionPolicy(
    const char* policy) {
  assert(policy);

  if (!strcasecmp(policy, "auto")) {
    return ROCKSDB_NAMESPACE::kAdmPolicyAuto;
  } else if (!strcasecmp(policy, "placeholder")) {
    return ROCKSDB_NAMESPACE::kAdmPolicyPlaceholder;
  } else if (!strcasecmp(policy, "allow_cache_hits")) {
    return ROCKSDB_NAMESPACE::kAdmPolicyAllowCacheHits;
  } else if (!strcasecmp(policy, "three_queue")) {
    return ROCKSDB_NAMESPACE::kAdmPolicyThreeQueue;
  } else if (!strcasecmp(policy, "allow_all")) {
    return ROCKSDB_NAMESPACE::kAdmPolicyAllowAll;
  } else {
    fprintf(stderr, "Cannot parse admission policy %s\n", policy);
    exit(1);
  }
}

static std::string ColumnFamilyName(size_t i) {
  if (i == 0) {
    return ROCKSDB_NAMESPACE::kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

DEFINE_string(compression_type, "snappy",
              "Algorithm to use to compress the database");
static enum ROCKSDB_NAMESPACE::CompressionType FLAGS_compression_type_e =
    ROCKSDB_NAMESPACE::kSnappyCompression;

DEFINE_int64(sample_for_compression, 0, "Sample every N block for compression");

DEFINE_int32(compression_level, ROCKSDB_NAMESPACE::CompressionOptions().level,
             "Compression level. The meaning of this value is library-"
             "dependent. If unset, we try to use the default for the library "
             "specified in `--compression_type`");

DEFINE_int32(compression_max_dict_bytes,
             ROCKSDB_NAMESPACE::CompressionOptions().max_dict_bytes,
             "Maximum size of dictionary used to prime the compression "
             "library.");

DEFINE_int32(compression_zstd_max_train_bytes,
             ROCKSDB_NAMESPACE::CompressionOptions().zstd_max_train_bytes,
             "Maximum size of training data passed to zstd's dictionary "
             "trainer.");

DEFINE_int32(min_level_to_compress, -1,
             "If non-negative, compression starts"
             " from this level. Levels with number < min_level_to_compress are"
             " not compressed. Otherwise, apply compression_type to "
             "all levels.");

DEFINE_int32(compression_parallel_threads, 1,
             "Number of threads for parallel compression.");

DEFINE_uint64(compression_max_dict_buffer_bytes,
              ROCKSDB_NAMESPACE::CompressionOptions().max_dict_buffer_bytes,
              "Maximum bytes to buffer to collect samples for dictionary.");

DEFINE_bool(compression_use_zstd_dict_trainer,
            ROCKSDB_NAMESPACE::CompressionOptions().use_zstd_dict_trainer,
            "If true, use ZSTD_TrainDictionary() to create dictionary, else"
            "use ZSTD_FinalizeDictionary() to create dictionary");

static bool ValidateTableCacheNumshardbits(const char* flagname,
                                           int32_t value) {
  if (0 >= value || value >= 20) {
    fprintf(stderr, "Invalid value for --%s: %d, must be  0 < val < 20\n",
            flagname, value);
    return false;
  }
  return true;
}
DEFINE_int32(table_cache_numshardbits, 4, "");

DEFINE_string(env_uri, "",
              "URI for registry Env lookup. Mutually exclusive with --fs_uri");
DEFINE_string(fs_uri, "",
              "URI for registry Filesystem lookup. Mutually exclusive"
              " with --env_uri."
              " Creates a default environment with the specified filesystem.");
DEFINE_string(simulate_hybrid_fs_file, "",
              "File for Store Metadata for Simulate hybrid FS. Empty means "
              "disable the feature. Now, if it is set, last_level_temperature "
              "is set to kWarm.");
DEFINE_int32(simulate_hybrid_hdd_multipliers, 1,
             "In simulate_hybrid_fs_file or simulate_hdd mode, how many HDDs "
             "are simulated.");
DEFINE_bool(simulate_hdd, false, "Simulate read/write latency on HDD.");

DEFINE_int64(
    preclude_last_level_data_seconds, 0,
    "Preclude the latest data from the last level. (Used for tiered storage)");

DEFINE_int64(preserve_internal_time_seconds, 0,
             "Preserve the internal time information which stores with SST.");

static std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;

static ROCKSDB_NAMESPACE::Env* FLAGS_env = ROCKSDB_NAMESPACE::Env::Default();

DEFINE_int64(stats_interval, 0,
             "Stats are reported every N operations when this is greater than "
             "zero. When 0 the interval grows over time.");

DEFINE_int64(stats_interval_seconds, 0,
             "Report stats every N seconds. This overrides stats_interval when"
             " both are > 0.");

DEFINE_int32(stats_per_interval, 0,
             "Reports additional stats per interval when this is greater than "
             "0.");

DEFINE_uint64(slow_usecs, 1000000,
              "A message is printed for operations that take at least this "
              "many microseconds.");

DEFINE_int64(report_interval_seconds, 0,
             "If greater than zero, it will write simple stats in CSV format "
             "to --report_file every N seconds");

DEFINE_string(report_file, "report.csv",
              "Filename where some simple stats are reported to (if "
              "--report_interval_seconds is bigger than 0)");

DEFINE_int32(thread_status_per_interval, 0,
             "Takes and report a snapshot of the current status of each thread"
             " when this is greater than 0.");

DEFINE_int32(perf_level, ROCKSDB_NAMESPACE::PerfLevel::kDisable,
             "Level of perf collection");

DEFINE_uint64(soft_pending_compaction_bytes_limit, 64ull * 1024 * 1024 * 1024,
              "Slowdown writes if pending compaction bytes exceed this number");

DEFINE_uint64(hard_pending_compaction_bytes_limit, 128ull * 1024 * 1024 * 1024,
              "Stop writes if pending compaction bytes exceed this number");

DEFINE_uint64(delayed_write_rate, 8388608u,
              "Limited bytes allowed to DB when soft_rate_limit or "
              "level0_slowdown_writes_trigger triggers");

DEFINE_bool(enable_pipelined_write, true,
            "Allow WAL and memtable writes to be pipelined");

DEFINE_bool(
    unordered_write, false,
    "Enable the unordered write feature, which provides higher throughput but "
    "relaxes the guarantees around atomic reads and immutable snapshots");

DEFINE_bool(allow_concurrent_memtable_write, true,
            "Allow multi-writers to update mem tables in parallel.");

DEFINE_double(experimental_mempurge_threshold, 0.0,
              "Maximum useful payload ratio estimate that triggers a mempurge "
              "(memtable garbage collection).");

DEFINE_bool(inplace_update_support,
            ROCKSDB_NAMESPACE::Options().inplace_update_support,
            "Support in-place memtable update for smaller or same-size values");

DEFINE_uint64(inplace_update_num_locks,
              ROCKSDB_NAMESPACE::Options().inplace_update_num_locks,
              "Number of RW locks to protect in-place memtable updates");

DEFINE_bool(enable_write_thread_adaptive_yield, true,
            "Use a yielding spin loop for brief writer thread waits.");

DEFINE_uint64(
    write_thread_max_yield_usec, 100,
    "Maximum microseconds for enable_write_thread_adaptive_yield operation.");

DEFINE_uint64(write_thread_slow_yield_usec, 3,
              "The threshold at which a slow yield is considered a signal that "
              "other processes or threads want the core.");

DEFINE_uint64(rate_limiter_bytes_per_sec, 0, "Set options.rate_limiter value.");

DEFINE_int64(rate_limiter_refill_period_us, 100 * 1000,
             "Set refill period on rate limiter.");

DEFINE_bool(rate_limiter_auto_tuned, false,
            "Enable dynamic adjustment of rate limit according to demand for "
            "background I/O");

DEFINE_int64(rate_limiter_single_burst_bytes, 0,
             "Set single burst bytes on background I/O rate limiter.");

DEFINE_bool(sine_write_rate, false, "Use a sine wave write_rate_limit");

DEFINE_uint64(
    sine_write_rate_interval_milliseconds, 10000,
    "Interval of which the sine wave write_rate_limit is recalculated");

DEFINE_double(sine_a, 1, "A in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_b, 1, "B in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_c, 0, "C in f(x) = A sin(bx + c) + d");

DEFINE_double(sine_d, 1, "D in f(x) = A sin(bx + c) + d");

DEFINE_bool(rate_limit_bg_reads, false,
            "Use options.rate_limiter on compaction reads");

DEFINE_uint64(
    benchmark_write_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the writes going into RocksDB. This "
    "is the global rate in bytes/second.");

// the parameters of mix_graph
DEFINE_double(keyrange_dist_a, 0.0,
              "The parameter 'a' of prefix average access distribution "
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_double(keyrange_dist_b, 0.0,
              "The parameter 'b' of prefix average access distribution "
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_double(keyrange_dist_c, 0.0,
              "The parameter 'c' of prefix average access distribution"
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_double(keyrange_dist_d, 0.0,
              "The parameter 'd' of prefix average access distribution"
              "f(x)=a*exp(b*x)+c*exp(d*x)");
DEFINE_int64(keyrange_num, 1,
             "The number of key ranges that are in the same prefix "
             "group, each prefix range will have its key access distribution");
DEFINE_double(key_dist_a, 0.0,
              "The parameter 'a' of key access distribution model f(x)=a*x^b");
DEFINE_double(key_dist_b, 0.0,
              "The parameter 'b' of key access distribution model f(x)=a*x^b");
DEFINE_double(value_theta, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
// Use reasonable defaults based on the mixgraph paper
DEFINE_double(value_k, 0.2615,
              "The parameter 'k' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
// Use reasonable defaults based on the mixgraph paper
DEFINE_double(value_sigma, 25.45,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(iter_theta, 0.0,
              "The parameter 'theta' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
// Use reasonable defaults based on the mixgraph paper
DEFINE_double(iter_k, 2.517,
              "The parameter 'k' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
// Use reasonable defaults based on the mixgraph paper
DEFINE_double(iter_sigma, 14.236,
              "The parameter 'sigma' of Generized Pareto Distribution "
              "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)");
DEFINE_double(mix_get_ratio, 1.0,
              "The ratio of Get queries of mix_graph workload");
DEFINE_double(mix_put_ratio, 0.0,
              "The ratio of Put queries of mix_graph workload");
DEFINE_double(mix_seek_ratio, 0.0,
              "The ratio of Seek queries of mix_graph workload");
DEFINE_int64(mix_max_scan_len, 10000, "The max scan length of Iterator");
DEFINE_int64(mix_max_value_size, 1024, "The max value size of this workload");
DEFINE_double(
    sine_mix_rate_noise, 0.0,
    "Add the noise ratio to the sine rate, it is between 0.0 and 1.0");
DEFINE_bool(sine_mix_rate, false,
            "Enable the sine QPS control on the mix workload");
DEFINE_uint64(
    sine_mix_rate_interval_milliseconds, 10000,
    "Interval of which the sine wave read_rate_limit is recalculated");
DEFINE_int64(mix_accesses, -1,
             "The total query accesses of mix_graph workload");

DEFINE_uint64(
    benchmark_read_rate_limit, 0,
    "If non-zero, db_bench will rate-limit the reads from RocksDB. This "
    "is the global rate in ops/second.");

DEFINE_uint64(max_compaction_bytes,
              ROCKSDB_NAMESPACE::Options().max_compaction_bytes,
              "Max bytes allowed in one compaction");

DEFINE_bool(readonly, false, "Run read only benchmarks.");

DEFINE_bool(print_malloc_stats, false,
            "Print malloc stats to stdout after benchmarks finish.");

DEFINE_bool(disable_auto_compactions, false, "Do not auto trigger compactions");

DEFINE_uint64(wal_ttl_seconds, 0, "Set the TTL for the WAL Files in seconds.");
DEFINE_uint64(wal_size_limit_MB, 0,
              "Set the size limit for the WAL Files in MB.");
DEFINE_uint64(max_total_wal_size, 0, "Set total max WAL size");

DEFINE_bool(mmap_read, ROCKSDB_NAMESPACE::Options().allow_mmap_reads,
            "Allow reads to occur via mmap-ing files");

DEFINE_bool(mmap_write, ROCKSDB_NAMESPACE::Options().allow_mmap_writes,
            "Allow writes to occur via mmap-ing files");

DEFINE_bool(use_direct_reads, ROCKSDB_NAMESPACE::Options().use_direct_reads,
            "Use O_DIRECT for reading data");

DEFINE_bool(use_direct_io_for_flush_and_compaction,
            ROCKSDB_NAMESPACE::Options().use_direct_io_for_flush_and_compaction,
            "Use O_DIRECT for background flush and compaction writes");

DEFINE_bool(advise_random_on_open,
            ROCKSDB_NAMESPACE::Options().advise_random_on_open,
            "Advise random access on table file open");

DEFINE_bool(use_tailing_iterator, false,
            "Use tailing iterator to access a series of keys instead of get");

DEFINE_bool(use_adaptive_mutex, ROCKSDB_NAMESPACE::Options().use_adaptive_mutex,
            "Use adaptive mutex");

DEFINE_uint64(bytes_per_sync, ROCKSDB_NAMESPACE::Options().bytes_per_sync,
              "Allows OS to incrementally sync SST files to disk while they are"
              " being written, in the background. Issue one request for every"
              " bytes_per_sync written. 0 turns it off.");

DEFINE_uint64(wal_bytes_per_sync,
              ROCKSDB_NAMESPACE::Options().wal_bytes_per_sync,
              "Allows OS to incrementally sync WAL files to disk while they are"
              " being written, in the background. Issue one request for every"
              " wal_bytes_per_sync written. 0 turns it off.");

DEFINE_bool(use_single_deletes, true,
            "Use single deletes (used in RandomReplaceKeys only).");

DEFINE_double(stddev, 2000.0,
              "Standard deviation of normal distribution used for picking keys"
              " (used in RandomReplaceKeys only).");

DEFINE_int32(key_id_range, 100000,
             "Range of possible value of key id (used in TimeSeries only).");

DEFINE_string(expire_style, "none",
              "Style to remove expired time entries. Can be one of the options "
              "below: none (do not expired data), compaction_filter (use a "
              "compaction filter to remove expired data), delete (seek IDs and "
              "remove expired data) (used in TimeSeries only).");

DEFINE_uint64(
    time_range, 100000,
    "Range of timestamp that store in the database (used in TimeSeries"
    " only).");

DEFINE_int32(num_deletion_threads, 1,
             "Number of threads to do deletion (used in TimeSeries and delete "
             "expire_style only).");

DEFINE_int32(max_successive_merges, 0,
             "Maximum number of successive merge operations on a key in the "
             "memtable");

DEFINE_bool(strict_max_successive_merges, false,
            "Whether to issue filesystem reads to keep within "
            "`max_successive_merges` limit");

static bool ValidatePrefixSize(const char* flagname, int32_t value) {
  if (value < 0 || value >= 2000000000) {
    fprintf(stderr, "Invalid value for --%s: %d. 0<= PrefixSize <=2000000000\n",
            flagname, value);
    return false;
  }
  return true;
}

DEFINE_int32(prefix_size, 0,
             "control the prefix size for HashSkipList and plain table");
DEFINE_int64(keys_per_prefix, 0,
             "control average number of keys generated per prefix, 0 means no "
             "special handling of the prefix, i.e. use the prefix comes with "
             "the generated random number.");
DEFINE_bool(total_order_seek, false,
            "Enable total order seek regardless of index format.");
DEFINE_bool(prefix_same_as_start, false,
            "Enforce iterator to return keys with prefix same as seek key.");
DEFINE_bool(
    seek_missing_prefix, false,
    "Iterator seek to keys with non-exist prefixes. Require prefix_size > 8");

DEFINE_int32(memtable_insert_with_hint_prefix_size, 0,
             "If non-zero, enable "
             "memtable insert with hint with the given prefix size.");
DEFINE_bool(enable_io_prio, false,
            "Lower the background flush/compaction threads' IO priority");
DEFINE_bool(enable_cpu_prio, false,
            "Lower the background flush/compaction threads' CPU priority");
DEFINE_bool(identity_as_first_hash, false,
            "the first hash function of cuckoo table becomes an identity "
            "function. This is only valid when key is 8 bytes");
DEFINE_bool(dump_malloc_stats, true, "Dump malloc stats in LOG ");
DEFINE_uint64(stats_dump_period_sec,
              ROCKSDB_NAMESPACE::Options().stats_dump_period_sec,
              "Gap between printing stats to log in seconds");
DEFINE_uint64(stats_persist_period_sec,
              ROCKSDB_NAMESPACE::Options().stats_persist_period_sec,
              "Gap between persisting stats in seconds");
DEFINE_bool(persist_stats_to_disk,
            ROCKSDB_NAMESPACE::Options().persist_stats_to_disk,
            "whether to persist stats to disk");
DEFINE_uint64(stats_history_buffer_size,
              ROCKSDB_NAMESPACE::Options().stats_history_buffer_size,
              "Max number of stats snapshots to keep in memory");
DEFINE_bool(avoid_flush_during_recovery,
            ROCKSDB_NAMESPACE::Options().avoid_flush_during_recovery,
            "If true, avoids flushing the recovered WAL data where possible.");
DEFINE_int64(multiread_stride, 0,
             "Stride length for the keys in a MultiGet batch");
DEFINE_bool(multiread_batched, false, "Use the new MultiGet API");

DEFINE_string(memtablerep, "skip_list", "");
DEFINE_int64(hash_bucket_count, 1024 * 1024, "hash bucket count");
DEFINE_bool(use_plain_table, false,
            "if use plain table instead of block-based table format");
DEFINE_bool(use_cuckoo_table, false, "if use cuckoo table format");
DEFINE_double(cuckoo_hash_ratio, 0.9, "Hash ratio for Cuckoo SST table.");
DEFINE_bool(use_hash_search, false,
            "if use kHashSearch instead of kBinarySearch. "
            "This is valid if only we use BlockTable");
DEFINE_string(merge_operator, "",
              "The merge operator to use with the database."
              "If a new merge operator is specified, be sure to use fresh"
              " database The possible merge operators are defined in"
              " utilities/merge_operators.h");
DEFINE_int32(skip_list_lookahead, 0,
             "Used with skip_list memtablerep; try linear search first for "
             "this many steps from the previous position");
DEFINE_bool(report_file_operations, false,
            "if report number of file operations");
DEFINE_bool(report_open_timing, false, "if report open timing");
DEFINE_int32(readahead_size, 0, "Iterator readahead size");

DEFINE_bool(read_with_latest_user_timestamp, true,
            "If true, always use the current latest timestamp for read. If "
            "false, choose a random timestamp from the past.");

DEFINE_string(cache_uri, "", "Full URI for creating a custom cache object");
DEFINE_string(secondary_cache_uri, "",
              "Full URI for creating a custom secondary cache object");
static class std::shared_ptr<ROCKSDB_NAMESPACE::SecondaryCache> secondary_cache;

static const bool FLAGS_prefix_size_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_prefix_size, &ValidatePrefixSize);

static const bool FLAGS_key_size_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_key_size, &ValidateKeySize);

static const bool FLAGS_cache_numshardbits_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_cache_numshardbits,
                          &ValidateCacheNumshardbits);

static const bool FLAGS_readwritepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_readwritepercent, &ValidateInt32Percent);

DEFINE_int32(disable_seek_compaction, false,
             "Not used, left here for backwards compatibility");

DEFINE_bool(allow_data_in_errors,
            ROCKSDB_NAMESPACE::Options().allow_data_in_errors,
            "If true, allow logging data, e.g. key, value in LOG files.");

static const bool FLAGS_deletepercent_dummy __attribute__((__unused__)) =
    RegisterFlagValidator(&FLAGS_deletepercent, &ValidateInt32Percent);
static const bool FLAGS_table_cache_numshardbits_dummy
    __attribute__((__unused__)) = RegisterFlagValidator(
        &FLAGS_table_cache_numshardbits, &ValidateTableCacheNumshardbits);

DEFINE_uint32(write_batch_protection_bytes_per_key, 0,
              "Size of per-key-value checksum in each write batch. Currently "
              "only value 0 and 8 are supported.");

DEFINE_uint32(
    memtable_protection_bytes_per_key, 0,
    "Enable memtable per key-value checksum protection. "
    "Each entry in memtable will be suffixed by a per key-value checksum. "
    "This options determines the size of such checksums. "
    "Supported values: 0, 1, 2, 4, 8.");

DEFINE_uint32(block_protection_bytes_per_key, 0,
              "Enable block per key-value checksum protection. "
              "Supported values: 0, 1, 2, 4, 8.");

DEFINE_bool(build_info, false,
            "Print the build info via GetRocksBuildInfoAsString");

DEFINE_bool(track_and_verify_wals_in_manifest, false,
            "If true, enable WAL tracking in the MANIFEST");

namespace ROCKSDB_NAMESPACE {
namespace {
static Status CreateMemTableRepFactory(
    const ConfigOptions& config_options,
    std::shared_ptr<MemTableRepFactory>* factory) {
  Status s;
  if (!strcasecmp(FLAGS_memtablerep.c_str(), SkipListFactory::kNickName())) {
    factory->reset(new SkipListFactory(FLAGS_skip_list_lookahead));
  } else if (!strcasecmp(FLAGS_memtablerep.c_str(), "prefix_hash")) {
    factory->reset(NewHashSkipListRepFactory(FLAGS_hash_bucket_count));
  } else if (!strcasecmp(FLAGS_memtablerep.c_str(),
                         VectorRepFactory::kNickName())) {
    factory->reset(new VectorRepFactory());
  } else if (!strcasecmp(FLAGS_memtablerep.c_str(), "hash_linkedlist")) {
    factory->reset(NewHashLinkListRepFactory(FLAGS_hash_bucket_count));
  } else {
    std::unique_ptr<MemTableRepFactory> unique;
    s = MemTableRepFactory::CreateFromString(config_options, FLAGS_memtablerep,
                                             &unique);
    if (s.ok()) {
      factory->reset(unique.release());
    }
  }
  return s;
}

}  // namespace

enum DistributionType : unsigned char { kFixed = 0, kUniform, kNormal };

static enum DistributionType FLAGS_value_size_distribution_type_e = kFixed;

static enum DistributionType StringToDistributionType(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "fixed")) {
    return kFixed;
  } else if (!strcasecmp(ctype, "uniform")) {
    return kUniform;
  } else if (!strcasecmp(ctype, "normal")) {
    return kNormal;
  }

  fprintf(stdout, "Cannot parse distribution type '%s'\n", ctype);
  exit(1);
}

class BaseDistribution {
 public:
  BaseDistribution(unsigned int _min, unsigned int _max)
      : min_value_size_(_min), max_value_size_(_max) {}
  virtual ~BaseDistribution() = default;

  unsigned int Generate() {
    auto val = Get();
    if (NeedTruncate()) {
      val = std::max(min_value_size_, val);
      val = std::min(max_value_size_, val);
    }
    return val;
  }

 private:
  virtual unsigned int Get() = 0;
  virtual bool NeedTruncate() { return true; }
  unsigned int min_value_size_;
  unsigned int max_value_size_;
};

class FixedDistribution : public BaseDistribution {
 public:
  FixedDistribution(unsigned int size)
      : BaseDistribution(size, size), size_(size) {}

 private:
  unsigned int Get() override { return size_; }
  bool NeedTruncate() override { return false; }
  unsigned int size_;
};

class NormalDistribution : public BaseDistribution,
                           public std::normal_distribution<double> {
 public:
  NormalDistribution(unsigned int _min, unsigned int _max)
      : BaseDistribution(_min, _max),
        // 99.7% values within the range [min, max].
        std::normal_distribution<double>(
            (double)(_min + _max) / 2.0 /*mean*/,
            (double)(_max - _min) / 6.0 /*stddev*/),
        gen_(rd_()) {}

 private:
  unsigned int Get() override {
    return static_cast<unsigned int>((*this)(gen_));
  }
  std::random_device rd_;
  std::mt19937 gen_;
};

class UniformDistribution : public BaseDistribution,
                            public std::uniform_int_distribution<unsigned int> {
 public:
  UniformDistribution(unsigned int _min, unsigned int _max)
      : BaseDistribution(_min, _max),
        std::uniform_int_distribution<unsigned int>(_min, _max),
        gen_(rd_()) {}

 private:
  unsigned int Get() override { return (*this)(gen_); }
  bool NeedTruncate() override { return false; }
  std::random_device rd_;
  std::mt19937 gen_;
};

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  unsigned int pos_;
  std::unique_ptr<BaseDistribution> dist_;

 public:
  RandomGenerator() {
    auto max_value_size = FLAGS_value_size_max;
    switch (FLAGS_value_size_distribution_type_e) {
      case kUniform:
        dist_.reset(new UniformDistribution(FLAGS_value_size_min,
                                            FLAGS_value_size_max));
        break;
      case kNormal:
        dist_.reset(
            new NormalDistribution(FLAGS_value_size_min, FLAGS_value_size_max));
        break;
      case kFixed:
      default:
        dist_.reset(new FixedDistribution(value_size));
        max_value_size = value_size;
    }
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < (unsigned)std::max(1048576, max_value_size)) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(unsigned int len) {
    assert(len <= data_.size());
    if (pos_ + len > data_.size()) {
      pos_ = 0;
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }

  Slice Generate() {
    auto len = dist_->Generate();
    return Generate(len);
  }
};

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) {
    return;
  }
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

struct DBWithColumnFamilies {
  std::vector<ColumnFamilyHandle*> cfh;
  DB* db;
  OptimisticTransactionDB* opt_txn_db;
  std::atomic<size_t> num_created;  // Need to be updated after all the
                                    // new entries in cfh are set.
  size_t num_hot;  // Number of column families to be queried at each moment.
                   // After each CreateNewCf(), another num_hot number of new
                   // Column families will be created and used to be queried.
  port::Mutex create_cf_mutex;  // Only one thread can execute CreateNewCf()
  std::vector<int> cfh_idx_to_prob;  // ith index holds probability of operating
                                     // on cfh[i].

  DBWithColumnFamilies() : db(nullptr), opt_txn_db(nullptr) {
    cfh.clear();
    num_created = 0;
    num_hot = 0;
  }

  DBWithColumnFamilies(const DBWithColumnFamilies& other)
      : cfh(other.cfh),
        db(other.db),
        opt_txn_db(other.opt_txn_db),
        num_created(other.num_created.load()),
        num_hot(other.num_hot),
        cfh_idx_to_prob(other.cfh_idx_to_prob) {}

  void DeleteDBs() {
    std::for_each(cfh.begin(), cfh.end(),
                  [](ColumnFamilyHandle* cfhi) { delete cfhi; });
    cfh.clear();
    if (opt_txn_db) {
      delete opt_txn_db;
      opt_txn_db = nullptr;
    } else {
      delete db;
      db = nullptr;
    }
  }

  ColumnFamilyHandle* GetCfh(int64_t rand_num) {
    assert(num_hot > 0);
    size_t rand_offset = 0;
    if (!cfh_idx_to_prob.empty()) {
      assert(cfh_idx_to_prob.size() == num_hot);
      int sum = 0;
      while (sum + cfh_idx_to_prob[rand_offset] < rand_num % 100) {
        sum += cfh_idx_to_prob[rand_offset];
        ++rand_offset;
      }
      assert(rand_offset < cfh_idx_to_prob.size());
    } else {
      rand_offset = rand_num % num_hot;
    }
    return cfh[num_created.load(std::memory_order_acquire) - num_hot +
               rand_offset];
  }

  // stage: assume CF from 0 to stage * num_hot has be created. Need to create
  //        stage * num_hot + 1 to stage * (num_hot + 1).
  void CreateNewCf(ColumnFamilyOptions options, int64_t stage) {
    MutexLock l(&create_cf_mutex);
    if ((stage + 1) * num_hot <= num_created) {
      // Already created.
      return;
    }
    auto new_num_created = num_created + num_hot;
    assert(new_num_created <= cfh.size());
    for (size_t i = num_created; i < new_num_created; i++) {
      Status s =
          db->CreateColumnFamily(options, ColumnFamilyName(i), &(cfh[i]));
      if (!s.ok()) {
        fprintf(stderr, "create column family error: %s\n",
                s.ToString().c_str());
        abort();
      }
    }
    num_created.store(new_num_created, std::memory_order_release);
  }
};

// A class that reports stats to CSV file.
class ReporterAgent {
 public:
  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs)
      : env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());
    if (s.ok()) {
      s = report_file_->Append(Header() + "\n");
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }

    reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
  }

  ~ReporterAgent() {
    {
      std::unique_lock<std::mutex> lk(mutex_);
      stop_ = true;
      stop_cv_.notify_all();
    }
    reporting_thread_.join();
  }

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

 private:
  std::string Header() const { return "secs_elapsed,interval_qps"; }
  void SleepAndReport() {
    auto* clock = env_->GetSystemClock().get();
    auto time_started = clock->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      auto secs_elapsed =
          (clock->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      std::string report =
          std::to_string(secs_elapsed) + "," +
          std::to_string(total_ops_done_snapshot - last_report_) + "\n";
      auto s = report_file_->Append(report);
      if (s.ok()) {
        s = report_file_->Flush();
      }
      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }

  Env* env_;
  std::unique_ptr<WritableFile> report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  ROCKSDB_NAMESPACE::port::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
};

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
    OperationTypeString = {{kRead, "read"},         {kWrite, "write"},
                           {kDelete, "delete"},     {kSeek, "seek"},
                           {kMerge, "merge"},       {kUpdate, "update"},
                           {kCompress, "compress"}, {kCompress, "uncompress"},
                           {kCrc, "crc"},           {kHash, "hash"},
                           {kOthers, "op"}};

class CombinedStats;
class Stats {
 private:
  SystemClock* clock_;
  int id_;
  uint64_t start_ = 0;
  uint64_t sine_interval_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>>
      hist_;
  std::string message_;
  bool exclude_from_merge_;
  ReporterAgent* reporter_agent_;  // does not own
  friend class CombinedStats;

 public:
  Stats() : clock_(FLAGS_env->GetSystemClock().get()) { Start(-1); }

  void SetReporterAgent(ReporterAgent* reporter_agent) {
    reporter_agent_ = reporter_agent;
  }

  void Start(int id) {
    id_ = id;
    next_report_ = FLAGS_stats_interval ? FLAGS_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = clock_->NowMicros();
    sine_interval_ = clock_->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const Stats& other) {
    if (other.exclude_from_merge_) {
      return;
    }

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({it->first, it->second});
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) {
      start_ = other.start_;
    }
    if (other.finish_ > finish_) {
      finish_ = other.finish_;
    }

    // Just keep the messages from one thread.
    if (message_.empty()) {
      message_ = other.message_;
    }
  }

  void Stop() {
    finish_ = clock_->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

  void PrintThreadStatus() {
    std::vector<ThreadStatus> thread_list;
    FLAGS_env->GetThreadList(&thread_list);

    fprintf(stderr, "\n%18s %10s %12s %20s %13s %45s %12s %s\n", "ThreadID",
            "ThreadType", "cfName", "Operation", "ElapsedTime", "Stage",
            "State", "OperationProperties");

    int64_t current_time = 0;
    clock_->GetCurrentTime(&current_time).PermitUncheckedError();
    for (auto ts : thread_list) {
      fprintf(stderr, "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
              ts.thread_id,
              ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
              ts.cf_name.c_str(),
              ThreadStatus::GetOperationName(ts.operation_type).c_str(),
              ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
              ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
              ThreadStatus::GetStateName(ts.state_type).c_str());

      auto op_properties = ThreadStatus::InterpretOperationProperties(
          ts.operation_type, ts.op_properties);
      for (const auto& op_prop : op_properties) {
        fprintf(stderr, " %s %" PRIu64 " |", op_prop.first.c_str(),
                op_prop.second);
      }
      fprintf(stderr, "\n");
    }
  }

  void ResetSineInterval() { sine_interval_ = clock_->NowMicros(); }

  uint64_t GetSineInterval() { return sine_interval_; }

  uint64_t GetStart() { return start_; }

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds.
    last_op_finish_ = clock_->NowMicros();
  }

  void FinishedOps(DBWithColumnFamilies* db_with_cfh, DB* db, int64_t num_ops,
                   enum OperationType op_type = kOthers) {
    if (reporter_agent_) {
      reporter_agent_->ReportFinishedOps(num_ops);
    }
    if (FLAGS_histogram) {
      uint64_t now = clock_->NowMicros();
      uint64_t micros = now - last_op_finish_;

      if (hist_.find(op_type) == hist_.end()) {
        auto hist_temp = std::make_shared<HistogramImpl>();
        hist_.insert({op_type, std::move(hist_temp)});
      }
      hist_[op_type]->Add(micros);

      if (micros >= FLAGS_slow_usecs && !FLAGS_stats_interval) {
        fprintf(stderr, "long op: %" PRIu64 " micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_ += num_ops;
    if (done_ >= next_report_ && FLAGS_progress_reports) {
      if (!FLAGS_stats_interval) {
        if (next_report_ < 1000) {
          next_report_ += 100;
        } else if (next_report_ < 5000) {
          next_report_ += 500;
        } else if (next_report_ < 10000) {
          next_report_ += 1000;
        } else if (next_report_ < 50000) {
          next_report_ += 5000;
        } else if (next_report_ < 100000) {
          next_report_ += 10000;
        } else if (next_report_ < 500000) {
          next_report_ += 50000;
        } else {
          next_report_ += 100000;
        }
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = clock_->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (FLAGS_stats_interval_seconds &&
            usecs_since_last < (FLAGS_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations.
          next_report_ += FLAGS_stats_interval;

        } else {
          fprintf(stderr,
                  "%s ... thread %d: (%" PRIu64 ",%" PRIu64
                  ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  clock_->TimeToString(now / 1000000).c_str(), id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          if (id_ == 0 && FLAGS_stats_per_interval) {
            std::string stats;

            if (db_with_cfh && db_with_cfh->num_created.load()) {
              for (size_t i = 0; i < db_with_cfh->num_created.load(); ++i) {
                if (db->GetProperty(db_with_cfh->cfh[i], "rocksdb.cfstats",
                                    &stats)) {
                  fprintf(stderr, "%s\n", stats.c_str());
                }
                if (FLAGS_show_table_properties) {
                  for (int level = 0; level < FLAGS_num_levels; ++level) {
                    if (db->GetProperty(
                            db_with_cfh->cfh[i],
                            "rocksdb.aggregated-table-properties-at-level" +
                                std::to_string(level),
                            &stats)) {
                      if (stats.find("# entries=0") == std::string::npos) {
                        fprintf(stderr, "Level[%d]: %s\n", level,
                                stats.c_str());
                      }
                    }
                  }
                }
              }
            } else if (db) {
              if (db->GetProperty("rocksdb.stats", &stats)) {
                fprintf(stderr, "%s", stats.c_str());
              }
              if (db->GetProperty("rocksdb.num-running-compactions", &stats)) {
                fprintf(stderr, "num-running-compactions: %s\n", stats.c_str());
              }
              if (db->GetProperty("rocksdb.num-running-flushes", &stats)) {
                fprintf(stderr, "num-running-flushes: %s\n\n", stats.c_str());
              }
              if (FLAGS_show_table_properties) {
                for (int level = 0; level < FLAGS_num_levels; ++level) {
                  if (db->GetProperty(
                          "rocksdb.aggregated-table-properties-at-level" +
                              std::to_string(level),
                          &stats)) {
                    if (stats.find("# entries=0") == std::string::npos) {
                      fprintf(stderr, "Level[%d]: %s\n", level, stats.c_str());
                    }
                  }
                }
              }
            }
          }

          next_report_ += FLAGS_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      if (id_ == 0 && FLAGS_thread_status_per_interval) {
        PrintThreadStatus();
      }
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) {
      done_ = 1;
    }

    std::string extra;
    double elapsed = (finish_ - start_) * 1e-6;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double throughput = (double)done_ / elapsed;

    fprintf(stdout,
            "%-12s : %11.3f micros/op %ld ops/sec %.3f seconds %" PRIu64
            " operations;%s%s\n",
            name.ToString().c_str(), seconds_ * 1e6 / done_, (long)throughput,
            elapsed, done_, (extra.empty() ? "" : " "), extra.c_str());
    if (FLAGS_histogram) {
      for (auto it = hist_.begin(); it != hist_.end(); ++it) {
        fprintf(stdout, "Microseconds per %s:\n%s\n",
                OperationTypeString[it->first].c_str(),
                it->second->ToString().c_str());
      }
    }
    if (FLAGS_report_file_operations) {
      auto* counted_fs =
          FLAGS_env->GetFileSystem()->CheckedCast<CountedFileSystem>();
      assert(counted_fs);
      fprintf(stdout, "%s", counted_fs->PrintCounters().c_str());
      counted_fs->ResetCounters();
    }
    fflush(stdout);
  }
};

class CombinedStats {
 public:
  void AddStats(const Stats& stat) {
    uint64_t total_ops = stat.done_;
    uint64_t total_bytes_ = stat.bytes_;
    double elapsed;

    if (total_ops < 1) {
      total_ops = 1;
    }

    elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    if (total_bytes_ > 0) {
      double mbs = (total_bytes_ / 1048576.0);
      throughput_mbs_.emplace_back(mbs / elapsed);
    }
  }

  void Report(const std::string& bench_name) {
    if (throughput_ops_.size() < 2) {
      // skip if there are not enough samples
      return;
    }

    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      fprintf(stdout,
              "%s [AVG %d runs] : %d (\xC2\xB1 %d) ops/sec; %6.1f (\xC2\xB1 "
              "%.1f) MB/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              static_cast<int>(CalcConfidence95(throughput_ops_)),
              CalcAvg(throughput_mbs_), CalcConfidence95(throughput_mbs_));
    } else {
      fprintf(stdout, "%s [AVG %d runs] : %d (\xC2\xB1 %d) ops/sec\n", name,
              num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              static_cast<int>(CalcConfidence95(throughput_ops_)));
    }
  }

  void ReportWithConfidenceIntervals(const std::string& bench_name) {
    if (throughput_ops_.size() < 2) {
      // skip if there are not enough samples
      return;
    }

    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    int ops_avg = static_cast<int>(CalcAvg(throughput_ops_));
    int ops_confidence_95 = static_cast<int>(CalcConfidence95(throughput_ops_));

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      double mbs_avg = CalcAvg(throughput_mbs_);
      double mbs_confidence_95 = CalcConfidence95(throughput_mbs_);
      fprintf(stdout,
              "%s [CI95 %d runs] : (%d, %d) ops/sec; (%.1f, %.1f) MB/sec\n",
              name, num_runs, ops_avg - ops_confidence_95,
              ops_avg + ops_confidence_95, mbs_avg - mbs_confidence_95,
              mbs_avg + mbs_confidence_95);
    } else {
      fprintf(stdout, "%s [CI95 %d runs] : (%d, %d) ops/sec\n", name, num_runs,
              ops_avg - ops_confidence_95, ops_avg + ops_confidence_95);
    }
  }

  void ReportFinal(const std::string& bench_name) {
    if (throughput_ops_.size() < 2) {
      // skip if there are not enough samples
      return;
    }

    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      // \xC2\xB1 is +/- character in UTF-8
      fprintf(stdout,
              "%s [AVG    %d runs] : %d (\xC2\xB1 %d) ops/sec; %6.1f (\xC2\xB1 "
              "%.1f) MB/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              static_cast<int>(CalcConfidence95(throughput_ops_)),
              CalcAvg(throughput_mbs_), CalcConfidence95(throughput_mbs_), name,
              num_runs, static_cast<int>(CalcMedian(throughput_ops_)),
              CalcMedian(throughput_mbs_));
    } else {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d (\xC2\xB1 %d) ops/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              static_cast<int>(CalcConfidence95(throughput_ops_)), name,
              num_runs, static_cast<int>(CalcMedian(throughput_ops_)));
    }
  }

 private:
  double CalcAvg(std::vector<double>& data) {
    double avg = 0;
    for (double x : data) {
      avg += x;
    }
    avg = avg / data.size();
    return avg;
  }

  // Calculates 95% CI assuming a normal distribution of samples.
  // Samples are not from a normal distribution, but it still
  // provides useful approximation.
  double CalcConfidence95(std::vector<double>& data) {
    assert(data.size() > 1);
    double avg = CalcAvg(data);
    double std_error = CalcStdDev(data, avg) / std::sqrt(data.size());

    // Z score for the 97.5 percentile
    // see https://en.wikipedia.org/wiki/1.96
    return 1.959964 * std_error;
  }

  double CalcMedian(std::vector<double>& data) {
    assert(data.size() > 0);
    std::sort(data.begin(), data.end());

    size_t mid = data.size() / 2;
    if (data.size() % 2 == 1) {
      // Odd number of entries
      return data[mid];
    } else {
      // Even number of entries
      return (data[mid] + data[mid - 1]) / 2;
    }
  }

  double CalcStdDev(std::vector<double>& data, double average) {
    assert(data.size() > 1);
    double squared_sum = 0.0;
    for (double x : data) {
      squared_sum += std::pow(x - average, 2);
    }

    // using samples count - 1 following Bessel's correction
    // see https://en.wikipedia.org/wiki/Bessel%27s_correction
    return std::sqrt(squared_sum / (data.size() - 1));
  }

  std::vector<double> throughput_ops_;
  std::vector<double> throughput_mbs_;
};

class TimestampEmulator {
 private:
  std::atomic<uint64_t> timestamp_;

 public:
  TimestampEmulator() : timestamp_(0) {}
  uint64_t Get() const { return timestamp_.load(); }
  void Inc() { timestamp_++; }
  Slice Allocate(char* scratch) {
    // TODO: support larger timestamp sizes
    assert(FLAGS_user_timestamp_size == 8);
    assert(scratch);
    uint64_t ts = timestamp_.fetch_add(1);
    EncodeFixed64(scratch, ts);
    return Slice(scratch, FLAGS_user_timestamp_size);
  }
  Slice GetTimestampForRead(Random64& rand, char* scratch) {
    assert(FLAGS_user_timestamp_size == 8);
    assert(scratch);
    if (FLAGS_read_with_latest_user_timestamp) {
      return Allocate(scratch);
    }
    // Choose a random timestamp from the past.
    uint64_t ts = rand.Next() % Get();
    EncodeFixed64(scratch, ts);
    return Slice(scratch, FLAGS_user_timestamp_size);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;
  int perf_level;
  std::shared_ptr<RateLimiter> write_rate_limiter;
  std::shared_ptr<RateLimiter> read_rate_limiter;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  long num_initialized;
  long num_done;
  bool start;

  SharedState() : cv(&mu), perf_level(FLAGS_perf_level) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;        // 0..n-1 when running in n threads
  Random64 rand;  // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  explicit ThreadState(int index, int my_seed)
      : tid(index), rand(*seed_base + my_seed) {}
};

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_ = max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = FLAGS_env->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) {
      increment = 1;  // avoid Done(0) and infinite loops
    }
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = FLAGS_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = FLAGS_env->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Benchmark {
 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const SliceTransform> prefix_extractor_;
  DBWithColumnFamilies db_;
  std::vector<DBWithColumnFamilies> multi_dbs_;
  int64_t num_;
  int key_size_;
  int user_timestamp_size_;
  int prefix_size_;
  int total_thread_count_;
  int64_t keys_per_prefix_;
  int64_t entries_per_batch_;
  int64_t writes_before_delete_range_;
  int64_t writes_per_range_tombstone_;
  int64_t range_tombstone_width_;
  int64_t max_num_range_tombstones_;
  ReadOptions read_options_;
  WriteOptions write_options_;
  Options open_options_;  // keep options around to properly destroy db later
  TraceOptions trace_options_;
  TraceOptions block_cache_trace_options_;
  int64_t reads_;
  int64_t deletes_;
  double read_random_exp_range_;
  int64_t writes_;
  int64_t readwrites_;
  int64_t merge_keys_;
  bool report_file_operations_;
  bool use_blob_db_;    // Stacked BlobDB
  bool read_operands_;  // read via GetMergeOperands()
  std::vector<std::string> keys_;

  class ErrorHandlerListener : public EventListener {
   public:
    ErrorHandlerListener()
        : mutex_(),
          cv_(&mutex_),
          no_auto_recovery_(false),
          recovery_complete_(false) {}

    ~ErrorHandlerListener() override = default;

    const char* Name() const override { return kClassName(); }
    static const char* kClassName() { return "ErrorHandlerListener"; }

    void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
                              Status /*bg_error*/,
                              bool* auto_recovery) override {
      if (*auto_recovery && no_auto_recovery_) {
        *auto_recovery = false;
      }
    }

    void OnErrorRecoveryCompleted(Status /*old_bg_error*/) override {
      InstrumentedMutexLock l(&mutex_);
      recovery_complete_ = true;
      cv_.SignalAll();
    }

    bool WaitForRecovery(uint64_t abs_time_us) {
      InstrumentedMutexLock l(&mutex_);
      if (!recovery_complete_) {
        cv_.TimedWait(abs_time_us);
      }
      if (recovery_complete_) {
        recovery_complete_ = false;
        return true;
      }
      return false;
    }

    void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

   private:
    InstrumentedMutex mutex_;
    InstrumentedCondVar cv_;
    bool no_auto_recovery_;
    bool recovery_complete_;
  };

  std::shared_ptr<ErrorHandlerListener> listener_;

  std::unique_ptr<TimestampEmulator> mock_app_clock_;

  bool SanityCheck() {
    if (FLAGS_compression_ratio > 1) {
      fprintf(stderr, "compression_ratio should be between 0 and 1\n");
      return false;
    }
    return true;
  }

  inline bool CompressSlice(const CompressionInfo& compression_info,
                            const Slice& input, std::string* compressed) {
    constexpr uint32_t compress_format_version = 2;

    return CompressData(input, compression_info, compress_format_version,
                        compressed);
  }

  void PrintHeader(const Options& options) {
    PrintEnvironment();
    fprintf(stdout,
            "Keys:       %d bytes each (+ %d bytes user-defined timestamp)\n",
            FLAGS_key_size, FLAGS_user_timestamp_size);
    auto avg_value_size = FLAGS_value_size;
    if (FLAGS_value_size_distribution_type_e == kFixed) {
      fprintf(stdout,
              "Values:     %d bytes each (%d bytes after compression)\n",
              avg_value_size,
              static_cast<int>(avg_value_size * FLAGS_compression_ratio + 0.5));
    } else {
      avg_value_size = (FLAGS_value_size_min + FLAGS_value_size_max) / 2;
      fprintf(stdout,
              "Values:     %d avg bytes each (%d bytes after compression)\n",
              avg_value_size,
              static_cast<int>(avg_value_size * FLAGS_compression_ratio + 0.5));
      fprintf(stdout, "Values Distribution: %s (min: %d, max: %d)\n",
              FLAGS_value_size_distribution_type.c_str(), FLAGS_value_size_min,
              FLAGS_value_size_max);
    }
    fprintf(stdout, "Entries:    %" PRIu64 "\n", num_);
    fprintf(stdout, "Prefix:    %d bytes\n", FLAGS_prefix_size);
    fprintf(stdout, "Keys per prefix:    %" PRIu64 "\n", keys_per_prefix_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(FLAGS_key_size + avg_value_size) * num_) /
             1048576.0));
    fprintf(
        stdout, "FileSize:   %.1f MB (estimated)\n",
        (((FLAGS_key_size + avg_value_size * FLAGS_compression_ratio) * num_) /
         1048576.0));
    fprintf(stdout, "Write rate: %" PRIu64 " bytes/second\n",
            FLAGS_benchmark_write_rate_limit);
    fprintf(stdout, "Read rate: %" PRIu64 " ops/second\n",
            FLAGS_benchmark_read_rate_limit);
    if (FLAGS_enable_numa) {
      fprintf(stderr, "Running in NUMA enabled mode.\n");
#ifndef NUMA
      fprintf(stderr, "NUMA is not defined in the system.\n");
      exit(1);
#else
      if (numa_available() == -1) {
        fprintf(stderr, "NUMA is not supported by the system.\n");
        exit(1);
      }
#endif
    }

    auto compression = CompressionTypeToString(FLAGS_compression_type_e);
    fprintf(stdout, "Compression: %s\n", compression.c_str());
    fprintf(stdout, "Compression sampling rate: %" PRId64 "\n",
            FLAGS_sample_for_compression);
    if (options.memtable_factory != nullptr) {
      fprintf(stdout, "Memtablerep: %s\n",
              options.memtable_factory->GetId().c_str());
    }
    fprintf(stdout, "Perf Level: %d\n", FLAGS_perf_level);

    PrintWarnings(compression.c_str());
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings(const char* compression) {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(
        stdout,
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    if (FLAGS_compression_type_e != ROCKSDB_NAMESPACE::kNoCompression) {
      // The test string should not be too small.
      const int len = FLAGS_block_size;
      std::string input_str(len, 'y');
      std::string compressed;
      CompressionOptions opts;
      CompressionContext context(FLAGS_compression_type_e, opts);
      CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                           FLAGS_compression_type_e,
                           FLAGS_sample_for_compression);
      bool result = CompressSlice(info, Slice(input_str), &compressed);

      if (!result) {
        fprintf(stdout, "WARNING: %s compression is not enabled\n",
                compression);
      } else if (compressed.size() >= input_str.size()) {
        fprintf(stdout, "WARNING: %s compression is not effective\n",
                compression);
      }
    }
  }

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
  static Slice TrimSpace(Slice s) {
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit - 1])) {
      limit--;
    }
    return Slice(s.data() + start, limit - start);
  }
#endif

  void PrintEnvironment() {
    fprintf(stderr, "RocksDB:    version %s\n",
            GetRocksVersionAsString(true).c_str());

#if defined(__linux) || defined(__APPLE__) || defined(__FreeBSD__)
    time_t now = time(nullptr);
    char buf[52];
    // Lint complains about ctime() usage, so replace it with ctime_r(). The
    // requirement is to provide a buffer which is at least 26 bytes.
    fprintf(stderr, "Date:       %s",
            ctime_r(&now, buf));  // ctime_r() adds newline

#if defined(__linux)
    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#elif defined(__APPLE__)
    struct host_basic_info h;
    size_t hlen = HOST_BASIC_INFO_COUNT;
    if (host_info(mach_host_self(), HOST_BASIC_INFO, (host_info_t)&h,
                  (uint32_t*)&hlen) == KERN_SUCCESS) {
      std::string cpu_type;
      std::string cache_size;
      size_t hcache_size;
      hlen = sizeof(hcache_size);
      if (sysctlbyname("hw.cachelinesize", &hcache_size, &hlen, NULL, 0) == 0) {
        cache_size = std::to_string(hcache_size);
      }
      switch (h.cpu_type) {
        case CPU_TYPE_X86_64:
          cpu_type = "x86_64";
          break;
        case CPU_TYPE_ARM64:
          cpu_type = "arm64";
          break;
        default:
          break;
      }
      fprintf(stderr, "CPU:        %d * %s\n", h.max_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#elif defined(__FreeBSD__)
    int ncpus;
    size_t len = sizeof(ncpus);
    int mib[2] = {CTL_HW, HW_NCPU};
    if (sysctl(mib, 2, &ncpus, &len, nullptr, 0) == 0) {
      char cpu_type[16];
      len = sizeof(cpu_type) - 1;
      mib[1] = HW_MACHINE;
      if (sysctl(mib, 2, cpu_type, &len, nullptr, 0) == 0) cpu_type[len] = 0;

      fprintf(stderr, "CPU:        %d * %s\n", ncpus, cpu_type);
      // no programmatic way to get the cache line size except on PPC
    }
#endif
#endif
  }

  static bool KeyExpired(const TimestampEmulator* timestamp_emulator,
                         const Slice& key) {
    const char* pos = key.data();
    pos += 8;
    uint64_t timestamp = 0;
    if (port::kLittleEndian) {
      int bytes_to_fill = 8;
      for (int i = 0; i < bytes_to_fill; ++i) {
        timestamp |= (static_cast<uint64_t>(static_cast<unsigned char>(pos[i]))
                      << ((bytes_to_fill - i - 1) << 3));
      }
    } else {
      memcpy(&timestamp, pos, sizeof(timestamp));
    }
    return timestamp_emulator->Get() - timestamp > FLAGS_time_range;
  }

  class ExpiredTimeFilter : public CompactionFilter {
   public:
    explicit ExpiredTimeFilter(
        const std::shared_ptr<TimestampEmulator>& timestamp_emulator)
        : timestamp_emulator_(timestamp_emulator) {}
    bool Filter(int /*level*/, const Slice& key,
                const Slice& /*existing_value*/, std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      return KeyExpired(timestamp_emulator_.get(), key);
    }
    const char* Name() const override { return "ExpiredTimeFilter"; }

   private:
    std::shared_ptr<TimestampEmulator> timestamp_emulator_;
  };

  class KeepFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      return false;
    }

    const char* Name() const override { return "KeepFilter"; }
  };

  static std::shared_ptr<MemoryAllocator> GetCacheAllocator() {
    std::shared_ptr<MemoryAllocator> allocator;

    if (FLAGS_use_cache_jemalloc_no_dump_allocator) {
      JemallocAllocatorOptions jemalloc_options;
      if (!NewJemallocNodumpAllocator(jemalloc_options, &allocator).ok()) {
        fprintf(stderr, "JemallocNodumpAllocator not supported.\n");
        exit(1);
      }
    } else if (FLAGS_use_cache_memkind_kmem_allocator) {
#ifdef MEMKIND
      allocator = std::make_shared<MemkindKmemAllocator>();
#else
      fprintf(stderr, "Memkind library is not linked with the binary.\n");
      exit(1);
#endif
    }

    return allocator;
  }

  static int32_t GetCacheHashSeed() {
    // For a fixed Cache seed, need a non-negative int32
    return static_cast<int32_t>(*seed_base) & 0x7fffffff;
  }

  static std::shared_ptr<Cache> NewCache(int64_t capacity) {
    CompressedSecondaryCacheOptions secondary_cache_opts;
    TieredAdmissionPolicy adm_policy = TieredAdmissionPolicy::kAdmPolicyAuto;
    bool use_tiered_cache = false;
    if (capacity <= 0) {
      return nullptr;
    }
    if (FLAGS_use_compressed_secondary_cache) {
      secondary_cache_opts.capacity = FLAGS_compressed_secondary_cache_size;
      secondary_cache_opts.num_shard_bits =
          FLAGS_compressed_secondary_cache_numshardbits;
      secondary_cache_opts.high_pri_pool_ratio =
          FLAGS_compressed_secondary_cache_high_pri_pool_ratio;
      secondary_cache_opts.low_pri_pool_ratio =
          FLAGS_compressed_secondary_cache_low_pri_pool_ratio;
      secondary_cache_opts.compression_type =
          FLAGS_compressed_secondary_cache_compression_type_e;
      secondary_cache_opts.compression_opts.level =
          FLAGS_compressed_secondary_cache_compression_level;
      secondary_cache_opts.compress_format_version =
          FLAGS_compressed_secondary_cache_compress_format_version;
      if (FLAGS_use_tiered_cache) {
        use_tiered_cache = true;
        adm_policy = StringToAdmissionPolicy(FLAGS_tiered_adm_policy.c_str());
      }
    }
    if (!FLAGS_secondary_cache_uri.empty()) {
      if (!use_tiered_cache && FLAGS_use_compressed_secondary_cache) {
        fprintf(
            stderr,
            "Cannot specify both --secondary_cache_uri and "
            "--use_compressed_secondary_cache when using a non-tiered cache\n");
        exit(1);
      }
      Status s = SecondaryCache::CreateFromString(
          ConfigOptions(), FLAGS_secondary_cache_uri, &secondary_cache);
      if (secondary_cache == nullptr) {
        fprintf(stderr,
                "No secondary cache registered matching string: %s status=%s\n",
                FLAGS_secondary_cache_uri.c_str(), s.ToString().c_str());
        exit(1);
      }
    }

    std::shared_ptr<Cache> block_cache;
    if (!FLAGS_cache_uri.empty()) {
      Status s = Cache::CreateFromString(ConfigOptions(), FLAGS_cache_uri,
                                         &block_cache);
      if (block_cache == nullptr) {
        fprintf(stderr, "No  cache registered matching string: %s status=%s\n",
                FLAGS_cache_uri.c_str(), s.ToString().c_str());
        exit(1);
      }
    } else if (FLAGS_cache_type == "clock_cache") {
      fprintf(stderr, "Old clock cache implementation has been removed.\n");
      exit(1);
    } else if (EndsWith(FLAGS_cache_type, "hyper_clock_cache")) {
      size_t estimated_entry_charge;
      if (FLAGS_cache_type == "fixed_hyper_clock_cache" ||
          FLAGS_cache_type == "hyper_clock_cache") {
        estimated_entry_charge = FLAGS_block_size;
      } else if (FLAGS_cache_type == "auto_hyper_clock_cache") {
        estimated_entry_charge = 0;
      } else {
        fprintf(stderr, "Cache type not supported.");
        exit(1);
      }
      HyperClockCacheOptions opts(FLAGS_cache_size, estimated_entry_charge,
                                  FLAGS_cache_numshardbits);
      opts.hash_seed = GetCacheHashSeed();
      if (use_tiered_cache) {
        TieredCacheOptions tiered_opts;
        tiered_opts.cache_type = PrimaryCacheType::kCacheTypeHCC;
        tiered_opts.cache_opts = &opts;
        tiered_opts.total_capacity =
            opts.capacity + secondary_cache_opts.capacity;
        tiered_opts.compressed_secondary_ratio =
            secondary_cache_opts.capacity * 1.0 / tiered_opts.total_capacity;
        tiered_opts.comp_cache_opts = secondary_cache_opts;
        tiered_opts.nvm_sec_cache = secondary_cache;
        tiered_opts.adm_policy = adm_policy;
        block_cache = NewTieredCache(tiered_opts);
      } else {
        if (!FLAGS_secondary_cache_uri.empty()) {
          opts.secondary_cache = secondary_cache;
        } else if (FLAGS_use_compressed_secondary_cache) {
          opts.secondary_cache =
              NewCompressedSecondaryCache(secondary_cache_opts);
        }
        block_cache = opts.MakeSharedCache();
      }
    } else if (FLAGS_cache_type == "lru_cache") {
      LRUCacheOptions opts(
          static_cast<size_t>(capacity), FLAGS_cache_numshardbits,
          false /*strict_capacity_limit*/, FLAGS_cache_high_pri_pool_ratio,
          GetCacheAllocator(), kDefaultToAdaptiveMutex,
          kDefaultCacheMetadataChargePolicy, FLAGS_cache_low_pri_pool_ratio);
      opts.hash_seed = GetCacheHashSeed();
      if (use_tiered_cache) {
        TieredCacheOptions tiered_opts;
        tiered_opts.cache_type = PrimaryCacheType::kCacheTypeLRU;
        tiered_opts.cache_opts = &opts;
        tiered_opts.total_capacity =
            opts.capacity + secondary_cache_opts.capacity;
        tiered_opts.compressed_secondary_ratio =
            secondary_cache_opts.capacity * 1.0 / tiered_opts.total_capacity;
        tiered_opts.comp_cache_opts = secondary_cache_opts;
        tiered_opts.nvm_sec_cache = secondary_cache;
        tiered_opts.adm_policy = adm_policy;
        block_cache = NewTieredCache(tiered_opts);
      } else {
        if (!FLAGS_secondary_cache_uri.empty()) {
          opts.secondary_cache = secondary_cache;
        } else if (FLAGS_use_compressed_secondary_cache) {
          opts.secondary_cache =
              NewCompressedSecondaryCache(secondary_cache_opts);
        }
        block_cache = opts.MakeSharedCache();
      }
    } else {
      fprintf(stderr, "Cache type not supported.");
      exit(1);
    }

    if (!block_cache) {
      fprintf(stderr, "Unable to allocate block cache\n");
      exit(1);
    }
    return block_cache;
  }

 public:
  Benchmark()
      : cache_(NewCache(FLAGS_cache_size)),
        compressed_cache_(NewCache(FLAGS_compressed_cache_size)),
        prefix_extractor_(FLAGS_prefix_size != 0
                              ? NewFixedPrefixTransform(FLAGS_prefix_size)
                              : nullptr),
        num_(FLAGS_num),
        key_size_(FLAGS_key_size),
        user_timestamp_size_(FLAGS_user_timestamp_size),
        prefix_size_(FLAGS_prefix_size),
        total_thread_count_(0),
        keys_per_prefix_(FLAGS_keys_per_prefix),
        entries_per_batch_(1),
        reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
        read_random_exp_range_(0.0),
        writes_(FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes),
        readwrites_(
            (FLAGS_writes < 0 && FLAGS_reads < 0)
                ? FLAGS_num
                : ((FLAGS_writes > FLAGS_reads) ? FLAGS_writes : FLAGS_reads)),
        merge_keys_(FLAGS_merge_keys < 0 ? FLAGS_num : FLAGS_merge_keys),
        report_file_operations_(FLAGS_report_file_operations),
        use_blob_db_(FLAGS_use_blob_db),  // Stacked BlobDB
        read_operands_(false) {
    // use simcache instead of cache
    if (FLAGS_simcache_size >= 0) {
      if (FLAGS_cache_numshardbits >= 1) {
        cache_ =
            NewSimCache(cache_, FLAGS_simcache_size, FLAGS_cache_numshardbits);
      } else {
        cache_ = NewSimCache(cache_, FLAGS_simcache_size, 0);
      }
    }

    if (report_file_operations_) {
      FLAGS_env = new CompositeEnvWrapper(
          FLAGS_env,
          std::make_shared<CountedFileSystem>(FLAGS_env->GetFileSystem()));
    }

    if (FLAGS_prefix_size > FLAGS_key_size) {
      fprintf(stderr, "prefix size is larger than key size");
      exit(1);
    }

    std::vector<std::string> files;
    FLAGS_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        FLAGS_env->DeleteFile(FLAGS_db + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      Options options;
      options.env = FLAGS_env;
      if (!FLAGS_wal_dir.empty()) {
        options.wal_dir = FLAGS_wal_dir;
      }
      if (use_blob_db_) {
        // Stacked BlobDB
        blob_db::DestroyBlobDB(FLAGS_db, options, blob_db::BlobDBOptions());
      }
      DestroyDB(FLAGS_db, options);
      if (!FLAGS_wal_dir.empty()) {
        FLAGS_env->DeleteDir(FLAGS_wal_dir);
      }

      if (FLAGS_num_multi_db > 1) {
        FLAGS_env->CreateDir(FLAGS_db);
        if (!FLAGS_wal_dir.empty()) {
          FLAGS_env->CreateDir(FLAGS_wal_dir);
        }
      }
    }

    listener_.reset(new ErrorHandlerListener());
    if (user_timestamp_size_ > 0) {
      mock_app_clock_.reset(new TimestampEmulator());
    }
  }

  void DeleteDBs() {
    db_.DeleteDBs();
    for (const DBWithColumnFamilies& dbwcf : multi_dbs_) {
      delete dbwcf.db;
    }
  }

  ~Benchmark() {
    DeleteDBs();
    if (cache_.get() != nullptr) {
      // Clear cache reference first
      open_options_.write_buffer_manager.reset();
      // this will leak, but we're shutting down so nobody cares
      cache_->DisownData();
    }
  }

  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
  }

  // Generate key according to the given specification and random number.
  // The resulting key will have the following format:
  //   - If keys_per_prefix_ is positive, extra trailing bytes are either cut
  //     off or padded with '0'.
  //     The prefix value is derived from key value.
  //     ----------------------------
  //     | prefix 00000 | key 00000 |
  //     ----------------------------
  //
  //   - If keys_per_prefix_ is 0, the key is simply a binary representation of
  //     random number followed by trailing '0's
  //     ----------------------------
  //     |        key 00000         |
  //     ----------------------------
  void GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
    if (!keys_.empty()) {
      assert(FLAGS_use_existing_keys);
      assert(keys_.size() == static_cast<size_t>(num_keys));
      assert(v < static_cast<uint64_t>(num_keys));
      *key = keys_[v];
      return;
    }
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    if (keys_per_prefix_ > 0) {
      int64_t num_prefix = num_keys / keys_per_prefix_;
      int64_t prefix = v % num_prefix;
      int bytes_to_fill = std::min(prefix_size_, 8);
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (prefix >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&prefix), bytes_to_fill);
      }
      if (prefix_size_ > 8) {
        // fill the rest with 0s
        memset(pos + 8, '0', prefix_size_ - 8);
      }
      pos += prefix_size_;
    }

    int bytes_to_fill = std::min(key_size_ - static_cast<int>(pos - start), 8);
    if (port::kLittleEndian) {
      for (int i = 0; i < bytes_to_fill; ++i) {
        pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
      }
    } else {
      memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    }
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
      memset(pos, '0', key_size_ - (pos - start));
    }
  }

  void GenerateKeyFromIntForSeek(uint64_t v, int64_t num_keys, Slice* key) {
    GenerateKeyFromInt(v, num_keys, key);
    if (FLAGS_seek_missing_prefix) {
      assert(prefix_size_ > 8);
      char* key_ptr = const_cast<char*>(key->data());
      // This rely on GenerateKeyFromInt filling paddings with '0's.
      // Putting a '1' will create a non-existing prefix.
      key_ptr[8] = '1';
    }
  }

  std::string GetPathForMultiple(std::string base_name, size_t id) {
    if (!base_name.empty()) {
#ifndef OS_WIN
      if (base_name.back() != '/') {
        base_name += '/';
      }
#else
      if (base_name.back() != '\\') {
        base_name += '\\';
      }
#endif
    }
    return base_name + std::to_string(id);
  }

  void VerifyDBFromDB(std::string& truth_db_name) {
    DBWithColumnFamilies truth_db;
    auto s = DB::OpenForReadOnly(open_options_, truth_db_name, &truth_db.db);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
    ReadOptions ro;
    ro.total_order_seek = true;
    std::unique_ptr<Iterator> truth_iter(truth_db.db->NewIterator(ro));
    std::unique_ptr<Iterator> db_iter(db_.db->NewIterator(ro));
    // Verify that all the key/values in truth_db are retrivable in db with
    // ::Get
    fprintf(stderr, "Verifying db >= truth_db with ::Get...\n");
    for (truth_iter->SeekToFirst(); truth_iter->Valid(); truth_iter->Next()) {
      std::string value;
      s = db_.db->Get(ro, truth_iter->key(), &value);
      assert(s.ok());
      // TODO(myabandeh): provide debugging hints
      assert(Slice(value) == truth_iter->value());
    }
    // Verify that the db iterator does not give any extra key/value
    fprintf(stderr, "Verifying db == truth_db...\n");
    for (db_iter->SeekToFirst(), truth_iter->SeekToFirst(); db_iter->Valid();
         db_iter->Next(), truth_iter->Next()) {
      assert(truth_iter->Valid());
      assert(truth_iter->value() == db_iter->value());
    }
    // No more key should be left unchecked in truth_db
    assert(!truth_iter->Valid());
    fprintf(stderr, "...Verified\n");
  }

  void ErrorExit() {
    DeleteDBs();
    exit(1);
  }

  void Run() {
    if (!SanityCheck()) {
      ErrorExit();
    }
    Open(&open_options_);
    PrintHeader(open_options_);
    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    std::unique_ptr<ExpiredTimeFilter> filter;
    while (std::getline(benchmark_stream, name, ',')) {
      // Sanitize parameters
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      writes_ = (FLAGS_writes < 0 ? FLAGS_num : FLAGS_writes);
      deletes_ = (FLAGS_deletes < 0 ? FLAGS_num : FLAGS_deletes);
      value_size = FLAGS_value_size;
      key_size_ = FLAGS_key_size;
      entries_per_batch_ = FLAGS_batch_size;
      writes_before_delete_range_ = FLAGS_writes_before_delete_range;
      writes_per_range_tombstone_ = FLAGS_writes_per_range_tombstone;
      range_tombstone_width_ = FLAGS_range_tombstone_width;
      max_num_range_tombstones_ = FLAGS_max_num_range_tombstones;
      write_options_ = WriteOptions();
      read_random_exp_range_ = FLAGS_read_random_exp_range;
      if (FLAGS_sync) {
        write_options_.sync = true;
      }
      write_options_.disableWAL = FLAGS_disable_wal;
      write_options_.rate_limiter_priority =
          FLAGS_rate_limit_auto_wal_flush ? Env::IO_USER : Env::IO_TOTAL;
      read_options_ = ReadOptions(FLAGS_verify_checksum, true);
      read_options_.total_order_seek = FLAGS_total_order_seek;
      read_options_.prefix_same_as_start = FLAGS_prefix_same_as_start;
      read_options_.rate_limiter_priority =
          FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
      read_options_.tailing = FLAGS_use_tailing_iterator;
      read_options_.readahead_size = FLAGS_readahead_size;
      read_options_.adaptive_readahead = FLAGS_adaptive_readahead;
      read_options_.async_io = FLAGS_async_io;
      read_options_.optimize_multiget_for_io = FLAGS_optimize_multiget_for_io;
      read_options_.auto_readahead_size = FLAGS_auto_readahead_size;

      void (Benchmark::*method)(ThreadState*) = nullptr;
      void (Benchmark::*post_process_method)() = nullptr;

      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      int num_repeat = 1;
      int num_warmup = 0;
      if (!name.empty() && *name.rbegin() == ']') {
        auto it = name.find('[');
        if (it == std::string::npos) {
          fprintf(stderr, "unknown benchmark arguments '%s'\n", name.c_str());
          ErrorExit();
        }
        std::string args = name.substr(it + 1);
        args.resize(args.size() - 1);
        name.resize(it);

        std::string bench_arg;
        std::stringstream args_stream(args);
        while (std::getline(args_stream, bench_arg, '-')) {
          if (bench_arg.empty()) {
            continue;
          }
          if (bench_arg[0] == 'X') {
            // Repeat the benchmark n times
            std::string num_str = bench_arg.substr(1);
            num_repeat = std::stoi(num_str);
          } else if (bench_arg[0] == 'W') {
            // Warm up the benchmark for n times
            std::string num_str = bench_arg.substr(1);
            num_warmup = std::stoi(num_str);
          }
        }
      }

      // Both fillseqdeterministic and filluniquerandomdeterministic
      // fill the levels except the max level with UNIQUE_RANDOM
      // and fill the max level with fillseq and filluniquerandom, respectively
      if (name == "fillseqdeterministic" ||
          name == "filluniquerandomdeterministic") {
        if (!FLAGS_disable_auto_compactions) {
          fprintf(stderr,
                  "Please disable_auto_compactions in FillDeterministic "
                  "benchmark\n");
          ErrorExit();
        }
        if (num_threads > 1) {
          fprintf(stderr,
                  "filldeterministic multithreaded not supported"
                  ", use 1 thread\n");
          num_threads = 1;
        }
        fresh_db = true;
        if (name == "fillseqdeterministic") {
          method = &Benchmark::WriteSeqDeterministic;
        } else {
          method = &Benchmark::WriteUniqueRandomDeterministic;
        }
      } else if (name == "fillseq") {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillbatch") {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == "fillrandom") {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "filluniquerandom" ||
                 name == "fillanddeleteuniquerandom") {
        fresh_db = true;
        if (num_threads > 1) {
          fprintf(stderr,
                  "filluniquerandom and fillanddeleteuniquerandom "
                  "multithreaded not supported, use 1 thread");
          num_threads = 1;
        }
        method = &Benchmark::WriteUniqueRandom;
      } else if (name == "overwrite") {
        method = &Benchmark::WriteRandom;
      } else if (name == "fillsync") {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == "fill100K") {
        fresh_db = true;
        num_ /= 1000;
        value_size = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == "readseq") {
        method = &Benchmark::ReadSequential;
      } else if (name == "readtorowcache") {
        if (!FLAGS_use_existing_keys || !FLAGS_row_cache_size) {
          fprintf(stderr,
                  "Please set use_existing_keys to true and specify a "
                  "row cache size in readtorowcache benchmark\n");
          ErrorExit();
        }
        method = &Benchmark::ReadToRowCache;
      } else if (name == "readtocache") {
        method = &Benchmark::ReadSequential;
        num_threads = 1;
        reads_ = num_;
      } else if (name == "readreverse") {
        method = &Benchmark::ReadReverse;
      } else if (name == "readrandom") {
        if (FLAGS_multiread_stride) {
          fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                  entries_per_batch_);
        }
        method = &Benchmark::ReadRandom;
      } else if (name == "readrandomfast") {
        method = &Benchmark::ReadRandomFast;
      } else if (name == "multireadrandom") {
        fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                entries_per_batch_);
        method = &Benchmark::MultiReadRandom;
      } else if (name == "multireadwhilewriting") {
        fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                entries_per_batch_);
        num_threads++;
        method = &Benchmark::MultiReadWhileWriting;
      } else if (name == "approximatesizerandom") {
        fprintf(stderr, "entries_per_batch = %" PRIi64 "\n",
                entries_per_batch_);
        method = &Benchmark::ApproximateSizeRandom;
      } else if (name == "approximatememtablestats") {
        method = &Benchmark::ApproximateMemtableStats;
      } else if (name == "mixgraph") {
        method = &Benchmark::MixGraph;
      } else if (name == "readmissing") {
        ++key_size_;
        method = &Benchmark::ReadRandom;
      } else if (name == "newiterator") {
        method = &Benchmark::IteratorCreation;
      } else if (name == "newiteratorwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::IteratorCreationWhileWriting;
      } else if (name == "seekrandom") {
        method = &Benchmark::SeekRandom;
      } else if (name == "seekrandomwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::SeekRandomWhileWriting;
      } else if (name == "seekrandomwhilemerging") {
        num_threads++;  // Add extra thread for merging
        method = &Benchmark::SeekRandomWhileMerging;
      } else if (name == "readrandomsmall") {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == "deleteseq") {
        method = &Benchmark::DeleteSeq;
      } else if (name == "deleterandom") {
        method = &Benchmark::DeleteRandom;
      } else if (name == "readwhilewriting") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == "readwhilemerging") {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileMerging;
      } else if (name == "readwhilescanning") {
        num_threads++;  // Add extra thread for scaning
        method = &Benchmark::ReadWhileScanning;
      } else if (name == "readrandomwriterandom") {
        method = &Benchmark::ReadRandomWriteRandom;
      } else if (name == "readrandommergerandom") {
        if (FLAGS_merge_operator.empty()) {
          fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                  name.c_str());
          ErrorExit();
        }
        method = &Benchmark::ReadRandomMergeRandom;
      } else if (name == "updaterandom") {
        method = &Benchmark::UpdateRandom;
      } else if (name == "xorupdaterandom") {
        method = &Benchmark::XORUpdateRandom;
      } else if (name == "appendrandom") {
        method = &Benchmark::AppendRandom;
      } else if (name == "mergerandom") {
        if (FLAGS_merge_operator.empty()) {
          fprintf(stdout, "%-12s : skipped (--merge_operator is unknown)\n",
                  name.c_str());
          exit(1);
        }
        method = &Benchmark::MergeRandom;
      } else if (name == "randomwithverify") {
        method = &Benchmark::RandomWithVerify;
      } else if (name == "fillseekseq") {
        method = &Benchmark::WriteSeqSeekSeq;
      } else if (name == "compact") {
        method = &Benchmark::Compact;
      } else if (name == "compactall") {
        CompactAll();
      } else if (name == "compact0") {
        CompactLevel(0);
      } else if (name == "compact1") {
        CompactLevel(1);
      } else if (name == "waitforcompaction") {
        WaitForCompaction();
      } else if (name == "flush") {
        Flush();
      } else if (name == "crc32c") {
        method = &Benchmark::Crc32c;
      } else if (name == "xxhash") {
        method = &Benchmark::xxHash;
      } else if (name == "xxhash64") {
        method = &Benchmark::xxHash64;
      } else if (name == "xxh3") {
        method = &Benchmark::xxh3;
      } else if (name == "acquireload") {
        method = &Benchmark::AcquireLoad;
      } else if (name == "compress") {
        method = &Benchmark::Compress;
      } else if (name == "uncompress") {
        method = &Benchmark::Uncompress;
      } else if (name == "randomtransaction") {
        method = &Benchmark::RandomTransaction;
        post_process_method = &Benchmark::RandomTransactionVerify;
      } else if (name == "randomreplacekeys") {
        fresh_db = true;
        method = &Benchmark::RandomReplaceKeys;
      } else if (name == "timeseries") {
        timestamp_emulator_.reset(new TimestampEmulator());
        if (FLAGS_expire_style == "compaction_filter") {
          filter.reset(new ExpiredTimeFilter(timestamp_emulator_));
          fprintf(stdout, "Compaction filter is used to remove expired data");
          open_options_.compaction_filter = filter.get();
        }
        fresh_db = true;
        method = &Benchmark::TimeSeries;
      } else if (name == "block_cache_entry_stats") {
        // DB::Properties::kBlockCacheEntryStats
        PrintStats("rocksdb.block-cache-entry-stats");
      } else if (name == "cache_report_problems") {
        CacheReportProblems();
      } else if (name == "stats") {
        PrintStats("rocksdb.stats");
      } else if (name == "resetstats") {
        ResetStats();
      } else if (name == "verify") {
        VerifyDBFromDB(FLAGS_truth_db);
      } else if (name == "levelstats") {
        PrintStats("rocksdb.levelstats");
      } else if (name == "memstats") {
        std::vector<std::string> keys{"rocksdb.num-immutable-mem-table",
                                      "rocksdb.cur-size-active-mem-table",
                                      "rocksdb.cur-size-all-mem-tables",
                                      "rocksdb.size-all-mem-tables",
                                      "rocksdb.num-entries-active-mem-table",
                                      "rocksdb.num-entries-imm-mem-tables"};
        PrintStats(keys);
      } else if (name == "sstables") {
        PrintStats("rocksdb.sstables");
      } else if (name == "stats_history") {
        PrintStatsHistory();
      } else if (name == "replay") {
        if (num_threads > 1) {
          fprintf(stderr, "Multi-threaded replay is not yet supported\n");
          ErrorExit();
        }
        if (FLAGS_trace_file == "") {
          fprintf(stderr, "Please set --trace_file to be replayed from\n");
          ErrorExit();
        }
        method = &Benchmark::Replay;
      } else if (name == "getmergeoperands") {
        method = &Benchmark::GetMergeOperands;
      } else if (name == "verifychecksum") {
        method = &Benchmark::VerifyChecksum;
      } else if (name == "verifyfilechecksums") {
        method = &Benchmark::VerifyFileChecksums;
      } else if (name == "readrandomoperands") {
        read_operands_ = true;
        method = &Benchmark::ReadRandom;
      } else if (name == "backup") {
        method = &Benchmark::Backup;
      } else if (name == "restore") {
        method = &Benchmark::Restore;
      } else if (!name.empty()) {  // No error message for empty name
        fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
        ErrorExit();
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.c_str());
          method = nullptr;
        } else {
          if (db_.db != nullptr) {
            db_.DeleteDBs();
            DestroyDB(FLAGS_db, open_options_);
          }
          Options options = open_options_;
          for (size_t i = 0; i < multi_dbs_.size(); i++) {
            delete multi_dbs_[i].db;
            if (!open_options_.wal_dir.empty()) {
              options.wal_dir = GetPathForMultiple(open_options_.wal_dir, i);
            }
            DestroyDB(GetPathForMultiple(FLAGS_db, i), options);
          }
          multi_dbs_.clear();
        }
        Open(&open_options_);  // use open_options for the last accessed
      }

      if (method != nullptr) {
        fprintf(stdout, "DB path: [%s]\n", FLAGS_db.c_str());

        if (name == "backup") {
          std::cout << "Backup path: [" << FLAGS_backup_dir << "]" << std::endl;
        } else if (name == "restore") {
          std::cout << "Backup path: [" << FLAGS_backup_dir << "]" << std::endl;
          std::cout << "Restore path: [" << FLAGS_restore_dir << "]"
                    << std::endl;
        }
        // A trace_file option can be provided both for trace and replay
        // operations. But db_bench does not support tracing and replaying at
        // the same time, for now. So, start tracing only when it is not a
        // replay.
        if (FLAGS_trace_file != "" && name != "replay") {
          std::unique_ptr<TraceWriter> trace_writer;
          Status s = NewFileTraceWriter(FLAGS_env, EnvOptions(),
                                        FLAGS_trace_file, &trace_writer);
          if (!s.ok()) {
            fprintf(stderr, "Encountered an error starting a trace, %s\n",
                    s.ToString().c_str());
            ErrorExit();
          }
          s = db_.db->StartTrace(trace_options_, std::move(trace_writer));
          if (!s.ok()) {
            fprintf(stderr, "Encountered an error starting a trace, %s\n",
                    s.ToString().c_str());
            ErrorExit();
          }
          fprintf(stdout, "Tracing the workload to: [%s]\n",
                  FLAGS_trace_file.c_str());
        }
        // Start block cache tracing.
        if (!FLAGS_block_cache_trace_file.empty()) {
          // Sanity checks.
          if (FLAGS_block_cache_trace_sampling_frequency <= 0) {
            fprintf(stderr,
                    "Block cache trace sampling frequency must be higher than "
                    "0.\n");
            ErrorExit();
          }
          if (FLAGS_block_cache_trace_max_trace_file_size_in_bytes <= 0) {
            fprintf(stderr,
                    "The maximum file size for block cache tracing must be "
                    "higher than 0.\n");
            ErrorExit();
          }
          block_cache_trace_options_.max_trace_file_size =
              FLAGS_block_cache_trace_max_trace_file_size_in_bytes;
          block_cache_trace_options_.sampling_frequency =
              FLAGS_block_cache_trace_sampling_frequency;
          std::unique_ptr<TraceWriter> block_cache_trace_writer;
          Status s = NewFileTraceWriter(FLAGS_env, EnvOptions(),
                                        FLAGS_block_cache_trace_file,
                                        &block_cache_trace_writer);
          if (!s.ok()) {
            fprintf(stderr,
                    "Encountered an error when creating trace writer, %s\n",
                    s.ToString().c_str());
            ErrorExit();
          }
          s = db_.db->StartBlockCacheTrace(block_cache_trace_options_,
                                           std::move(block_cache_trace_writer));
          if (!s.ok()) {
            fprintf(
                stderr,
                "Encountered an error when starting block cache tracing, %s\n",
                s.ToString().c_str());
            ErrorExit();
          }
          fprintf(stdout, "Tracing block cache accesses to: [%s]\n",
                  FLAGS_block_cache_trace_file.c_str());
        }

        if (num_warmup > 0) {
          printf("Warming up benchmark by running %d times\n", num_warmup);
        }

        for (int i = 0; i < num_warmup; i++) {
          RunBenchmark(num_threads, name, method);
        }

        if (num_repeat > 1) {
          printf("Running benchmark for %d times\n", num_repeat);
        }

        CombinedStats combined_stats;
        for (int i = 0; i < num_repeat; i++) {
          Stats stats = RunBenchmark(num_threads, name, method);
          combined_stats.AddStats(stats);
          if (FLAGS_confidence_interval_only) {
            combined_stats.ReportWithConfidenceIntervals(name);
          } else {
            combined_stats.Report(name);
          }
        }
        if (num_repeat > 1) {
          combined_stats.ReportFinal(name);
        }
      }
      if (post_process_method != nullptr) {
        (this->*post_process_method)();
      }
    }

    if (secondary_update_thread_) {
      secondary_update_stopped_.store(1, std::memory_order_relaxed);
      secondary_update_thread_->join();
      secondary_update_thread_.reset();
    }

    if (name != "replay" && FLAGS_trace_file != "") {
      Status s = db_.db->EndTrace();
      if (!s.ok()) {
        fprintf(stderr, "Encountered an error ending the trace, %s\n",
                s.ToString().c_str());
      }
    }
    if (!FLAGS_block_cache_trace_file.empty()) {
      Status s = db_.db->EndBlockCacheTrace();
      if (!s.ok()) {
        fprintf(stderr,
                "Encountered an error ending the block cache tracing, %s\n",
                s.ToString().c_str());
      }
    }

    if (FLAGS_statistics) {
      fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
    }
    if (FLAGS_simcache_size >= 0) {
      fprintf(
          stdout, "SIMULATOR CACHE STATISTICS:\n%s\n",
          static_cast_with_check<SimCache>(cache_.get())->ToString().c_str());
    }

    if (FLAGS_use_secondary_db) {
      fprintf(stdout, "Secondary instance updated  %" PRIu64 " times.\n",
              secondary_db_updates_);
    }
  }

 private:
  std::shared_ptr<TimestampEmulator> timestamp_emulator_;
  std::unique_ptr<port::Thread> secondary_update_thread_;
  std::atomic<int> secondary_update_stopped_{0};
  uint64_t secondary_db_updates_ = 0;
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = static_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    SetPerfLevel(static_cast<PerfLevel>(shared->perf_level));
    perf_context.EnablePerLevelPerfContext();
    thread->stats.Start(thread->tid);
    (arg->bm->*(arg->method))(thread);
    if (FLAGS_perf_level > ROCKSDB_NAMESPACE::PerfLevel::kDisable) {
      thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                               get_perf_context()->ToString());
    }
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  Stats RunBenchmark(int n, Slice name,
                     void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      shared.write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }
    if (FLAGS_benchmark_read_rate_limit > 0) {
      shared.read_rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_benchmark_read_rate_limit, 100000 /* refill_period_us */,
          10 /* fairness */, RateLimiter::Mode::kReadsOnly));
    }

    std::unique_ptr<ReporterAgent> reporter_agent;
    if (FLAGS_report_interval_seconds > 0) {
      reporter_agent.reset(new ReporterAgent(FLAGS_env, FLAGS_report_file,
                                             FLAGS_report_interval_seconds));
    }

    ThreadArg* arg = new ThreadArg[n];

    for (int i = 0; i < n; i++) {
#ifdef NUMA
      if (FLAGS_enable_numa) {
        // Performs a local allocation of memory to threads in numa node.
        int n_nodes = numa_num_task_nodes();  // Number of nodes in NUMA.
        numa_exit_on_error = 1;
        int numa_node = i % n_nodes;
        bitmask* nodes = numa_allocate_nodemask();
        numa_bitmask_clearall(nodes);
        numa_bitmask_setbit(nodes, numa_node);
        // numa_bind() call binds the process to the node and these
        // properties are passed on to the thread that is created in
        // StartThread method called later in the loop.
        numa_bind(nodes);
        numa_set_strict(1);
        numa_free_nodemask(nodes);
      }
#endif
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      total_thread_count_++;
      arg[i].thread = new ThreadState(i, total_thread_count_);
      arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
      arg[i].thread->shared = &shared;
      FLAGS_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
      merge_stats.Merge(arg[i].thread->stats);
    }
    merge_stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
  }

  template <OperationType kOpType, typename FnType, typename... Args>
  static inline void ChecksumBenchmark(FnType fn, ThreadState* thread,
                                       Args... args) {
    const int size = FLAGS_block_size;  // use --block_size option for db_bench
    std::string labels = "(" + std::to_string(FLAGS_block_size) + " per op)";
    const char* label = labels.c_str();

    std::string data(size, 'x');
    uint64_t bytes = 0;
    uint32_t val = 0;
    while (bytes < 5000U * uint64_t{1048576}) {  // ~5GB
      val += static_cast<uint32_t>(fn(data.data(), size, args...));
      thread->stats.FinishedOps(nullptr, nullptr, 1, kOpType);
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... val=0x%x\r", static_cast<unsigned int>(val));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void Crc32c(ThreadState* thread) {
    ChecksumBenchmark<kCrc>(crc32c::Value, thread);
  }

  void xxHash(ThreadState* thread) {
    ChecksumBenchmark<kHash>(XXH32, thread, /*seed*/ 0);
  }

  void xxHash64(ThreadState* thread) {
    ChecksumBenchmark<kHash>(XXH64, thread, /*seed*/ 0);
  }

  void xxh3(ThreadState* thread) {
    ChecksumBenchmark<kHash>(XXH3_64bits, thread);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    std::atomic<void*> ap(&dummy);
    int count = 0;
    void* ptr = nullptr;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.load(std::memory_order_acquire);
      }
      count++;
      thread->stats.FinishedOps(nullptr, nullptr, 1, kOthers);
    }
    if (ptr == nullptr) {
      exit(1);  // Disable unused variable warning.
    }
  }

  void Compress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(FLAGS_block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    CompressionOptions opts;
    opts.level = FLAGS_compression_level;
    CompressionContext context(FLAGS_compression_type_e, opts);
    CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                         FLAGS_compression_type_e,
                         FLAGS_sample_for_compression);
    // Compress 1G
    while (ok && bytes < int64_t(1) << 30) {
      compressed.clear();
      ok = CompressSlice(info, input, &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedOps(nullptr, nullptr, 1, kCompress);
    }

    if (!ok) {
      thread->stats.AddMessage("(compression failure)");
    } else {
      char buf[340];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void Uncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(FLAGS_block_size);
    std::string compressed;

    CompressionOptions compression_opts;
    compression_opts.level = FLAGS_compression_level;
    CompressionContext compression_ctx(FLAGS_compression_type_e,
                                       compression_opts);
    CompressionInfo compression_info(
        compression_opts, compression_ctx, CompressionDict::GetEmptyDict(),
        FLAGS_compression_type_e, FLAGS_sample_for_compression);
    UncompressionContext uncompression_ctx(FLAGS_compression_type_e);
    UncompressionInfo uncompression_info(uncompression_ctx,
                                         UncompressionDict::GetEmptyDict(),
                                         FLAGS_compression_type_e);

    bool ok = CompressSlice(compression_info, input, &compressed);
    int64_t bytes = 0;
    size_t uncompressed_size = 0;
    while (ok && bytes < 1024 * 1048576) {
      constexpr uint32_t compress_format_version = 2;

      CacheAllocationPtr uncompressed = UncompressData(
          uncompression_info, compressed.data(), compressed.size(),
          &uncompressed_size, compress_format_version);

      ok = uncompressed.get() != nullptr;
      bytes += input.size();
      thread->stats.FinishedOps(nullptr, nullptr, 1, kUncompress);
    }

    if (!ok) {
      thread->stats.AddMessage("(compression failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  // Returns true if the options is initialized from the specified
  // options file.
  bool InitializeOptionsFromFile(Options* opts) {
    printf("Initializing RocksDB Options from the specified file\n");
    DBOptions db_opts;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    if (FLAGS_options_file != "") {
      ConfigOptions config_opts;
      config_opts.ignore_unknown_options = false;
      config_opts.input_strings_escaped = true;
      config_opts.env = FLAGS_env;
      auto s = LoadOptionsFromFile(config_opts, FLAGS_options_file, &db_opts,
                                   &cf_descs);
      db_opts.env = FLAGS_env;
      if (s.ok()) {
        *opts = Options(db_opts, cf_descs[0].options);
        return true;
      }
      fprintf(stderr, "Unable to load options file %s --- %s\n",
              FLAGS_options_file.c_str(), s.ToString().c_str());
      exit(1);
    }
    return false;
  }

  void InitializeOptionsFromFlags(Options* opts) {
    printf("Initializing RocksDB Options from command-line flags\n");
    Options& options = *opts;
    ConfigOptions config_options(options);
    config_options.ignore_unsupported_options = false;

    assert(db_.db == nullptr);

    options.env = FLAGS_env;
    options.wal_dir = FLAGS_wal_dir;
    options.dump_malloc_stats = FLAGS_dump_malloc_stats;
    options.stats_dump_period_sec =
        static_cast<unsigned int>(FLAGS_stats_dump_period_sec);
    options.stats_persist_period_sec =
        static_cast<unsigned int>(FLAGS_stats_persist_period_sec);
    options.persist_stats_to_disk = FLAGS_persist_stats_to_disk;
    options.stats_history_buffer_size =
        static_cast<size_t>(FLAGS_stats_history_buffer_size);
    options.avoid_flush_during_recovery = FLAGS_avoid_flush_during_recovery;

    options.compression_opts.level = FLAGS_compression_level;
    options.compression_opts.max_dict_bytes = FLAGS_compression_max_dict_bytes;
    options.compression_opts.zstd_max_train_bytes =
        FLAGS_compression_zstd_max_train_bytes;
    options.compression_opts.parallel_threads =
        FLAGS_compression_parallel_threads;
    options.compression_opts.max_dict_buffer_bytes =
        FLAGS_compression_max_dict_buffer_bytes;
    options.compression_opts.use_zstd_dict_trainer =
        FLAGS_compression_use_zstd_dict_trainer;

    options.max_open_files = FLAGS_open_files;
    if (FLAGS_cost_write_buffer_to_cache || FLAGS_db_write_buffer_size != 0) {
      options.write_buffer_manager.reset(
          new WriteBufferManager(FLAGS_db_write_buffer_size, cache_));
    }
    options.arena_block_size = FLAGS_arena_block_size;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
        FLAGS_min_write_buffer_number_to_merge;
    options.max_write_buffer_number_to_maintain =
        FLAGS_max_write_buffer_number_to_maintain;
    options.max_write_buffer_size_to_maintain =
        FLAGS_max_write_buffer_size_to_maintain;
    options.max_background_jobs = FLAGS_max_background_jobs;
    options.max_background_compactions = FLAGS_max_background_compactions;
    options.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
    options.max_background_flushes = FLAGS_max_background_flushes;
    options.compaction_style = FLAGS_compaction_style_e;
    options.compaction_pri = FLAGS_compaction_pri_e;
    options.allow_mmap_reads = FLAGS_mmap_read;
    options.allow_mmap_writes = FLAGS_mmap_write;
    options.use_direct_reads = FLAGS_use_direct_reads;
    options.use_direct_io_for_flush_and_compaction =
        FLAGS_use_direct_io_for_flush_and_compaction;
    options.manual_wal_flush = FLAGS_manual_wal_flush;
    options.wal_compression = FLAGS_wal_compression_e;
    options.ttl = FLAGS_fifo_compaction_ttl;
    options.compaction_options_fifo = CompactionOptionsFIFO(
        FLAGS_fifo_compaction_max_table_files_size_mb * 1024 * 1024,
        FLAGS_fifo_compaction_allow_compaction);
    options.compaction_options_fifo.age_for_warm = FLAGS_fifo_age_for_warm;
    options.prefix_extractor = prefix_extractor_;
    if (FLAGS_use_uint64_comparator) {
      options.comparator = test::Uint64Comparator();
      if (FLAGS_key_size != 8) {
        fprintf(stderr, "Using Uint64 comparator but key size is not 8.\n");
        exit(1);
      }
    }
    if (FLAGS_use_stderr_info_logger) {
      options.info_log = std::make_shared<StderrLogger>();
    }
    options.memtable_huge_page_size = FLAGS_memtable_use_huge_page ? 2048 : 0;
    options.memtable_prefix_bloom_size_ratio = FLAGS_memtable_bloom_size_ratio;
    options.memtable_whole_key_filtering = FLAGS_memtable_whole_key_filtering;
    if (FLAGS_memtable_insert_with_hint_prefix_size > 0) {
      options.memtable_insert_with_hint_prefix_extractor.reset(
          NewCappedPrefixTransform(
              FLAGS_memtable_insert_with_hint_prefix_size));
    }
    options.bloom_locality = FLAGS_bloom_locality;
    options.max_file_opening_threads = FLAGS_file_opening_threads;
    options.compaction_readahead_size = FLAGS_compaction_readahead_size;
    options.log_readahead_size = FLAGS_log_readahead_size;
    options.random_access_max_buffer_size = FLAGS_random_access_max_buffer_size;
    options.writable_file_max_buffer_size = FLAGS_writable_file_max_buffer_size;
    options.use_fsync = FLAGS_use_fsync;
    options.num_levels = FLAGS_num_levels;
    options.target_file_size_base = FLAGS_target_file_size_base;
    options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
    options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
    options.level_compaction_dynamic_level_bytes =
        FLAGS_level_compaction_dynamic_level_bytes;
    options.max_bytes_for_level_multiplier =
        FLAGS_max_bytes_for_level_multiplier;
    options.uncache_aggressiveness = FLAGS_uncache_aggressiveness;
    Status s =
        CreateMemTableRepFactory(config_options, &options.memtable_factory);
    if (!s.ok()) {
      fprintf(stderr, "Could not create memtable factory: %s\n",
              s.ToString().c_str());
      exit(1);
    } else if ((FLAGS_prefix_size == 0) &&
               (options.memtable_factory->IsInstanceOf("prefix_hash") ||
                options.memtable_factory->IsInstanceOf("hash_linkedlist"))) {
      fprintf(stderr,
              "prefix_size should be non-zero if PrefixHash or "
              "HashLinkedList memtablerep is used\n");
      exit(1);
    }
    if (FLAGS_use_plain_table) {
      if (!options.memtable_factory->IsInstanceOf("prefix_hash") &&
          !options.memtable_factory->IsInstanceOf("hash_linkedlist")) {
        fprintf(stderr, "Warning: plain table is used with %s\n",
                options.memtable_factory->Name());
      }

      int bloom_bits_per_key = FLAGS_bloom_bits;
      if (bloom_bits_per_key < 0) {
        bloom_bits_per_key = PlainTableOptions().bloom_bits_per_key;
      }

      PlainTableOptions plain_table_options;
      plain_table_options.user_key_len = FLAGS_key_size;
      plain_table_options.bloom_bits_per_key = bloom_bits_per_key;
      plain_table_options.hash_table_ratio = 0.75;
      options.table_factory = std::shared_ptr<TableFactory>(
          NewPlainTableFactory(plain_table_options));
    } else if (FLAGS_use_cuckoo_table) {
      if (FLAGS_cuckoo_hash_ratio > 1 || FLAGS_cuckoo_hash_ratio < 0) {
        fprintf(stderr, "Invalid cuckoo_hash_ratio\n");
        exit(1);
      }

      if (!FLAGS_mmap_read) {
        fprintf(stderr, "cuckoo table format requires mmap read to operate\n");
        exit(1);
      }

      ROCKSDB_NAMESPACE::CuckooTableOptions table_options;
      table_options.hash_table_ratio = FLAGS_cuckoo_hash_ratio;
      table_options.identity_as_first_hash = FLAGS_identity_as_first_hash;
      options.table_factory =
          std::shared_ptr<TableFactory>(NewCuckooTableFactory(table_options));
    } else {
      BlockBasedTableOptions block_based_options;
      block_based_options.checksum =
          static_cast<ChecksumType>(FLAGS_checksum_type);
      if (FLAGS_use_hash_search) {
        if (FLAGS_prefix_size == 0) {
          fprintf(stderr,
                  "prefix_size not assigned when enable use_hash_search \n");
          exit(1);
        }
        block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
      } else {
        block_based_options.index_type = BlockBasedTableOptions::kBinarySearch;
      }
      block_based_options.decouple_partitioned_filters =
          FLAGS_decouple_partitioned_filters;
      if (FLAGS_partition_index_and_filters || FLAGS_partition_index) {
        if (FLAGS_index_with_first_key) {
          fprintf(stderr,
                  "--index_with_first_key is not compatible with"
                  " partition index.");
        }
        if (FLAGS_use_hash_search) {
          fprintf(stderr,
                  "use_hash_search is incompatible with "
                  "partition index and is ignored");
        }
        block_based_options.index_type =
            BlockBasedTableOptions::kTwoLevelIndexSearch;
        block_based_options.metadata_block_size = FLAGS_metadata_block_size;
        if (FLAGS_partition_index_and_filters) {
          block_based_options.partition_filters = true;
        }
      } else if (FLAGS_index_with_first_key) {
        block_based_options.index_type =
            BlockBasedTableOptions::kBinarySearchWithFirstKey;
      }
      BlockBasedTableOptions::IndexShorteningMode index_shortening =
          block_based_options.index_shortening;
      switch (FLAGS_index_shortening_mode) {
        case 0:
          index_shortening =
              BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
          break;
        case 1:
          index_shortening =
              BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators;
          break;
        case 2:
          index_shortening = BlockBasedTableOptions::IndexShorteningMode::
              kShortenSeparatorsAndSuccessor;
          break;
        default:
          fprintf(stderr, "Unknown key shortening mode\n");
      }
      block_based_options.optimize_filters_for_memory =
          FLAGS_optimize_filters_for_memory;
      block_based_options.index_shortening = index_shortening;
      if (cache_ == nullptr) {
        block_based_options.no_block_cache = true;
      }
      block_based_options.cache_index_and_filter_blocks =
          FLAGS_cache_index_and_filter_blocks;
      block_based_options.pin_l0_filter_and_index_blocks_in_cache =
          FLAGS_pin_l0_filter_and_index_blocks_in_cache;
      block_based_options.pin_top_level_index_and_filter =
          FLAGS_pin_top_level_index_and_filter;
      if (FLAGS_cache_high_pri_pool_ratio > 1e-6) {  // > 0.0 + eps
        block_based_options.cache_index_and_filter_blocks_with_high_priority =
            true;
      }
      if (FLAGS_cache_high_pri_pool_ratio + FLAGS_cache_low_pri_pool_ratio >
          1.0) {
        fprintf(stderr,
                "Sum of high_pri_pool_ratio and low_pri_pool_ratio "
                "cannot exceed 1.0.\n");
      }
      block_based_options.block_cache = cache_;
      block_based_options.cache_usage_options.options_overrides.insert(
          {CacheEntryRole::kCompressionDictionaryBuildingBuffer,
           {/*.charged = */ FLAGS_charge_compression_dictionary_building_buffer
                ? CacheEntryRoleOptions::Decision::kEnabled
                : CacheEntryRoleOptions::Decision::kDisabled}});
      block_based_options.cache_usage_options.options_overrides.insert(
          {CacheEntryRole::kFilterConstruction,
           {/*.charged = */ FLAGS_charge_filter_construction
                ? CacheEntryRoleOptions::Decision::kEnabled
                : CacheEntryRoleOptions::Decision::kDisabled}});
      block_based_options.cache_usage_options.options_overrides.insert(
          {CacheEntryRole::kBlockBasedTableReader,
           {/*.charged = */ FLAGS_charge_table_reader
                ? CacheEntryRoleOptions::Decision::kEnabled
                : CacheEntryRoleOptions::Decision::kDisabled}});
      block_based_options.cache_usage_options.options_overrides.insert(
          {CacheEntryRole::kFileMetadata,
           {/*.charged = */ FLAGS_charge_file_metadata
                ? CacheEntryRoleOptions::Decision::kEnabled
                : CacheEntryRoleOptions::Decision::kDisabled}});
      block_based_options.cache_usage_options.options_overrides.insert(
          {CacheEntryRole::kBlobCache,
           {/*.charged = */ FLAGS_charge_blob_cache
                ? CacheEntryRoleOptions::Decision::kEnabled
                : CacheEntryRoleOptions::Decision::kDisabled}});
      block_based_options.block_size = FLAGS_block_size;
      block_based_options.block_restart_interval = FLAGS_block_restart_interval;
      block_based_options.index_block_restart_interval =
          FLAGS_index_block_restart_interval;
      block_based_options.format_version =
          static_cast<uint32_t>(FLAGS_format_version);
      block_based_options.read_amp_bytes_per_bit = FLAGS_read_amp_bytes_per_bit;
      block_based_options.enable_index_compression =
          FLAGS_enable_index_compression;
      block_based_options.block_align = FLAGS_block_align;
      block_based_options.whole_key_filtering = FLAGS_whole_key_filtering;
      block_based_options.max_auto_readahead_size =
          FLAGS_max_auto_readahead_size;
      block_based_options.initial_auto_readahead_size =
          FLAGS_initial_auto_readahead_size;
      block_based_options.num_file_reads_for_auto_readahead =
          FLAGS_num_file_reads_for_auto_readahead;
      BlockBasedTableOptions::PrepopulateBlockCache prepopulate_block_cache =
          block_based_options.prepopulate_block_cache;
      switch (FLAGS_prepopulate_block_cache) {
        case 0:
          prepopulate_block_cache =
              BlockBasedTableOptions::PrepopulateBlockCache::kDisable;
          break;
        case 1:
          prepopulate_block_cache =
              BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;
          break;
        default:
          fprintf(stderr, "Unknown prepopulate block cache mode\n");
      }
      block_based_options.prepopulate_block_cache = prepopulate_block_cache;
      if (FLAGS_use_data_block_hash_index) {
        block_based_options.data_block_index_type =
            ROCKSDB_NAMESPACE::BlockBasedTableOptions::kDataBlockBinaryAndHash;
      } else {
        block_based_options.data_block_index_type =
            ROCKSDB_NAMESPACE::BlockBasedTableOptions::kDataBlockBinarySearch;
      }
      block_based_options.data_block_hash_table_util_ratio =
          FLAGS_data_block_hash_table_util_ratio;
      if (FLAGS_read_cache_path != "") {
        Status rc_status;

        // Read cache need to be provided with a the Logger, we will put all
        // reac cache logs in the read cache path in a file named rc_LOG
        rc_status = FLAGS_env->CreateDirIfMissing(FLAGS_read_cache_path);
        std::shared_ptr<Logger> read_cache_logger;
        if (rc_status.ok()) {
          rc_status = FLAGS_env->NewLogger(FLAGS_read_cache_path + "/rc_LOG",
                                           &read_cache_logger);
        }

        if (rc_status.ok()) {
          PersistentCacheConfig rc_cfg(FLAGS_env, FLAGS_read_cache_path,
                                       FLAGS_read_cache_size,
                                       read_cache_logger);

          rc_cfg.enable_direct_reads = FLAGS_read_cache_direct_read;
          rc_cfg.enable_direct_writes = FLAGS_read_cache_direct_write;
          rc_cfg.writer_qdepth = 4;
          rc_cfg.writer_dispatch_size = 4 * 1024;

          auto pcache = std::make_shared<BlockCacheTier>(rc_cfg);
          block_based_options.persistent_cache = pcache;
          rc_status = pcache->Open();
        }

        if (!rc_status.ok()) {
          fprintf(stderr, "Error initializing read cache, %s\n",
                  rc_status.ToString().c_str());
          exit(1);
        }
      }

      if (FLAGS_use_blob_cache) {
        if (FLAGS_use_shared_block_and_blob_cache) {
          options.blob_cache = cache_;
        } else {
          if (FLAGS_blob_cache_size > 0) {
            LRUCacheOptions co;
            co.capacity = FLAGS_blob_cache_size;
            co.num_shard_bits = FLAGS_blob_cache_numshardbits;
            co.memory_allocator = GetCacheAllocator();

            options.blob_cache = NewLRUCache(co);
          } else {
            fprintf(
                stderr,
                "Unable to create a standalone blob cache if blob_cache_size "
                "<= 0.\n");
            exit(1);
          }
        }
        switch (FLAGS_prepopulate_blob_cache) {
          case 0:
            options.prepopulate_blob_cache = PrepopulateBlobCache::kDisable;
            break;
          case 1:
            options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
            break;
          default:
            fprintf(stderr, "Unknown prepopulate blob cache mode\n");
            exit(1);
        }

        fprintf(stdout,
                "Integrated BlobDB: blob cache enabled"
                ", block and blob caches shared: %d",
                FLAGS_use_shared_block_and_blob_cache);
        if (!FLAGS_use_shared_block_and_blob_cache) {
          fprintf(stdout,
                  ", blob cache size %" PRIu64
                  ", blob cache num shard bits: %d",
                  FLAGS_blob_cache_size, FLAGS_blob_cache_numshardbits);
        }
        fprintf(stdout, ", blob cache prepopulated: %d\n",
                FLAGS_prepopulate_blob_cache);
      } else {
        fprintf(stdout, "Integrated BlobDB: blob cache disabled\n");
      }

      options.table_factory.reset(
          NewBlockBasedTableFactory(block_based_options));
    }
    if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() > 0) {
      if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() !=
          static_cast<unsigned int>(FLAGS_num_levels)) {
        fprintf(stderr, "Insufficient number of fanouts specified %d\n",
                static_cast<int>(
                    FLAGS_max_bytes_for_level_multiplier_additional_v.size()));
        exit(1);
      }
      options.max_bytes_for_level_multiplier_additional =
          FLAGS_max_bytes_for_level_multiplier_additional_v;
    }
    options.level0_stop_writes_trigger = FLAGS_level0_stop_writes_trigger;
    options.level0_file_num_compaction_trigger =
        FLAGS_level0_file_num_compaction_trigger;
    options.level0_slowdown_writes_trigger =
        FLAGS_level0_slowdown_writes_trigger;
    options.compression = FLAGS_compression_type_e;
    if (FLAGS_simulate_hybrid_fs_file != "") {
      options.last_level_temperature = Temperature::kWarm;
    }
    options.preclude_last_level_data_seconds =
        FLAGS_preclude_last_level_data_seconds;
    options.preserve_internal_time_seconds =
        FLAGS_preserve_internal_time_seconds;
    options.sample_for_compression = FLAGS_sample_for_compression;
    options.WAL_ttl_seconds = FLAGS_wal_ttl_seconds;
    options.WAL_size_limit_MB = FLAGS_wal_size_limit_MB;
    options.max_total_wal_size = FLAGS_max_total_wal_size;

    if (FLAGS_min_level_to_compress >= 0) {
      assert(FLAGS_min_level_to_compress <= FLAGS_num_levels);
      options.compression_per_level.resize(FLAGS_num_levels);
      for (int i = 0; i < FLAGS_min_level_to_compress; i++) {
        options.compression_per_level[i] = kNoCompression;
      }
      for (int i = FLAGS_min_level_to_compress; i < FLAGS_num_levels; i++) {
        options.compression_per_level[i] = FLAGS_compression_type_e;
      }
    }
    options.soft_pending_compaction_bytes_limit =
        FLAGS_soft_pending_compaction_bytes_limit;
    options.hard_pending_compaction_bytes_limit =
        FLAGS_hard_pending_compaction_bytes_limit;
    options.delayed_write_rate = FLAGS_delayed_write_rate;
    options.allow_concurrent_memtable_write =
        FLAGS_allow_concurrent_memtable_write;
    options.experimental_mempurge_threshold =
        FLAGS_experimental_mempurge_threshold;
    options.inplace_update_support = FLAGS_inplace_update_support;
    options.inplace_update_num_locks = FLAGS_inplace_update_num_locks;
    options.enable_write_thread_adaptive_yield =
        FLAGS_enable_write_thread_adaptive_yield;
    options.enable_pipelined_write = FLAGS_enable_pipelined_write;
    options.unordered_write = FLAGS_unordered_write;
    options.write_thread_max_yield_usec = FLAGS_write_thread_max_yield_usec;
    options.write_thread_slow_yield_usec = FLAGS_write_thread_slow_yield_usec;
    options.table_cache_numshardbits = FLAGS_table_cache_numshardbits;
    options.max_compaction_bytes = FLAGS_max_compaction_bytes;
    options.disable_auto_compactions = FLAGS_disable_auto_compactions;
    options.optimize_filters_for_hits = FLAGS_optimize_filters_for_hits;
    options.paranoid_checks = FLAGS_paranoid_checks;
    options.force_consistency_checks = FLAGS_force_consistency_checks;
    options.periodic_compaction_seconds = FLAGS_periodic_compaction_seconds;
    options.ttl = FLAGS_ttl_seconds;
    // fill storage options
    options.advise_random_on_open = FLAGS_advise_random_on_open;
    options.use_adaptive_mutex = FLAGS_use_adaptive_mutex;
    options.bytes_per_sync = FLAGS_bytes_per_sync;
    options.wal_bytes_per_sync = FLAGS_wal_bytes_per_sync;

    // merge operator options
    if (!FLAGS_merge_operator.empty()) {
      s = MergeOperator::CreateFromString(config_options, FLAGS_merge_operator,
                                          &options.merge_operator);
      if (!s.ok()) {
        fprintf(stderr, "invalid merge operator[%s]: %s\n",
                FLAGS_merge_operator.c_str(), s.ToString().c_str());
        exit(1);
      }
    }
    options.max_successive_merges = FLAGS_max_successive_merges;
    options.strict_max_successive_merges = FLAGS_strict_max_successive_merges;
    options.report_bg_io_stats = FLAGS_report_bg_io_stats;

    // set universal style compaction configurations, if applicable
    if (FLAGS_universal_size_ratio != 0) {
      options.compaction_options_universal.size_ratio =
          FLAGS_universal_size_ratio;
    }
    if (FLAGS_universal_min_merge_width != 0) {
      options.compaction_options_universal.min_merge_width =
          FLAGS_universal_min_merge_width;
    }
    if (FLAGS_universal_max_merge_width != 0) {
      options.compaction_options_universal.max_merge_width =
          FLAGS_universal_max_merge_width;
    }
    if (FLAGS_universal_max_size_amplification_percent != 0) {
      options.compaction_options_universal.max_size_amplification_percent =
          FLAGS_universal_max_size_amplification_percent;
    }
    if (FLAGS_universal_compression_size_percent != -1) {
      options.compaction_options_universal.compression_size_percent =
          FLAGS_universal_compression_size_percent;
    }
    options.compaction_options_universal.max_read_amp =
        FLAGS_universal_max_read_amp;
    options.compaction_options_universal.allow_trivial_move =
        FLAGS_universal_allow_trivial_move;
    options.compaction_options_universal.incremental =
        FLAGS_universal_incremental;
    options.compaction_options_universal.stop_style =
        static_cast<CompactionStopStyle>(FLAGS_universal_stop_style);
    if (FLAGS_thread_status_per_interval > 0) {
      options.enable_thread_tracking = true;
    }

    if (FLAGS_user_timestamp_size > 0) {
      if (FLAGS_user_timestamp_size != 8) {
        fprintf(stderr, "Only 64 bits timestamps are supported.\n");
        exit(1);
      }
      options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    }

    options.allow_data_in_errors = FLAGS_allow_data_in_errors;
    options.track_and_verify_wals_in_manifest =
        FLAGS_track_and_verify_wals_in_manifest;

    // Integrated BlobDB
    options.enable_blob_files = FLAGS_enable_blob_files;
    options.min_blob_size = FLAGS_min_blob_size;
    options.blob_file_size = FLAGS_blob_file_size;
    options.blob_compression_type =
        StringToCompressionType(FLAGS_blob_compression_type.c_str());
    options.enable_blob_garbage_collection =
        FLAGS_enable_blob_garbage_collection;
    options.blob_garbage_collection_age_cutoff =
        FLAGS_blob_garbage_collection_age_cutoff;
    options.blob_garbage_collection_force_threshold =
        FLAGS_blob_garbage_collection_force_threshold;
    options.blob_compaction_readahead_size =
        FLAGS_blob_compaction_readahead_size;
    options.blob_file_starting_level = FLAGS_blob_file_starting_level;

    if (FLAGS_readonly && FLAGS_transaction_db) {
      fprintf(stderr, "Cannot use readonly flag with transaction_db\n");
      exit(1);
    }
    if (FLAGS_use_secondary_db &&
        (FLAGS_transaction_db || FLAGS_optimistic_transaction_db)) {
      fprintf(stderr, "Cannot use use_secondary_db flag with transaction_db\n");
      exit(1);
    }
    options.memtable_protection_bytes_per_key =
        FLAGS_memtable_protection_bytes_per_key;
    options.block_protection_bytes_per_key =
        FLAGS_block_protection_bytes_per_key;
    options.paranoid_memory_checks = FLAGS_paranoid_memory_checks;
  }

  void InitializeOptionsGeneral(Options* opts) {
    // Be careful about what is set here to avoid accidentally overwriting
    // settings already configured by OPTIONS file. Only configure settings that
    // are needed for the benchmark to run, settings for shared objects that
    // were not configured already, settings that require dynamically invoking
    // APIs, and settings for the benchmark itself.
    Options& options = *opts;

    // Always set these since they are harmless when not needed and prevent
    // a guaranteed failure when they are needed.
    options.create_missing_column_families = true;
    options.create_if_missing = true;

    if (options.statistics == nullptr) {
      options.statistics = dbstats;
    }

    auto table_options =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (table_options != nullptr) {
      if (FLAGS_cache_size > 0) {
        // This violates this function's rules on when to set options. But we
        // have to do it because the case of unconfigured block cache in OPTIONS
        // file is indistinguishable (it is sanitized to 32MB by this point, not
        // nullptr), and our regression tests assume this will be the shared
        // block cache, even with OPTIONS file provided.
        table_options->block_cache = cache_;
      }
      if (table_options->filter_policy == nullptr) {
        if (FLAGS_bloom_bits < 0) {
          table_options->filter_policy = BlockBasedTableOptions().filter_policy;
        } else if (FLAGS_bloom_bits == 0) {
          table_options->filter_policy.reset();
        } else {
          table_options->filter_policy.reset(
              FLAGS_use_ribbon_filter ? NewRibbonFilterPolicy(FLAGS_bloom_bits)
                                      : NewBloomFilterPolicy(FLAGS_bloom_bits));
        }
      }
    }

    if (options.row_cache == nullptr) {
      if (FLAGS_row_cache_size) {
        if (FLAGS_cache_numshardbits >= 1) {
          options.row_cache =
              NewLRUCache(FLAGS_row_cache_size, FLAGS_cache_numshardbits);
        } else {
          options.row_cache = NewLRUCache(FLAGS_row_cache_size);
        }
      }
    }

    if (options.env == Env::Default()) {
      options.env = FLAGS_env;
    }
    if (FLAGS_enable_io_prio) {
      options.env->LowerThreadPoolIOPriority(Env::LOW);
      options.env->LowerThreadPoolIOPriority(Env::HIGH);
    }
    if (FLAGS_enable_cpu_prio) {
      options.env->LowerThreadPoolCPUPriority(Env::LOW);
      options.env->LowerThreadPoolCPUPriority(Env::HIGH);
    }

    if (FLAGS_sine_write_rate) {
      FLAGS_benchmark_write_rate_limit = static_cast<uint64_t>(SineRate(0));
    }

    if (options.rate_limiter == nullptr) {
      if (FLAGS_rate_limiter_bytes_per_sec > 0) {
        options.rate_limiter.reset(NewGenericRateLimiter(
            FLAGS_rate_limiter_bytes_per_sec,
            FLAGS_rate_limiter_refill_period_us, 10 /* fairness */,
            // TODO: replace this with a more general FLAG for deciding
            // RateLimiter::Mode as now we also rate-limit foreground reads e.g,
            // Get()/MultiGet()
            FLAGS_rate_limit_bg_reads ? RateLimiter::Mode::kReadsOnly
                                      : RateLimiter::Mode::kWritesOnly,
            FLAGS_rate_limiter_auto_tuned,
            FLAGS_rate_limiter_single_burst_bytes));
      }
    }

    options.listeners.emplace_back(listener_);

    if (options.file_checksum_gen_factory == nullptr) {
      if (FLAGS_file_checksum) {
        options.file_checksum_gen_factory.reset(
            new FileChecksumGenCrc32cFactory());
      }
    }

    if (FLAGS_num_multi_db <= 1) {
      OpenDb(options, FLAGS_db, &db_);
    } else {
      multi_dbs_.clear();
      multi_dbs_.resize(FLAGS_num_multi_db);
      auto wal_dir = options.wal_dir;
      for (int i = 0; i < FLAGS_num_multi_db; i++) {
        if (!wal_dir.empty()) {
          options.wal_dir = GetPathForMultiple(wal_dir, i);
        }
        OpenDb(options, GetPathForMultiple(FLAGS_db, i), &multi_dbs_[i]);
      }
      options.wal_dir = wal_dir;
    }

    // KeepFilter is a noop filter, this can be used to test compaction filter
    if (options.compaction_filter == nullptr) {
      if (FLAGS_use_keep_filter) {
        options.compaction_filter = new KeepFilter();
        fprintf(stdout, "A noop compaction filter is used\n");
      }
    }

    if (FLAGS_use_existing_keys) {
      // Only work on single database
      assert(db_.db != nullptr);
      ReadOptions read_opts;  // before read_options_ initialized
      read_opts.total_order_seek = true;
      Iterator* iter = db_.db->NewIterator(read_opts);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        keys_.emplace_back(iter->key().ToString());
      }
      delete iter;
      FLAGS_num = keys_.size();
    }
  }

  void Open(Options* opts) {
    if (!InitializeOptionsFromFile(opts)) {
      InitializeOptionsFromFlags(opts);
    }

    InitializeOptionsGeneral(opts);
  }

  void OpenDb(Options options, const std::string& db_name,
              DBWithColumnFamilies* db) {
    uint64_t open_start = FLAGS_report_open_timing ? FLAGS_env->NowNanos() : 0;
    Status s;
    // Open with column families if necessary.
    if (FLAGS_num_column_families > 1) {
      size_t num_hot = FLAGS_num_column_families;
      if (FLAGS_num_hot_column_families > 0 &&
          FLAGS_num_hot_column_families < FLAGS_num_column_families) {
        num_hot = FLAGS_num_hot_column_families;
      } else {
        FLAGS_num_hot_column_families = FLAGS_num_column_families;
      }
      std::vector<ColumnFamilyDescriptor> column_families;
      for (size_t i = 0; i < num_hot; i++) {
        column_families.emplace_back(ColumnFamilyName(i),
                                     ColumnFamilyOptions(options));
      }
      std::vector<int> cfh_idx_to_prob;
      if (!FLAGS_column_family_distribution.empty()) {
        std::stringstream cf_prob_stream(FLAGS_column_family_distribution);
        std::string cf_prob;
        int sum = 0;
        while (std::getline(cf_prob_stream, cf_prob, ',')) {
          cfh_idx_to_prob.push_back(std::stoi(cf_prob));
          sum += cfh_idx_to_prob.back();
        }
        if (sum != 100) {
          fprintf(stderr, "column_family_distribution items must sum to 100\n");
          exit(1);
        }
        if (cfh_idx_to_prob.size() != num_hot) {
          fprintf(stderr,
                  "got %" ROCKSDB_PRIszt
                  " column_family_distribution items; expected "
                  "%" ROCKSDB_PRIszt "\n",
                  cfh_idx_to_prob.size(), num_hot);
          exit(1);
        }
      }
      if (FLAGS_readonly) {
        s = DB::OpenForReadOnly(options, db_name, column_families, &db->cfh,
                                &db->db);
      } else if (FLAGS_optimistic_transaction_db) {
        s = OptimisticTransactionDB::Open(options, db_name, column_families,
                                          &db->cfh, &db->opt_txn_db);
        if (s.ok()) {
          db->db = db->opt_txn_db->GetBaseDB();
        }
      } else if (FLAGS_transaction_db) {
        TransactionDB* ptr;
        TransactionDBOptions txn_db_options;
        if (options.unordered_write) {
          options.two_write_queues = true;
          txn_db_options.skip_concurrency_control = true;
          txn_db_options.write_policy = WRITE_PREPARED;
        }
        s = TransactionDB::Open(options, txn_db_options, db_name,
                                column_families, &db->cfh, &ptr);
        if (s.ok()) {
          db->db = ptr;
        }
      } else {
        s = DB::Open(options, db_name, column_families, &db->cfh, &db->db);
      }
      db->cfh.resize(FLAGS_num_column_families);
      db->num_created = num_hot;
      db->num_hot = num_hot;
      db->cfh_idx_to_prob = std::move(cfh_idx_to_prob);
    } else if (FLAGS_readonly) {
      s = DB::OpenForReadOnly(options, db_name, &db->db);
    } else if (FLAGS_optimistic_transaction_db) {
      s = OptimisticTransactionDB::Open(options, db_name, &db->opt_txn_db);
      if (s.ok()) {
        db->db = db->opt_txn_db->GetBaseDB();
      }
    } else if (FLAGS_transaction_db) {
      TransactionDB* ptr = nullptr;
      TransactionDBOptions txn_db_options;
      if (options.unordered_write) {
        options.two_write_queues = true;
        txn_db_options.skip_concurrency_control = true;
        txn_db_options.write_policy = WRITE_PREPARED;
      }
      s = CreateLoggerFromOptions(db_name, options, &options.info_log);
      if (s.ok()) {
        s = TransactionDB::Open(options, txn_db_options, db_name, &ptr);
      }
      if (s.ok()) {
        db->db = ptr;
      }
    } else if (FLAGS_use_blob_db) {
      // Stacked BlobDB
      blob_db::BlobDBOptions blob_db_options;
      blob_db_options.enable_garbage_collection = FLAGS_blob_db_enable_gc;
      blob_db_options.garbage_collection_cutoff = FLAGS_blob_db_gc_cutoff;
      blob_db_options.is_fifo = FLAGS_blob_db_is_fifo;
      blob_db_options.max_db_size = FLAGS_blob_db_max_db_size;
      blob_db_options.ttl_range_secs = FLAGS_blob_db_ttl_range_secs;
      blob_db_options.min_blob_size = FLAGS_blob_db_min_blob_size;
      blob_db_options.bytes_per_sync = FLAGS_blob_db_bytes_per_sync;
      blob_db_options.blob_file_size = FLAGS_blob_db_file_size;
      blob_db_options.compression = FLAGS_blob_db_compression_type_e;
      blob_db::BlobDB* ptr = nullptr;
      s = blob_db::BlobDB::Open(options, blob_db_options, db_name, &ptr);
      if (s.ok()) {
        db->db = ptr;
      }
    } else if (FLAGS_use_secondary_db) {
      if (FLAGS_secondary_path.empty()) {
        std::string default_secondary_path;
        FLAGS_env->GetTestDirectory(&default_secondary_path);
        default_secondary_path += "/dbbench_secondary";
        FLAGS_secondary_path = default_secondary_path;
      }
      s = DB::OpenAsSecondary(options, db_name, FLAGS_secondary_path, &db->db);
      if (s.ok() && FLAGS_secondary_update_interval > 0) {
        secondary_update_thread_.reset(new port::Thread(
            [this](int interval, DBWithColumnFamilies* _db) {
              while (0 == secondary_update_stopped_.load(
                              std::memory_order_relaxed)) {
                Status secondary_update_status =
                    _db->db->TryCatchUpWithPrimary();
                if (!secondary_update_status.ok()) {
                  fprintf(stderr, "Failed to catch up with primary: %s\n",
                          secondary_update_status.ToString().c_str());
                  break;
                }
                ++secondary_db_updates_;
                FLAGS_env->SleepForMicroseconds(interval * 1000000);
              }
            },
            FLAGS_secondary_update_interval, db));
      }
    } else if (FLAGS_open_as_follower) {
      std::unique_ptr<DB> dbptr;
      s = DB::OpenAsFollower(options, db_name, FLAGS_leader_path, &dbptr);
      if (s.ok()) {
        db->db = dbptr.release();
      }
    } else {
      s = DB::Open(options, db_name, &db->db);
    }
    if (FLAGS_report_open_timing) {
      std::cout << "OpenDb:     "
                << (FLAGS_env->NowNanos() - open_start) / 1000000.0
                << " milliseconds\n";
    }
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  enum WriteMode { RANDOM, SEQUENTIAL, UNIQUE_RANDOM };

  void WriteSeqDeterministic(ThreadState* thread) {
    DoDeterministicCompact(thread, open_options_.compaction_style, SEQUENTIAL);
  }

  void WriteUniqueRandomDeterministic(ThreadState* thread) {
    DoDeterministicCompact(thread, open_options_.compaction_style,
                           UNIQUE_RANDOM);
  }

  void WriteSeq(ThreadState* thread) { DoWrite(thread, SEQUENTIAL); }

  void WriteRandom(ThreadState* thread) { DoWrite(thread, RANDOM); }

  void WriteUniqueRandom(ThreadState* thread) {
    DoWrite(thread, UNIQUE_RANDOM);
  }

  class KeyGenerator {
   public:
    KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
        : rand_(rand), mode_(mode), num_(num), next_(0) {
      if (mode_ == UNIQUE_RANDOM) {
        // NOTE: if memory consumption of this approach becomes a concern,
        // we can either break it into pieces and only random shuffle a section
        // each time. Alternatively, use a bit map implementation
        // (https://reviews.facebook.net/differential/diff/54627/)
        values_.resize(num_);
        for (uint64_t i = 0; i < num_; ++i) {
          values_[i] = i;
        }
        RandomShuffle(values_.begin(), values_.end(),
                      static_cast<uint32_t>(*seed_base));
      }
    }

    uint64_t Next() {
      switch (mode_) {
        case SEQUENTIAL:
          return next_++;
        case RANDOM:
          return rand_->Next() % num_;
        case UNIQUE_RANDOM:
          assert(next_ < num_);
          return values_[next_++];
      }
      assert(false);
      return std::numeric_limits<uint64_t>::max();
    }

    // Only available for UNIQUE_RANDOM mode.
    uint64_t Fetch(uint64_t index) {
      assert(mode_ == UNIQUE_RANDOM);
      assert(index < values_.size());
      return values_[index];
    }

   private:
    Random64* rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
  };

  DB* SelectDB(ThreadState* thread) { return SelectDBWithCfh(thread)->db; }

  DBWithColumnFamilies* SelectDBWithCfh(ThreadState* thread) {
    return SelectDBWithCfh(thread->rand.Next());
  }

  DBWithColumnFamilies* SelectDBWithCfh(uint64_t rand_int) {
    if (db_.db != nullptr) {
      return &db_;
    } else {
      return &multi_dbs_[rand_int % multi_dbs_.size()];
    }
  }

  double SineRate(double x) {
    return FLAGS_sine_a * sin((FLAGS_sine_b * x) + FLAGS_sine_c) + FLAGS_sine_d;
  }

  void DoWrite(ThreadState* thread, WriteMode write_mode) {
    const int test_duration = write_mode == RANDOM ? FLAGS_duration : 0;
    const int64_t num_ops = writes_ == 0 ? num_ : writes_;

    size_t num_key_gens = 1;
    if (db_.db == nullptr) {
      num_key_gens = multi_dbs_.size();
    }
    std::vector<std::unique_ptr<KeyGenerator>> key_gens(num_key_gens);
    int64_t max_ops = num_ops * num_key_gens;
    int64_t ops_per_stage = max_ops;
    if (FLAGS_num_column_families > 1 && FLAGS_num_hot_column_families > 0) {
      ops_per_stage = (max_ops - 1) / (FLAGS_num_column_families /
                                       FLAGS_num_hot_column_families) +
                      1;
    }

    Duration duration(test_duration, max_ops, ops_per_stage);
    const uint64_t num_per_key_gen = num_ + max_num_range_tombstones_;
    for (size_t i = 0; i < num_key_gens; i++) {
      key_gens[i].reset(new KeyGenerator(&(thread->rand), write_mode,
                                         num_per_key_gen, ops_per_stage));
    }

    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%" PRIu64 " ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch(/*reserved_bytes=*/0, /*max_bytes=*/0,
                     FLAGS_write_batch_protection_bytes_per_key,
                     user_timestamp_size_);
    Status s;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<const char[]> begin_key_guard;
    Slice begin_key = AllocateKey(&begin_key_guard);
    std::unique_ptr<const char[]> end_key_guard;
    Slice end_key = AllocateKey(&end_key_guard);
    double p = 0.0;
    uint64_t num_overwrites = 0, num_unique_keys = 0, num_selective_deletes = 0;
    // If user set overwrite_probability flag,
    // check if value is in [0.0,1.0].
    if (FLAGS_overwrite_probability > 0.0) {
      p = FLAGS_overwrite_probability > 1.0 ? 1.0 : FLAGS_overwrite_probability;
      // If overwrite set by user, and UNIQUE_RANDOM mode on,
      // the overwrite_window_size must be > 0.
      if (write_mode == UNIQUE_RANDOM && FLAGS_overwrite_window_size == 0) {
        fprintf(stderr,
                "Overwrite_window_size must be  strictly greater than 0.\n");
        ErrorExit();
      }
    }

    // Default_random_engine provides slightly
    // improved throughput over mt19937.
    std::default_random_engine overwrite_gen{
        static_cast<unsigned int>(*seed_base)};
    std::bernoulli_distribution overwrite_decider(p);

    // Inserted key window is filled with the last N
    // keys previously inserted into the DB (with
    // N=FLAGS_overwrite_window_size).
    // We use a deque struct because:
    // - random access is O(1)
    // - insertion/removal at beginning/end is also O(1).
    std::deque<int64_t> inserted_key_window;
    Random64 reservoir_id_gen(*seed_base);

    // --- Variables used in disposable/persistent keys simulation:
    // The following variables are used when
    // disposable_entries_batch_size is >0. We simualte a workload
    // where the following sequence is repeated multiple times:
    // "A set of keys S1 is inserted ('disposable entries'), then after
    // some delay another set of keys S2 is inserted ('persistent entries')
    // and the first set of keys S1 is deleted. S2 artificially represents
    // the insertion of hypothetical results from some undefined computation
    // done on the first set of keys S1. The next sequence can start as soon
    // as the last disposable entry in the set S1 of this sequence is
    // inserted, if the delay is non negligible"
    bool skip_for_loop = false, is_disposable_entry = true;
    std::vector<uint64_t> disposable_entries_index(num_key_gens, 0);
    std::vector<uint64_t> persistent_ent_and_del_index(num_key_gens, 0);
    const uint64_t kNumDispAndPersEntries =
        FLAGS_disposable_entries_batch_size +
        FLAGS_persistent_entries_batch_size;
    if (kNumDispAndPersEntries > 0) {
      if ((write_mode != UNIQUE_RANDOM) || (writes_per_range_tombstone_ > 0) ||
          (p > 0.0)) {
        fprintf(
            stderr,
            "Disposable/persistent deletes are not compatible with overwrites "
            "and DeleteRanges; and are only supported in filluniquerandom.\n");
        ErrorExit();
      }
      if (FLAGS_disposable_entries_value_size < 0 ||
          FLAGS_persistent_entries_value_size < 0) {
        fprintf(
            stderr,
            "disposable_entries_value_size and persistent_entries_value_size"
            "have to be positive.\n");
        ErrorExit();
      }
    }
    Random rnd_disposable_entry(static_cast<uint32_t>(*seed_base));
    std::string random_value;
    // Queue that stores scheduled timestamp of disposable entries deletes,
    // along with starting index of disposable entry keys to delete.
    std::vector<std::queue<std::pair<uint64_t, uint64_t>>> disposable_entries_q(
        num_key_gens);
    // --- End of variables used in disposable/persistent keys simulation.

    std::vector<std::unique_ptr<const char[]>> expanded_key_guards;
    std::vector<Slice> expanded_keys;
    if (FLAGS_expand_range_tombstones) {
      expanded_key_guards.resize(range_tombstone_width_);
      for (auto& expanded_key_guard : expanded_key_guards) {
        expanded_keys.emplace_back(AllocateKey(&expanded_key_guard));
      }
    }

    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }

    int64_t stage = 0;
    int64_t num_written = 0;
    int64_t next_seq_db_at = num_ops;
    size_t id = 0;
    int64_t num_range_deletions = 0;

    while ((num_per_key_gen != 0) && !duration.Done(entries_per_batch_)) {
      if (duration.GetStage() != stage) {
        stage = duration.GetStage();
        if (db_.db != nullptr) {
          db_.CreateNewCf(open_options_, stage);
        } else {
          for (auto& db : multi_dbs_) {
            db.CreateNewCf(open_options_, stage);
          }
        }
      }

      if (write_mode != SEQUENTIAL) {
        id = thread->rand.Next() % num_key_gens;
      } else {
        // When doing a sequential load with multiple databases, load them in
        // order rather than all at the same time to avoid:
        // 1) long delays between flushing memtables
        // 2) flushing memtables for all of them at the same point in time
        // 3) not putting the same number of keys in each database
        if (num_written >= next_seq_db_at) {
          next_seq_db_at += num_ops;
          id++;
          if (id >= num_key_gens) {
            fprintf(stderr, "Logic error. Filled all databases\n");
            ErrorExit();
          }
        }
      }
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(id);

      batch.Clear();
      int64_t batch_bytes = 0;

      for (int64_t j = 0; j < entries_per_batch_; j++) {
        int64_t rand_num = 0;
        if ((write_mode == UNIQUE_RANDOM) && (p > 0.0)) {
          if ((inserted_key_window.size() > 0) &&
              overwrite_decider(overwrite_gen)) {
            num_overwrites++;
            rand_num = inserted_key_window[reservoir_id_gen.Next() %
                                           inserted_key_window.size()];
          } else {
            num_unique_keys++;
            rand_num = key_gens[id]->Next();
            if (inserted_key_window.size() < FLAGS_overwrite_window_size) {
              inserted_key_window.push_back(rand_num);
            } else {
              inserted_key_window.pop_front();
              inserted_key_window.push_back(rand_num);
            }
          }
        } else if (kNumDispAndPersEntries > 0) {
          // Check if queue is non-empty and if we need to insert
          // 'persistent' KV entries (KV entries that are never deleted)
          // and delete disposable entries previously inserted.
          if (!disposable_entries_q[id].empty() &&
              (disposable_entries_q[id].front().first <
               FLAGS_env->NowMicros())) {
            // If we need to perform a "merge op" pattern,
            // we first write all the persistent KV entries not targeted
            // by deletes, and then we write the disposable entries deletes.
            if (persistent_ent_and_del_index[id] <
                FLAGS_persistent_entries_batch_size) {
              // Generate key to insert.
              rand_num =
                  key_gens[id]->Fetch(disposable_entries_q[id].front().second +
                                      FLAGS_disposable_entries_batch_size +
                                      persistent_ent_and_del_index[id]);
              persistent_ent_and_del_index[id]++;
              is_disposable_entry = false;
              skip_for_loop = false;
            } else if (persistent_ent_and_del_index[id] <
                       kNumDispAndPersEntries) {
              // Find key of the entry to delete.
              rand_num =
                  key_gens[id]->Fetch(disposable_entries_q[id].front().second +
                                      (persistent_ent_and_del_index[id] -
                                       FLAGS_persistent_entries_batch_size));
              persistent_ent_and_del_index[id]++;
              GenerateKeyFromInt(rand_num, FLAGS_num, &key);
              // For the delete operation, everything happens here and we
              // skip the rest of the for-loop, which is designed for
              // inserts.
              if (FLAGS_num_column_families <= 1) {
                batch.Delete(key);
              } else {
                // We use same rand_num as seed for key and column family so
                // that we can deterministically find the cfh corresponding to a
                // particular key while reading the key.
                batch.Delete(db_with_cfh->GetCfh(rand_num), key);
              }
              // A delete only includes Key+Timestamp (no value).
              batch_bytes += key_size_ + user_timestamp_size_;
              bytes += key_size_ + user_timestamp_size_;
              num_selective_deletes++;
              // Skip rest of the for-loop (j=0, j<entries_per_batch_,j++).
              skip_for_loop = true;
            } else {
              assert(false);  // should never reach this point.
            }
            // If disposable_entries_q needs to be updated (ie: when a selective
            // insert+delete was successfully completed, pop the job out of the
            // queue).
            if (!disposable_entries_q[id].empty() &&
                (disposable_entries_q[id].front().first <
                 FLAGS_env->NowMicros()) &&
                persistent_ent_and_del_index[id] == kNumDispAndPersEntries) {
              disposable_entries_q[id].pop();
              persistent_ent_and_del_index[id] = 0;
            }

            // If we are deleting disposable entries, skip the rest of the
            // for-loop since there is no key-value inserts at this moment in
            // time.
            if (skip_for_loop) {
              continue;
            }

          }
          // If no job is in the queue, then we keep inserting disposable KV
          // entries that will be deleted later by a series of deletes.
          else {
            rand_num = key_gens[id]->Fetch(disposable_entries_index[id]);
            disposable_entries_index[id]++;
            is_disposable_entry = true;
            if ((disposable_entries_index[id] %
                 FLAGS_disposable_entries_batch_size) == 0) {
              // Skip the persistent KV entries inserts for now
              disposable_entries_index[id] +=
                  FLAGS_persistent_entries_batch_size;
            }
          }
        } else {
          rand_num = key_gens[id]->Next();
        }
        GenerateKeyFromInt(rand_num, FLAGS_num, &key);
        Slice val;
        if (kNumDispAndPersEntries > 0) {
          random_value = rnd_disposable_entry.RandomString(
              is_disposable_entry ? FLAGS_disposable_entries_value_size
                                  : FLAGS_persistent_entries_value_size);
          val = Slice(random_value);
          num_unique_keys++;
        } else {
          val = gen.Generate();
        }
        if (use_blob_db_) {
          // Stacked BlobDB
          blob_db::BlobDB* blobdb =
              static_cast<blob_db::BlobDB*>(db_with_cfh->db);
          if (FLAGS_blob_db_max_ttl_range > 0) {
            int ttl = rand() % FLAGS_blob_db_max_ttl_range;
            s = blobdb->PutWithTTL(write_options_, key, val, ttl);
          } else {
            s = blobdb->Put(write_options_, key, val);
          }
        } else if (FLAGS_num_column_families <= 1) {
          batch.Put(key, val);
        } else {
          // We use same rand_num as seed for key and column family so that we
          // can deterministically find the cfh corresponding to a particular
          // key while reading the key.
          batch.Put(db_with_cfh->GetCfh(rand_num), key, val);
        }
        batch_bytes += val.size() + key_size_ + user_timestamp_size_;
        bytes += val.size() + key_size_ + user_timestamp_size_;
        ++num_written;

        // If all disposable entries have been inserted, then we need to
        // add in the job queue a call for 'persistent entry insertions +
        // disposable entry deletions'.
        if (kNumDispAndPersEntries > 0 && is_disposable_entry &&
            ((disposable_entries_index[id] % kNumDispAndPersEntries) == 0)) {
          // Queue contains [timestamp, starting_idx],
          // timestamp = current_time + delay (minimum aboslute time when to
          // start inserting the selective deletes) starting_idx = index in the
          // keygen of the rand_num to generate the key of the first KV entry to
          // delete (= key of the first selective delete).
          disposable_entries_q[id].push(std::make_pair(
              FLAGS_env->NowMicros() +
                  FLAGS_disposable_entries_delete_delay /* timestamp */,
              disposable_entries_index[id] - kNumDispAndPersEntries
              /*starting idx*/));
        }
        if (writes_per_range_tombstone_ > 0 &&
            num_written > writes_before_delete_range_ &&
            (num_written - writes_before_delete_range_) /
                    writes_per_range_tombstone_ <=
                max_num_range_tombstones_ &&
            (num_written - writes_before_delete_range_) %
                    writes_per_range_tombstone_ ==
                0) {
          num_range_deletions++;
          int64_t begin_num = key_gens[id]->Next();
          if (FLAGS_expand_range_tombstones) {
            for (int64_t offset = 0; offset < range_tombstone_width_;
                 ++offset) {
              GenerateKeyFromInt(begin_num + offset, FLAGS_num,
                                 &expanded_keys[offset]);
              if (use_blob_db_) {
                // Stacked BlobDB
                s = db_with_cfh->db->Delete(write_options_,
                                            expanded_keys[offset]);
              } else if (FLAGS_num_column_families <= 1) {
                batch.Delete(expanded_keys[offset]);
              } else {
                batch.Delete(db_with_cfh->GetCfh(rand_num),
                             expanded_keys[offset]);
              }
            }
          } else {
            GenerateKeyFromInt(begin_num, FLAGS_num, &begin_key);
            GenerateKeyFromInt(begin_num + range_tombstone_width_, FLAGS_num,
                               &end_key);
            if (use_blob_db_) {
              // Stacked BlobDB
              s = db_with_cfh->db->DeleteRange(
                  write_options_, db_with_cfh->db->DefaultColumnFamily(),
                  begin_key, end_key);
            } else if (FLAGS_num_column_families <= 1) {
              batch.DeleteRange(begin_key, end_key);
            } else {
              batch.DeleteRange(db_with_cfh->GetCfh(rand_num), begin_key,
                                end_key);
            }
          }
        }
      }
      if (thread->shared->write_rate_limiter.get() != nullptr) {
        thread->shared->write_rate_limiter->Request(
            batch_bytes, Env::IO_HIGH, nullptr /* stats */,
            RateLimiter::OpType::kWrite);
        // Set time at which last op finished to Now() to hide latency and
        // sleep from rate limiter. Also, do the check once per batch, not
        // once per write.
        thread->stats.ResetLastOpTime();
      }
      if (user_timestamp_size_ > 0) {
        Slice user_ts = mock_app_clock_->Allocate(ts_guard.get());
        s = batch.UpdateTimestamps(
            user_ts, [this](uint32_t) { return user_timestamp_size_; });
        if (!s.ok()) {
          fprintf(stderr, "assign timestamp to write batch: %s\n",
                  s.ToString().c_str());
          ErrorExit();
        }
      }
      if (!use_blob_db_) {
        // Not stacked BlobDB
        s = db_with_cfh->db->Write(write_options_, &batch);
      }
      thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db,
                                entries_per_batch_, kWrite);
      if (FLAGS_sine_write_rate) {
        uint64_t now = FLAGS_env->NowMicros();

        uint64_t usecs_since_last;
        if (now > thread->stats.GetSineInterval()) {
          usecs_since_last = now - thread->stats.GetSineInterval();
        } else {
          usecs_since_last = 0;
        }

        if (usecs_since_last >
            (FLAGS_sine_write_rate_interval_milliseconds * uint64_t{1000})) {
          double usecs_since_start =
              static_cast<double>(now - thread->stats.GetStart());
          thread->stats.ResetSineInterval();
          uint64_t write_rate =
              static_cast<uint64_t>(SineRate(usecs_since_start / 1000000.0));
          thread->shared->write_rate_limiter.reset(
              NewGenericRateLimiter(write_rate));
        }
      }
      if (!s.ok()) {
        s = listener_->WaitForRecovery(600000000) ? Status::OK() : s;
      }

      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        ErrorExit();
      }
    }
    if ((write_mode == UNIQUE_RANDOM) && (p > 0.0)) {
      fprintf(stdout,
              "Number of unique keys inserted: %" PRIu64
              ".\nNumber of overwrites: %" PRIu64 "\n",
              num_unique_keys, num_overwrites);
    } else if (kNumDispAndPersEntries > 0) {
      fprintf(stdout,
              "Number of unique keys inserted (disposable+persistent): %" PRIu64
              ".\nNumber of 'disposable entry delete': %" PRIu64 "\n",
              num_written, num_selective_deletes);
    }
    if (num_range_deletions > 0) {
      std::cout << "Number of range deletions: " << num_range_deletions
                << std::endl;
    }
    thread->stats.AddBytes(bytes);
  }

  Status DoDeterministicCompact(ThreadState* thread,
                                CompactionStyle compaction_style,
                                WriteMode write_mode) {
    ColumnFamilyMetaData meta;
    std::vector<DB*> db_list;
    if (db_.db != nullptr) {
      db_list.push_back(db_.db);
    } else {
      for (auto& db : multi_dbs_) {
        db_list.push_back(db.db);
      }
    }
    std::vector<Options> options_list;
    for (auto db : db_list) {
      options_list.push_back(db->GetOptions());
      if (compaction_style != kCompactionStyleFIFO) {
        db->SetOptions({{"disable_auto_compactions", "1"},
                        {"level0_slowdown_writes_trigger", "400000000"},
                        {"level0_stop_writes_trigger", "400000000"}});
      } else {
        db->SetOptions({{"disable_auto_compactions", "1"}});
      }
    }

    assert(!db_list.empty());
    auto num_db = db_list.size();
    size_t num_levels = static_cast<size_t>(open_options_.num_levels);
    size_t output_level = open_options_.num_levels - 1;
    std::vector<std::vector<std::vector<SstFileMetaData>>> sorted_runs(num_db);
    std::vector<size_t> num_files_at_level0(num_db, 0);
    if (compaction_style == kCompactionStyleLevel) {
      if (num_levels == 0) {
        return Status::InvalidArgument("num_levels should be larger than 1");
      }
      bool should_stop = false;
      while (!should_stop) {
        if (sorted_runs[0].empty()) {
          DoWrite(thread, write_mode);
        } else {
          DoWrite(thread, UNIQUE_RANDOM);
        }
        for (size_t i = 0; i < num_db; i++) {
          auto db = db_list[i];
          db->Flush(FlushOptions());
          db->GetColumnFamilyMetaData(&meta);
          if (num_files_at_level0[i] == meta.levels[0].files.size() ||
              writes_ == 0) {
            should_stop = true;
            continue;
          }
          sorted_runs[i].emplace_back(
              meta.levels[0].files.begin(),
              meta.levels[0].files.end() - num_files_at_level0[i]);
          num_files_at_level0[i] = meta.levels[0].files.size();
          if (sorted_runs[i].back().size() == 1) {
            should_stop = true;
            continue;
          }
          if (sorted_runs[i].size() == output_level) {
            auto& L1 = sorted_runs[i].back();
            L1.erase(L1.begin(), L1.begin() + L1.size() / 3);
            should_stop = true;
            continue;
          }
        }
        writes_ /=
            static_cast<int64_t>(open_options_.max_bytes_for_level_multiplier);
      }
      for (size_t i = 0; i < num_db; i++) {
        if (sorted_runs[i].size() < num_levels - 1) {
          fprintf(stderr, "n is too small to fill %" ROCKSDB_PRIszt " levels\n",
                  num_levels);
          exit(1);
        }
      }
      for (size_t i = 0; i < num_db; i++) {
        auto db = db_list[i];
        auto compactionOptions = CompactionOptions();
        compactionOptions.compression = FLAGS_compression_type_e;
        auto options = db->GetOptions();
        MutableCFOptions mutable_cf_options(options);
        for (size_t j = 0; j < sorted_runs[i].size(); j++) {
          compactionOptions.output_file_size_limit = MaxFileSizeForLevel(
              mutable_cf_options, static_cast<int>(output_level),
              compaction_style);
          std::cout << sorted_runs[i][j].size() << std::endl;
          db->CompactFiles(
              compactionOptions,
              {sorted_runs[i][j].back().name, sorted_runs[i][j].front().name},
              static_cast<int>(output_level - j) /*level*/);
        }
      }
    } else if (compaction_style == kCompactionStyleUniversal) {
      auto ratio = open_options_.compaction_options_universal.size_ratio;
      bool should_stop = false;
      while (!should_stop) {
        if (sorted_runs[0].empty()) {
          DoWrite(thread, write_mode);
        } else {
          DoWrite(thread, UNIQUE_RANDOM);
        }
        for (size_t i = 0; i < num_db; i++) {
          auto db = db_list[i];
          db->Flush(FlushOptions());
          db->GetColumnFamilyMetaData(&meta);
          if (num_files_at_level0[i] == meta.levels[0].files.size() ||
              writes_ == 0) {
            should_stop = true;
            continue;
          }
          sorted_runs[i].emplace_back(
              meta.levels[0].files.begin(),
              meta.levels[0].files.end() - num_files_at_level0[i]);
          num_files_at_level0[i] = meta.levels[0].files.size();
          if (sorted_runs[i].back().size() == 1) {
            should_stop = true;
            continue;
          }
          num_files_at_level0[i] = meta.levels[0].files.size();
        }
        writes_ = static_cast<int64_t>(writes_ * static_cast<double>(100) /
                                       (ratio + 200));
      }
      for (size_t i = 0; i < num_db; i++) {
        if (sorted_runs[i].size() < num_levels) {
          fprintf(stderr, "n is too small to fill %" ROCKSDB_PRIszt " levels\n",
                  num_levels);
          exit(1);
        }
      }
      for (size_t i = 0; i < num_db; i++) {
        auto db = db_list[i];
        auto compactionOptions = CompactionOptions();
        compactionOptions.compression = FLAGS_compression_type_e;
        auto options = db->GetOptions();
        MutableCFOptions mutable_cf_options(options);
        for (size_t j = 0; j < sorted_runs[i].size(); j++) {
          compactionOptions.output_file_size_limit = MaxFileSizeForLevel(
              mutable_cf_options, static_cast<int>(output_level),
              compaction_style);
          db->CompactFiles(
              compactionOptions,
              {sorted_runs[i][j].back().name, sorted_runs[i][j].front().name},
              (output_level > j ? static_cast<int>(output_level - j)
                                : 0) /*level*/);
        }
      }
    } else if (compaction_style == kCompactionStyleFIFO) {
      if (num_levels != 1) {
        return Status::InvalidArgument(
            "num_levels should be 1 for FIFO compaction");
      }
      if (FLAGS_num_multi_db != 0) {
        return Status::InvalidArgument("Doesn't support multiDB");
      }
      auto db = db_list[0];
      std::vector<std::string> file_names;
      while (true) {
        if (sorted_runs[0].empty()) {
          DoWrite(thread, write_mode);
        } else {
          DoWrite(thread, UNIQUE_RANDOM);
        }
        db->Flush(FlushOptions());
        db->GetColumnFamilyMetaData(&meta);
        auto total_size = meta.levels[0].size;
        if (total_size >=
            db->GetOptions().compaction_options_fifo.max_table_files_size) {
          for (const auto& file_meta : meta.levels[0].files) {
            file_names.emplace_back(file_meta.name);
          }
          break;
        }
      }
      // TODO(shuzhang1989): Investigate why CompactFiles not working
      // auto compactionOptions = CompactionOptions();
      // db->CompactFiles(compactionOptions, file_names, 0);
      auto compactionOptions = CompactRangeOptions();
      compactionOptions.max_subcompactions =
          static_cast<uint32_t>(FLAGS_subcompactions);
      db->CompactRange(compactionOptions, nullptr, nullptr);
    } else {
      fprintf(stdout,
              "%-12s : skipped (-compaction_stype=kCompactionStyleNone)\n",
              "filldeterministic");
      return Status::InvalidArgument("None compaction is not supported");
    }

// Verify seqno and key range
// Note: the seqno get changed at the max level by implementation
// optimization, so skip the check of the max level.
#ifndef NDEBUG
    for (size_t k = 0; k < num_db; k++) {
      auto db = db_list[k];
      db->GetColumnFamilyMetaData(&meta);
      // verify the number of sorted runs
      if (compaction_style == kCompactionStyleLevel) {
        assert(num_levels - 1 == sorted_runs[k].size());
      } else if (compaction_style == kCompactionStyleUniversal) {
        assert(meta.levels[0].files.size() + num_levels - 1 ==
               sorted_runs[k].size());
      } else if (compaction_style == kCompactionStyleFIFO) {
        // TODO(gzh): FIFO compaction
        db->GetColumnFamilyMetaData(&meta);
        auto total_size = meta.levels[0].size;
        assert(total_size <=
               db->GetOptions().compaction_options_fifo.max_table_files_size);
        break;
      }

      // verify smallest/largest seqno and key range of each sorted run
      auto max_level = num_levels - 1;
      int level;
      for (size_t i = 0; i < sorted_runs[k].size(); i++) {
        level = static_cast<int>(max_level - i);
        SequenceNumber sorted_run_smallest_seqno = kMaxSequenceNumber;
        SequenceNumber sorted_run_largest_seqno = 0;
        std::string sorted_run_smallest_key, sorted_run_largest_key;
        bool first_key = true;
        for (const auto& fileMeta : sorted_runs[k][i]) {
          sorted_run_smallest_seqno =
              std::min(sorted_run_smallest_seqno, fileMeta.smallest_seqno);
          sorted_run_largest_seqno =
              std::max(sorted_run_largest_seqno, fileMeta.largest_seqno);
          if (first_key ||
              db->DefaultColumnFamily()->GetComparator()->Compare(
                  fileMeta.smallestkey, sorted_run_smallest_key) < 0) {
            sorted_run_smallest_key = fileMeta.smallestkey;
          }
          if (first_key ||
              db->DefaultColumnFamily()->GetComparator()->Compare(
                  fileMeta.largestkey, sorted_run_largest_key) > 0) {
            sorted_run_largest_key = fileMeta.largestkey;
          }
          first_key = false;
        }
        if (compaction_style == kCompactionStyleLevel ||
            (compaction_style == kCompactionStyleUniversal && level > 0)) {
          SequenceNumber level_smallest_seqno = kMaxSequenceNumber;
          SequenceNumber level_largest_seqno = 0;
          for (const auto& fileMeta : meta.levels[level].files) {
            level_smallest_seqno =
                std::min(level_smallest_seqno, fileMeta.smallest_seqno);
            level_largest_seqno =
                std::max(level_largest_seqno, fileMeta.largest_seqno);
          }
          assert(sorted_run_smallest_key ==
                 meta.levels[level].files.front().smallestkey);
          assert(sorted_run_largest_key ==
                 meta.levels[level].files.back().largestkey);
          if (level != static_cast<int>(max_level)) {
            // compaction at max_level would change sequence number
            assert(sorted_run_smallest_seqno == level_smallest_seqno);
            assert(sorted_run_largest_seqno == level_largest_seqno);
          }
        } else if (compaction_style == kCompactionStyleUniversal) {
          // level <= 0 means sorted runs on level 0
          auto level0_file =
              meta.levels[0].files[sorted_runs[k].size() - 1 - i];
          assert(sorted_run_smallest_key == level0_file.smallestkey);
          assert(sorted_run_largest_key == level0_file.largestkey);
          if (level != static_cast<int>(max_level)) {
            assert(sorted_run_smallest_seqno == level0_file.smallest_seqno);
            assert(sorted_run_largest_seqno == level0_file.largest_seqno);
          }
        }
      }
    }
#endif
    // print the size of each sorted_run
    for (size_t k = 0; k < num_db; k++) {
      auto db = db_list[k];
      fprintf(stdout,
              "---------------------- DB %" ROCKSDB_PRIszt
              " LSM ---------------------\n",
              k);
      db->GetColumnFamilyMetaData(&meta);
      for (auto& levelMeta : meta.levels) {
        if (levelMeta.files.empty()) {
          continue;
        }
        if (levelMeta.level == 0) {
          for (auto& fileMeta : levelMeta.files) {
            fprintf(stdout, "Level[%d]: %s(size: %" PRIi64 " bytes)\n",
                    levelMeta.level, fileMeta.name.c_str(), fileMeta.size);
          }
        } else {
          fprintf(stdout, "Level[%d]: %s - %s(total size: %" PRIi64 " bytes)\n",
                  levelMeta.level, levelMeta.files.front().name.c_str(),
                  levelMeta.files.back().name.c_str(), levelMeta.size);
        }
      }
    }
    for (size_t i = 0; i < num_db; i++) {
      db_list[i]->SetOptions(
          {{"disable_auto_compactions",
            std::to_string(options_list[i].disable_auto_compactions)},
           {"level0_slowdown_writes_trigger",
            std::to_string(options_list[i].level0_slowdown_writes_trigger)},
           {"level0_stop_writes_trigger",
            std::to_string(options_list[i].level0_stop_writes_trigger)}});
    }
    return Status::OK();
  }

  void ReadSequential(ThreadState* thread) {
    if (db_.db != nullptr) {
      ReadSequential(thread, db_.db);
    } else {
      for (const auto& db_with_cfh : multi_dbs_) {
        ReadSequential(thread, db_with_cfh.db);
      }
    }
  }

  void ReadSequential(ThreadState* thread, DB* db) {
    ReadOptions options = read_options_;
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
      options.timestamp = &ts;
    }

    options.adaptive_readahead = FLAGS_adaptive_readahead;
    options.async_io = FLAGS_async_io;
    options.auto_readahead_size = FLAGS_auto_readahead_size;

    Iterator* iter = db->NewIterator(options);
    int64_t i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedOps(nullptr, db, 1, kRead);
      ++i;

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          i % 1024 == 1023) {
        thread->shared->read_rate_limiter->Request(1024, Env::IO_HIGH,
                                                   nullptr /* stats */,
                                                   RateLimiter::OpType::kRead);
      }
    }

    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadToRowCache(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    int64_t key_rand = 0;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;

    while (key_rand < FLAGS_num) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      // We use same key_rand as seed for key and column family so that we can
      // deterministically find the cfh corresponding to a particular key, as it
      // is done in DoWrite method.
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      key_rand++;
      read++;
      Status s;
      if (FLAGS_num_column_families > 1) {
        s = db_with_cfh->db->Get(read_options_, db_with_cfh->GetCfh(key_rand),
                                 key, &pinnable_val);
      } else {
        pinnable_val.Reset();
        s = db_with_cfh->db->Get(read_options_,
                                 db_with_cfh->db->DefaultColumnFamily(), key,
                                 &pinnable_val);
      }

      if (s.ok()) {
        found++;
        bytes += key.size() + pinnable_val.size();
      } else if (!s.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
        abort();
      }

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          read % 256 == 255) {
        thread->shared->read_rate_limiter->Request(
            256, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n", found,
             read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  void ReadReverse(ThreadState* thread) {
    if (db_.db != nullptr) {
      ReadReverse(thread, db_.db);
    } else {
      for (const auto& db_with_cfh : multi_dbs_) {
        ReadReverse(thread, db_with_cfh.db);
      }
    }
  }

  void ReadReverse(ThreadState* thread, DB* db) {
    Iterator* iter = db->NewIterator(read_options_);
    int64_t i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedOps(nullptr, db, 1, kRead);
      ++i;
      if (thread->shared->read_rate_limiter.get() != nullptr &&
          i % 1024 == 1023) {
        thread->shared->read_rate_limiter->Request(1024, Env::IO_HIGH,
                                                   nullptr /* stats */,
                                                   RateLimiter::OpType::kRead);
      }
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandomFast(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t nonexist = 0;
    ReadOptions options = read_options_;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::string value;
    Slice ts;
    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }
    DB* db = SelectDBWithCfh(thread)->db;

    int64_t pot = 1;
    while (pot < FLAGS_num) {
      pot <<= 1;
    }

    Duration duration(FLAGS_duration, reads_);
    do {
      for (int i = 0; i < 100; ++i) {
        int64_t key_rand = thread->rand.Next() & (pot - 1);
        GenerateKeyFromInt(key_rand, FLAGS_num, &key);
        ++read;
        std::string ts_ret;
        std::string* ts_ptr = nullptr;
        if (user_timestamp_size_ > 0) {
          ts = mock_app_clock_->GetTimestampForRead(thread->rand,
                                                    ts_guard.get());
          options.timestamp = &ts;
          ts_ptr = &ts_ret;
        }
        auto status = db->Get(options, key, &value, ts_ptr);
        if (status.ok()) {
          ++found;
        } else if (!status.IsNotFound()) {
          fprintf(stderr, "Get returned an error: %s\n",
                  status.ToString().c_str());
          abort();
        }
        if (key_rand >= FLAGS_num) {
          ++nonexist;
        }
      }
      if (thread->shared->read_rate_limiter.get() != nullptr) {
        thread->shared->read_rate_limiter->Request(
            100, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(nullptr, db, 100, kRead);
    } while (!duration.Done(100));

    char msg[100];
    snprintf(msg, sizeof(msg),
             "(%" PRIu64 " of %" PRIu64
             " found, "
             "issued %" PRIu64 " non-exist keys)\n",
             found, read, nonexist);

    thread->stats.AddMessage(msg);
  }

  int64_t GetRandomKey(Random64* rand) {
    uint64_t rand_int = rand->Next();
    int64_t key_rand;
    if (read_random_exp_range_ == 0) {
      key_rand = rand_int % FLAGS_num;
    } else {
      const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
      long double order = -static_cast<long double>(rand_int % kBigInt) /
                          static_cast<long double>(kBigInt) *
                          read_random_exp_range_;
      long double exp_ran = std::exp(order);
      uint64_t rand_num =
          static_cast<int64_t>(exp_ran * static_cast<long double>(FLAGS_num));
      // Map to a different number to avoid locality.
      const uint64_t kBigPrime = 0x5bd1e995;
      // Overflow is like %(2^64). Will have little impact of results.
      key_rand = static_cast<int64_t>((rand_num * kBigPrime) % FLAGS_num);
    }
    return key_rand;
  }

  void ReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    int num_keys = 0;
    int64_t key_rand = 0;
    ReadOptions options = read_options_;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;
    std::vector<PinnableSlice> pinnable_vals;
    if (read_operands_) {
      // Start off with a small-ish value that'll be increased later if
      // `GetMergeOperands()` tells us it is not large enough.
      pinnable_vals.resize(8);
    }
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      // We use same key_rand as seed for key and column family so that we can
      // deterministically find the cfh corresponding to a particular key, as it
      // is done in DoWrite method.
      if (entries_per_batch_ > 1 && FLAGS_multiread_stride) {
        if (++num_keys == entries_per_batch_) {
          num_keys = 0;
          key_rand = GetRandomKey(&thread->rand);
          if ((key_rand + (entries_per_batch_ - 1) * FLAGS_multiread_stride) >=
              FLAGS_num) {
            key_rand = FLAGS_num - entries_per_batch_ * FLAGS_multiread_stride;
          }
        } else {
          key_rand += FLAGS_multiread_stride;
        }
      } else {
        key_rand = GetRandomKey(&thread->rand);
      }
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      read++;
      std::string ts_ret;
      std::string* ts_ptr = nullptr;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
        options.timestamp = &ts;
        ts_ptr = &ts_ret;
      }
      Status s;
      pinnable_val.Reset();
      for (size_t i = 0; i < pinnable_vals.size(); ++i) {
        pinnable_vals[i].Reset();
      }
      ColumnFamilyHandle* cfh;
      if (FLAGS_num_column_families > 1) {
        cfh = db_with_cfh->GetCfh(key_rand);
      } else {
        cfh = db_with_cfh->db->DefaultColumnFamily();
      }
      if (read_operands_) {
        GetMergeOperandsOptions get_merge_operands_options;
        get_merge_operands_options.expected_max_number_of_operands =
            static_cast<int>(pinnable_vals.size());
        int number_of_operands;
        s = db_with_cfh->db->GetMergeOperands(
            options, cfh, key, pinnable_vals.data(),
            &get_merge_operands_options, &number_of_operands);
        if (s.IsIncomplete()) {
          // Should only happen a few times when we encounter a key that had
          // more merge operands than any key seen so far. Production use case
          // would typically retry in such event to get all the operands so do
          // that here.
          pinnable_vals.resize(number_of_operands);
          get_merge_operands_options.expected_max_number_of_operands =
              static_cast<int>(pinnable_vals.size());
          s = db_with_cfh->db->GetMergeOperands(
              options, cfh, key, pinnable_vals.data(),
              &get_merge_operands_options, &number_of_operands);
        }
      } else {
        s = db_with_cfh->db->Get(options, cfh, key, &pinnable_val, ts_ptr);
      }

      if (s.ok()) {
        found++;
        bytes += key.size() + pinnable_val.size() + user_timestamp_size_;
        for (size_t i = 0; i < pinnable_vals.size(); ++i) {
          bytes += pinnable_vals[i].size();
          pinnable_vals[i].Reset();
        }
      } else if (!s.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
        abort();
      }

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          read % 256 == 255) {
        thread->shared->read_rate_limiter->Request(
            256, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n", found,
             read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Calls MultiGet over a list of keys from a random distribution.
  // Returns the total number of keys found.
  void MultiReadRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t bytes = 0;
    int64_t num_multireads = 0;
    int64_t found = 0;
    ReadOptions options = read_options_;
    std::vector<Slice> keys;
    std::vector<std::unique_ptr<const char[]>> key_guards;
    std::vector<std::string> values(entries_per_batch_);
    PinnableSlice* pin_values = new PinnableSlice[entries_per_batch_];
    std::unique_ptr<PinnableSlice[]> pin_values_guard(pin_values);
    std::vector<Status> stat_list(entries_per_batch_);
    while (static_cast<int64_t>(keys.size()) < entries_per_batch_) {
      key_guards.push_back(std::unique_ptr<const char[]>());
      keys.push_back(AllocateKey(&key_guards.back()));
    }

    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(entries_per_batch_)) {
      DB* db = SelectDB(thread);
      if (FLAGS_multiread_stride) {
        int64_t key = GetRandomKey(&thread->rand);
        if ((key + (entries_per_batch_ - 1) * FLAGS_multiread_stride) >=
            static_cast<int64_t>(FLAGS_num)) {
          key = FLAGS_num - entries_per_batch_ * FLAGS_multiread_stride;
        }
        for (int64_t i = 0; i < entries_per_batch_; ++i) {
          GenerateKeyFromInt(key, FLAGS_num, &keys[i]);
          key += FLAGS_multiread_stride;
        }
      } else {
        for (int64_t i = 0; i < entries_per_batch_; ++i) {
          GenerateKeyFromInt(GetRandomKey(&thread->rand), FLAGS_num, &keys[i]);
        }
      }
      Slice ts;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
        options.timestamp = &ts;
      }
      if (!FLAGS_multiread_batched) {
        std::vector<Status> statuses = db->MultiGet(options, keys, &values);
        assert(static_cast<int64_t>(statuses.size()) == entries_per_batch_);

        read += entries_per_batch_;
        num_multireads++;
        for (int64_t i = 0; i < entries_per_batch_; ++i) {
          if (statuses[i].ok()) {
            bytes += keys[i].size() + values[i].size() + user_timestamp_size_;
            ++found;
          } else if (!statuses[i].IsNotFound()) {
            fprintf(stderr, "MultiGet returned an error: %s\n",
                    statuses[i].ToString().c_str());
            abort();
          }
        }
      } else {
        db->MultiGet(options, db->DefaultColumnFamily(), keys.size(),
                     keys.data(), pin_values, stat_list.data());

        read += entries_per_batch_;
        num_multireads++;
        for (int64_t i = 0; i < entries_per_batch_; ++i) {
          if (stat_list[i].ok()) {
            bytes +=
                keys[i].size() + pin_values[i].size() + user_timestamp_size_;
            ++found;
          } else if (!stat_list[i].IsNotFound()) {
            fprintf(stderr, "MultiGet returned an error: %s\n",
                    stat_list[i].ToString().c_str());
            abort();
          }
          stat_list[i] = Status::OK();
          pin_values[i].Reset();
        }
      }
      if (thread->shared->read_rate_limiter.get() != nullptr &&
          num_multireads % 256 == 255) {
        thread->shared->read_rate_limiter->Request(
            256 * entries_per_batch_, Env::IO_HIGH, nullptr /* stats */,
            RateLimiter::OpType::kRead);
      }
      thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kRead);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found,
             read);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  void ApproximateMemtableStats(ThreadState* thread) {
    const size_t batch_size = entries_per_batch_;
    std::unique_ptr<const char[]> skey_guard;
    Slice skey = AllocateKey(&skey_guard);
    std::unique_ptr<const char[]> ekey_guard;
    Slice ekey = AllocateKey(&ekey_guard);
    Duration duration(FLAGS_duration, reads_);
    if (FLAGS_num < static_cast<int64_t>(batch_size)) {
      std::terminate();
    }
    uint64_t range = static_cast<uint64_t>(FLAGS_num) - batch_size;
    auto count_hist = std::make_shared<HistogramImpl>();
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      uint64_t start_key = thread->rand.Uniform(range);
      GenerateKeyFromInt(start_key, FLAGS_num, &skey);
      uint64_t end_key = start_key + batch_size;
      GenerateKeyFromInt(end_key, FLAGS_num, &ekey);
      uint64_t count = UINT64_MAX;
      uint64_t size = UINT64_MAX;
      db->GetApproximateMemTableStats({skey, ekey}, &count, &size);
      count_hist->Add(count);
      thread->stats.FinishedOps(nullptr, db, 1, kOthers);
    }
    thread->stats.AddMessage("\nReported entry count stats (expected " +
                             std::to_string(batch_size) + "):");
    thread->stats.AddMessage("\n" + count_hist->ToString());
  }

  // Calls ApproximateSize over random key ranges.
  void ApproximateSizeRandom(ThreadState* thread) {
    int64_t size_sum = 0;
    int64_t num_sizes = 0;
    const size_t batch_size = entries_per_batch_;
    std::vector<Range> ranges;
    std::vector<Slice> lkeys;
    std::vector<std::unique_ptr<const char[]>> lkey_guards;
    std::vector<Slice> rkeys;
    std::vector<std::unique_ptr<const char[]>> rkey_guards;
    std::vector<uint64_t> sizes;
    while (ranges.size() < batch_size) {
      // Ugly without C++17 return from emplace_back
      lkey_guards.emplace_back();
      rkey_guards.emplace_back();
      lkeys.emplace_back(AllocateKey(&lkey_guards.back()));
      rkeys.emplace_back(AllocateKey(&rkey_guards.back()));
      ranges.emplace_back(lkeys.back(), rkeys.back());
      sizes.push_back(0);
    }
    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      for (size_t i = 0; i < batch_size; ++i) {
        int64_t lkey = GetRandomKey(&thread->rand);
        int64_t rkey = GetRandomKey(&thread->rand);
        if (lkey > rkey) {
          std::swap(lkey, rkey);
        }
        GenerateKeyFromInt(lkey, FLAGS_num, &lkeys[i]);
        GenerateKeyFromInt(rkey, FLAGS_num, &rkeys[i]);
      }
      db->GetApproximateSizes(
          ranges.data(), static_cast<int>(entries_per_batch_), sizes.data());
      num_sizes += entries_per_batch_;
      for (int64_t size : sizes) {
        size_sum += size;
      }
      thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kOthers);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(Avg approx size=%g)",
             static_cast<double>(size_sum) / static_cast<double>(num_sizes));
    thread->stats.AddMessage(msg);
  }

  // The inverse function of Pareto distribution
  int64_t ParetoCdfInversion(double u, double theta, double k, double sigma) {
    double ret;
    if (k == 0.0) {
      ret = theta - sigma * std::log(u);
    } else {
      ret = theta + sigma * (std::pow(u, -1 * k) - 1) / k;
    }
    return static_cast<int64_t>(ceil(ret));
  }
  // The inverse function of power distribution (y=ax^b)
  int64_t PowerCdfInversion(double u, double a, double b) {
    double ret;
    ret = std::pow((u / a), (1 / b));
    return static_cast<int64_t>(ceil(ret));
  }

  // Add the noice to the QPS
  double AddNoise(double origin, double noise_ratio) {
    if (noise_ratio < 0.0 || noise_ratio > 1.0) {
      return origin;
    }
    int band_int = static_cast<int>(FLAGS_sine_a);
    double delta = (rand() % band_int - band_int / 2) * noise_ratio;
    if (origin + delta < 0) {
      return origin;
    } else {
      return (origin + delta);
    }
  }

  // Decide the ratio of different query types
  // 0 Get, 1 Put, 2 Seek, 3 SeekForPrev, 4 Delete, 5 SingleDelete, 6 merge
  class QueryDecider {
   public:
    std::vector<int> type_;
    std::vector<double> ratio_;
    int range_;

    QueryDecider() = default;
    ~QueryDecider() = default;

    Status Initiate(std::vector<double> ratio_input) {
      int range_max = 1000;
      double sum = 0.0;
      for (auto& ratio : ratio_input) {
        sum += ratio;
      }
      range_ = 0;
      for (auto& ratio : ratio_input) {
        range_ += static_cast<int>(ceil(range_max * (ratio / sum)));
        type_.push_back(range_);
        ratio_.push_back(ratio / sum);
      }
      return Status::OK();
    }

    int GetType(int64_t rand_num) {
      if (rand_num < 0) {
        rand_num = rand_num * (-1);
      }
      assert(range_ != 0);
      int pos = static_cast<int>(rand_num % range_);
      for (int i = 0; i < static_cast<int>(type_.size()); i++) {
        if (pos < type_[i]) {
          return i;
        }
      }
      return 0;
    }
  };

  // KeyrangeUnit is the struct of a keyrange. It is used in a keyrange vector
  // to transfer a random value to one keyrange based on the hotness.
  struct KeyrangeUnit {
    int64_t keyrange_start;
    int64_t keyrange_access;
    int64_t keyrange_keys;
  };

  // From our observations, the prefix hotness (key-range hotness) follows
  // the two-term-exponential distribution: f(x) = a*exp(b*x) + c*exp(d*x).
  // However, we cannot directly use the inverse function to decide a
  // key-range from a random distribution. To achieve it, we create a list of
  // KeyrangeUnit, each KeyrangeUnit occupies a range of integers whose size is
  // decided based on the hotness of the key-range. When a random value is
  // generated based on uniform distribution, we map it to the KeyrangeUnit Vec
  // and one KeyrangeUnit is selected. The probability of a  KeyrangeUnit being
  // selected is the same as the hotness of this KeyrangeUnit. After that, the
  // key can be randomly allocated to the key-range of this KeyrangeUnit, or we
  // can based on the power distribution (y=ax^b) to generate the offset of
  // the key in the selected key-range. In this way, we generate the keyID
  // based on the hotness of the prefix and also the key hotness distribution.
  class GenerateTwoTermExpKeys {
   public:
    // Avoid uninitialized warning-as-error in some compilers
    int64_t keyrange_rand_max_ = 0;
    int64_t keyrange_size_ = 0;
    int64_t keyrange_num_ = 0;
    std::vector<KeyrangeUnit> keyrange_set_;

    // Initiate the KeyrangeUnit vector and calculate the size of each
    // KeyrangeUnit.
    Status InitiateExpDistribution(int64_t total_keys, double prefix_a,
                                   double prefix_b, double prefix_c,
                                   double prefix_d) {
      int64_t amplify = 0;
      int64_t keyrange_start = 0;
      if (FLAGS_keyrange_num <= 0) {
        keyrange_num_ = 1;
      } else {
        keyrange_num_ = FLAGS_keyrange_num;
      }
      keyrange_size_ = total_keys / keyrange_num_;

      // Calculate the key-range shares size based on the input parameters
      for (int64_t pfx = keyrange_num_; pfx >= 1; pfx--) {
        // Step 1. Calculate the probability that this key range will be
        // accessed in a query. It is based on the two-term expoential
        // distribution
        double keyrange_p = prefix_a * std::exp(prefix_b * pfx) +
                            prefix_c * std::exp(prefix_d * pfx);
        if (keyrange_p < std::pow(10.0, -16.0)) {
          keyrange_p = 0.0;
        }
        // Step 2. Calculate the amplify
        // In order to allocate a query to a key-range based on the random
        // number generated for this query, we need to extend the probability
        // of each key range from [0,1] to [0, amplify]. Amplify is calculated
        // by 1/(smallest key-range probability). In this way, we ensure that
        // all key-ranges are assigned with an Integer that  >=0
        if (amplify == 0 && keyrange_p > 0) {
          amplify = static_cast<int64_t>(std::floor(1 / keyrange_p)) + 1;
        }

        // Step 3. For each key-range, we calculate its position in the
        // [0, amplify] range, including the start, the size (keyrange_access)
        KeyrangeUnit p_unit;
        p_unit.keyrange_start = keyrange_start;
        if (0.0 >= keyrange_p) {
          p_unit.keyrange_access = 0;
        } else {
          p_unit.keyrange_access =
              static_cast<int64_t>(std::floor(amplify * keyrange_p));
        }
        p_unit.keyrange_keys = keyrange_size_;
        keyrange_set_.push_back(p_unit);
        keyrange_start += p_unit.keyrange_access;
      }
      keyrange_rand_max_ = keyrange_start;

      // Step 4. Shuffle the key-ranges randomly
      // Since the access probability is calculated from small to large,
      // If we do not re-allocate them, hot key-ranges are always at the end
      // and cold key-ranges are at the begin of the key space. Therefore, the
      // key-ranges are shuffled and the rand seed is only decide by the
      // key-range hotness distribution. With the same distribution parameters
      // the shuffle results are the same.
      Random64 rand_loca(keyrange_rand_max_);
      for (int64_t i = 0; i < FLAGS_keyrange_num; i++) {
        int64_t pos = rand_loca.Next() % FLAGS_keyrange_num;
        assert(i >= 0 && i < static_cast<int64_t>(keyrange_set_.size()) &&
               pos >= 0 && pos < static_cast<int64_t>(keyrange_set_.size()));
        std::swap(keyrange_set_[i], keyrange_set_[pos]);
      }

      // Step 5. Recalculate the prefix start postion after shuffling
      int64_t offset = 0;
      for (auto& p_unit : keyrange_set_) {
        p_unit.keyrange_start = offset;
        offset += p_unit.keyrange_access;
      }

      return Status::OK();
    }

    // Generate the Key ID according to the input ini_rand and key distribution
    int64_t DistGetKeyID(int64_t ini_rand, double key_dist_a,
                         double key_dist_b) {
      int64_t keyrange_rand = ini_rand % keyrange_rand_max_;

      // Calculate and select one key-range that contains the new key
      int64_t start = 0, end = static_cast<int64_t>(keyrange_set_.size());
      while (start + 1 < end) {
        int64_t mid = start + (end - start) / 2;
        assert(mid >= 0 && mid < static_cast<int64_t>(keyrange_set_.size()));
        if (keyrange_rand < keyrange_set_[mid].keyrange_start) {
          end = mid;
        } else {
          start = mid;
        }
      }
      int64_t keyrange_id = start;

      // Select one key in the key-range and compose the keyID
      int64_t key_offset = 0, key_seed;
      if (key_dist_a == 0.0 || key_dist_b == 0.0) {
        key_offset = ini_rand % keyrange_size_;
      } else {
        double u =
            static_cast<double>(ini_rand % keyrange_size_) / keyrange_size_;
        key_seed = static_cast<int64_t>(
            ceil(std::pow((u / key_dist_a), (1 / key_dist_b))));
        Random64 rand_key(key_seed);
        key_offset = rand_key.Next() % keyrange_size_;
      }
      return keyrange_size_ * keyrange_id + key_offset;
    }
  };

  // The social graph workload mixed with Get, Put, Iterator queries.
  // The value size and iterator length follow Pareto distribution.
  // The overall key access follow power distribution. If user models the
  // workload based on different key-ranges (or different prefixes), user
  // can use two-term-exponential distribution to fit the workload. User
  // needs to decide the ratio between Get, Put, Iterator queries before
  // starting the benchmark.
  void MixGraph(ThreadState* thread) {
    int64_t gets = 0;
    int64_t puts = 0;
    int64_t get_found = 0;
    int64_t seek = 0;
    int64_t seek_found = 0;
    int64_t bytes = 0;
    double total_scan_length = 0;
    double total_val_size = 0;
    const int64_t default_value_max = 1 * 1024 * 1024;
    int64_t value_max = default_value_max;
    int64_t scan_len_max = FLAGS_mix_max_scan_len;
    double write_rate = 1000000.0;
    double read_rate = 1000000.0;
    bool use_prefix_modeling = false;
    bool use_random_modeling = false;
    GenerateTwoTermExpKeys gen_exp;
    std::vector<double> ratio{FLAGS_mix_get_ratio, FLAGS_mix_put_ratio,
                              FLAGS_mix_seek_ratio};
    char value_buffer[default_value_max];
    QueryDecider query;
    RandomGenerator gen;
    Status s;
    if (value_max > FLAGS_mix_max_value_size) {
      value_max = FLAGS_mix_max_value_size;
    }

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    PinnableSlice pinnable_val;
    query.Initiate(ratio);

    // the limit of qps initiation
    if (FLAGS_sine_mix_rate) {
      thread->shared->read_rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(read_rate)));
      thread->shared->write_rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(write_rate)));
    }

    // Decide if user wants to use prefix based key generation
    if (FLAGS_keyrange_dist_a != 0.0 || FLAGS_keyrange_dist_b != 0.0 ||
        FLAGS_keyrange_dist_c != 0.0 || FLAGS_keyrange_dist_d != 0.0) {
      use_prefix_modeling = true;
      gen_exp.InitiateExpDistribution(
          FLAGS_num, FLAGS_keyrange_dist_a, FLAGS_keyrange_dist_b,
          FLAGS_keyrange_dist_c, FLAGS_keyrange_dist_d);
    }
    if (FLAGS_key_dist_a == 0 || FLAGS_key_dist_b == 0) {
      use_random_modeling = true;
    }

    Duration duration(FLAGS_duration, reads_);
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      int64_t ini_rand, rand_v, key_rand, key_seed;
      ini_rand = GetRandomKey(&thread->rand);
      rand_v = ini_rand % FLAGS_num;
      double u = static_cast<double>(rand_v) / FLAGS_num;

      // Generate the keyID based on the key hotness and prefix hotness
      if (use_random_modeling) {
        key_rand = ini_rand;
      } else if (use_prefix_modeling) {
        key_rand =
            gen_exp.DistGetKeyID(ini_rand, FLAGS_key_dist_a, FLAGS_key_dist_b);
      } else {
        key_seed = PowerCdfInversion(u, FLAGS_key_dist_a, FLAGS_key_dist_b);
        Random64 rand(key_seed);
        key_rand = static_cast<int64_t>(rand.Next()) % FLAGS_num;
      }
      GenerateKeyFromInt(key_rand, FLAGS_num, &key);
      int query_type = query.GetType(rand_v);

      // change the qps
      uint64_t now = FLAGS_env->NowMicros();
      uint64_t usecs_since_last;
      if (now > thread->stats.GetSineInterval()) {
        usecs_since_last = now - thread->stats.GetSineInterval();
      } else {
        usecs_since_last = 0;
      }

      if (FLAGS_sine_mix_rate &&
          usecs_since_last >
              (FLAGS_sine_mix_rate_interval_milliseconds * uint64_t{1000})) {
        double usecs_since_start =
            static_cast<double>(now - thread->stats.GetStart());
        thread->stats.ResetSineInterval();
        double mix_rate_with_noise = AddNoise(
            SineRate(usecs_since_start / 1000000.0), FLAGS_sine_mix_rate_noise);
        read_rate = mix_rate_with_noise * (query.ratio_[0] + query.ratio_[2]);
        write_rate = mix_rate_with_noise * query.ratio_[1];

        if (read_rate > 0) {
          thread->shared->read_rate_limiter->SetBytesPerSecond(
              static_cast<int64_t>(read_rate));
        }
        if (write_rate > 0) {
          thread->shared->write_rate_limiter->SetBytesPerSecond(
              static_cast<int64_t>(write_rate));
        }
      }
      // Start the query
      if (query_type == 0) {
        // the Get query
        gets++;
        if (FLAGS_num_column_families > 1) {
          s = db_with_cfh->db->Get(read_options_, db_with_cfh->GetCfh(key_rand),
                                   key, &pinnable_val);
        } else {
          pinnable_val.Reset();
          s = db_with_cfh->db->Get(read_options_,
                                   db_with_cfh->db->DefaultColumnFamily(), key,
                                   &pinnable_val);
        }

        if (s.ok()) {
          get_found++;
          bytes += key.size() + pinnable_val.size();
        } else if (!s.IsNotFound()) {
          fprintf(stderr, "Get returned an error: %s\n", s.ToString().c_str());
          abort();
        }

        if (thread->shared->read_rate_limiter && (gets + seek) % 100 == 0) {
          thread->shared->read_rate_limiter->Request(100, Env::IO_HIGH,
                                                     nullptr /*stats*/);
        }
        thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kRead);
      } else if (query_type == 1) {
        // the Put query
        puts++;
        int64_t val_size = ParetoCdfInversion(u, FLAGS_value_theta,
                                              FLAGS_value_k, FLAGS_value_sigma);
        if (val_size < 10) {
          val_size = 10;
        } else if (val_size > value_max) {
          val_size = val_size % value_max;
        }
        total_val_size += val_size;

        s = db_with_cfh->db->Put(
            write_options_, key,
            gen.Generate(static_cast<unsigned int>(val_size)));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          ErrorExit();
        }

        if (thread->shared->write_rate_limiter && puts % 100 == 0) {
          thread->shared->write_rate_limiter->Request(100, Env::IO_HIGH,
                                                      nullptr /*stats*/);
        }
        thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kWrite);
      } else if (query_type == 2) {
        // Seek query
        if (db_with_cfh->db != nullptr) {
          Iterator* single_iter = nullptr;
          single_iter = db_with_cfh->db->NewIterator(read_options_);
          if (single_iter != nullptr) {
            single_iter->Seek(key);
            seek++;
            if (single_iter->Valid() && single_iter->key().compare(key) == 0) {
              seek_found++;
            }
            int64_t scan_length =
                ParetoCdfInversion(u, FLAGS_iter_theta, FLAGS_iter_k,
                                   FLAGS_iter_sigma) %
                scan_len_max;
            for (int64_t j = 0; j < scan_length && single_iter->Valid(); j++) {
              Slice value = single_iter->value();
              memcpy(value_buffer, value.data(),
                     std::min(value.size(), sizeof(value_buffer)));
              bytes += single_iter->key().size() + single_iter->value().size();
              single_iter->Next();
              assert(single_iter->status().ok());
              total_scan_length++;
            }
          }
          delete single_iter;
        }
        thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kSeek);
      }
    }
    char msg[256];
    snprintf(msg, sizeof(msg),
             "( Gets:%" PRIu64 " Puts:%" PRIu64 " Seek:%" PRIu64
             ", reads %" PRIu64 " in %" PRIu64
             " found, "
             "avg size: %.1f value, %.1f scan)\n",
             gets, puts, seek, get_found + seek_found, gets + seek,
             total_val_size / puts, total_scan_length / seek);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  void IteratorCreation(ThreadState* thread) {
    Duration duration(FLAGS_duration, reads_);
    ReadOptions options = read_options_;
    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      Slice ts;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
        options.timestamp = &ts;
      }
      Iterator* iter = db->NewIterator(options);
      delete iter;
      thread->stats.FinishedOps(nullptr, db, 1, kOthers);
    }
  }

  void IteratorCreationWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      IteratorCreation(thread);
    } else {
      BGWriter(thread, kWrite);
    }
  }

  void SeekRandom(ThreadState* thread) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;
    ReadOptions options = read_options_;
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
      options.timestamp = &ts;
    }

    std::vector<Iterator*> tailing_iters;
    if (FLAGS_use_tailing_iterator) {
      if (db_.db != nullptr) {
        tailing_iters.push_back(db_.db->NewIterator(options));
      } else {
        for (const auto& db_with_cfh : multi_dbs_) {
          tailing_iters.push_back(db_with_cfh.db->NewIterator(options));
        }
      }
    }
    options.auto_prefix_mode = FLAGS_auto_prefix_mode;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    std::unique_ptr<const char[]> upper_bound_key_guard;
    Slice upper_bound = AllocateKey(&upper_bound_key_guard);
    std::unique_ptr<const char[]> lower_bound_key_guard;
    Slice lower_bound = AllocateKey(&lower_bound_key_guard);

    Duration duration(FLAGS_duration, reads_);
    char value_buffer[256];
    while (!duration.Done(1)) {
      int64_t seek_pos = thread->rand.Next() % FLAGS_num;
      GenerateKeyFromIntForSeek(static_cast<uint64_t>(seek_pos), FLAGS_num,
                                &key);
      if (FLAGS_max_scan_distance != 0) {
        if (FLAGS_reverse_iterator) {
          GenerateKeyFromInt(
              static_cast<uint64_t>(std::max(
                  static_cast<int64_t>(0), seek_pos - FLAGS_max_scan_distance)),
              FLAGS_num, &lower_bound);
          options.iterate_lower_bound = &lower_bound;
        } else {
          auto min_num =
              std::min(FLAGS_num, seek_pos + FLAGS_max_scan_distance);
          GenerateKeyFromInt(static_cast<uint64_t>(min_num), FLAGS_num,
                             &upper_bound);
          options.iterate_upper_bound = &upper_bound;
        }
      } else if (FLAGS_auto_prefix_mode && prefix_extractor_ &&
                 !FLAGS_reverse_iterator) {
        // Set upper bound to next prefix
        auto mutable_upper_bound = const_cast<char*>(upper_bound.data());
        std::memcpy(mutable_upper_bound, key.data(), prefix_size_);
        mutable_upper_bound[prefix_size_ - 1]++;
        upper_bound = Slice(upper_bound.data(), prefix_size_);
        options.iterate_upper_bound = &upper_bound;
      }

      // Pick a Iterator to use
      uint64_t db_idx_to_use =
          (db_.db == nullptr)
              ? (uint64_t{thread->rand.Next()} % multi_dbs_.size())
              : 0;
      std::unique_ptr<Iterator> single_iter;
      Iterator* iter_to_use;
      if (FLAGS_use_tailing_iterator) {
        iter_to_use = tailing_iters[db_idx_to_use];
      } else {
        if (db_.db != nullptr) {
          single_iter.reset(db_.db->NewIterator(options));
        } else {
          single_iter.reset(multi_dbs_[db_idx_to_use].db->NewIterator(options));
        }
        iter_to_use = single_iter.get();
      }

      iter_to_use->Seek(key);
      read++;
      if (iter_to_use->Valid() && iter_to_use->key().compare(key) == 0) {
        found++;
      }

      for (int j = 0; j < FLAGS_seek_nexts && iter_to_use->Valid(); ++j) {
        // Copy out iterator's value to make sure we read them.
        Slice value = iter_to_use->value();
        memcpy(value_buffer, value.data(),
               std::min(value.size(), sizeof(value_buffer)));
        bytes += iter_to_use->key().size() + iter_to_use->value().size();

        if (!FLAGS_reverse_iterator) {
          iter_to_use->Next();
        } else {
          iter_to_use->Prev();
        }
        assert(iter_to_use->status().ok());
      }

      if (thread->shared->read_rate_limiter.get() != nullptr &&
          read % 256 == 255) {
        thread->shared->read_rate_limiter->Request(
            256, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }

      thread->stats.FinishedOps(&db_, db_.db, 1, kSeek);
    }
    for (auto iter : tailing_iters) {
      delete iter;
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)\n", found,
             read);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  void SeekRandomWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      SeekRandom(thread);
    } else {
      BGWriter(thread, kWrite);
    }
  }

  void SeekRandomWhileMerging(ThreadState* thread) {
    if (thread->tid > 0) {
      SeekRandom(thread);
    } else {
      BGWriter(thread, kMerge);
    }
  }

  void DoDelete(ThreadState* thread, bool seq) {
    WriteBatch batch(/*reserved_bytes=*/0, /*max_bytes=*/0,
                     FLAGS_write_batch_protection_bytes_per_key,
                     user_timestamp_size_);
    Duration duration(seq ? 0 : FLAGS_duration, deletes_);
    int64_t i = 0;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }

    while (!duration.Done(entries_per_batch_)) {
      DB* db = SelectDB(thread);
      batch.Clear();
      for (int64_t j = 0; j < entries_per_batch_; ++j) {
        const int64_t k = seq ? i + j : (thread->rand.Next() % FLAGS_num);
        GenerateKeyFromInt(k, FLAGS_num, &key);
        batch.Delete(key);
      }
      Status s;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->Allocate(ts_guard.get());
        s = batch.UpdateTimestamps(
            ts, [this](uint32_t) { return user_timestamp_size_; });
        if (!s.ok()) {
          fprintf(stderr, "assign timestamp: %s\n", s.ToString().c_str());
          ErrorExit();
        }
      }
      s = db->Write(write_options_, &batch);
      thread->stats.FinishedOps(nullptr, db, entries_per_batch_, kDelete);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
      i += entries_per_batch_;
    }
  }

  void DeleteSeq(ThreadState* thread) { DoDelete(thread, true); }

  void DeleteRandom(ThreadState* thread) { DoDelete(thread, false); }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      BGWriter(thread, kWrite);
    }
  }

  void MultiReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      MultiReadRandom(thread);
    } else {
      BGWriter(thread, kWrite);
    }
  }

  void ReadWhileMerging(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      BGWriter(thread, kMerge);
    }
  }

  void BGWriter(ThreadState* thread, enum OperationType write_merge) {
    // Special thread that keeps writing until other threads are done.
    RandomGenerator gen;
    int64_t bytes = 0;

    std::unique_ptr<RateLimiter> write_rate_limiter;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    // Don't merge stats from this thread with the readers.
    thread->stats.SetExcludeFromMerge();

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<char[]> ts_guard;
    std::unique_ptr<const char[]> begin_key_guard;
    Slice begin_key = AllocateKey(&begin_key_guard);
    std::unique_ptr<const char[]> end_key_guard;
    Slice end_key = AllocateKey(&end_key_guard);
    uint64_t num_range_deletions = 0;
    std::vector<std::unique_ptr<const char[]>> expanded_key_guards;
    std::vector<Slice> expanded_keys;
    if (FLAGS_expand_range_tombstones) {
      expanded_key_guards.resize(range_tombstone_width_);
      for (auto& expanded_key_guard : expanded_key_guards) {
        expanded_keys.emplace_back(AllocateKey(&expanded_key_guard));
      }
    }
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }
    uint32_t written = 0;
    bool hint_printed = false;

    while (true) {
      DB* db = SelectDB(thread);
      {
        MutexLock l(&thread->shared->mu);
        if (FLAGS_finish_after_writes && written == writes_) {
          fprintf(stderr, "Exiting the writer after %u writes...\n", written);
          break;
        }
        if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
          // Other threads have finished
          if (FLAGS_finish_after_writes) {
            // Wait for the writes to be finished
            if (!hint_printed) {
              fprintf(stderr, "Reads are finished. Have %d more writes to do\n",
                      static_cast<int>(writes_) - written);
              hint_printed = true;
            }
          } else {
            // Finish the write immediately
            break;
          }
        }
      }

      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      Status s;

      Slice val = gen.Generate();
      Slice ts;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->Allocate(ts_guard.get());
      }
      if (write_merge == kWrite) {
        if (user_timestamp_size_ == 0) {
          s = db->Put(write_options_, key, val);
        } else {
          s = db->Put(write_options_, key, ts, val);
        }
      } else {
        s = db->Merge(write_options_, key, val);
      }
      // Restore write_options_
      written++;

      if (!s.ok()) {
        fprintf(stderr, "put or merge error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + val.size() + user_timestamp_size_;
      thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);

      if (FLAGS_benchmark_write_rate_limit > 0) {
        write_rate_limiter->Request(key.size() + val.size(), Env::IO_HIGH,
                                    nullptr /* stats */,
                                    RateLimiter::OpType::kWrite);
      }

      if (writes_per_range_tombstone_ > 0 &&
          written > writes_before_delete_range_ &&
          (written - writes_before_delete_range_) /
                  writes_per_range_tombstone_ <=
              max_num_range_tombstones_ &&
          (written - writes_before_delete_range_) %
                  writes_per_range_tombstone_ ==
              0) {
        num_range_deletions++;
        int64_t begin_num = thread->rand.Next() % FLAGS_num;
        if (FLAGS_expand_range_tombstones) {
          for (int64_t offset = 0; offset < range_tombstone_width_; ++offset) {
            GenerateKeyFromInt(begin_num + offset, FLAGS_num,
                               &expanded_keys[offset]);
            if (!db->Delete(write_options_, expanded_keys[offset]).ok()) {
              fprintf(stderr, "delete error: %s\n", s.ToString().c_str());
              exit(1);
            }
          }
        } else {
          GenerateKeyFromInt(begin_num, FLAGS_num, &begin_key);
          GenerateKeyFromInt(begin_num + range_tombstone_width_, FLAGS_num,
                             &end_key);
          if (!db->DeleteRange(write_options_, db->DefaultColumnFamily(),
                               begin_key, end_key)
                   .ok()) {
            fprintf(stderr, "deleterange error: %s\n", s.ToString().c_str());
            exit(1);
          }
        }
        thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
        // TODO: DeleteRange is not included in calculcation of bytes/rate
        // limiter request
      }
    }
    if (num_range_deletions > 0) {
      std::cout << "Number of range deletions: " << num_range_deletions
                << std::endl;
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadWhileScanning(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      BGScan(thread);
    }
  }

  void BGScan(ThreadState* thread) {
    if (FLAGS_num_multi_db > 0) {
      fprintf(stderr, "Not supporting multiple DBs.\n");
      abort();
    }
    assert(db_.db != nullptr);
    ReadOptions read_options = read_options_;
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
      read_options.timestamp = &ts;
    }
    Iterator* iter = db_.db->NewIterator(read_options);

    fprintf(stderr, "num reads to do %" PRIu64 "\n", reads_);
    Duration duration(FLAGS_duration, reads_);
    uint64_t num_seek_to_first = 0;
    uint64_t num_next = 0;
    while (!duration.Done(1)) {
      if (!iter->Valid()) {
        iter->SeekToFirst();
        num_seek_to_first++;
      } else if (!iter->status().ok()) {
        fprintf(stderr, "Iterator error: %s\n",
                iter->status().ToString().c_str());
        abort();
      } else {
        iter->Next();
        num_next++;
      }

      thread->stats.FinishedOps(&db_, db_.db, 1, kSeek);
    }
    (void)num_seek_to_first;
    (void)num_next;
    delete iter;
  }

  // Given a key K and value V, this puts (K+"0", V), (K+"1", V), (K+"2", V)
  // in DB atomically i.e in a single batch. Also refer GetMany.
  Status PutMany(DB* db, const WriteOptions& writeoptions, const Slice& key,
                 const Slice& value) {
    std::string suffixes[3] = {"2", "1", "0"};
    std::string keys[3];

    WriteBatch batch(/*reserved_bytes=*/0, /*max_bytes=*/0,
                     FLAGS_write_batch_protection_bytes_per_key,
                     user_timestamp_size_);
    Status s;
    for (int i = 0; i < 3; i++) {
      keys[i] = key.ToString() + suffixes[i];
      batch.Put(keys[i], value);
    }

    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      Slice ts = mock_app_clock_->Allocate(ts_guard.get());
      s = batch.UpdateTimestamps(
          ts, [this](uint32_t) { return user_timestamp_size_; });
      if (!s.ok()) {
        fprintf(stderr, "assign timestamp to batch: %s\n",
                s.ToString().c_str());
        ErrorExit();
      }
    }

    s = db->Write(writeoptions, &batch);
    return s;
  }

  // Given a key K, this deletes (K+"0", V), (K+"1", V), (K+"2", V)
  // in DB atomically i.e in a single batch. Also refer GetMany.
  Status DeleteMany(DB* db, const WriteOptions& writeoptions,
                    const Slice& key) {
    std::string suffixes[3] = {"1", "2", "0"};
    std::string keys[3];

    WriteBatch batch(0, 0, FLAGS_write_batch_protection_bytes_per_key,
                     user_timestamp_size_);
    Status s;
    for (int i = 0; i < 3; i++) {
      keys[i] = key.ToString() + suffixes[i];
      batch.Delete(keys[i]);
    }

    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      Slice ts = mock_app_clock_->Allocate(ts_guard.get());
      s = batch.UpdateTimestamps(
          ts, [this](uint32_t) { return user_timestamp_size_; });
      if (!s.ok()) {
        fprintf(stderr, "assign timestamp to batch: %s\n",
                s.ToString().c_str());
        ErrorExit();
      }
    }

    s = db->Write(writeoptions, &batch);
    return s;
  }

  // Given a key K and value V, this gets values for K+"0", K+"1" and K+"2"
  // in the same snapshot, and verifies that all the values are identical.
  // ASSUMES that PutMany was used to put (K, V) into the DB.
  Status GetMany(DB* db, const Slice& key, std::string* value) {
    std::string suffixes[3] = {"0", "1", "2"};
    std::string keys[3];
    Slice key_slices[3];
    std::string values[3];
    ReadOptions readoptionscopy = read_options_;

    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      ts = mock_app_clock_->Allocate(ts_guard.get());
      readoptionscopy.timestamp = &ts;
    }

    readoptionscopy.snapshot = db->GetSnapshot();
    Status s;
    for (int i = 0; i < 3; i++) {
      keys[i] = key.ToString() + suffixes[i];
      key_slices[i] = keys[i];
      s = db->Get(readoptionscopy, key_slices[i], value);
      if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "get error: %s\n", s.ToString().c_str());
        values[i] = "";
        // we continue after error rather than exiting so that we can
        // find more errors if any
      } else if (s.IsNotFound()) {
        values[i] = "";
      } else {
        values[i] = *value;
      }
    }
    db->ReleaseSnapshot(readoptionscopy.snapshot);

    if ((values[0] != values[1]) || (values[1] != values[2])) {
      fprintf(stderr, "inconsistent values for key %s: %s, %s, %s\n",
              key.ToString().c_str(), values[0].c_str(), values[1].c_str(),
              values[2].c_str());
      // we continue after error rather than exiting so that we can
      // find more errors if any
    }

    return s;
  }

  // Differs from readrandomwriterandom in the following ways:
  // (a) Uses GetMany/PutMany to read/write key values. Refer to those funcs.
  // (b) Does deletes as well (per FLAGS_deletepercent)
  // (c) In order to achieve high % of 'found' during lookups, and to do
  //     multiple writes (including puts and deletes) it uses upto
  //     FLAGS_numdistinct distinct keys instead of FLAGS_num distinct keys.
  // (d) Does not have a MultiGet option.
  void RandomWithVerify(ThreadState* thread) {
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int delete_weight = 0;
    int64_t gets_done = 0;
    int64_t puts_done = 0;
    int64_t deletes_done = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    // the number of iterations is the larger of read_ or write_
    for (int64_t i = 0; i < readwrites_; i++) {
      DB* db = SelectDB(thread);
      if (get_weight == 0 && put_weight == 0 && delete_weight == 0) {
        // one batch completed, reinitialize for next batch
        get_weight = FLAGS_readwritepercent;
        delete_weight = FLAGS_deletepercent;
        put_weight = 100 - get_weight - delete_weight;
      }
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_numdistinct,
                         FLAGS_numdistinct, &key);
      if (get_weight > 0) {
        // do all the gets first
        Status s = GetMany(db, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "getmany error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
        }
        get_weight--;
        gets_done++;
        thread->stats.FinishedOps(&db_, db_.db, 1, kRead);
      } else if (put_weight > 0) {
        // then do all the corresponding number of puts
        // for all the gets we have done earlier
        Status s = PutMany(db, write_options_, key, gen.Generate());
        if (!s.ok()) {
          fprintf(stderr, "putmany error: %s\n", s.ToString().c_str());
          exit(1);
        }
        put_weight--;
        puts_done++;
        thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
      } else if (delete_weight > 0) {
        Status s = DeleteMany(db, write_options_, key);
        if (!s.ok()) {
          fprintf(stderr, "deletemany error: %s\n", s.ToString().c_str());
          exit(1);
        }
        delete_weight--;
        deletes_done++;
        thread->stats.FinishedOps(&db_, db_.db, 1, kDelete);
      }
    }
    char msg[128];
    snprintf(msg, sizeof(msg),
             "( get:%" PRIu64 " put:%" PRIu64 " del:%" PRIu64 " total:%" PRIu64
             " found:%" PRIu64 ")",
             gets_done, puts_done, deletes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  // This is different from ReadWhileWriting because it does not use
  // an extra thread.
  void ReadRandomWriteRandom(ThreadState* thread) {
    ReadOptions options = read_options_;
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int get_weight = 0;
    int put_weight = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }

    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      if (get_weight == 0 && put_weight == 0) {
        // one batch completed, reinitialize for next batch
        get_weight = FLAGS_readwritepercent;
        put_weight = 100 - get_weight;
      }
      if (get_weight > 0) {
        // do all the gets first
        Slice ts;
        if (user_timestamp_size_ > 0) {
          ts = mock_app_clock_->GetTimestampForRead(thread->rand,
                                                    ts_guard.get());
          options.timestamp = &ts;
        }
        Status s = db->Get(options, key, &value);
        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          found++;
        }
        get_weight--;
        reads_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      } else if (put_weight > 0) {
        // then do all the corresponding number of puts
        // for all the gets we have done earlier
        Status s;
        if (user_timestamp_size_ > 0) {
          Slice ts = mock_app_clock_->Allocate(ts_guard.get());
          s = db->Put(write_options_, key, ts, gen.Generate());
        } else {
          s = db->Put(write_options_, key, gen.Generate());
        }
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          ErrorExit();
        }
        put_weight--;
        writes_done++;
        thread->stats.FinishedOps(nullptr, db, 1, kWrite);
      }
    }
    char msg[100];
    snprintf(msg, sizeof(msg),
             "( reads:%" PRIu64 " writes:%" PRIu64 " total:%" PRIu64
             " found:%" PRIu64 ")",
             reads_done, writes_done, readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  //
  // Read-modify-write for random keys
  void UpdateRandom(ThreadState* thread) {
    ReadOptions options = read_options_;
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t bytes = 0;
    Duration duration(FLAGS_duration, readwrites_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      Slice ts;
      if (user_timestamp_size_ > 0) {
        // Read with newest timestamp because we are doing rmw.
        ts = mock_app_clock_->Allocate(ts_guard.get());
        options.timestamp = &ts;
      }

      auto status = db->Get(options, key, &value);
      if (status.ok()) {
        ++found;
        bytes += key.size() + value.size() + user_timestamp_size_;
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        abort();
      }

      if (thread->shared->write_rate_limiter) {
        thread->shared->write_rate_limiter->Request(
            key.size() + value.size(), Env::IO_HIGH, nullptr /*stats*/,
            RateLimiter::OpType::kWrite);
      }

      Slice val = gen.Generate();
      Status s;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->Allocate(ts_guard.get());
        s = db->Put(write_options_, key, ts, val);
      } else {
        s = db->Put(write_options_, key, val);
      }
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + val.size() + user_timestamp_size_;
      thread->stats.FinishedOps(nullptr, db, 1, kUpdate);
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 " found:%" PRIu64 ")",
             readwrites_, found);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read-XOR-write for random keys. Xors the existing value with a randomly
  // generated value, and stores the result. Assuming A in the array of bytes
  // representing the existing value, we generate an array B of the same size,
  // then compute C = A^B as C[i]=A[i]^B[i], and store C
  void XORUpdateRandom(ThreadState* thread) {
    ReadOptions options = read_options_;
    RandomGenerator gen;
    std::string existing_value;
    int64_t found = 0;
    Duration duration(FLAGS_duration, readwrites_);

    BytesXOROperator xor_operator;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }
    // the number of iterations is the larger of read_ or write_
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      Slice ts;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->Allocate(ts_guard.get());
        options.timestamp = &ts;
      }

      auto status = db->Get(options, key, &existing_value);
      if (status.ok()) {
        ++found;
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        exit(1);
      }

      Slice value =
          gen.Generate(static_cast<unsigned int>(existing_value.size()));
      std::string new_value;

      if (status.ok()) {
        Slice existing_value_slice = Slice(existing_value);
        xor_operator.XOR(&existing_value_slice, value, &new_value);
      } else {
        xor_operator.XOR(nullptr, value, &new_value);
      }

      Status s;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->Allocate(ts_guard.get());
        s = db->Put(write_options_, key, ts, Slice(new_value));
      } else {
        s = db->Put(write_options_, key, Slice(new_value));
      }
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        ErrorExit();
      }
      thread->stats.FinishedOps(nullptr, db, 1);
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 " found:%" PRIu64 ")",
             readwrites_, found);
    thread->stats.AddMessage(msg);
  }

  // Read-modify-write for random keys.
  // Each operation causes the key grow by value_size (simulating an append).
  // Generally used for benchmarking against merges of similar type
  void AppendRandom(ThreadState* thread) {
    ReadOptions options = read_options_;
    RandomGenerator gen;
    std::string value;
    int64_t found = 0;
    int64_t bytes = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }
    // The number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % FLAGS_num, FLAGS_num, &key);
      Slice ts;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->Allocate(ts_guard.get());
        options.timestamp = &ts;
      }

      auto status = db->Get(options, key, &value);
      if (status.ok()) {
        ++found;
        bytes += key.size() + value.size() + user_timestamp_size_;
      } else if (!status.IsNotFound()) {
        fprintf(stderr, "Get returned an error: %s\n",
                status.ToString().c_str());
        abort();
      } else {
        // If not existing, then just assume an empty string of data
        value.clear();
      }

      // Update the value (by appending data)
      Slice operand = gen.Generate();
      if (value.size() > 0) {
        // Use a delimiter to match the semantics for StringAppendOperator
        value.append(1, ',');
      }
      value.append(operand.data(), operand.size());

      Status s;
      if (user_timestamp_size_ > 0) {
        ts = mock_app_clock_->Allocate(ts_guard.get());
        s = db->Put(write_options_, key, ts, value);
      } else {
        // Write back to the database
        s = db->Put(write_options_, key, value);
      }
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        ErrorExit();
      }
      bytes += key.size() + value.size() + user_timestamp_size_;
      thread->stats.FinishedOps(nullptr, db, 1, kUpdate);
    }

    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 " found:%" PRIu64 ")",
             readwrites_, found);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read-modify-write for random keys (using MergeOperator)
  // The merge operator to use should be defined by FLAGS_merge_operator
  // Adjust FLAGS_value_size so that the keys are reasonable for this operator
  // Assumes that the merge operator is non-null (i.e.: is well-defined)
  //
  // For example, use FLAGS_merge_operator="uint64add" and FLAGS_value_size=8
  // to simulate random additions over 64-bit integers using merge.
  //
  // The number of merges on the same key can be controlled by adjusting
  // FLAGS_merge_keys.
  void MergeRandom(ThreadState* thread) {
    RandomGenerator gen;
    int64_t bytes = 0;
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // The number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(thread);
      int64_t key_rand = thread->rand.Next() % merge_keys_;
      GenerateKeyFromInt(key_rand, merge_keys_, &key);

      Status s;
      Slice val = gen.Generate();
      if (FLAGS_num_column_families > 1) {
        s = db_with_cfh->db->Merge(write_options_,
                                   db_with_cfh->GetCfh(key_rand), key, val);
      } else {
        s = db_with_cfh->db->Merge(
            write_options_, db_with_cfh->db->DefaultColumnFamily(), key, val);
      }

      if (!s.ok()) {
        fprintf(stderr, "merge error: %s\n", s.ToString().c_str());
        exit(1);
      }
      bytes += key.size() + val.size();
      thread->stats.FinishedOps(nullptr, db_with_cfh->db, 1, kMerge);
    }

    // Print some statistics
    char msg[100];
    snprintf(msg, sizeof(msg), "( updates:%" PRIu64 ")", readwrites_);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  // Read and merge random keys. The amount of reads and merges are controlled
  // by adjusting FLAGS_num and FLAGS_mergereadpercent. The number of distinct
  // keys (and thus also the number of reads and merges on the same key) can be
  // adjusted with FLAGS_merge_keys.
  //
  // As with MergeRandom, the merge operator to use should be defined by
  // FLAGS_merge_operator.
  void ReadRandomMergeRandom(ThreadState* thread) {
    RandomGenerator gen;
    std::string value;
    int64_t num_hits = 0;
    int64_t num_gets = 0;
    int64_t num_merges = 0;
    size_t max_length = 0;

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    // the number of iterations is the larger of read_ or write_
    Duration duration(FLAGS_duration, readwrites_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);
      GenerateKeyFromInt(thread->rand.Next() % merge_keys_, merge_keys_, &key);

      bool do_merge = int(thread->rand.Next() % 100) < FLAGS_mergereadpercent;

      if (do_merge) {
        Status s = db->Merge(write_options_, key, gen.Generate());
        if (!s.ok()) {
          fprintf(stderr, "merge error: %s\n", s.ToString().c_str());
          exit(1);
        }
        num_merges++;
        thread->stats.FinishedOps(nullptr, db, 1, kMerge);
      } else {
        Status s = db->Get(read_options_, key, &value);
        if (value.length() > max_length) {
          max_length = value.length();
        }

        if (!s.ok() && !s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          // we continue after error rather than exiting so that we can
          // find more errors if any
        } else if (!s.IsNotFound()) {
          num_hits++;
        }
        num_gets++;
        thread->stats.FinishedOps(nullptr, db, 1, kRead);
      }
    }

    char msg[100];
    snprintf(msg, sizeof(msg),
             "(reads:%" PRIu64 " merges:%" PRIu64 " total:%" PRIu64
             " hits:%" PRIu64 " maxlength:%" ROCKSDB_PRIszt ")",
             num_gets, num_merges, readwrites_, num_hits, max_length);
    thread->stats.AddMessage(msg);
  }

  void WriteSeqSeekSeq(ThreadState* thread) {
    writes_ = FLAGS_num;
    DoWrite(thread, SEQUENTIAL);
    // exclude writes from the ops/sec calculation
    thread->stats.Start(thread->tid);

    DB* db = SelectDB(thread);
    ReadOptions read_opts = read_options_;
    std::unique_ptr<char[]> ts_guard;
    Slice ts;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
      ts = mock_app_clock_->GetTimestampForRead(thread->rand, ts_guard.get());
      read_opts.timestamp = &ts;
    }
    std::unique_ptr<Iterator> iter(db->NewIterator(read_opts));

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    for (int64_t i = 0; i < FLAGS_num; ++i) {
      GenerateKeyFromInt(i, FLAGS_num, &key);
      iter->Seek(key);
      assert(iter->Valid() && iter->key() == key);
      thread->stats.FinishedOps(nullptr, db, 1, kSeek);

      for (int j = 0; j < FLAGS_seek_nexts && i + 1 < FLAGS_num; ++j) {
        if (!FLAGS_reverse_iterator) {
          iter->Next();
        } else {
          iter->Prev();
        }
        GenerateKeyFromInt(++i, FLAGS_num, &key);
        assert(iter->Valid() && iter->key() == key);
        thread->stats.FinishedOps(nullptr, db, 1, kSeek);
      }

      iter->Seek(key);
      assert(iter->Valid() && iter->key() == key);
      thread->stats.FinishedOps(nullptr, db, 1, kSeek);
    }
  }

  bool binary_search(std::vector<int>& data, int start, int end, int key) {
    if (data.empty()) {
      return false;
    }
    if (start > end) {
      return false;
    }
    int mid = start + (end - start) / 2;
    if (mid > static_cast<int>(data.size()) - 1) {
      return false;
    }
    if (data[mid] == key) {
      return true;
    } else if (data[mid] > key) {
      return binary_search(data, start, mid - 1, key);
    } else {
      return binary_search(data, mid + 1, end, key);
    }
  }

  // Does a bunch of merge operations for a key(key1) where the merge operand
  // is a sorted list. Next performance comparison is done between doing a Get
  // for key1 followed by searching for another key(key2) in the large sorted
  // list vs calling GetMergeOperands for key1 and then searching for the key2
  // in all the sorted sub-lists. Later case is expected to be a lot faster.
  void GetMergeOperands(ThreadState* thread) {
    DB* db = SelectDB(thread);
    const int kTotalValues = 100000;
    const int kListSize = 100;
    std::string key = "my_key";
    std::string value;

    for (int i = 1; i < kTotalValues; i++) {
      if (i % kListSize == 0) {
        // Remove trailing ','
        value.pop_back();
        db->Merge(WriteOptions(), key, value);
        value.clear();
      } else {
        value.append(std::to_string(i)).append(",");
      }
    }

    SortList s;
    std::vector<int> data;
    // This value can be experimented with and it will demonstrate the
    // perf difference between doing a Get and searching for lookup_key in the
    // resultant large sorted list vs doing GetMergeOperands and searching
    // for lookup_key within this resultant sorted sub-lists.
    int lookup_key = 1;

    // Get API call
    std::cout << "--- Get API call --- \n";
    PinnableSlice p_slice;
    uint64_t st = FLAGS_env->NowNanos();
    db->Get(ReadOptions(), db->DefaultColumnFamily(), key, &p_slice);
    s.MakeVector(data, p_slice);
    bool found =
        binary_search(data, 0, static_cast<int>(data.size() - 1), lookup_key);
    std::cout << "Found key? " << std::to_string(found) << "\n";
    uint64_t sp = FLAGS_env->NowNanos();
    std::cout << "Get: " << (sp - st) / 1000000000.0 << " seconds\n";
    std::string* dat_ = p_slice.GetSelf();
    std::cout << "Sample data from Get API call: " << dat_->substr(0, 10)
              << "\n";
    data.clear();

    // GetMergeOperands API call
    std::cout << "--- GetMergeOperands API --- \n";
    std::vector<PinnableSlice> a_slice((kTotalValues / kListSize) + 1);
    st = FLAGS_env->NowNanos();
    int number_of_operands = 0;
    GetMergeOperandsOptions get_merge_operands_options;
    get_merge_operands_options.expected_max_number_of_operands =
        (kTotalValues / 100) + 1;
    db->GetMergeOperands(ReadOptions(), db->DefaultColumnFamily(), key,
                         a_slice.data(), &get_merge_operands_options,
                         &number_of_operands);
    for (PinnableSlice& psl : a_slice) {
      s.MakeVector(data, psl);
      found =
          binary_search(data, 0, static_cast<int>(data.size() - 1), lookup_key);
      data.clear();
      if (found) {
        break;
      }
    }
    std::cout << "Found key? " << std::to_string(found) << "\n";
    sp = FLAGS_env->NowNanos();
    std::cout << "Get Merge operands: " << (sp - st) / 1000000000.0
              << " seconds \n";
    int to_print = 0;
    std::cout << "Sample data from GetMergeOperands API call: ";
    for (PinnableSlice& psl : a_slice) {
      std::cout << "List: " << to_print << " : " << *psl.GetSelf() << "\n";
      if (to_print++ > 2) {
        break;
      }
    }
  }

  void VerifyChecksum(ThreadState* thread) {
    DB* db = SelectDB(thread);
    ReadOptions ro;
    ro.adaptive_readahead = FLAGS_adaptive_readahead;
    ro.async_io = FLAGS_async_io;
    ro.rate_limiter_priority =
        FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
    ro.readahead_size = FLAGS_readahead_size;
    ro.auto_readahead_size = FLAGS_auto_readahead_size;
    Status s = db->VerifyChecksum(ro);
    if (!s.ok()) {
      fprintf(stderr, "VerifyChecksum() failed: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void VerifyFileChecksums(ThreadState* thread) {
    DB* db = SelectDB(thread);
    ReadOptions ro;
    ro.adaptive_readahead = FLAGS_adaptive_readahead;
    ro.async_io = FLAGS_async_io;
    ro.rate_limiter_priority =
        FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
    ro.readahead_size = FLAGS_readahead_size;
    ro.auto_readahead_size = FLAGS_auto_readahead_size;
    Status s = db->VerifyFileChecksums(ro);
    if (!s.ok()) {
      fprintf(stderr, "VerifyFileChecksums() failed: %s\n",
              s.ToString().c_str());
      exit(1);
    }
  }

  // This benchmark stress tests Transactions.  For a given --duration (or
  // total number of --writes, a Transaction will perform a read-modify-write
  // to increment the value of a key in each of N(--transaction-sets) sets of
  // keys (where each set has --num keys).  If --threads is set, this will be
  // done in parallel.
  //
  // To test transactions, use --transaction_db=true.  Not setting this
  // parameter
  // will run the same benchmark without transactions.
  //
  // RandomTransactionVerify() will then validate the correctness of the results
  // by checking if the sum of all keys in each set is the same.
  void RandomTransaction(ThreadState* thread) {
    Duration duration(FLAGS_duration, readwrites_);
    uint16_t num_prefix_ranges = static_cast<uint16_t>(FLAGS_transaction_sets);
    uint64_t transactions_done = 0;

    if (num_prefix_ranges == 0 || num_prefix_ranges > 9999) {
      fprintf(stderr, "invalid value for transaction_sets\n");
      abort();
    }

    TransactionOptions txn_options;
    txn_options.lock_timeout = FLAGS_transaction_lock_timeout;
    txn_options.set_snapshot = FLAGS_transaction_set_snapshot;

    RandomTransactionInserter inserter(&thread->rand, write_options_,
                                       read_options_, FLAGS_num,
                                       num_prefix_ranges);

    if (FLAGS_num_multi_db > 1) {
      fprintf(stderr,
              "Cannot run RandomTransaction benchmark with "
              "FLAGS_multi_db > 1.");
      abort();
    }

    while (!duration.Done(1)) {
      bool success;

      // RandomTransactionInserter will attempt to insert a key for each
      // # of FLAGS_transaction_sets
      if (FLAGS_optimistic_transaction_db) {
        success = inserter.OptimisticTransactionDBInsert(db_.opt_txn_db);
      } else if (FLAGS_transaction_db) {
        TransactionDB* txn_db = static_cast<TransactionDB*>(db_.db);
        success = inserter.TransactionDBInsert(txn_db, txn_options);
      } else {
        success = inserter.DBInsert(db_.db);
      }

      if (!success) {
        fprintf(stderr, "Unexpected error: %s\n",
                inserter.GetLastStatus().ToString().c_str());
        abort();
      }

      thread->stats.FinishedOps(nullptr, db_.db, 1, kOthers);
      transactions_done++;
    }

    char msg[100];
    if (FLAGS_optimistic_transaction_db || FLAGS_transaction_db) {
      snprintf(msg, sizeof(msg),
               "( transactions:%" PRIu64 " aborts:%" PRIu64 ")",
               transactions_done, inserter.GetFailureCount());
    } else {
      snprintf(msg, sizeof(msg), "( batches:%" PRIu64 " )", transactions_done);
    }
    thread->stats.AddMessage(msg);
    thread->stats.AddBytes(static_cast<int64_t>(inserter.GetBytesInserted()));
  }

  // Verifies consistency of data after RandomTransaction() has been run.
  // Since each iteration of RandomTransaction() incremented a key in each set
  // by the same value, the sum of the keys in each set should be the same.
  void RandomTransactionVerify() {
    if (!FLAGS_transaction_db && !FLAGS_optimistic_transaction_db) {
      // transactions not used, nothing to verify.
      return;
    }

    Status s = RandomTransactionInserter::Verify(
        db_.db, static_cast<uint16_t>(FLAGS_transaction_sets));

    if (s.ok()) {
      fprintf(stdout, "RandomTransactionVerify Success.\n");
    } else {
      fprintf(stdout, "RandomTransactionVerify FAILED!!\n");
    }
  }

  // Writes and deletes random keys without overwriting keys.
  //
  // This benchmark is intended to partially replicate the behavior of MyRocks
  // secondary indices: All data is stored in keys and updates happen by
  // deleting the old version of the key and inserting the new version.
  void RandomReplaceKeys(ThreadState* thread) {
    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);
    std::unique_ptr<char[]> ts_guard;
    if (user_timestamp_size_ > 0) {
      ts_guard.reset(new char[user_timestamp_size_]);
    }
    std::vector<uint32_t> counters(FLAGS_numdistinct, 0);
    size_t max_counter = 50;
    RandomGenerator gen;

    Status s;
    DB* db = SelectDB(thread);
    for (int64_t i = 0; i < FLAGS_numdistinct; i++) {
      GenerateKeyFromInt(i * max_counter, FLAGS_num, &key);
      if (user_timestamp_size_ > 0) {
        Slice ts = mock_app_clock_->Allocate(ts_guard.get());
        s = db->Put(write_options_, key, ts, gen.Generate());
      } else {
        s = db->Put(write_options_, key, gen.Generate());
      }
      if (!s.ok()) {
        fprintf(stderr, "Operation failed: %s\n", s.ToString().c_str());
        exit(1);
      }
    }

    db->GetSnapshot();

    std::default_random_engine generator;
    std::normal_distribution<double> distribution(FLAGS_numdistinct / 2.0,
                                                  FLAGS_stddev);
    Duration duration(FLAGS_duration, FLAGS_num);
    while (!duration.Done(1)) {
      int64_t rnd_id = static_cast<int64_t>(distribution(generator));
      int64_t key_id = std::max(std::min(FLAGS_numdistinct - 1, rnd_id),
                                static_cast<int64_t>(0));
      GenerateKeyFromInt(key_id * max_counter + counters[key_id], FLAGS_num,
                         &key);
      if (user_timestamp_size_ > 0) {
        Slice ts = mock_app_clock_->Allocate(ts_guard.get());
        s = FLAGS_use_single_deletes ? db->SingleDelete(write_options_, key, ts)
                                     : db->Delete(write_options_, key, ts);
      } else {
        s = FLAGS_use_single_deletes ? db->SingleDelete(write_options_, key)
                                     : db->Delete(write_options_, key);
      }
      if (s.ok()) {
        counters[key_id] = (counters[key_id] + 1) % max_counter;
        GenerateKeyFromInt(key_id * max_counter + counters[key_id], FLAGS_num,
                           &key);
        if (user_timestamp_size_ > 0) {
          Slice ts = mock_app_clock_->Allocate(ts_guard.get());
          s = db->Put(write_options_, key, ts, Slice());
        } else {
          s = db->Put(write_options_, key, Slice());
        }
      }

      if (!s.ok()) {
        fprintf(stderr, "Operation failed: %s\n", s.ToString().c_str());
        exit(1);
      }

      thread->stats.FinishedOps(nullptr, db, 1, kOthers);
    }

    char msg[200];
    snprintf(msg, sizeof(msg),
             "use single deletes: %d, "
             "standard deviation: %lf\n",
             FLAGS_use_single_deletes, FLAGS_stddev);
    thread->stats.AddMessage(msg);
  }

  void TimeSeriesReadOrDelete(ThreadState* thread, bool do_deletion) {
    int64_t read = 0;
    int64_t found = 0;
    int64_t bytes = 0;

    Iterator* iter = nullptr;
    // Only work on single database
    assert(db_.db != nullptr);
    iter = db_.db->NewIterator(read_options_);

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    char value_buffer[256];
    while (true) {
      {
        MutexLock l(&thread->shared->mu);
        if (thread->shared->num_done >= 1) {
          // Write thread have finished
          break;
        }
      }
      if (!FLAGS_use_tailing_iterator) {
        delete iter;
        iter = db_.db->NewIterator(read_options_);
      }
      // Pick a Iterator to use

      int64_t key_id = thread->rand.Next() % FLAGS_key_id_range;
      GenerateKeyFromInt(key_id, FLAGS_num, &key);
      // Reset last 8 bytes to 0
      char* start = const_cast<char*>(key.data());
      start += key.size() - 8;
      memset(start, 0, 8);
      ++read;

      bool key_found = false;
      // Seek the prefix
      for (iter->Seek(key); iter->Valid() && iter->key().starts_with(key);
           iter->Next()) {
        key_found = true;
        // Copy out iterator's value to make sure we read them.
        if (do_deletion) {
          bytes += iter->key().size();
          if (KeyExpired(timestamp_emulator_.get(), iter->key())) {
            thread->stats.FinishedOps(&db_, db_.db, 1, kDelete);
            db_.db->Delete(write_options_, iter->key());
          } else {
            break;
          }
        } else {
          bytes += iter->key().size() + iter->value().size();
          thread->stats.FinishedOps(&db_, db_.db, 1, kRead);
          Slice value = iter->value();
          memcpy(value_buffer, value.data(),
                 std::min(value.size(), sizeof(value_buffer)));

          assert(iter->status().ok());
        }
      }
      found += key_found;

      if (thread->shared->read_rate_limiter.get() != nullptr) {
        thread->shared->read_rate_limiter->Request(
            1, Env::IO_HIGH, nullptr /* stats */, RateLimiter::OpType::kRead);
      }
    }
    delete iter;

    char msg[100];
    snprintf(msg, sizeof(msg), "(%" PRIu64 " of %" PRIu64 " found)", found,
             read);
    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);
  }

  void TimeSeriesWrite(ThreadState* thread) {
    // Special thread that keeps writing until other threads are done.
    RandomGenerator gen;
    int64_t bytes = 0;

    // Don't merge stats from this thread with the readers.
    thread->stats.SetExcludeFromMerge();

    std::unique_ptr<RateLimiter> write_rate_limiter;
    if (FLAGS_benchmark_write_rate_limit > 0) {
      write_rate_limiter.reset(
          NewGenericRateLimiter(FLAGS_benchmark_write_rate_limit));
    }

    std::unique_ptr<const char[]> key_guard;
    Slice key = AllocateKey(&key_guard);

    Duration duration(FLAGS_duration, writes_);
    while (!duration.Done(1)) {
      DB* db = SelectDB(thread);

      uint64_t key_id = thread->rand.Next() % FLAGS_key_id_range;
      // Write key id
      GenerateKeyFromInt(key_id, FLAGS_num, &key);
      // Write timestamp

      char* start = const_cast<char*>(key.data());
      char* pos = start + 8;
      int bytes_to_fill =
          std::min(key_size_ - static_cast<int>(pos - start), 8);
      uint64_t timestamp_value = timestamp_emulator_->Get();
      if (port::kLittleEndian) {
        for (int i = 0; i < bytes_to_fill; ++i) {
          pos[i] = (timestamp_value >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
        }
      } else {
        memcpy(pos, static_cast<void*>(&timestamp_value), bytes_to_fill);
      }

      timestamp_emulator_->Inc();

      Status s;
      Slice val = gen.Generate();
      s = db->Put(write_options_, key, val);

      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        ErrorExit();
      }
      bytes = key.size() + val.size();
      thread->stats.FinishedOps(&db_, db_.db, 1, kWrite);
      thread->stats.AddBytes(bytes);

      if (FLAGS_benchmark_write_rate_limit > 0) {
        write_rate_limiter->Request(key.size() + val.size(), Env::IO_HIGH,
                                    nullptr /* stats */,
                                    RateLimiter::OpType::kWrite);
      }
    }
  }

  void TimeSeries(ThreadState* thread) {
    if (thread->tid > 0) {
      bool do_deletion = FLAGS_expire_style == "delete" &&
                         thread->tid <= FLAGS_num_deletion_threads;
      TimeSeriesReadOrDelete(thread, do_deletion);
    } else {
      TimeSeriesWrite(thread);
      thread->stats.Stop();
      thread->stats.Report("timeseries write");
    }
  }

  void Compact(ThreadState* thread) {
    DB* db = SelectDB(thread);
    CompactRangeOptions cro;
    cro.bottommost_level_compaction =
        BottommostLevelCompaction::kForceOptimized;
    cro.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
    db->CompactRange(cro, nullptr, nullptr);
  }

  void CompactAll() {
    CompactRangeOptions cro;
    cro.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
    if (db_.db != nullptr) {
      db_.db->CompactRange(cro, nullptr, nullptr);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      db_with_cfh.db->CompactRange(cro, nullptr, nullptr);
    }
  }

  void WaitForCompactionHelper(DBWithColumnFamilies& db) {
    fprintf(stdout, "waitforcompaction(%s): started\n",
            db.db->GetName().c_str());

    Status s = db.db->WaitForCompact(WaitForCompactOptions());

    fprintf(stdout, "waitforcompaction(%s): finished with status (%s)\n",
            db.db->GetName().c_str(), s.ToString().c_str());
  }

  void WaitForCompaction() {
    // Give background threads a chance to wake
    FLAGS_env->SleepForMicroseconds(5 * 1000000);

    if (db_.db != nullptr) {
      WaitForCompactionHelper(db_);
    } else {
      for (auto& db_with_cfh : multi_dbs_) {
        WaitForCompactionHelper(db_with_cfh);
      }
    }
  }

  bool CompactLevelHelper(DBWithColumnFamilies& db_with_cfh, int from_level) {
    std::vector<LiveFileMetaData> files;
    db_with_cfh.db->GetLiveFilesMetaData(&files);

    assert(from_level == 0 || from_level == 1);

    int real_from_level = from_level;
    if (real_from_level > 0) {
      // With dynamic leveled compaction the first level with data beyond L0
      // might not be L1.
      real_from_level = std::numeric_limits<int>::max();

      for (auto& f : files) {
        if (f.level > 0 && f.level < real_from_level) {
          real_from_level = f.level;
        }
      }

      if (real_from_level == std::numeric_limits<int>::max()) {
        fprintf(stdout, "compact%d found 0 files to compact\n", from_level);
        return true;
      }
    }

    // The goal is to compact from from_level to the level that follows it,
    // and with dynamic leveled compaction the next level might not be
    // real_from_level+1
    int next_level = std::numeric_limits<int>::max();

    std::vector<std::string> files_to_compact;
    for (auto& f : files) {
      if (f.level == real_from_level) {
        files_to_compact.push_back(f.name);
      } else if (f.level > real_from_level && f.level < next_level) {
        next_level = f.level;
      }
    }

    if (files_to_compact.empty()) {
      fprintf(stdout, "compact%d found 0 files to compact\n", from_level);
      return true;
    } else if (next_level == std::numeric_limits<int>::max()) {
      // There is no data beyond real_from_level. So we are done.
      fprintf(stdout, "compact%d found no data beyond L%d\n", from_level,
              real_from_level);
      return true;
    }

    fprintf(stdout, "compact%d found %d files to compact from L%d to L%d\n",
            from_level, static_cast<int>(files_to_compact.size()),
            real_from_level, next_level);

    ROCKSDB_NAMESPACE::CompactionOptions options;
    // Lets RocksDB use the configured compression for this level
    options.compression = ROCKSDB_NAMESPACE::kDisableCompressionOption;

    ROCKSDB_NAMESPACE::ColumnFamilyDescriptor cfDesc;
    db_with_cfh.db->DefaultColumnFamily()->GetDescriptor(&cfDesc);
    options.output_file_size_limit = cfDesc.options.target_file_size_base;

    Status status =
        db_with_cfh.db->CompactFiles(options, files_to_compact, next_level);
    if (!status.ok()) {
      // This can fail for valid reasons including the operation was aborted
      // or a filename is invalid because background compaction removed it.
      // Having read the current cases for which an error is raised I prefer
      // not to figure out whether an exception should be thrown here.
      fprintf(stderr, "compact%d CompactFiles failed: %s\n", from_level,
              status.ToString().c_str());
      return false;
    }
    return true;
  }

  void CompactLevel(int from_level) {
    if (db_.db != nullptr) {
      while (!CompactLevelHelper(db_, from_level)) {
        WaitForCompaction();
      }
    }
    for (auto& db_with_cfh : multi_dbs_) {
      while (!CompactLevelHelper(db_with_cfh, from_level)) {
        WaitForCompaction();
      }
    }
  }

  void Flush() {
    FlushOptions flush_opt;
    flush_opt.wait = true;

    if (db_.db != nullptr) {
      Status s;
      if (FLAGS_num_column_families > 1) {
        s = db_.db->Flush(flush_opt, db_.cfh);
      } else {
        s = db_.db->Flush(flush_opt, db_.db->DefaultColumnFamily());
      }

      if (!s.ok()) {
        fprintf(stderr, "Flush failed: %s\n", s.ToString().c_str());
        exit(1);
      }
    } else {
      for (const auto& db_with_cfh : multi_dbs_) {
        Status s;
        if (FLAGS_num_column_families > 1) {
          s = db_with_cfh.db->Flush(flush_opt, db_with_cfh.cfh);
        } else {
          s = db_with_cfh.db->Flush(flush_opt,
                                    db_with_cfh.db->DefaultColumnFamily());
        }

        if (!s.ok()) {
          fprintf(stderr, "Flush failed: %s\n", s.ToString().c_str());
          exit(1);
        }
      }
    }
    fprintf(stdout, "flush memtable\n");
  }

  void ResetStats() {
    if (db_.db != nullptr) {
      db_.db->ResetStats();
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      db_with_cfh.db->ResetStats();
    }
  }

  void PrintStatsHistory() {
    if (db_.db != nullptr) {
      PrintStatsHistoryImpl(db_.db, false);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStatsHistoryImpl(db_with_cfh.db, true);
    }
  }

  void PrintStatsHistoryImpl(DB* db, bool print_header) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }

    std::unique_ptr<StatsHistoryIterator> shi;
    Status s =
        db->GetStatsHistory(0, std::numeric_limits<uint64_t>::max(), &shi);
    if (!s.ok()) {
      fprintf(stdout, "%s\n", s.ToString().c_str());
      return;
    }
    assert(shi);
    while (shi->Valid()) {
      uint64_t stats_time = shi->GetStatsTime();
      fprintf(stdout, "------ %s ------\n",
              TimeToHumanString(static_cast<int>(stats_time)).c_str());
      for (auto& entry : shi->GetStatsMap()) {
        fprintf(stdout, " %" PRIu64 "   %s  %" PRIu64 "\n", stats_time,
                entry.first.c_str(), entry.second);
      }
      shi->Next();
    }
  }

  void CacheReportProblems() {
    auto debug_logger = std::make_shared<StderrLogger>(DEBUG_LEVEL);
    cache_->ReportProblems(debug_logger);
  }

  void PrintStats(const char* key) {
    if (db_.db != nullptr) {
      PrintStats(db_.db, key, false);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStats(db_with_cfh.db, key, true);
    }
  }

  void PrintStats(DB* db, const char* key, bool print_header = false) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }
    std::string stats;
    if (!db->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
  }

  void PrintStats(const std::vector<std::string>& keys) {
    if (db_.db != nullptr) {
      PrintStats(db_.db, keys);
    }
    for (const auto& db_with_cfh : multi_dbs_) {
      PrintStats(db_with_cfh.db, keys, true);
    }
  }

  void PrintStats(DB* db, const std::vector<std::string>& keys,
                  bool print_header = false) {
    if (print_header) {
      fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
    }

    for (const auto& key : keys) {
      std::string stats;
      if (!db->GetProperty(key, &stats)) {
        stats = "(failed)";
      }
      fprintf(stdout, "%s: %s\n", key.c_str(), stats.c_str());
    }
  }

  void Replay(ThreadState* thread) {
    if (db_.db != nullptr) {
      Replay(thread, &db_);
    }
  }

  void Replay(ThreadState* /*thread*/, DBWithColumnFamilies* db_with_cfh) {
    Status s;
    std::unique_ptr<TraceReader> trace_reader;
    s = NewFileTraceReader(FLAGS_env, EnvOptions(), FLAGS_trace_file,
                           &trace_reader);
    if (!s.ok()) {
      fprintf(
          stderr,
          "Encountered an error creating a TraceReader from the trace file. "
          "Error: %s\n",
          s.ToString().c_str());
      exit(1);
    }
    std::unique_ptr<Replayer> replayer;
    s = db_with_cfh->db->NewDefaultReplayer(db_with_cfh->cfh,
                                            std::move(trace_reader), &replayer);
    if (!s.ok()) {
      fprintf(stderr,
              "Encountered an error creating a default Replayer. "
              "Error: %s\n",
              s.ToString().c_str());
      exit(1);
    }
    s = replayer->Prepare();
    if (!s.ok()) {
      fprintf(stderr, "Prepare for replay failed. Error: %s\n",
              s.ToString().c_str());
    }
    s = replayer->Replay(
        ReplayOptions(static_cast<uint32_t>(FLAGS_trace_replay_threads),
                      FLAGS_trace_replay_fast_forward),
        nullptr);
    replayer.reset();
    if (s.ok()) {
      fprintf(stdout, "Replay completed from trace_file: %s\n",
              FLAGS_trace_file.c_str());
    } else {
      fprintf(stderr, "Replay failed. Error: %s\n", s.ToString().c_str());
    }
  }

  void Backup(ThreadState* thread) {
    DB* db = SelectDB(thread);
    std::unique_ptr<BackupEngineOptions> engine_options(
        new BackupEngineOptions(FLAGS_backup_dir));
    Status s;
    BackupEngine* backup_engine;
    if (FLAGS_backup_rate_limit > 0) {
      engine_options->backup_rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_backup_rate_limit, 100000 /* refill_period_us */,
          10 /* fairness */, RateLimiter::Mode::kAllIo));
    }
    // Build new backup of the entire DB
    engine_options->destroy_old_data = true;
    s = BackupEngine::Open(FLAGS_env, *engine_options, &backup_engine);
    assert(s.ok());
    s = backup_engine->CreateNewBackup(db);
    assert(s.ok());
    std::vector<BackupInfo> backup_info;
    backup_engine->GetBackupInfo(&backup_info);
    // Verify that a new backup is created
    assert(backup_info.size() == 1);
  }

  void Restore(ThreadState* /* thread */) {
    std::unique_ptr<BackupEngineOptions> engine_options(
        new BackupEngineOptions(FLAGS_backup_dir));
    if (FLAGS_restore_rate_limit > 0) {
      engine_options->restore_rate_limiter.reset(NewGenericRateLimiter(
          FLAGS_restore_rate_limit, 100000 /* refill_period_us */,
          10 /* fairness */, RateLimiter::Mode::kAllIo));
    }
    BackupEngineReadOnly* backup_engine;
    Status s =
        BackupEngineReadOnly::Open(FLAGS_env, *engine_options, &backup_engine);
    assert(s.ok());
    s = backup_engine->RestoreDBFromLatestBackup(FLAGS_restore_dir,
                                                 FLAGS_restore_dir);
    assert(s.ok());
    delete backup_engine;
  }
};

int db_bench_tool(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ConfigOptions config_options;
  static bool initialized = false;
  if (!initialized) {
    SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                    " [OPTIONS]...");
    SetVersionString(GetRocksVersionAsString(true));
    initialized = true;
  }
  ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_compaction_style_e =
      (ROCKSDB_NAMESPACE::CompactionStyle)FLAGS_compaction_style;
  if (FLAGS_statistics && !FLAGS_statistics_string.empty()) {
    fprintf(stderr,
            "Cannot provide both --statistics and --statistics_string.\n");
    exit(1);
  }
  if (!FLAGS_statistics_string.empty()) {
    Status s = Statistics::CreateFromString(config_options,
                                            FLAGS_statistics_string, &dbstats);
    if (dbstats == nullptr) {
      fprintf(stderr,
              "No Statistics registered matching string: %s status=%s\n",
              FLAGS_statistics_string.c_str(), s.ToString().c_str());
      exit(1);
    }
  }
  if (FLAGS_statistics) {
    dbstats = ROCKSDB_NAMESPACE::CreateDBStatistics();
  }
  if (dbstats) {
    dbstats->set_stats_level(static_cast<StatsLevel>(FLAGS_stats_level));
  }
  FLAGS_compaction_pri_e =
      (ROCKSDB_NAMESPACE::CompactionPri)FLAGS_compaction_pri;

  std::vector<std::string> fanout = ROCKSDB_NAMESPACE::StringSplit(
      FLAGS_max_bytes_for_level_multiplier_additional, ',');
  for (size_t j = 0; j < fanout.size(); j++) {
    FLAGS_max_bytes_for_level_multiplier_additional_v.push_back(
#ifndef CYGWIN
        std::stoi(fanout[j]));
#else
        stoi(fanout[j]));
#endif
  }

  FLAGS_compression_type_e =
      StringToCompressionType(FLAGS_compression_type.c_str());

  FLAGS_wal_compression_e =
      StringToCompressionType(FLAGS_wal_compression.c_str());

  FLAGS_compressed_secondary_cache_compression_type_e = StringToCompressionType(
      FLAGS_compressed_secondary_cache_compression_type.c_str());

  // Stacked BlobDB
  FLAGS_blob_db_compression_type_e =
      StringToCompressionType(FLAGS_blob_db_compression_type.c_str());

  int env_opts = !FLAGS_env_uri.empty() + !FLAGS_fs_uri.empty();
  if (env_opts > 1) {
    fprintf(stderr, "Error: --env_uri and --fs_uri are mutually exclusive\n");
    exit(1);
  }

  if (env_opts == 1) {
    Status s = Env::CreateFromUri(config_options, FLAGS_env_uri, FLAGS_fs_uri,
                                  &FLAGS_env, &env_guard);
    if (!s.ok()) {
      fprintf(stderr, "Failed creating env: %s\n", s.ToString().c_str());
      exit(1);
    }
  } else if (FLAGS_simulate_hdd || FLAGS_simulate_hybrid_fs_file != "") {
    //**TODO: Make the simulate fs something that can be loaded
    // from the ObjectRegistry...
    static std::shared_ptr<ROCKSDB_NAMESPACE::Env> composite_env =
        NewCompositeEnv(std::make_shared<SimulatedHybridFileSystem>(
            FileSystem::Default(), FLAGS_simulate_hybrid_fs_file,
            /*throughput_multiplier=*/
            int{FLAGS_simulate_hybrid_hdd_multipliers},
            /*is_full_fs_warm=*/FLAGS_simulate_hdd));
    FLAGS_env = composite_env.get();
  }

  // Let -readonly imply -use_existing_db
  FLAGS_use_existing_db |= FLAGS_readonly;

  if (FLAGS_build_info) {
    std::string build_info;
    std::cout << GetRocksBuildInfoAsString(build_info, true) << std::endl;
    // Similar to --version, nothing else will be done when this flag is set
    exit(0);
  }

  if (!FLAGS_seed) {
    uint64_t now = FLAGS_env->GetSystemClock()->NowMicros();
    seed_base = static_cast<int64_t>(now);
    fprintf(stdout, "Set seed to %" PRIu64 " because --seed was 0\n",
            *seed_base);
  } else {
    seed_base = FLAGS_seed;
  }

  if (FLAGS_use_existing_keys && !FLAGS_use_existing_db) {
    fprintf(stderr,
            "`-use_existing_db` must be true for `-use_existing_keys` to be "
            "settable\n");
    exit(1);
  }

  FLAGS_value_size_distribution_type_e =
      StringToDistributionType(FLAGS_value_size_distribution_type.c_str());

  // Note options sanitization may increase thread pool sizes according to
  // max_background_flushes/max_background_compactions/max_background_jobs
  FLAGS_env->SetBackgroundThreads(FLAGS_num_high_pri_threads,
                                  ROCKSDB_NAMESPACE::Env::Priority::HIGH);
  FLAGS_env->SetBackgroundThreads(FLAGS_num_bottom_pri_threads,
                                  ROCKSDB_NAMESPACE::Env::Priority::BOTTOM);
  FLAGS_env->SetBackgroundThreads(FLAGS_num_low_pri_threads,
                                  ROCKSDB_NAMESPACE::Env::Priority::LOW);

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    std::string default_db_path;
    FLAGS_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path;
  }

  if (FLAGS_backup_dir.empty()) {
    FLAGS_backup_dir = FLAGS_db + "/backup";
  }

  if (FLAGS_restore_dir.empty()) {
    FLAGS_restore_dir = FLAGS_db + "/restore";
  }

  if (FLAGS_stats_interval_seconds > 0) {
    // When both are set then FLAGS_stats_interval determines the frequency
    // at which the timer is checked for FLAGS_stats_interval_seconds
    FLAGS_stats_interval = 1000;
  }

  if (FLAGS_seek_missing_prefix && FLAGS_prefix_size <= 8) {
    fprintf(stderr, "prefix_size > 8 required by --seek_missing_prefix\n");
    exit(1);
  }

  ROCKSDB_NAMESPACE::Benchmark benchmark;
  benchmark.Run();

  if (FLAGS_print_malloc_stats) {
    std::string stats_string;
    ROCKSDB_NAMESPACE::DumpMallocStats(&stats_string);
    fprintf(stdout, "Malloc stats:\n%s\n", stats_string.c_str());
  }

  return 0;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
