//  Copyright (c) 2015, Microsoft, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.


#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/options_converter.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "util/statistics.h"
#include "util/string_util.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"
#include "port/win/port_win.h"

#include <gflags/gflags.h>

using namespace gflags;

DEFINE_int32(num_column_families, 1, "Number of Column Families to use.");

DEFINE_int32(bloom_locality, 0, "Control bloom filter probes locality.");

DEFINE_int32(threads, 1, "Number of concurrent threads to run.");

DEFINE_int32(value_size, 100, "Size of each value.");

DEFINE_bool(use_uint64_comparator, false, "use Uint64 user comparator.");

DEFINE_int32(key_size, 16, "size of each key.");

DEFINE_int32(num_multi_db, 0,
             "Number of DBs used in the benchmark. 0 means single DB.");

DEFINE_double(compression_ratio, 0.5, "Arrange to generate values that shrink"
              " to this fraction of their original size after compression.");

DEFINE_bool(histogram, false, "Print histogram of operation timings.");

DEFINE_int64(write_buffer_size, rocksdb::Options().write_buffer_size,
             "Number of bytes to buffer in memtable before compacting.");

DEFINE_int32(max_write_buffer_number,
             rocksdb::Options().max_write_buffer_number,
             "The number of in-memory memtables. Each memtable is of size"
             "write_buffer_size.");

DEFINE_int32(min_write_buffer_number_to_merge,
             rocksdb::Options().min_write_buffer_number_to_merge,
             "The minimum number of write buffers that will be merged together"
             " before writing to storage. This is cheap because it is an"
             " in-memory merge. If this feature is not enabled, then all these"
             " write buffers are flushed to L0 as separate files and this"
             " increases read amplification because a get request has to check"
             " in all of these files. Also, an in-memory merge may result in"
             " writing less data to storage if there are duplicate records"
             " in each of these individual write buffers.");

DEFINE_int32(max_background_compactions,
             rocksdb::Options().max_background_compactions,
             "The maximum number of concurrent background compactions"
             " that can occur in parallel.");

DEFINE_int32(max_background_flushes,
             rocksdb::Options().max_background_flushes,
             "The maximum number of concurrent background flushes"
             " that can occur in parallel.");

static rocksdb::CompactionStyle FLAGS_compaction_style_e;
DEFINE_int32(compaction_style, (int32_t) rocksdb::Options().compaction_style,
             "style of compaction: level-based vs universal.");

DEFINE_int32(universal_size_ratio, 0,
             "Percentage flexibility while comparing file size"
             " (for universal compaction only).");

DEFINE_int32(universal_min_merge_width, 0, "The minimum number of files in a"
             " single compaction run (for universal compaction only).");

DEFINE_int32(universal_max_merge_width, 0, "The max number of files to compact"
             " in universal style compaction.");

DEFINE_int32(universal_max_size_amplification_percent, 0,
             "The max size amplification for universal style compaction.");

DEFINE_int32(universal_compression_size_percent, -1,
             "The percentage of the database to compress for universal "
             "compaction. -1 means compress everything.");

DEFINE_int64(cache_size, -1, "Number of bytes to use as a cache of uncompressed"
             "data. Negative means use default settings.");

DEFINE_int32(block_size, rocksdb::BlockBasedTableOptions().block_size,
             "Number of bytes in a block.");

DEFINE_int32(block_restart_interval,
             rocksdb::BlockBasedTableOptions().block_restart_interval,
             "Number of keys between restart points "
             "for delta encoding of keys.");

DEFINE_int64(compressed_cache_size, -1,
             "Number of bytes to use as a cache of compressed data.");

DEFINE_int32(open_files, rocksdb::Options().max_open_files,
             "Maximum number of files to keep open at the same time"
             " (use default if == 0).");

DEFINE_int32(bloom_bits, -1, "Bloom filter bits per key. Negative means"
             " use default settings.");
DEFINE_int32(memtable_bloom_bits, 0, "Bloom filter bits per key for memtable. "
             "Negative means no bloom filter.");

DEFINE_bool(use_existing_db, false, "If true, do not destroy the existing"
            " database.  If you set this flag and also specify a benchmark that"
            " wants a fresh database, that benchmark will fail.");

DEFINE_string(db, "", "Use the db with the following name.");

static bool ValidateCacheNumshardbits(const char* flagname, int32_t value) {
  if (value >= 20) {
    char errMsg[256];
    sprintf_s(errMsg, sizeof(errMsg), "Invalid value for --%s: %d, must be < 20",
            flagname, value);
    throw std::exception(errMsg);
  }
  return true;
}
DEFINE_int32(cache_numshardbits, -1, "Number of shards for the block cache"
             " is 2 ** cache_numshardbits. Negative means use default settings."
             " This is applied only if FLAGS_cache_size is non-negative.");

DEFINE_int32(cache_remove_scan_count_limit, 32, "");

DEFINE_bool(verify_checksum, false, "Verify checksum for every block read"
            " from storage");

DEFINE_bool(statistics, false, "Database statistics");
static class std::shared_ptr<rocksdb::Statistics> dbstats;

DEFINE_bool(sync, false, "Sync all writes to disk");

DEFINE_bool(disable_data_sync, false, "If true, do not wait until data is"
            " synced to disk.");

DEFINE_bool(use_fsync, false, "If true, issue fsync instead of fdatasync.");

DEFINE_bool(disable_wal, false, "If true, do not write WAL for write.");

DEFINE_string(wal_dir, "", "If not empty, use the given dir for WAL.");

DEFINE_int32(num_levels, 7, "The total number of levels.");

DEFINE_int32(target_file_size_base, 2 * 1048576, "Target file size at level-1.");

DEFINE_int32(target_file_size_multiplier, 1,
             "A multiplier to compute target level-N file size (N >= 2).");

DEFINE_uint64(max_bytes_for_level_base,  10 * 1048576, "Max bytes for level-1.");

DEFINE_int32(max_bytes_for_level_multiplier, 10,
             "A multiplier to compute max bytes for level-N (N >= 2).");

static std::vector<int> FLAGS_max_bytes_for_level_multiplier_additional_v;
DEFINE_string(max_bytes_for_level_multiplier_additional, "",
              "A vector that specifies additional fanout per level.");

DEFINE_int32(level0_stop_writes_trigger, 12, "Number of files in level-0"
             " that will trigger put stop.");

DEFINE_int32(level0_slowdown_writes_trigger, 8, "Number of files in level-0"
             " that will slow down writes.");

DEFINE_int32(level0_file_num_compaction_trigger, 4, "Number of files in level-0"
             " when compactions start.");

static bool ValidateInt32Percent(const char* flagname, int32_t value) {
  if (value <= 0 || value>=100) {
    char errMsg[256];
    sprintf_s(errMsg, sizeof(errMsg), "Invalid value for --%s: %d, 0< pct <100 ",
            flagname, value);
    throw std::exception(errMsg);
  }
  return true;
}

DEFINE_uint64(delete_obsolete_files_period_micros, 0, "Option to delete "
              "obsolete files periodically. 0 means that obsolete files are"
              " deleted after every compaction run.");

namespace {
enum rocksdb::CompressionType StringToCompressionType(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "none"))
    return rocksdb::kNoCompression;
  else if (!strcasecmp(ctype, "snappy"))
    return rocksdb::kSnappyCompression;
  else if (!strcasecmp(ctype, "zlib"))
    return rocksdb::kZlibCompression;
  else if (!strcasecmp(ctype, "bzip2"))
    return rocksdb::kBZip2Compression;
  else if (!strcasecmp(ctype, "lz4"))
    return rocksdb::kLZ4Compression;
  else if (!strcasecmp(ctype, "lz4hc"))
    return rocksdb::kLZ4HCCompression;

  char errMsg[256];
  sprintf_s(errMsg, sizeof(errMsg), "Cannot parse compression type '%s'", ctype);
  throw std::exception(errMsg);
}
}  // namespace

DEFINE_string(compression_type, "snappy",
              "Algorithm to use to compress the database");
static enum rocksdb::CompressionType FLAGS_compression_type_e =
    rocksdb::kSnappyCompression;

DEFINE_int32(compression_level, -1,
             "Compression level. For zlib this should be -1 for the "
             "default level, or between 0 and 9.");

static bool ValidateCompressionLevel(const char* flagname, int32_t value) {
  if (value < -1 || value > 9) {
    char errMsg[256];
    sprintf_s(errMsg, sizeof(errMsg), "Invalid value for --%s: %d, must be between -1 and 9",
            flagname, value);
    throw std::exception(errMsg);
  }
  return true;
}

static const bool FLAGS_compression_level_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_compression_level, &ValidateCompressionLevel);

DEFINE_int32(min_level_to_compress, -1, "If non-negative, compression starts"
             " from this level. Levels with number < min_level_to_compress are"
             " not compressed. Otherwise, apply compression_type to "
             "all levels.");

static bool ValidateTableCacheNumshardbits(const char* flagname,
                                           int32_t value) {
  if (0 >= value || value > 20) {
    char errMsg[256];
    sprintf_s(errMsg, sizeof(errMsg), "Invalid value for --%s: %d, must be  0 < val <= 20",
            flagname, value);
    throw std::exception(errMsg);
  }
  return true;
}
DEFINE_int32(table_cache_numshardbits, 4, "");

// posix or hdfs environment
static rocksdb::Env* FLAGS_env = rocksdb::Env::Default();

DEFINE_int64(stats_interval, 0, "Stats are reported every N operations when "
             "this is greater than zero. When 0 the interval grows over time.");

DEFINE_int32(stats_per_interval, 0, "Reports additional stats per interval when"
             " this is greater than 0.");

static bool ValidateRateLimit(const char* flagname, double value) {
  static const /*constexpr*/ double EPSILON = 1e-10;
  if ( value < -EPSILON ) {
    char errMsg[256];
    sprintf_s(errMsg, sizeof(errMsg), "Invalid value for --%s: %12.6f, must be >= 0.0",
            flagname, value);
    throw std::exception(errMsg);
  }
  return true;
}
DEFINE_double(soft_rate_limit, 0.0, "");

DEFINE_double(hard_rate_limit, 0.0, "When not equal to 0 this make threads "
              "sleep at each stats reporting interval until the compaction"
              " score for all levels is less than or equal to this value.");

DEFINE_int32(rate_limit_delay_max_milliseconds, 1000,
             "When hard_rate_limit is set then this is the max time a put will"
             " be stalled.");

DEFINE_int32(max_grandparent_overlap_factor, 10, "Control maximum bytes of "
             "overlaps in grandparent (i.e., level+2) before we stop building a"
             " single file in a level->level+1 compaction.");

DEFINE_bool(readonly, false, "Run read only benchmarks.");

DEFINE_bool(disable_auto_compactions, false, "Do not auto trigger compactions");

DEFINE_int32(source_compaction_factor, 1, "Cap the size of data in level-K for"
             " a compaction run that compacts Level-K with Level-(K+1) (for"
             " K >= 1)");

DEFINE_uint64(wal_ttl_seconds, 0, "Set the TTL for the WAL Files in seconds.");
DEFINE_uint64(wal_size_limit_MB, 0, "Set the size limit for the WAL Files"
              " in MB.");

DEFINE_bool(bufferedio, rocksdb::EnvOptions().use_os_buffer,
            "Allow buffered io using OS buffers");

DEFINE_bool(mmap_read, rocksdb::EnvOptions().use_mmap_reads,
            "Allow reads to occur via mmap-ing files");

DEFINE_bool(mmap_write, rocksdb::EnvOptions().use_mmap_writes,
            "Allow writes to occur via mmap-ing files");

DEFINE_bool(advise_random_on_open, rocksdb::Options().advise_random_on_open,
            "Advise random access on table file open");

DEFINE_bool(use_tailing_iterator, false,
            "Use tailing iterator to access a series of keys instead of get");

DEFINE_int64(iter_refresh_interval_us, -1,
             "How often to refresh iterators. Disable refresh when -1");

DEFINE_bool(use_adaptive_mutex, rocksdb::Options().use_adaptive_mutex,
            "Use adaptive mutex");

DEFINE_uint64(bytes_per_sync,  rocksdb::Options().bytes_per_sync,
              "Allows OS to incrementally sync files to disk while they are"
              " being written, in the background. Issue one request for every"
              " bytes_per_sync written. 0 turns it off.");
DEFINE_bool(filter_deletes, false, " On true, deletes use bloom-filter and drop"
            " the delete if key not present");

DEFINE_int32(max_successive_merges, 0, "Maximum number of successive merge"
             " operations on a key in the memtable");

static bool ValidatePrefixSize(const char* flagname, int32_t value) {
  if (value < 0 || value>=2000000000) {
    char errMsg[256];
    sprintf_s(errMsg, sizeof(errMsg), "Invalid value for --%s: %d. 0<= PrefixSize <=2000000000",
            flagname, value);
    throw std::exception(errMsg);
  }
  return true;
}
DEFINE_int32(prefix_size, 0, "control the prefix size for HashSkipList and "
             "plain table");
DEFINE_int64(keys_per_prefix, 0, "control average number of keys generated "
             "per prefix, 0 means no special handling of the prefix, "
             "i.e. use the prefix comes with the generated random number.");
DEFINE_bool(enable_io_prio, false, "Lower the background flush/compaction "
            "threads' IO priority");

enum RepFactory {
  kSkipList,
  kPrefixHash,
  kVectorRep,
  kHashLinkedList,
  kCuckoo
};

namespace {
enum RepFactory StringToRepFactory(const char* ctype) {
  assert(ctype);

  if (!strcasecmp(ctype, "skip_list"))
    return kSkipList;
  else if (!strcasecmp(ctype, "prefix_hash"))
    return kPrefixHash;
  else if (!strcasecmp(ctype, "vector"))
    return kVectorRep;
  else if (!strcasecmp(ctype, "hash_linkedlist"))
    return kHashLinkedList;
  else if (!strcasecmp(ctype, "cuckoo"))
    return kCuckoo;

  char errMsg[256];
  sprintf_s(errMsg, sizeof(errMsg), "Cannot parse memreptable %s", ctype);
  throw std::exception(errMsg);
}
}  // namespace

static enum RepFactory FLAGS_rep_factory;
DEFINE_string(memtablerep, "skip_list", "");
DEFINE_int64(hash_bucket_count, 1024 * 1024, "hash bucket count");
DEFINE_bool(use_plain_table, false, "if use plain table "
            "instead of block-based table format");
DEFINE_bool(use_cuckoo_table, false, "if use cuckoo table format");
DEFINE_double(cuckoo_hash_ratio, 0.9, "Hash ratio for Cuckoo SST table.");
DEFINE_bool(use_hash_search, false, "if use kHashSearch "
            "instead of kBinarySearch. "
            "This is valid if only we use BlockTable");
DEFINE_bool(use_block_based_filter, false, "if use kBlockBasedFilter "
            "instead of kFullFilter for filter block. "
            "This is valid if only we use BlockTable");
DEFINE_string(merge_operator, "", "The merge operator to use with the database."
              "If a new merge operator is specified, be sure to use fresh"
              " database The possible merge operators are defined in"
              " utilities/merge_operators.h");

static const bool FLAGS_soft_rate_limit_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_soft_rate_limit, &ValidateRateLimit);

static const bool FLAGS_hard_rate_limit_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_hard_rate_limit, &ValidateRateLimit);

static const bool FLAGS_prefix_size_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_prefix_size, &ValidatePrefixSize);

static const bool FLAGS_cache_numshardbits_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_cache_numshardbits,
                          &ValidateCacheNumshardbits);

DEFINE_int32(disable_seek_compaction, false,
             "Not used, left here for backwards compatibility");

static const bool FLAGS_table_cache_numshardbits_dummy __attribute__((unused)) =
    RegisterFlagValidator(&FLAGS_table_cache_numshardbits,
                          &ValidateTableCacheNumshardbits);

namespace rocksdb {Options ConvertStringToOptions(const std::string& optionsStr) {
  std::vector<std::string> tokens = stringSplit(optionsStr, ' ');
      
  int argc = tokens.size();
  char** argv = new char*[argc];
  for (int i = 0; i < argc; ++i) {
    argv[i] = new char[tokens[i].size() + 1];
    std::strcpy(argv[i], tokens[i].c_str());
  }
 
  Options options = ConvertStringToOptions(argc, argv);
 
  for (int i = 0; i < argc; ++i) {
    delete[] argv[i];
  }
  delete[] argv;
 
  return options;
}

Options ConvertStringToOptions(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, false);

  FLAGS_compaction_style_e = (rocksdb::CompactionStyle) FLAGS_compaction_style;
  if (FLAGS_statistics) {
    dbstats = rocksdb::CreateDBStatistics();
  }

  std::vector<std::string> fanout =
    rocksdb::stringSplit(FLAGS_max_bytes_for_level_multiplier_additional, ',');
  for (unsigned int j= 0; j < fanout.size(); j++) {
    FLAGS_max_bytes_for_level_multiplier_additional_v.push_back(
      std::stoi(fanout[j]));
  }

  FLAGS_compression_type_e =
    StringToCompressionType(FLAGS_compression_type.c_str());

  FLAGS_rep_factory = StringToRepFactory(FLAGS_memtablerep.c_str());

  // The number of background threads should be at least as much the
  // max number of concurrent compactions.
  FLAGS_env->SetBackgroundThreads(FLAGS_max_background_compactions);
  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db.empty()) {
    std::string default_db_path;
    rocksdb::Env::Default()->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path;
  }

  Options options;
  options.create_if_missing = !FLAGS_use_existing_db;
  options.create_missing_column_families = FLAGS_num_column_families > 1;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.max_write_buffer_number = FLAGS_max_write_buffer_number;
  options.min_write_buffer_number_to_merge =
    FLAGS_min_write_buffer_number_to_merge;
  options.max_background_compactions = FLAGS_max_background_compactions;
  options.max_background_flushes = FLAGS_max_background_flushes;
  options.compaction_style = FLAGS_compaction_style_e;
  if (FLAGS_prefix_size != 0) {
    options.prefix_extractor.reset(
        NewFixedPrefixTransform(FLAGS_prefix_size));
  }
  if (FLAGS_use_uint64_comparator) {
    options.comparator = test::Uint64Comparator();
    if (FLAGS_key_size != 8) {
      throw std::exception("Using Uint64 comparator but key size is not 8.");
    }
  }
  options.memtable_prefix_bloom_bits = FLAGS_memtable_bloom_bits;
  options.bloom_locality = FLAGS_bloom_locality;
  options.max_open_files = FLAGS_open_files;
  options.statistics = dbstats;
  if (FLAGS_enable_io_prio) {
    FLAGS_env->LowerThreadPoolIOPriority(Env::LOW);
    FLAGS_env->LowerThreadPoolIOPriority(Env::HIGH);
  }
  options.env = FLAGS_env;
  options.disableDataSync = FLAGS_disable_data_sync;
  options.use_fsync = FLAGS_use_fsync;
  options.wal_dir = FLAGS_wal_dir;
  options.num_levels = FLAGS_num_levels;
  options.target_file_size_base = FLAGS_target_file_size_base;
  options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
  options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
  options.max_bytes_for_level_multiplier =
      FLAGS_max_bytes_for_level_multiplier;
  options.filter_deletes = FLAGS_filter_deletes;
  if ((FLAGS_prefix_size == 0) && (FLAGS_rep_factory == kPrefixHash ||
                                    FLAGS_rep_factory == kHashLinkedList)) {
    const char* errMsg = "prefix_size should be non-zero if PrefixHash or "
                          "HashLinkedList memtablerep is used";
    throw std::exception(errMsg);
  }
  switch (FLAGS_rep_factory) {
    case kPrefixHash:
      options.memtable_factory.reset(NewHashSkipListRepFactory(
          FLAGS_hash_bucket_count));
      break;
    case kSkipList:
      // no need to do anything
      break;
    case kHashLinkedList:
      options.memtable_factory.reset(NewHashLinkListRepFactory(
          FLAGS_hash_bucket_count));
      break;
    case kVectorRep:
      options.memtable_factory.reset(
        new VectorRepFactory
      );
      break;
    case kCuckoo:
      options.memtable_factory.reset(NewHashCuckooRepFactory(
          options.write_buffer_size, FLAGS_key_size + FLAGS_value_size));
      break;
  }
  if (FLAGS_use_plain_table) {
    if (FLAGS_rep_factory != kPrefixHash &&
        FLAGS_rep_factory != kHashLinkedList) {
      throw std::exception("Waring: plain table is used with skipList");

    }
    if (!FLAGS_mmap_read && !FLAGS_mmap_write) {
      throw std::exception("plain table format requires mmap to operate");
    }

    int bloom_bits_per_key = FLAGS_bloom_bits;
    if (bloom_bits_per_key < 0) {
      bloom_bits_per_key = 0;
    }

    PlainTableOptions plain_table_options;
    plain_table_options.user_key_len = FLAGS_key_size;
    plain_table_options.bloom_bits_per_key = bloom_bits_per_key;
    plain_table_options.hash_table_ratio = 0.75;
    options.table_factory = std::shared_ptr<TableFactory>(
        NewPlainTableFactory(plain_table_options));
  } else if (FLAGS_use_cuckoo_table) {
    if (FLAGS_cuckoo_hash_ratio > 1 || FLAGS_cuckoo_hash_ratio < 0) {
      throw std::exception("Invalid cuckoo_hash_ratio");
    }
    options.table_factory = std::shared_ptr<TableFactory>(
        NewCuckooTableFactory(FLAGS_cuckoo_hash_ratio));
  } else {
    BlockBasedTableOptions block_based_options;
    if (FLAGS_use_hash_search) {
      if (FLAGS_prefix_size == 0) {
        throw std::exception("prefix_size not assigned when enable use_hash_search ");
      }
      block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
    } else {
        block_based_options.index_type = BlockBasedTableOptions::kBinarySearch;
    }
    std::shared_ptr<Cache> cache_ = 
        FLAGS_cache_size >= 0 ?
        (FLAGS_cache_numshardbits >= 1 ?
        NewLRUCache(FLAGS_cache_size, FLAGS_cache_numshardbits,
                    FLAGS_cache_remove_scan_count_limit) :
        NewLRUCache(FLAGS_cache_size)) : nullptr;
    if (cache_ == nullptr) {
      block_based_options.no_block_cache = true;
    }
    block_based_options.block_cache = cache_;
    block_based_options.block_cache_compressed = FLAGS_compressed_cache_size >= 0 ?
        (FLAGS_cache_numshardbits >= 1 ?
        NewLRUCache(FLAGS_compressed_cache_size, FLAGS_cache_numshardbits) :
        NewLRUCache(FLAGS_compressed_cache_size)) : nullptr;
    block_based_options.block_size = FLAGS_block_size;
    block_based_options.block_restart_interval = FLAGS_block_restart_interval;
    block_based_options.filter_policy.reset(FLAGS_bloom_bits >= 0 ? 
        NewBloomFilterPolicy(FLAGS_bloom_bits, FLAGS_use_block_based_filter):
        nullptr);
    options.table_factory.reset(
        NewBlockBasedTableFactory(block_based_options));
  }
  if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() > 0) {
    if (FLAGS_max_bytes_for_level_multiplier_additional_v.size() !=
        (unsigned int)FLAGS_num_levels) {
      
      char errMsg[256];
      sprintf_s(errMsg, sizeof(errMsg), "Insufficient number of fanouts specified %d",
              (int)FLAGS_max_bytes_for_level_multiplier_additional_v.size());
      throw std::exception(errMsg);
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
  options.compression_opts.level = FLAGS_compression_level;
  options.WAL_ttl_seconds = FLAGS_wal_ttl_seconds;
  options.WAL_size_limit_MB = FLAGS_wal_size_limit_MB;
  if (FLAGS_min_level_to_compress >= 0) {
    assert(FLAGS_min_level_to_compress <= FLAGS_num_levels);
    options.compression_per_level.resize(FLAGS_num_levels);
    for (int i = 0; i < FLAGS_min_level_to_compress; i++) {
      options.compression_per_level[i] = kNoCompression;
    }
    for (int i = FLAGS_min_level_to_compress;
          i < FLAGS_num_levels; i++) {
      options.compression_per_level[i] = FLAGS_compression_type_e;
    }
  }
  options.delete_obsolete_files_period_micros =
    FLAGS_delete_obsolete_files_period_micros;
  options.soft_rate_limit = FLAGS_soft_rate_limit;
  options.hard_rate_limit = FLAGS_hard_rate_limit;
  options.rate_limit_delay_max_milliseconds =
    FLAGS_rate_limit_delay_max_milliseconds;
  options.table_cache_numshardbits = FLAGS_table_cache_numshardbits;
  options.max_grandparent_overlap_factor =
    FLAGS_max_grandparent_overlap_factor;
  options.disable_auto_compactions = FLAGS_disable_auto_compactions;
  options.source_compaction_factor = FLAGS_source_compaction_factor;

  // fill storage options
  options.allow_os_buffer = FLAGS_bufferedio;
  options.allow_mmap_reads = FLAGS_mmap_read;
  options.allow_mmap_writes = FLAGS_mmap_write;
  options.advise_random_on_open = FLAGS_advise_random_on_open;
  //options.access_hint_on_compaction_start = FLAGS_compaction_fadvice_e;
  options.use_adaptive_mutex = FLAGS_use_adaptive_mutex;
  options.bytes_per_sync = FLAGS_bytes_per_sync;

  // merge operator options
  options.merge_operator = MergeOperators::CreateFromStringId(
      FLAGS_merge_operator);
  if (options.merge_operator == nullptr && !FLAGS_merge_operator.empty()) {
    char errMsg[256];
    sprintf_s(errMsg, sizeof(errMsg), "invalid merge operator: %s",
            FLAGS_merge_operator.c_str());
    throw std::exception(errMsg);
  }
  options.max_successive_merges = FLAGS_max_successive_merges;

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
  if (FLAGS_min_level_to_compress >= 0) {
    options.compression_per_level.clear();
  }

  return options;
}
   
}