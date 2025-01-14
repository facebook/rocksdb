// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "options/db_options.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
class Compressor;

// ImmutableCFOptions is a data struct used by RocksDB internal. It contains a
// subset of Options that should not be changed during the entire lifetime
// of DB. Raw pointers defined in this struct do not have ownership to the data
// they point to. Options contains std::shared_ptr to these data.
struct ImmutableCFOptions {
 public:
  static const char* kName() { return "ImmutableCFOptions"; }
  explicit ImmutableCFOptions();
  explicit ImmutableCFOptions(const ColumnFamilyOptions& cf_options);

  CompactionStyle compaction_style;

  CompactionPri compaction_pri;

  const Comparator* user_comparator;
  InternalKeyComparator internal_comparator;  // Only in Immutable

  std::shared_ptr<MergeOperator> merge_operator;

  const CompactionFilter* compaction_filter;

  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory;

  int min_write_buffer_number_to_merge;

  int max_write_buffer_number_to_maintain;

  int64_t max_write_buffer_size_to_maintain;

  bool inplace_update_support;

  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);

  std::shared_ptr<MemTableRepFactory> memtable_factory;

  Options::TablePropertiesCollectorFactories
      table_properties_collector_factories;

  // This options is required by PlainTableReader. May need to move it
  // to PlainTableOptions just like bloom_bits_per_key
  uint32_t bloom_locality;

  bool level_compaction_dynamic_level_bytes;

  int num_levels;

  bool optimize_filters_for_hits;

  bool force_consistency_checks;

  Temperature default_temperature;

  std::shared_ptr<const SliceTransform>
      memtable_insert_with_hint_prefix_extractor;

  std::vector<DbPath> cf_paths;

  std::shared_ptr<ConcurrentTaskLimiter> compaction_thread_limiter;

  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory;

  std::shared_ptr<Cache> blob_cache;

  bool persist_user_defined_timestamps;
};

struct ImmutableOptions : public ImmutableDBOptions, public ImmutableCFOptions {
  explicit ImmutableOptions();
  explicit ImmutableOptions(const Options& options);

  ImmutableOptions(const DBOptions& db_options,
                   const ColumnFamilyOptions& cf_options);

  ImmutableOptions(const ImmutableDBOptions& db_options,
                   const ImmutableCFOptions& cf_options);

  ImmutableOptions(const DBOptions& db_options,
                   const ImmutableCFOptions& cf_options);

  ImmutableOptions(const ImmutableDBOptions& db_options,
                   const ColumnFamilyOptions& cf_options);
};

struct MutableCFOptions {
  static const char* kName() { return "MutableCFOptions"; }
  MutableCFOptions();
  explicit MutableCFOptions(const ColumnFamilyOptions& options);
  explicit MutableCFOptions(const Options& options);

  // Must be called after any change to MutableCFOptions
  void RefreshDerivedOptions(int num_levels, CompactionStyle compaction_style);

  void RefreshDerivedOptions(const ImmutableCFOptions& ioptions) {
    RefreshDerivedOptions(ioptions.num_levels, ioptions.compaction_style);
  }

  int MaxBytesMultiplerAdditional(int level) const {
    if (level >=
        static_cast<int>(max_bytes_for_level_multiplier_additional.size())) {
      return 1;
    }
    return max_bytes_for_level_multiplier_additional[level];
  }

  void Dump(Logger* log) const;

  // Memtable related options
  size_t write_buffer_size;
  int max_write_buffer_number;
  size_t arena_block_size;
  double memtable_prefix_bloom_size_ratio;
  bool memtable_whole_key_filtering;
  size_t memtable_huge_page_size;
  size_t max_successive_merges;
  bool strict_max_successive_merges;
  size_t inplace_update_num_locks;
  // NOTE: if too many shared_ptr make their way into MutableCFOptions, the
  // copy performance might suffer enough to warrant aggregating them in an
  // immutable+copy-on-write sub-object managed through a single shared_ptr.
  std::shared_ptr<const SliceTransform> prefix_extractor;
  // [experimental]
  // Used to activate or deactive the Mempurge feature (memtable garbage
  // collection). (deactivated by default). At every flush, the total useful
  // payload (total entries minus garbage entries) is estimated as a ratio
  // [useful payload bytes]/[size of a memtable (in bytes)]. This ratio is then
  // compared to this `threshold` value:
  //     - if ratio<threshold: the flush is replaced by a mempurge operation
  //     - else: a regular flush operation takes place.
  // Threshold values:
  //   0.0: mempurge deactivated (default).
  //   1.0: recommended threshold value.
  //   >1.0 : aggressive mempurge.
  //   0 < threshold < 1.0: mempurge triggered only for very low useful payload
  //   ratios.
  // [experimental]
  double experimental_mempurge_threshold;

  // Compaction related options
  bool disable_auto_compactions;
  std::shared_ptr<TableFactory> table_factory;
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;
  int level0_file_num_compaction_trigger;
  int level0_slowdown_writes_trigger;
  int level0_stop_writes_trigger;
  uint64_t max_compaction_bytes;
  uint64_t target_file_size_base;
  int target_file_size_multiplier;
  uint64_t max_bytes_for_level_base;
  double max_bytes_for_level_multiplier;
  uint64_t ttl;
  uint64_t periodic_compaction_seconds;
  std::vector<int> max_bytes_for_level_multiplier_additional;
  CompactionOptionsFIFO compaction_options_fifo;
  CompactionOptionsUniversal compaction_options_universal;
  uint64_t preclude_last_level_data_seconds;
  uint64_t preserve_internal_time_seconds;

  // Blob file related options
  bool enable_blob_files;
  uint64_t min_blob_size;
  uint64_t blob_file_size;
  CompressionType blob_compression_type;
  std::shared_ptr<Compressor> blob_compressor;
  bool enable_blob_garbage_collection;
  double blob_garbage_collection_age_cutoff;
  double blob_garbage_collection_force_threshold;
  uint64_t blob_compaction_readahead_size;
  int blob_file_starting_level;
  PrepopulateBlobCache prepopulate_blob_cache;

  // Misc options
  uint64_t max_sequential_skip_in_iterations;
  bool paranoid_file_checks;
  bool report_bg_io_stats;
  CompressionType compression;
  std::shared_ptr<Compressor> compressor;
  CompressionType bottommost_compression;
  std::shared_ptr<Compressor> bottommost_compressor;
  CompressionOptions compression_opts;
  CompressionOptions bottommost_compression_opts;
  Temperature last_level_temperature;
  Temperature default_write_temperature;
  uint32_t memtable_protection_bytes_per_key;
  uint8_t block_protection_bytes_per_key;
  bool paranoid_memory_checks;

  uint64_t sample_for_compression;
  std::vector<CompressionType> compression_per_level;
  std::vector<std::shared_ptr<Compressor>> compressor_per_level;
  uint32_t memtable_max_range_deletions;
  uint32_t bottommost_file_compaction_delay;
  uint32_t uncache_aggressiveness;

  // Derived options
  // Per-level target file size.
  std::vector<uint64_t> max_file_size;
  // Compression settings derived from CompressionType or Compressor options
  std::shared_ptr<Compressor> derived_compressor;
  std::shared_ptr<Compressor> derived_bottommost_compressor;
  std::shared_ptr<Compressor> derived_blob_compressor;
  std::vector<std::shared_ptr<Compressor>> derived_compressor_per_level;
};

uint64_t MultiplyCheckOverflow(uint64_t op1, double op2);

// Get the max file size in a given level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options, int level,
                             CompactionStyle compaction_style,
                             int base_level = 1,
                             bool level_compaction_dynamic_level_bytes = false);

// Get the max size of an L0 file for which we will pin its meta-blocks when
// `pin_l0_filter_and_index_blocks_in_cache` is set.
size_t MaxFileSizeForL0MetaPin(const MutableCFOptions& cf_options);

Status GetStringFromMutableCFOptions(const ConfigOptions& config_options,
                                     const MutableCFOptions& mutable_opts,
                                     std::string* opt_string);

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableCFOptions* new_options);

#ifndef NDEBUG
std::vector<std::string> TEST_GetImmutableInMutableCFOptions();
extern bool TEST_allowSetOptionsImmutableInMutable;
#endif

}  // namespace ROCKSDB_NAMESPACE
