// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <vector>
#include "rocksdb/options.h"

namespace rocksdb {

// ImmutableCFOptions is a data struct used by RocksDB internal. It contains a
// subset of Options that should not be changed during the entire lifetime
// of DB. You shouldn't need to access this data structure unless you are
// implementing a new TableFactory.
struct ImmutableCFOptions {
  explicit ImmutableCFOptions(const Options& options);

  CompactionStyle compaction_style;

  CompactionOptionsUniversal compaction_options_universal;

  const SliceTransform* prefix_extractor;

  const Comparator* comparator;

  MergeOperator* merge_operator;

  Logger* info_log;

  Statistics* statistics;

  InfoLogLevel info_log_level;

  Env* env;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads;

  // Allow the OS to mmap file for writing. Default: false
  bool allow_mmap_writes;

  std::vector<DbPath> db_paths;

  TableFactory* table_factory;

  Options::TablePropertiesCollectorFactories
    table_properties_collector_factories;

  bool advise_random_on_open;

  // This options is required by PlainTableReader. May need to move it
  // to PlainTalbeOptions just like bloom_bits_per_key
  uint32_t bloom_locality;

  bool purge_redundant_kvs_while_flush;

  uint32_t min_partial_merge_operands;

  bool disable_data_sync;

  bool use_fsync;

  CompressionType compression;

  std::vector<CompressionType> compression_per_level;

  CompressionOptions compression_opts;
};

}  // namespace rocksdb
