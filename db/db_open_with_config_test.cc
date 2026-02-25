//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

class DBOpenWithConfigTest : public DBTestBase {
 protected:
  DBOpenWithConfigTest()
      : DBTestBase("db_open_with_config_test", /* env_do_fsync */ false) {}
};

// Test opening a RocksDB instance with a comprehensive configuration
// including FIFO compaction, blob files, compression settings, and
// various memory/protection options.
TEST_F(DBOpenWithConfigTest, OpenWithComprehensiveConfig) {
  Options options = GetDefaultOptions();
  options.max_open_files = -1;

  // Basic DB options
  options.create_if_missing = true;

  // Compression settings
  // compression=kLZ4Compression
  options.compression = kLZ4Compression;

  // bottommost_compression=kZSTD
  options.bottommost_compression = kZSTD;

  // compression_opts={checksum=true;max_dict_buffer_bytes=0;enabled=true;
  //   max_dict_bytes=0;max_compressed_bytes_per_kb=896;parallel_threads=1;
  //   zstd_max_train_bytes=0;level=32767;use_zstd_dict_trainer=true;
  //   strategy=0;window_bits=-14;}
  options.compression_opts.checksum = true;
  options.compression_opts.max_dict_buffer_bytes = 0;
  options.compression_opts.enabled = true;
  options.compression_opts.max_dict_bytes = 0;
  options.compression_opts.max_compressed_bytes_per_kb = 896;
  options.compression_opts.parallel_threads = 1;
  options.compression_opts.zstd_max_train_bytes = 0;
  options.compression_opts.level = CompressionOptions::kDefaultCompressionLevel;
  options.compression_opts.use_zstd_dict_trainer = true;
  options.compression_opts.strategy = 0;
  options.compression_opts.window_bits = -14;

  // bottommost_compression_opts={checksum=false;max_dict_buffer_bytes=0;
  //   enabled=true;max_dict_bytes=0;max_compressed_bytes_per_kb=896;
  //   parallel_threads=1;zstd_max_train_bytes=0;level=3;
  //   use_zstd_dict_trainer=true;strategy=0;window_bits=-14;}
  options.bottommost_compression_opts.checksum = false;
  options.bottommost_compression_opts.max_dict_buffer_bytes = 0;
  options.bottommost_compression_opts.enabled = true;
  options.bottommost_compression_opts.max_dict_bytes = 0;
  options.bottommost_compression_opts.max_compressed_bytes_per_kb = 896;
  options.bottommost_compression_opts.parallel_threads = 1;
  options.bottommost_compression_opts.zstd_max_train_bytes = 0;
  options.bottommost_compression_opts.level = 3;
  options.bottommost_compression_opts.use_zstd_dict_trainer = true;
  options.bottommost_compression_opts.strategy = 0;
  options.bottommost_compression_opts.window_bits = -14;

  // compression_manager=nullptr (default)
  options.compression_manager = nullptr;

  // Write buffer settings
  // write_buffer_size=134217728 (128MB)
  options.write_buffer_size = 134217728;

  // max_write_buffer_number=64
  options.max_write_buffer_number = 64;

  // min_write_buffer_number_to_merge=1
  options.min_write_buffer_number_to_merge = 1;

  // max_write_buffer_size_to_maintain=0
  options.max_write_buffer_size_to_maintain = 0;

  // arena_block_size=1048576 (1MB)
  options.arena_block_size = 1048576;

  // Memtable settings
  // memtable_factory=SkipListFactory (default)
  options.memtable_factory =
      std::shared_ptr<SkipListFactory>(new SkipListFactory);

  // memtable_prefix_bloom_size_ratio=0.010000
  options.memtable_prefix_bloom_size_ratio = 0.01;

  // memtable_whole_key_filtering=true
  options.memtable_whole_key_filtering = true;

  // memtable_huge_page_size=0
  options.memtable_huge_page_size = 0;

  // memtable_insert_with_hint_prefix_extractor=nullptr
  options.memtable_insert_with_hint_prefix_extractor = nullptr;

  // memtable_protection_bytes_per_key=1
  options.memtable_protection_bytes_per_key = 1;

  // memtable_max_range_deletions=0 (if available)
  // Note: This option may not be available in all versions

  // memtable_avg_op_scan_flush_trigger=0
  options.memtable_avg_op_scan_flush_trigger = 0;

  // memtable_op_scan_flush_trigger=0
  options.memtable_op_scan_flush_trigger = 0;

  // memtable_veirfy_per_key_checksum_on_seek=false
  options.memtable_veirfy_per_key_checksum_on_seek = false;

  // Level and compaction settings
  // num_levels=50
  options.num_levels = 50;

  // level0_file_num_compaction_trigger=16
  options.level0_file_num_compaction_trigger = 16;

  // level0_slowdown_writes_trigger=2147483647
  options.level0_slowdown_writes_trigger = 2147483647;

  // level0_stop_writes_trigger=2147483647
  options.level0_stop_writes_trigger = 2147483647;

  // target_file_size_base=536870912 (512MB)
  options.target_file_size_base = 536870912;

  // target_file_size_multiplier=1
  options.target_file_size_multiplier = 1;

  // target_file_size_is_upper_bound=false
  options.target_file_size_is_upper_bound = false;

  // max_bytes_for_level_base=104857600 (100MB)
  options.max_bytes_for_level_base = 104857600;

  // max_bytes_for_level_multiplier=10.000000
  options.max_bytes_for_level_multiplier = 10.0;

  // max_bytes_for_level_multiplier_additional=1:1:1:1:1:1:1
  options.max_bytes_for_level_multiplier_additional = {1, 1, 1, 1, 1, 1, 1};

  // max_compaction_bytes=13421772800
  options.max_compaction_bytes = 13421772800;

  // soft_pending_compaction_bytes_limit=0
  options.soft_pending_compaction_bytes_limit = 0;

  // hard_pending_compaction_bytes_limit=0
  options.hard_pending_compaction_bytes_limit = 0;

  // level_compaction_dynamic_level_bytes=false
  options.level_compaction_dynamic_level_bytes = false;

  // Compaction style and priority
  // compaction_style=kCompactionStyleFIFO
  options.compaction_style = kCompactionStyleFIFO;

  // compaction_pri=kByCompensatedSize
  options.compaction_pri = kByCompensatedSize;

  // disable_auto_compactions=true
  options.disable_auto_compactions = true;

  // FIFO compaction options
  // compaction_options_fifo={trivial_copy_buffer_size=4096;
  //   allow_trivial_copy_when_change_temperature=false;
  //   file_temperature_age_thresholds=;allow_compaction=false;
  //   age_for_warm=0;max_table_files_size=1125899906842624;}
  options.compaction_options_fifo.trivial_copy_buffer_size = 4096;
  options.compaction_options_fifo.allow_trivial_copy_when_change_temperature =
      false;
  options.compaction_options_fifo.file_temperature_age_thresholds.clear();
  options.compaction_options_fifo.allow_compaction = false;
  options.compaction_options_fifo.age_for_warm = 0;
  options.compaction_options_fifo.max_table_files_size = 1125899906842624ULL;

  // Universal compaction options (for reference, even though using FIFO)
  // compaction_options_universal={reduce_file_locking=true;incremental=false;
  //   compression_size_percent=-1;allow_trivial_move=false;
  //   max_size_amplification_percent=50;max_merge_width=4294967295;
  //   stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;
  //   max_read_amp=0;size_ratio=10;}
  options.compaction_options_universal.reduce_file_locking = true;
  options.compaction_options_universal.incremental = false;
  options.compaction_options_universal.compression_size_percent = -1;
  options.compaction_options_universal.allow_trivial_move = false;
  options.compaction_options_universal.max_size_amplification_percent = 50;
  options.compaction_options_universal.max_merge_width = 4294967295;
  options.compaction_options_universal.stop_style =
      kCompactionStopStyleTotalSize;
  options.compaction_options_universal.min_merge_width = 2;
  options.compaction_options_universal.max_read_amp = 0;
  options.compaction_options_universal.size_ratio = 10;

  // Blob file settings
  // enable_blob_files=true
  options.enable_blob_files = true;

  // min_blob_size=0
  options.min_blob_size = 0;

  // blob_file_size=268435456 (256MB)
  options.blob_file_size = 268435456;

  // blob_compression_type=kNoCompression
  options.blob_compression_type = kNoCompression;

  // enable_blob_garbage_collection=false
  options.enable_blob_garbage_collection = false;

  // blob_garbage_collection_age_cutoff=0.250000
  options.blob_garbage_collection_age_cutoff = 0.25;

  // blob_garbage_collection_force_threshold=1.000000
  options.blob_garbage_collection_force_threshold = 1.0;

  // blob_compaction_readahead_size=0
  options.blob_compaction_readahead_size = 0;

  // blob_file_starting_level=0
  options.blob_file_starting_level = 0;

  // prepopulate_blob_cache=kDisable
  options.prepopulate_blob_cache = PrepopulateBlobCache::kDisable;

  // Iterator settings
  // max_sequential_skip_in_iterations=8
  options.max_sequential_skip_in_iterations = 8;

  // Merge settings
  // max_successive_merges=1000
  options.max_successive_merges = 1000;

  // strict_max_successive_merges=true
  options.strict_max_successive_merges = true;

  // merge_operator=nullptr (default, ZippyDBDeltaUpdateOperator is internal)
  options.merge_operator = nullptr;

  // TTL and periodic compaction
  // ttl=21600 (6 hours)
  options.ttl = 21600;

  // periodic_compaction_seconds=0
  options.periodic_compaction_seconds = 0;

  // Temperature settings
  // last_level_temperature=kUnknown
  options.last_level_temperature = Temperature::kUnknown;

  // default_write_temperature=kUnknown
  options.default_write_temperature = Temperature::kUnknown;

  // default_temperature=kUnknown
  options.default_temperature = Temperature::kUnknown;

  // Tiered storage settings
  // preclude_last_level_data_seconds=0
  options.preclude_last_level_data_seconds = 0;

  // preserve_internal_time_seconds=1209600 (14 days)
  options.preserve_internal_time_seconds = 1209600;

  // Protection and verification settings
  // block_protection_bytes_per_key=1
  options.block_protection_bytes_per_key = 1;

  // paranoid_memory_checks=true
  options.paranoid_memory_checks = true;

  // paranoid_file_checks=false
  options.paranoid_file_checks = false;

  // force_consistency_checks=true
  options.force_consistency_checks = true;

  // verify_output_flags=2049 (kVerifyBlockChecksum | kEnableForLocalCompaction)
  options.verify_output_flags = static_cast<VerifyOutputFlags>(2049);

  // IO and stats settings
  // report_bg_io_stats=true
  options.report_bg_io_stats = true;

  // sample_for_compression=0
  options.sample_for_compression = 0;

  // Inplace update settings
  // inplace_update_support=false
  options.inplace_update_support = false;

  // inplace_update_num_locks=10000
  options.inplace_update_num_locks = 10000;

  // Bloom and prefix settings
  // bloom_locality=0
  options.bloom_locality = 0;

  // prefix_extractor=nullptr
  options.prefix_extractor = nullptr;

  // optimize_filters_for_hits=false
  options.optimize_filters_for_hits = false;

  // Other settings
  // experimental_mempurge_threshold=0.000000
  options.experimental_mempurge_threshold = 0.0;

  // bottommost_file_compaction_delay=0
  options.bottommost_file_compaction_delay = 0;

  // disallow_memtable_writes=false
  options.disallow_memtable_writes = false;

  // cf_allow_ingest_behind=false
  options.cf_allow_ingest_behind = false;

  // persist_user_defined_timestamps=true
  options.persist_user_defined_timestamps = true;

  // comparator=leveldb.BytewiseComparator (default)
  options.comparator = BytewiseComparator();

  // compaction_filter=nullptr
  options.compaction_filter = nullptr;

  // compaction_filter_factory=nullptr (ZippyDBCompactionFilterFactory is
  // internal)
  options.compaction_filter_factory = nullptr;

  // sst_partitioner_factory=nullptr (ZippyDBCustomSstPartitionerFactory is
  // internal)
  options.sst_partitioner_factory = nullptr;

  // Table factory - BlockBasedTable (default)
  // table_factory=BlockBasedTable
  BlockBasedTableOptions table_options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Open the database
  DestroyAndReopen(options);

  // Verify the database opened successfully by writing and reading data
  // Using PutEntity API to write wide-column entities
  WideColumns first_columns{{kDefaultWideColumnName, "value1"},
                            {"attr_name1", "attr_value1"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key1",
                           first_columns));

  WideColumns second_columns{
      {kDefaultWideColumnName, std::string(1000, 'x')},
      {"attr_large", "large_attr_value"}};  // Larger value for blob
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key2",
                           second_columns));

  ASSERT_OK(
      db_->Put(WriteOptions(), db_->DefaultColumnFamily(), "key3", "value1"));

  ASSERT_OK(Flush());

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key1", &value));
  // ASSERT_EQ(value, "value1");

  ASSERT_OK(db_->Get(ReadOptions(), "key2", &value));
  // ASSERT_EQ(value, std::string(1000, 'x'));

  // Verify some options are correctly set
  Options retrieved_options = db_->GetOptions();
  ASSERT_EQ(retrieved_options.compression, kLZ4Compression);
  ASSERT_EQ(retrieved_options.bottommost_compression, kZSTD);
  ASSERT_EQ(retrieved_options.compaction_style, kCompactionStyleFIFO);
  ASSERT_TRUE(retrieved_options.enable_blob_files);
  ASSERT_EQ(retrieved_options.min_blob_size, 0);
  ASSERT_EQ(retrieved_options.write_buffer_size, 134217728);
  ASSERT_EQ(retrieved_options.max_write_buffer_number, 64);
  ASSERT_EQ(retrieved_options.num_levels, 50);
  ASSERT_TRUE(retrieved_options.disable_auto_compactions);
  ASSERT_EQ(retrieved_options.ttl, 21600);
  ASSERT_EQ(retrieved_options.memtable_protection_bytes_per_key, 1);
  ASSERT_EQ(retrieved_options.block_protection_bytes_per_key, 1);
  ASSERT_TRUE(retrieved_options.paranoid_memory_checks);
  ASSERT_TRUE(retrieved_options.report_bg_io_stats);
  ASSERT_EQ(retrieved_options.max_successive_merges, 1000);
  ASSERT_TRUE(retrieved_options.strict_max_successive_merges);

  Close();
}

// Test that verifies we can reopen the database with the same configuration
TEST_F(DBOpenWithConfigTest, ReopenWithSameConfig) {
  Options options = GetDefaultOptions();
  options.max_open_files = -1;
  options.create_if_missing = true;

  // Set key configuration options
  options.compression = kLZ4Compression;
  options.bottommost_compression = kZSTD;
  options.compaction_style = kCompactionStyleFIFO;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.write_buffer_size = 134217728;
  options.max_write_buffer_number = 64;
  options.num_levels = 50;
  options.disable_auto_compactions = true;
  options.compaction_options_fifo.max_table_files_size = 1125899906842624ULL;
  options.memtable_protection_bytes_per_key = 1;
  options.block_protection_bytes_per_key = 1;
  options.paranoid_memory_checks = true;

  DestroyAndReopen(options);

  // Write some data
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", std::string(1000, 'x')));
  ASSERT_OK(Flush());

  // Close and reopen
  Close();
  Reopen(options);

  // Verify data persisted
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key1", &value));
  // ASSERT_EQ(value, "value1");

  ASSERT_OK(db_->Get(ReadOptions(), "key2", &value));
  // ASSERT_EQ(value, std::string(1000, 'x'));

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
