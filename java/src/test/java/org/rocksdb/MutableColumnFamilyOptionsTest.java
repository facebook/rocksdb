// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.Test;
import org.rocksdb.MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;

public class MutableColumnFamilyOptionsTest {

  @Test
  public void builder() {
    final MutableColumnFamilyOptionsBuilder builder =
        MutableColumnFamilyOptions.builder();
        builder
            .setWriteBufferSize(10)
            .setInplaceUpdateNumLocks(5)
            .setDisableAutoCompactions(true)
            .setParanoidFileChecks(true);

    assertThat(builder.writeBufferSize()).isEqualTo(10);
    assertThat(builder.inplaceUpdateNumLocks()).isEqualTo(5);
    assertThat(builder.disableAutoCompactions()).isEqualTo(true);
    assertThat(builder.paranoidFileChecks()).isEqualTo(true);
  }

  @Test(expected = NoSuchElementException.class)
  public void builder_getWhenNotSet() {
    final MutableColumnFamilyOptionsBuilder builder =
        MutableColumnFamilyOptions.builder();

    builder.writeBufferSize();
  }

  @Test
  public void builder_build() {
    final MutableColumnFamilyOptions options = MutableColumnFamilyOptions
        .builder()
          .setWriteBufferSize(10)
          .setParanoidFileChecks(true)
          .build();

    assertThat(options.getKeys().length).isEqualTo(2);
    assertThat(options.getValues().length).isEqualTo(2);
    assertThat(options.getKeys()[0])
        .isEqualTo(
            MutableColumnFamilyOptions.MemtableOption.write_buffer_size.name());
    assertThat(options.getValues()[0]).isEqualTo("10");
    assertThat(options.getKeys()[1])
        .isEqualTo(
            MutableColumnFamilyOptions.MiscOption.paranoid_file_checks.name());
    assertThat(options.getValues()[1]).isEqualTo("true");
  }

  @Test
  public void mutableColumnFamilyOptions_toString() {
    final String str = MutableColumnFamilyOptions.builder()
                           .setWriteBufferSize(10)
                           .setInplaceUpdateNumLocks(5)
                           .setDisableAutoCompactions(true)
                           .setParanoidFileChecks(true)
                           .setMaxBytesForLevelMultiplierAdditional(new int[] {2, 3, 5, 7, 11, 13})
                           .build()
                           .toString();

    assertThat(str).isEqualTo("write_buffer_size=10;inplace_update_num_locks=5;"
        + "disable_auto_compactions=true;paranoid_file_checks=true;max_bytes_for_level_multiplier_additional=2:3:5:7:11:13");
  }

  @Test
  public void mutableColumnFamilyOptions_parse() {
    final String str = "write_buffer_size=10;inplace_update_num_locks=5;"
        + "disable_auto_compactions=true;paranoid_file_checks=true;max_bytes_for_level_multiplier_additional=2:{3}:{5}:{7}:{11}:{13}";

    final MutableColumnFamilyOptionsBuilder builder =
        MutableColumnFamilyOptions.parse(str);

    assertThat(builder.writeBufferSize()).isEqualTo(10);
    assertThat(builder.inplaceUpdateNumLocks()).isEqualTo(5);
    assertThat(builder.disableAutoCompactions()).isEqualTo(true);
    assertThat(builder.paranoidFileChecks()).isEqualTo(true);
    assertThat(builder.maxBytesForLevelMultiplierAdditional())
        .isEqualTo(new int[] {2, 3, 5, 7, 11, 13});
  }

  /**
   * Extended parsing test to deal with all the options which C++ may return.
   * We have canned a set of options returned by {RocksDB#getOptions}
   */
  @Test
  public void mutableColumnFamilyOptions_parse_getOptions_output() {
    final String optionsString =
        "bottommost_compression=kDisableCompressionOption;  sample_for_compression=0;  "
        + "blob_garbage_collection_age_cutoff=0.250000;  blob_garbage_collection_force_threshold=0.800000;"
        + "arena_block_size=1048576;  enable_blob_garbage_collection=false;  level0_stop_writes_trigger=36;  min_blob_size=65536;"
        + "blob_compaction_readahead_size=262144;  blob_file_starting_level=5;  prepopulate_blob_cache=kDisable;"
        + "compaction_options_universal={allow_trivial_move=false;stop_style=kCompactionStopStyleTotalSize;min_merge_width=2;"
        + "compression_size_percent=-1;max_size_amplification_percent=200;max_merge_width=4294967295;size_ratio=1;};  "
        + "target_file_size_base=67108864;  max_bytes_for_level_base=268435456;  memtable_whole_key_filtering=false;  "
        + "soft_pending_compaction_bytes_limit=68719476736;  blob_compression_type=kNoCompression;  max_write_buffer_number=2;  "
        + "ttl=2592000;  compaction_options_fifo={allow_compaction=false;age_for_warm=0;max_table_files_size=1073741824;};  "
        + "check_flush_compaction_key_order=true;  max_successive_merges=0;  inplace_update_num_locks=10000;  "
        + "bottommost_compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;"
        + "strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;};  "
        + "target_file_size_multiplier=1;  max_bytes_for_level_multiplier_additional=5:{7}:{9}:{11}:{13}:{15}:{17};  "
        + "enable_blob_files=true;  level0_slowdown_writes_trigger=20;  compression=kLZ4HCCompression;  level0_file_num_compaction_trigger=4;  "
        + "blob_file_size=268435456;  prefix_extractor=nullptr;  max_bytes_for_level_multiplier=10.000000;  write_buffer_size=67108864;  "
        + "disable_auto_compactions=false;  max_compaction_bytes=1677721600;  memtable_huge_page_size=0;  "
        + "compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;"
        + "level=32767;window_bits=-14;};  "
        + "hard_pending_compaction_bytes_limit=274877906944;  periodic_compaction_seconds=0;  paranoid_file_checks=true;  "
        + "memtable_prefix_bloom_size_ratio=7.500000;  max_sequential_skip_in_iterations=8;  report_bg_io_stats=true;  "
        + "compaction_pri=kMinOverlappingRatio;  compaction_style=kCompactionStyleLevel;  memtable_factory=SkipListFactory;  "
        + "comparator=leveldb.BytewiseComparator;  bloom_locality=0;  compaction_filter_factory=nullptr;  "
        + "min_write_buffer_number_to_merge=1;  max_write_buffer_number_to_maintain=0;  compaction_filter=nullptr;  merge_operator=nullptr;  "
        + "num_levels=7;  optimize_filters_for_hits=false;  force_consistency_checks=true;  table_factory=BlockBasedTable;  "
        + "max_write_buffer_size_to_maintain=0;  memtable_insert_with_hint_prefix_extractor=nullptr;  level_compaction_dynamic_level_bytes=false;  "
        + "inplace_update_support=false;  experimental_mempurge_threshold=0.003";

    MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder cf =
        MutableColumnFamilyOptions.parse(optionsString, true);

    // Check the values from the parsed string which are column family options
    assertThat(cf.blobGarbageCollectionAgeCutoff()).isEqualTo(0.25);
    assertThat(cf.blobGarbageCollectionForceThreshold()).isEqualTo(0.80);
    assertThat(cf.arenaBlockSize()).isEqualTo(1048576);
    assertThat(cf.enableBlobGarbageCollection()).isEqualTo(false);
    assertThat(cf.level0StopWritesTrigger()).isEqualTo(36);
    assertThat(cf.minBlobSize()).isEqualTo(65536);
    assertThat(cf.blobCompactionReadaheadSize()).isEqualTo(262144);
    assertThat(cf.blobFileStartingLevel()).isEqualTo(5);
    assertThat(cf.prepopulateBlobCache()).isEqualTo(PrepopulateBlobCache.PREPOPULATE_BLOB_DISABLE);
    assertThat(cf.targetFileSizeBase()).isEqualTo(67108864);
    assertThat(cf.maxBytesForLevelBase()).isEqualTo(268435456);
    assertThat(cf.softPendingCompactionBytesLimit()).isEqualTo(68719476736L);
    assertThat(cf.blobCompressionType()).isEqualTo(CompressionType.NO_COMPRESSION);
    assertThat(cf.maxWriteBufferNumber()).isEqualTo(2);
    assertThat(cf.ttl()).isEqualTo(2592000);
    assertThat(cf.maxSuccessiveMerges()).isEqualTo(0);
    assertThat(cf.inplaceUpdateNumLocks()).isEqualTo(10000);
    assertThat(cf.targetFileSizeMultiplier()).isEqualTo(1);
    assertThat(cf.maxBytesForLevelMultiplierAdditional())
        .isEqualTo(new int[] {5, 7, 9, 11, 13, 15, 17});
    assertThat(cf.enableBlobFiles()).isEqualTo(true);
    assertThat(cf.level0SlowdownWritesTrigger()).isEqualTo(20);
    assertThat(cf.compressionType()).isEqualTo(CompressionType.LZ4HC_COMPRESSION);
    assertThat(cf.level0FileNumCompactionTrigger()).isEqualTo(4);
    assertThat(cf.blobFileSize()).isEqualTo(268435456);
    assertThat(cf.maxBytesForLevelMultiplier()).isEqualTo(10.0);
    assertThat(cf.writeBufferSize()).isEqualTo(67108864);
    assertThat(cf.disableAutoCompactions()).isEqualTo(false);
    assertThat(cf.maxCompactionBytes()).isEqualTo(1677721600);
    assertThat(cf.memtableHugePageSize()).isEqualTo(0);
    assertThat(cf.hardPendingCompactionBytesLimit()).isEqualTo(274877906944L);
    assertThat(cf.periodicCompactionSeconds()).isEqualTo(0);
    assertThat(cf.paranoidFileChecks()).isEqualTo(true);
    assertThat(cf.memtablePrefixBloomSizeRatio()).isEqualTo(7.5);
    assertThat(cf.experimentalMempurgeThreshold()).isEqualTo(0.003);
    assertThat(cf.maxSequentialSkipInIterations()).isEqualTo(8);
    assertThat(cf.reportBgIoStats()).isEqualTo(true);
  }
}
