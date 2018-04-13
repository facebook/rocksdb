// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for rocksdb::Options.

#include "rocksdb/table.h"
#include <jni.h>
#include "include/org_rocksdb_BlockBasedTableConfig.h"
#include "include/org_rocksdb_PlainTableConfig.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"

/*
 * Class:     org_rocksdb_PlainTableConfig
 * Method:    newTableFactoryHandle
 * Signature: (IIDIIBZZ)J
 */
jlong Java_org_rocksdb_PlainTableConfig_newTableFactoryHandle(
    JNIEnv * /*env*/, jobject /*jobj*/, jint jkey_size,
    jint jbloom_bits_per_key, jdouble jhash_table_ratio, jint jindex_sparseness,
    jint jhuge_page_tlb_size, jbyte jencoding_type, jboolean jfull_scan_mode,
    jboolean jstore_index_in_file) {
  rocksdb::PlainTableOptions options = rocksdb::PlainTableOptions();
  options.user_key_len = jkey_size;
  options.bloom_bits_per_key = jbloom_bits_per_key;
  options.hash_table_ratio = jhash_table_ratio;
  options.index_sparseness = jindex_sparseness;
  options.huge_page_tlb_size = jhuge_page_tlb_size;
  options.encoding_type = static_cast<rocksdb::EncodingType>(jencoding_type);
  options.full_scan_mode = jfull_scan_mode;
  options.store_index_in_file = jstore_index_in_file;
  return reinterpret_cast<jlong>(rocksdb::NewPlainTableFactory(options));
}

/*
 * Class:     org_rocksdb_BlockBasedTableConfig
 * Method:    newTableFactoryHandle
 * Signature: (ZJIJJIIZIZZZJIBBI)J
 */
jlong Java_org_rocksdb_BlockBasedTableConfig_newTableFactoryHandle(
    JNIEnv * /*env*/, jobject /*jobj*/, jboolean no_block_cache,
    jlong block_cache_size, jint block_cache_num_shardbits, jlong jblock_cache,
    jlong block_size, jint block_size_deviation, jint block_restart_interval,
    jboolean whole_key_filtering, jlong jfilter_policy,
    jboolean cache_index_and_filter_blocks,
    jboolean pin_l0_filter_and_index_blocks_in_cache,
    jboolean hash_index_allow_collision, jlong block_cache_compressed_size,
    jint block_cache_compressd_num_shard_bits, jbyte jchecksum_type,
    jbyte jindex_type, jint jformat_version) {
  rocksdb::BlockBasedTableOptions options;
  options.no_block_cache = no_block_cache;

  if (!no_block_cache) {
    if (jblock_cache > 0) {
      std::shared_ptr<rocksdb::Cache> *pCache =
          reinterpret_cast<std::shared_ptr<rocksdb::Cache> *>(jblock_cache);
      options.block_cache = *pCache;
    } else if (block_cache_size > 0) {
      if (block_cache_num_shardbits > 0) {
        options.block_cache =
            rocksdb::NewLRUCache(block_cache_size, block_cache_num_shardbits);
      } else {
        options.block_cache = rocksdb::NewLRUCache(block_cache_size);
      }
    }
  }
  options.block_size = block_size;
  options.block_size_deviation = block_size_deviation;
  options.block_restart_interval = block_restart_interval;
  options.whole_key_filtering = whole_key_filtering;
  if (jfilter_policy > 0) {
    std::shared_ptr<rocksdb::FilterPolicy> *pFilterPolicy =
        reinterpret_cast<std::shared_ptr<rocksdb::FilterPolicy> *>(
            jfilter_policy);
    options.filter_policy = *pFilterPolicy;
  }
  options.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
  options.pin_l0_filter_and_index_blocks_in_cache =
      pin_l0_filter_and_index_blocks_in_cache;
  options.hash_index_allow_collision = hash_index_allow_collision;
  if (block_cache_compressed_size > 0) {
    if (block_cache_compressd_num_shard_bits > 0) {
      options.block_cache = rocksdb::NewLRUCache(
          block_cache_compressed_size, block_cache_compressd_num_shard_bits);
    } else {
      options.block_cache = rocksdb::NewLRUCache(block_cache_compressed_size);
    }
  }
  options.checksum = static_cast<rocksdb::ChecksumType>(jchecksum_type);
  options.index_type =
      static_cast<rocksdb::BlockBasedTableOptions::IndexType>(jindex_type);
  options.format_version = jformat_version;

  return reinterpret_cast<jlong>(rocksdb::NewBlockBasedTableFactory(options));
}
