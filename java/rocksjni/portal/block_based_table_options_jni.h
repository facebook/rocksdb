// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.BlockBasedTableOptions
class BlockBasedTableOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::BlockBasedTableOptions*,
                                BlockBasedTableOptions> {
 public:
  /**
   * Get the Java Class org.rocksdb.BlockBasedTableConfig
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/BlockBasedTableConfig");
  }

  /**
   * Create a new Java org.rocksdb.BlockBasedTableConfig object with the
   * properties as the provided C++ ROCKSDB_NAMESPACE::BlockBasedTableOptions
   * object
   *
   * @param env A pointer to the Java environment
   * @param cfoptions A pointer to ROCKSDB_NAMESPACE::ColumnFamilyOptions object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(
      JNIEnv* env, const BlockBasedTableOptions* table_factory_options) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID method_id_init =
        env->GetMethodID(jclazz, "<init>", "(ZZZZBBDBZJIIIJZZZZZIIZZBBJD)V");
    if (method_id_init == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    FilterPolicyTypeJni filter_policy_type =
        FilterPolicyTypeJni::kUnknownFilterPolicy;
    jlong filter_policy_handle = 0L;
    jdouble filter_policy_config_value = 0.0;
    if (table_factory_options->filter_policy) {
      auto filter_policy = table_factory_options->filter_policy.get();
      filter_policy_type = FilterPolicyJni::getFilterPolicyType(
          filter_policy->CompatibilityName());
      if (FilterPolicyTypeJni::kUnknownFilterPolicy != filter_policy_type) {
        filter_policy_handle = GET_CPLUSPLUS_POINTER(filter_policy);
      }
    }

    jobject jcfd = env->NewObject(
        jclazz, method_id_init,
        table_factory_options->cache_index_and_filter_blocks,
        table_factory_options->cache_index_and_filter_blocks_with_high_priority,
        table_factory_options->pin_l0_filter_and_index_blocks_in_cache,
        table_factory_options->pin_top_level_index_and_filter,
        IndexTypeJni::toJavaIndexType(table_factory_options->index_type),
        DataBlockIndexTypeJni::toJavaDataBlockIndexType(
            table_factory_options->data_block_index_type),
        table_factory_options->data_block_hash_table_util_ratio,
        ChecksumTypeJni::toJavaChecksumType(table_factory_options->checksum),
        table_factory_options->no_block_cache,
        static_cast<jlong>(table_factory_options->block_size),
        table_factory_options->block_size_deviation,
        table_factory_options->block_restart_interval,
        table_factory_options->index_block_restart_interval,
        static_cast<jlong>(table_factory_options->metadata_block_size),
        table_factory_options->partition_filters,
        table_factory_options->optimize_filters_for_memory,
        table_factory_options->use_delta_encoding,
        table_factory_options->whole_key_filtering,
        table_factory_options->verify_compression,
        table_factory_options->read_amp_bytes_per_bit,
        table_factory_options->format_version,
        table_factory_options->enable_index_compression,
        table_factory_options->block_align,
        IndexShorteningModeJni::toJavaIndexShorteningMode(
            table_factory_options->index_shortening),
        FilterPolicyJni::toJavaIndexType(filter_policy_type),
        filter_policy_handle, filter_policy_config_value);
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jcfd;
  }
};
}  // namespace ROCKSDB_NAMESPACE
