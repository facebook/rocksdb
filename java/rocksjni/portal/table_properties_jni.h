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
#include "rocksjni/portal/common.h"
#include "rocksjni/portal/hash_map_jni.h"
#include "rocksjni/portal/java_class.h"
#include "rocksjni/portal/jni_util.h"

namespace ROCKSDB_NAMESPACE {
class TablePropertiesJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.TableProperties object.
   *
   * @param env A pointer to the Java environment
   * @param table_properties A Cpp table properties object
   *
   * @return A reference to a Java org.rocksdb.TableProperties object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppTableProperties(
      JNIEnv* env, const ROCKSDB_NAMESPACE::TableProperties& table_properties) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "<init>",
        "(JJJJJJJJJJJJJJJJJJJJJJ[BLjava/lang/String;Ljava/lang/String;Ljava/"
        "lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/"
        "String;Ljava/util/Map;Ljava/util/Map;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jcolumn_family_name = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, table_properties.column_family_name);
    if (jcolumn_family_name == nullptr) {
      // exception occurred creating java string
      return nullptr;
    }

    jstring jfilter_policy_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.filter_policy_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      return nullptr;
    }

    jstring jcomparator_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.comparator_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      return nullptr;
    }

    jstring jmerge_operator_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.merge_operator_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      return nullptr;
    }

    jstring jprefix_extractor_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.prefix_extractor_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      return nullptr;
    }

    jstring jproperty_collectors_names =
        ROCKSDB_NAMESPACE::JniUtil::toJavaString(
            env, &table_properties.property_collectors_names, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      return nullptr;
    }

    jstring jcompression_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &table_properties.compression_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      return nullptr;
    }

    // Map<String, String>
    jobject juser_collected_properties =
        ROCKSDB_NAMESPACE::HashMapJni::fromCppMap(
            env, &table_properties.user_collected_properties);
    if (env->ExceptionCheck()) {
      // exception occurred creating java map
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      env->DeleteLocalRef(jcompression_name);
      return nullptr;
    }

    // Map<String, String>
    jobject jreadable_properties = ROCKSDB_NAMESPACE::HashMapJni::fromCppMap(
        env, &table_properties.readable_properties);
    if (env->ExceptionCheck()) {
      // exception occurred creating java map
      env->DeleteLocalRef(jcolumn_family_name);
      env->DeleteLocalRef(jfilter_policy_name);
      env->DeleteLocalRef(jcomparator_name);
      env->DeleteLocalRef(jmerge_operator_name);
      env->DeleteLocalRef(jprefix_extractor_name);
      env->DeleteLocalRef(jproperty_collectors_names);
      env->DeleteLocalRef(jcompression_name);
      env->DeleteLocalRef(juser_collected_properties);
      return nullptr;
    }

    jobject jtable_properties = env->NewObject(
        jclazz, mid, static_cast<jlong>(table_properties.data_size),
        static_cast<jlong>(table_properties.index_size),
        static_cast<jlong>(table_properties.index_partitions),
        static_cast<jlong>(table_properties.top_level_index_size),
        static_cast<jlong>(table_properties.index_key_is_user_key),
        static_cast<jlong>(table_properties.index_value_is_delta_encoded),
        static_cast<jlong>(table_properties.filter_size),
        static_cast<jlong>(table_properties.raw_key_size),
        static_cast<jlong>(table_properties.raw_value_size),
        static_cast<jlong>(table_properties.num_data_blocks),
        static_cast<jlong>(table_properties.num_entries),
        static_cast<jlong>(table_properties.num_deletions),
        static_cast<jlong>(table_properties.num_merge_operands),
        static_cast<jlong>(table_properties.num_range_deletions),
        static_cast<jlong>(table_properties.format_version),
        static_cast<jlong>(table_properties.fixed_key_len),
        static_cast<jlong>(table_properties.column_family_id),
        static_cast<jlong>(table_properties.creation_time),
        static_cast<jlong>(table_properties.oldest_key_time),
        static_cast<jlong>(
            table_properties.slow_compression_estimated_data_size),
        static_cast<jlong>(
            table_properties.fast_compression_estimated_data_size),
        static_cast<jlong>(
            table_properties.external_sst_file_global_seqno_offset),
        jcolumn_family_name, jfilter_policy_name, jcomparator_name,
        jmerge_operator_name, jprefix_extractor_name,
        jproperty_collectors_names, jcompression_name,
        juser_collected_properties, jreadable_properties);

    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jtable_properties;
  }

 private:
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableProperties");
  }
};

}  // namespace ROCKSDB_NAMESPACE
