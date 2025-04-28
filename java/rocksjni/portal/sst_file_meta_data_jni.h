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
class SstFileMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.SstFileMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param sst_file_meta_data A Cpp sst file meta data object
   *
   * @return A reference to a Java org.rocksdb.SstFileMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppSstFileMetaData(
      JNIEnv* env,
      const ROCKSDB_NAMESPACE::SstFileMetaData* sst_file_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;JJJ[B[BJZJJ[B)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jfile_name = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &sst_file_meta_data->name, true);
    if (jfile_name == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    jstring jpath = ROCKSDB_NAMESPACE::JniUtil::toJavaString(
        env, &sst_file_meta_data->db_path, true);
    if (jpath == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      return nullptr;
    }

    jbyteArray jsmallest_key = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, sst_file_meta_data->smallestkey);
    if (jsmallest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      return nullptr;
    }

    jbyteArray jlargest_key = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, sst_file_meta_data->largestkey);
    if (jlargest_key == nullptr) {
      // exception occurred creating java byte array
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      return nullptr;
    }

    jbyteArray jfile_checksum = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, sst_file_meta_data->file_checksum);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      return nullptr;
    }

    jobject jsst_file_meta_data = env->NewObject(
        jclazz, mid, jfile_name, jpath,
        static_cast<jlong>(sst_file_meta_data->size),
        static_cast<jint>(sst_file_meta_data->smallest_seqno),
        static_cast<jlong>(sst_file_meta_data->largest_seqno), jsmallest_key,
        jlargest_key, static_cast<jlong>(sst_file_meta_data->num_reads_sampled),
        static_cast<jboolean>(sst_file_meta_data->being_compacted),
        static_cast<jlong>(sst_file_meta_data->num_entries),
        static_cast<jlong>(sst_file_meta_data->num_deletions), jfile_checksum);

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfile_name);
      env->DeleteLocalRef(jpath);
      env->DeleteLocalRef(jsmallest_key);
      env->DeleteLocalRef(jlargest_key);
      env->DeleteLocalRef(jfile_checksum);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jfile_name);
    env->DeleteLocalRef(jpath);
    env->DeleteLocalRef(jsmallest_key);
    env->DeleteLocalRef(jlargest_key);
    env->DeleteLocalRef(jfile_checksum);

    return jsst_file_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/SstFileMetaData");
  }
};
}  // namespace ROCKSDB_NAMESPACE
