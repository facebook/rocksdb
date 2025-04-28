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
// The portal class for org.rocksdb.ThreadStatus
class ThreadStatusJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.ThreadStatus
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ThreadStatus");
  }

  /**
   * Create a new Java org.rocksdb.ThreadStatus object with the same
   * properties as the provided C++ ROCKSDB_NAMESPACE::ThreadStatus object
   *
   * @param env A pointer to the Java environment
   * @param thread_status A pointer to ROCKSDB_NAMESPACE::ThreadStatus object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(
      JNIEnv* env, const ROCKSDB_NAMESPACE::ThreadStatus* thread_status) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(JBLjava/lang/String;Ljava/lang/String;BJB[JB)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jdb_name =
        JniUtil::toJavaString(env, &(thread_status->db_name), true);
    if (env->ExceptionCheck()) {
      // an error occurred
      return nullptr;
    }

    jstring jcf_name =
        JniUtil::toJavaString(env, &(thread_status->cf_name), true);
    if (env->ExceptionCheck()) {
      // an error occurred
      env->DeleteLocalRef(jdb_name);
      return nullptr;
    }

    // long[]
    const jsize len = static_cast<jsize>(
        ROCKSDB_NAMESPACE::ThreadStatus::kNumOperationProperties);
    jlongArray joperation_properties = env->NewLongArray(len);
    if (joperation_properties == nullptr) {
      // an exception occurred
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    jboolean is_copy;
    jlong* body = env->GetLongArrayElements(joperation_properties, &is_copy);
    if (body == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(joperation_properties);
      return nullptr;
    }
    for (size_t i = 0; i < len; ++i) {
      body[i] = static_cast<jlong>(thread_status->op_properties[i]);
    }
    env->ReleaseLongArrayElements(joperation_properties, body,
                                  is_copy == JNI_TRUE ? 0 : JNI_ABORT);

    jobject jcfd = env->NewObject(
        jclazz, mid, static_cast<jlong>(thread_status->thread_id),
        ThreadTypeJni::toJavaThreadType(thread_status->thread_type), jdb_name,
        jcf_name,
        OperationTypeJni::toJavaOperationType(thread_status->operation_type),
        static_cast<jlong>(thread_status->op_elapsed_micros),
        OperationStageJni::toJavaOperationStage(thread_status->operation_stage),
        joperation_properties,
        StateTypeJni::toJavaStateType(thread_status->state_type));
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(joperation_properties);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jdb_name);
    env->DeleteLocalRef(jcf_name);
    env->DeleteLocalRef(joperation_properties);

    return jcfd;
  }
};
}  // namespace ROCKSDB_NAMESPACE
