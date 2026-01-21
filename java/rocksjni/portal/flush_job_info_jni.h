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
#include "rocksjni/portal/java_class.h"
#include "rocksjni/portal/jni_util.h"
#include "rocksjni/portal/table_properties_jni.h"

namespace ROCKSDB_NAMESPACE {
class FlushJobInfoJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.FlushJobInfo object.
   *
   * @param env A pointer to the Java environment
   * @param flush_job_info A Cpp flush job info object
   *
   * @return A reference to a Java org.rocksdb.FlushJobInfo object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppFlushJobInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::FlushJobInfo* flush_job_info) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jcf_name = JniUtil::toJavaString(env, &flush_job_info->cf_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jstring jfile_path = JniUtil::toJavaString(env, &flush_job_info->file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfile_path);
      return nullptr;
    }
    jobject jtable_properties = TablePropertiesJni::fromCppTableProperties(
        env, flush_job_info->table_properties);
    if (jtable_properties == nullptr) {
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(jfile_path);
      return nullptr;
    }
    return env->NewObject(
        jclazz, ctor, static_cast<jlong>(flush_job_info->cf_id), jcf_name,
        jfile_path, static_cast<jlong>(flush_job_info->thread_id),
        static_cast<jint>(flush_job_info->job_id),
        static_cast<jboolean>(flush_job_info->triggered_writes_slowdown),
        static_cast<jboolean>(flush_job_info->triggered_writes_stop),
        static_cast<jlong>(flush_job_info->smallest_seqno),
        static_cast<jlong>(flush_job_info->largest_seqno), jtable_properties,
        static_cast<jbyte>(flush_job_info->flush_reason));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/FlushJobInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>",
                            "(JLjava/lang/String;Ljava/lang/String;JIZZJJLorg/"
                            "rocksdb/TableProperties;B)V");
  }
};

}  // namespace ROCKSDB_NAMESPACE
