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

namespace ROCKSDB_NAMESPACE {
class ExternalFileIngestionInfoJni : public JavaClass {
 public:
  static jobject fromCppExternalFileIngestionInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::ExternalFileIngestionInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jstring jexternal_file_path =
        JniUtil::toJavaString(env, &info->external_file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    jstring jinternal_file_path =
        JniUtil::toJavaString(env, &info->internal_file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(jexternal_file_path);
      return nullptr;
    }
    jobject jtable_properties =
        TablePropertiesJni::fromCppTableProperties(env, info->table_properties);
    if (jtable_properties == nullptr) {
      env->DeleteLocalRef(jcf_name);
      env->DeleteLocalRef(jexternal_file_path);
      env->DeleteLocalRef(jinternal_file_path);
      return nullptr;
    }
    return env->NewObject(
        jclazz, ctor, jcf_name, jexternal_file_path, jinternal_file_path,
        static_cast<jlong>(info->global_seqno), jtable_properties);
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ExternalFileIngestionInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>",
                            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/"
                            "String;JLorg/rocksdb/TableProperties;)V");
  }
};
}  // namespace ROCKSDB_NAMESPACE
