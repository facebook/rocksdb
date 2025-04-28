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
class TableFileCreationBriefInfoJni : public JavaClass {
 public:
  static jobject fromCppTableFileCreationBriefInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::TableFileCreationBriefInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jdb_name = JniUtil::toJavaString(env, &info->db_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jdb_name);
      return nullptr;
    }
    jstring jfile_path = JniUtil::toJavaString(env, &info->file_path);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jdb_name);
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, jdb_name, jcf_name, jfile_path,
                          static_cast<jint>(info->job_id),
                          static_cast<jbyte>(info->reason));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableFileCreationBriefInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(
        clazz, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IB)V");
  }
};
}  // namespace ROCKSDB_NAMESPACE
