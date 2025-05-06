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
#include "rocksjni/portal/status_jni.h"

namespace ROCKSDB_NAMESPACE {
class FileOperationInfoJni : public JavaClass {
 public:
  static jobject fromCppFileOperationInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::FileOperationInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jpath = JniUtil::toJavaString(env, &info->path);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jobject jstatus = StatusJni::construct(env, info->status);
    if (jstatus == nullptr) {
      env->DeleteLocalRef(jpath);
      return nullptr;
    }
    return env->NewObject(
        jclazz, ctor, jpath, static_cast<jlong>(info->offset),
        static_cast<jlong>(info->length),
        static_cast<jlong>(info->start_ts.time_since_epoch().count()),
        static_cast<jlong>(info->duration.count()), jstatus);
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/FileOperationInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>",
                            "(Ljava/lang/String;JJJJLorg/rocksdb/Status;)V");
  }
};

}  // namespace ROCKSDB_NAMESPACE
