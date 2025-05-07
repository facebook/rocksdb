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

namespace ROCKSDB_NAMESPACE {
class WriteStallInfoJni : public JavaClass {
 public:
  static jobject fromCppWriteStallInfo(
      JNIEnv* env, const ROCKSDB_NAMESPACE::WriteStallInfo* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jcf_name = JniUtil::toJavaString(env, &info->cf_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, jcf_name,
                          static_cast<jbyte>(info->condition.cur),
                          static_cast<jbyte>(info->condition.prev));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WriteStallInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(Ljava/lang/String;BB)V");
  }
};

}  // namespace ROCKSDB_NAMESPACE
