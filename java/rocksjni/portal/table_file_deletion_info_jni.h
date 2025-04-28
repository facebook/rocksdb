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
class TableFileDeletionInfoJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.TableFileDeletionInfo object.
   *
   * @param env A pointer to the Java environment
   * @param file_del_info A Cpp table file deletion info object
   *
   * @return A reference to a Java org.rocksdb.TableFileDeletionInfo object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppTableFileDeletionInfo(
      JNIEnv* env,
      const ROCKSDB_NAMESPACE::TableFileDeletionInfo* file_del_info) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    jstring jdb_name = JniUtil::toJavaString(env, &file_del_info->db_name);
    if (env->ExceptionCheck()) {
      return nullptr;
    }
    jobject jstatus = StatusJni::construct(env, file_del_info->status);
    if (jstatus == nullptr) {
      env->DeleteLocalRef(jdb_name);
      return nullptr;
    }
    return env->NewObject(jclazz, ctor, jdb_name,
                          JniUtil::toJavaString(env, &file_del_info->file_path),
                          static_cast<jint>(file_del_info->job_id), jstatus);
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TableFileDeletionInfo");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(
        clazz, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;ILorg/rocksdb/Status;)V");
  }
};
}  // namespace ROCKSDB_NAMESPACE
