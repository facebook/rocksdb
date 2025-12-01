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
#include "rocksjni/portal/wal_file_type_jni.h"

namespace ROCKSDB_NAMESPACE {
class LogFileJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LogFile object.
   *
   * @param env A pointer to the Java environment
   * @param log_file A Cpp log file object
   *
   * @return A reference to a Java org.rocksdb.LogFile object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLogFile(JNIEnv* env,
                                ROCKSDB_NAMESPACE::LogFile* log_file) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;JBJJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    std::string path_name = log_file->PathName();
    jstring jpath_name =
        ROCKSDB_NAMESPACE::JniUtil::toJavaString(env, &path_name, true);
    if (env->ExceptionCheck()) {
      // exception occurred creating java string
      return nullptr;
    }

    jobject jlog_file = env->NewObject(
        jclazz, mid, jpath_name, static_cast<jlong>(log_file->LogNumber()),
        ROCKSDB_NAMESPACE::WalFileTypeJni::toJavaWalFileType(log_file->Type()),
        static_cast<jlong>(log_file->StartSequence()),
        static_cast<jlong>(log_file->SizeFileBytes()));

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jpath_name);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jpath_name);

    return jlog_file;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LogFile");
  }
};

}  // namespace ROCKSDB_NAMESPACE
