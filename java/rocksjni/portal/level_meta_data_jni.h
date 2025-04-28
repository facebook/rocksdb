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
class LevelMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.LevelMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param level_meta_data A Cpp level meta data object
   *
   * @return A reference to a Java org.rocksdb.LevelMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppLevelMetaData(
      JNIEnv* env, const ROCKSDB_NAMESPACE::LevelMetaData* level_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>",
                                     "(IJ[Lorg/rocksdb/SstFileMetaData;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jsize jlen = static_cast<jsize>(level_meta_data->files.size());
    jobjectArray jfiles =
        env->NewObjectArray(jlen, SstFileMetaDataJni::getJClass(env), nullptr);
    if (jfiles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jsize i = 0;
    for (auto it = level_meta_data->files.begin();
         it != level_meta_data->files.end(); ++it) {
      jobject jfile = SstFileMetaDataJni::fromCppSstFileMetaData(env, &(*it));
      if (jfile == nullptr) {
        // exception occurred
        env->DeleteLocalRef(jfiles);
        return nullptr;
      }
      env->SetObjectArrayElement(jfiles, i++, jfile);
    }

    jobject jlevel_meta_data =
        env->NewObject(jclazz, mid, static_cast<jint>(level_meta_data->level),
                       static_cast<jlong>(level_meta_data->size), jfiles);

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jfiles);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jfiles);

    return jlevel_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/LevelMetaData");
  }
};
}  // namespace ROCKSDB_NAMESPACE
