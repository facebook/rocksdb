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
class ColumnFamilyMetaDataJni : public JavaClass {
 public:
  /**
   * Create a new Java org.rocksdb.ColumnFamilyMetaData object.
   *
   * @param env A pointer to the Java environment
   * @param column_famly_meta_data A Cpp live file meta data object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyMetaData object, or
   * nullptr if an an exception occurs
   */
  static jobject fromCppColumnFamilyMetaData(
      JNIEnv* env,
      const ROCKSDB_NAMESPACE::ColumnFamilyMetaData* column_famly_meta_data) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>",
                                     "(JJ[B[Lorg/rocksdb/LevelMetaData;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jbyteArray jname = ROCKSDB_NAMESPACE::JniUtil::copyBytes(
        env, column_famly_meta_data->name);
    if (jname == nullptr) {
      // exception occurred creating java byte array
      return nullptr;
    }

    const jsize jlen =
        static_cast<jsize>(column_famly_meta_data->levels.size());
    jobjectArray jlevels =
        env->NewObjectArray(jlen, LevelMetaDataJni::getJClass(env), nullptr);
    if (jlevels == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jname);
      return nullptr;
    }

    jsize i = 0;
    for (auto it = column_famly_meta_data->levels.begin();
         it != column_famly_meta_data->levels.end(); ++it) {
      jobject jlevel = LevelMetaDataJni::fromCppLevelMetaData(env, &(*it));
      if (jlevel == nullptr) {
        // exception occurred
        env->DeleteLocalRef(jname);
        env->DeleteLocalRef(jlevels);
        return nullptr;
      }
      env->SetObjectArrayElement(jlevels, i++, jlevel);
    }

    jobject jcolumn_family_meta_data = env->NewObject(
        jclazz, mid, static_cast<jlong>(column_famly_meta_data->size),
        static_cast<jlong>(column_famly_meta_data->file_count), jname, jlevels);

    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jname);
      env->DeleteLocalRef(jlevels);
      return nullptr;
    }

    // cleanup
    env->DeleteLocalRef(jname);
    env->DeleteLocalRef(jlevels);

    return jcolumn_family_meta_data;
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyMetaData");
  }
};
}  // namespace ROCKSDB_NAMESPACE
