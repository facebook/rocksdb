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
#include "rocksjni/portal/rocks_d_b_native_class.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.ColumnFamilyOptions
class ColumnFamilyOptionsJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::ColumnFamilyOptions*,
                                ColumnFamilyOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/ColumnFamilyOptions");
  }

  /**
   * Create a new Java org.rocksdb.ColumnFamilyOptions object with the same
   * properties as the provided C++ ROCKSDB_NAMESPACE::ColumnFamilyOptions
   * object
   *
   * @param env A pointer to the Java environment
   * @param cfoptions A pointer to ROCKSDB_NAMESPACE::ColumnFamilyOptions object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const ColumnFamilyOptions* cfoptions) {
    auto* cfo = new ROCKSDB_NAMESPACE::ColumnFamilyOptions(*cfoptions);
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jcfd = env->NewObject(jclazz, mid, GET_CPLUSPLUS_POINTER(cfo));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jcfd;
  }
};

}  // namespace ROCKSDB_NAMESPACE
