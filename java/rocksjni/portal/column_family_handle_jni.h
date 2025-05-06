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
/**
 * Get the Java Class org.rocksdb.FilterPolicy
 *
 * @param env A pointer to the Java environment
 *
 * @return The Java Class or nullptr if one of the
 *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
 *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
 */
// The portal class for org.rocksdb.ColumnFamilyHandle
class ColumnFamilyHandleJni
    : public RocksDBNativeClass<ROCKSDB_NAMESPACE::ColumnFamilyHandle*,
                                ColumnFamilyHandleJni> {
 public:
  static jobject fromCppColumnFamilyHandle(
      JNIEnv* env, const ROCKSDB_NAMESPACE::ColumnFamilyHandle* info) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    return env->NewObject(jclazz, ctor, GET_CPLUSPLUS_POINTER(info));
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(J)V");
  }

  /**
   * Get the Java Class org.rocksdb.ColumnFamilyHandle
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ColumnFamilyHandle");
  }
};

}  // namespace ROCKSDB_NAMESPACE
