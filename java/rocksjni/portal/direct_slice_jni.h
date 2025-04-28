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
// The portal class for org.rocksdb.DirectSlice
class DirectSliceJni
    : public NativeRocksMutableObject<const ROCKSDB_NAMESPACE::Slice*,
                                      AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DirectSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DirectSlice");
  }

  /**
   * Constructs a DirectSlice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java DirectSlice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if (mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jdirect_slice = env->NewObject(jclazz, mid);
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jdirect_slice;
  }
};
}  // namespace ROCKSDB_NAMESPACE
