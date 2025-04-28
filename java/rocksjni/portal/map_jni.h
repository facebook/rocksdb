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
class MapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.Map
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Map");
  }

  /**
   * Get the Java Method: Map#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getMapPutMethodId(JNIEnv* env) {
    jclass jlist_clazz = getJClass(env);
    if (jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jlist_clazz, "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }
};
}  // namespace ROCKSDB_NAMESPACE
