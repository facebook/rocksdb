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
class CompactRangeOptionsTimestampJni : public JavaClass {
 public:
  static jobject fromCppTimestamp(JNIEnv* env, const uint64_t start,
                                  const uint64_t range) {
    jclass jclazz = getJClass(env);
    assert(jclazz != nullptr);
    static jmethodID ctor = getConstructorMethodId(env, jclazz);
    assert(ctor != nullptr);
    return env->NewObject(jclazz, ctor, static_cast<jlong>(start),
                          static_cast<jlong>(range));
  }

  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
                                "org/rocksdb/CompactRangeOptions$Timestamp");
  }

  static jmethodID getConstructorMethodId(JNIEnv* env, jclass clazz) {
    return env->GetMethodID(clazz, "<init>", "(JJ)V");
  }
};
}  // namespace ROCKSDB_NAMESPACE
