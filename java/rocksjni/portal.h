// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#ifndef JAVA_ROCKSJNI_PORTAL_H_
#define JAVA_ROCKSJNI_PORTAL_H_

#include <jni.h>
#include "rocksdb/db.h"

namespace rocksdb {

// The portal class for org.rocksdb.RocksDB
class RocksDBJni {
 public:
  // Get the java class id of org.rocksdb.RocksDB.
  static jclass getJClass(JNIEnv* env) {
    static jclass jclazz = env->FindClass("org/rocksdb/RocksDB");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.RocksDB
  // that stores the pointer to rocksdb::DB.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::DB of the specified org.rocksdb.RocksDB.
  static rocksdb::DB* getHandle(JNIEnv* env, jobject jdb) {
    return reinterpret_cast<rocksdb::DB*>(
        env->GetLongField(jdb, getHandleFieldID(env)));
  }

  // Pass the rocksdb::DB pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jdb, rocksdb::DB* db) {
    env->SetLongField(
        jdb, getHandleFieldID(env),
        reinterpret_cast<jlong>(db));
  }
};

// The portal class for org.rocksdb.RocksDBException
class RocksDBExceptionJni {
 public:
  // Get the jclass of org.rocksdb.RocksDBException
  static jclass getJClass(JNIEnv* env) {
    static jclass jclazz = env->FindClass("org/rocksdb/RocksDBException");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Create and throw a java exception by converting the input
  // Status to an RocksDBException.
  //
  // In case s.ok() is true, then this function will not throw any
  // exception.
  static void ThrowNew(JNIEnv* env, Status s) {
    if (s.ok()) {
      return;
    }
    jstring msg = env->NewStringUTF(s.ToString().c_str());
    // get the constructor id of org.rocksdb.RocksDBException
    static jmethodID mid = env->GetMethodID(
        getJClass(env), "<init>", "(Ljava/lang/String;)V");
    assert(mid != nullptr);

    env->Throw((jthrowable)env->NewObject(getJClass(env), mid, msg));
  }
};

}  // namespace rocksdb
#endif  // JAVA_ROCKSJNI_PORTAL_H_
