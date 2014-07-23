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
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/backupable_db.h"

namespace rocksdb {

// The portal class for org.rocksdb.RocksDB
class RocksDBJni {
 public:
  // Get the java class id of org.rocksdb.RocksDB.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/RocksDB");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.RocksDB
  // that stores the pointer to rocksdb::DB.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
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
    jclass jclazz = env->FindClass("org/rocksdb/RocksDBException");
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

class OptionsJni {
 public:
  // Get the java class id of org.rocksdb.Options.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/Options");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Options
  // that stores the pointer to rocksdb::Options
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::Options
  static rocksdb::Options* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::Options*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::Options pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj, rocksdb::Options* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class WriteOptionsJni {
 public:
  // Get the java class id of org.rocksdb.WriteOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/WriteOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.WriteOptions
  // that stores the pointer to rocksdb::WriteOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::WriteOptions
  static rocksdb::WriteOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::WriteOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::WriteOptions pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj, rocksdb::WriteOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};


class ReadOptionsJni {
 public:
  // Get the java class id of org.rocksdb.ReadOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/ReadOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.ReadOptions
  // that stores the pointer to rocksdb::ReadOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::ReadOptions
  static rocksdb::ReadOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::ReadOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::ReadOptions pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jobj,
                        rocksdb::ReadOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};


class WriteBatchJni {
 public:
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/WriteBatch");
    assert(jclazz != nullptr);
    return jclazz;
  }

  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::WriteBatch of the specified
  // org.rocksdb.WriteBatch.
  static rocksdb::WriteBatch* getHandle(JNIEnv* env, jobject jwb) {
    return reinterpret_cast<rocksdb::WriteBatch*>(
        env->GetLongField(jwb, getHandleFieldID(env)));
  }

  // Pass the rocksdb::WriteBatch pointer to the java side.
  static void setHandle(JNIEnv* env, jobject jwb, rocksdb::WriteBatch* wb) {
    env->SetLongField(
        jwb, getHandleFieldID(env),
        reinterpret_cast<jlong>(wb));
  }
};

class HistogramDataJni {
 public:
  static jmethodID getConstructorMethodId(JNIEnv* env, jclass jclazz) {
    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(DDDDD)V");
    assert(mid != nullptr);
    return mid;
  }
};

class BackupableDBOptionsJni {
 public:
  // Get the java class id of org.rocksdb.BackupableDBOptions.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/BackupableDBOptions");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.BackupableDBOptions
  // that stores the pointer to rocksdb::BackupableDBOptions
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::BackupableDBOptions
  static rocksdb::BackupableDBOptions* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::BackupableDBOptions*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::BackupableDBOptions pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, rocksdb::BackupableDBOptions* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class IteratorJni {
 public:
  // Get the java class id of org.rocksdb.Iteartor.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/RocksIterator");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Iterator
  // that stores the pointer to rocksdb::Iterator.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::Iterator.
  static rocksdb::Iterator* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::Iterator*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::Iterator pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, rocksdb::Iterator* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class FilterJni {
 public:
  // Get the java class id of org.rocksdb.FilterPolicy.
  static jclass getJClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("org/rocksdb/Filter");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the field id of the member variable of org.rocksdb.Filter
  // that stores the pointer to rocksdb::FilterPolicy.
  static jfieldID getHandleFieldID(JNIEnv* env) {
    static jfieldID fid = env->GetFieldID(
        getJClass(env), "nativeHandle_", "J");
    assert(fid != nullptr);
    return fid;
  }

  // Get the pointer to rocksdb::FilterPolicy.
  static rocksdb::FilterPolicy* getHandle(JNIEnv* env, jobject jobj) {
    return reinterpret_cast<rocksdb::FilterPolicy*>(
        env->GetLongField(jobj, getHandleFieldID(env)));
  }

  // Pass the rocksdb::FilterPolicy pointer to the java side.
  static void setHandle(
      JNIEnv* env, jobject jobj, const rocksdb::FilterPolicy* op) {
    env->SetLongField(
        jobj, getHandleFieldID(env),
        reinterpret_cast<jlong>(op));
  }
};

class ListJni {
 public:
  // Get the java class id of java.util.List.
  static jclass getListClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("java/util/List");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the java class id of java.util.ArrayList.
  static jclass getArrayListClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("java/util/ArrayList");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the java class id of java.util.Iterator.
  static jclass getIteratorClass(JNIEnv* env) {
    jclass jclazz = env->FindClass("java/util/Iterator");
    assert(jclazz != nullptr);
    return jclazz;
  }

  // Get the java method id of java.util.List.iterator().
  static jmethodID getIteratorMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getListClass(env), "iterator", "()Ljava/util/Iterator;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.Iterator.hasNext().
  static jmethodID getHasNextMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getIteratorClass(env), "hasNext", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.Iterator.next().
  static jmethodID getNextMethod(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getIteratorClass(env), "next", "()Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of arrayList constructor.
  static jmethodID getArrayListConstructorMethodId(JNIEnv* env, jclass jclazz) {
    static jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(I)V");
    assert(mid != nullptr);
    return mid;
  }

  // Get the java method id of java.util.List.add().
  static jmethodID getListAddMethodId(JNIEnv* env) {
    static jmethodID mid = env->GetMethodID(
        getListClass(env), "add", "(Ljava/lang/Object;)Z");
    assert(mid != nullptr);
    return mid;
  }
};
}  // namespace rocksdb
#endif  // JAVA_ROCKSJNI_PORTAL_H_
