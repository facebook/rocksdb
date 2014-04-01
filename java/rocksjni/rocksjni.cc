// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::DB methods from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_RocksDB.h"
#include "rocksjni/portal.h"
#include "rocksdb/db.h"

void rocksdb_open_helper(
  JNIEnv* env, jobject java_db, jstring jdb_path, const rocksdb::Options& opt) {
  rocksdb::DB* db;

  const char* db_path = env->GetStringUTFChars(jdb_path, 0);
  rocksdb::Status s = rocksdb::DB::Open(opt, db_path, &db);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    rocksdb::RocksDBJni::setHandle(env, java_db, db);
    return;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    open0
 * Signature: (Ljava/lang/String;)V
 */
void Java_org_rocksdb_RocksDB_open0(
    JNIEnv* env, jobject jdb, jstring jdb_path) {
  rocksdb::Options options;
  options.create_if_missing = true;

  rocksdb_open_helper(env, jdb, jdb_path, options);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_RocksDB_open(
    JNIEnv* env, jobject jdb, jlong jopt_handle, jstring jdb_path) {
  auto options = reinterpret_cast<rocksdb::Options*>(jopt_handle);
  rocksdb_open_helper(env, jdb, jdb_path, *options);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: ([BI[BI)V
 */
void Java_org_rocksdb_RocksDB_put(
    JNIEnv* env, jobject jdb,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  rocksdb::DB* db = rocksdb::RocksDBJni::getHandle(env, jdb);

  jboolean isCopy;
  jbyte* key = env->GetByteArrayElements(jkey, &isCopy);
  jbyte* value = env->GetByteArrayElements(jvalue, &isCopy);
  rocksdb::Slice key_slice(
      reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(
      reinterpret_cast<char*>(value), jvalue_len);

  rocksdb::Status s = db->Put(
      rocksdb::WriteOptions(), key_slice, value_slice);

  // trigger java unref on key and value.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
  env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);

  if (s.ok()) {
    return;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: ([BI)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get___3BI(
    JNIEnv* env, jobject jdb, jbyteArray jkey, jint jkey_len) {
  rocksdb::DB* db = rocksdb::RocksDBJni::getHandle(env, jdb);

  jboolean isCopy;
  jbyte* key = env->GetByteArrayElements(jkey, &isCopy);
  rocksdb::Slice key_slice(
      reinterpret_cast<char*>(key), jkey_len);

  std::string value;
  rocksdb::Status s = db->Get(
      rocksdb::ReadOptions(),
      key_slice, &value);

  // trigger java unref on key.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jvalue = env->NewByteArray(value.size());
    env->SetByteArrayRegion(
        jvalue, 0, value.size(),
        reinterpret_cast<const jbyte*>(value.c_str()));
    return jvalue;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);

  return nullptr;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: ([BI[BI)I
 */
jint Java_org_rocksdb_RocksDB_get___3BI_3BI(
    JNIEnv* env, jobject jdb,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  rocksdb::DB* db = rocksdb::RocksDBJni::getHandle(env, jdb);

  jboolean isCopy;
  jbyte* key = env->GetByteArrayElements(jkey, &isCopy);
  jbyte* value = env->GetByteArrayElements(jvalue, &isCopy);
  rocksdb::Slice key_slice(
      reinterpret_cast<char*>(key), jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  rocksdb::Status s = db->Get(
      rocksdb::ReadOptions(), key_slice, &cvalue);

  // trigger java unref on key.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (s.IsNotFound()) {
    env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
    return kNotFound;
  } else if (!s.ok()) {
    env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
    // Here since we are throwing a Java exception from c++ side.
    // As a result, c++ does not know calling this function will in fact
    // throwing an exception.  As a result, the execution flow will
    // not stop here, and codes after this throw will still be
    // executed.
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);

    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  int cvalue_len = static_cast<int>(cvalue.size());
  int length = std::min(jvalue_len, cvalue_len);

  memcpy(value, cvalue.c_str(), length);
  env->ReleaseByteArrayElements(jvalue, value, JNI_COMMIT);
  return static_cast<jint>(cvalue_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    close0
 * Signature: ()V
 */
void Java_org_rocksdb_RocksDB_close0(
    JNIEnv* env, jobject java_db) {
  rocksdb::DB* db = rocksdb::RocksDBJni::getHandle(env, java_db);
  delete db;
  db = nullptr;

  rocksdb::RocksDBJni::setHandle(env, java_db, db);
}
