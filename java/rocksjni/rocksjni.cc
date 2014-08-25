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
#include <vector>

#include "include/org_rocksdb_RocksDB.h"
#include "rocksjni/portal.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Open

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    open
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_RocksDB_open(
    JNIEnv* env, jobject jdb, jlong jopt_handle, jstring jdb_path) {
  auto opt = reinterpret_cast<rocksdb::Options*>(jopt_handle);
  rocksdb::DB* db = nullptr;
  const char* db_path = env->GetStringUTFChars(jdb_path, 0);
  rocksdb::Status s = rocksdb::DB::Open(*opt, db_path, &db);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    rocksdb::RocksDBJni::setHandle(env, jdb, db);
    return;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Put

void rocksdb_put_helper(
    JNIEnv* env, rocksdb::DB* db, const rocksdb::WriteOptions& write_options,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {

  jbyte* key = env->GetByteArrayElements(jkey, 0);
  jbyte* value = env->GetByteArrayElements(jvalue, 0);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value), jvalue_len);

  rocksdb::Status s = db->Put(write_options, key_slice, value_slice);

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
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_rocksdb_RocksDB_put__J_3BI_3BI(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  auto db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();

  rocksdb_put_helper(env, db, default_write_options,
                     jkey, jkey_len,
                     jvalue, jvalue_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    put
 * Signature: (JJ[BI[BI)V
 */
void Java_org_rocksdb_RocksDB_put__JJ_3BI_3BI(
    JNIEnv* env, jobject jdb,
    jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  auto db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto write_options = reinterpret_cast<rocksdb::WriteOptions*>(
      jwrite_options_handle);

  rocksdb_put_helper(env, db, *write_options,
                     jkey, jkey_len,
                     jvalue, jvalue_len);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Write
/*
 * Class:     org_rocksdb_RocksDB
 * Method:    write
 * Signature: (JJ)V
 */
void Java_org_rocksdb_RocksDB_write(
    JNIEnv* env, jobject jdb,
    jlong jwrite_options_handle, jlong jbatch_handle) {
  rocksdb::DB* db = rocksdb::RocksDBJni::getHandle(env, jdb);
  auto write_options = reinterpret_cast<rocksdb::WriteOptions*>(
      jwrite_options_handle);
  auto batch = reinterpret_cast<rocksdb::WriteBatch*>(jbatch_handle);

  rocksdb::Status s = db->Write(*write_options, batch);

  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Get

jbyteArray rocksdb_get_helper(
    JNIEnv* env, rocksdb::DB* db, const rocksdb::ReadOptions& read_opt,
    jbyteArray jkey, jint jkey_len) {
  jboolean isCopy;
  jbyte* key = env->GetByteArrayElements(jkey, &isCopy);
  rocksdb::Slice key_slice(
      reinterpret_cast<char*>(key), jkey_len);

  std::string value;
  rocksdb::Status s = db->Get(
      read_opt, key_slice, &value);

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
 * Signature: (J[BI)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__J_3BI(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jbyteArray jkey, jint jkey_len) {
  return rocksdb_get_helper(env,
      reinterpret_cast<rocksdb::DB*>(jdb_handle),
      rocksdb::ReadOptions(),
      jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BI)[B
 */
jbyteArray Java_org_rocksdb_RocksDB_get__JJ_3BI(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_len) {
  return rocksdb_get_helper(env,
      reinterpret_cast<rocksdb::DB*>(jdb_handle),
      *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle),
      jkey, jkey_len);
}

jint rocksdb_get_helper(
    JNIEnv* env, rocksdb::DB* db, const rocksdb::ReadOptions& read_options,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  jbyte* key = env->GetByteArrayElements(jkey, 0);
  rocksdb::Slice key_slice(
      reinterpret_cast<char*>(key), jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  rocksdb::Status s = db->Get(
      read_options, key_slice, &cvalue);

  // trigger java unref on key.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
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

  env->SetByteArrayRegion(
      jvalue, 0, length,
      reinterpret_cast<const jbyte*>(cvalue.c_str()));
  return cvalue_len;
}

jobject multi_get_helper(JNIEnv* env, jobject jdb, rocksdb::DB* db,
    const rocksdb::ReadOptions& rOpt, jobject jkey_list, jint jkeys_count) {
  std::vector<rocksdb::Slice> keys;
  std::vector<jbyte*> keys_to_free;

  // get iterator
  jobject iteratorObj = env->CallObjectMethod(
      jkey_list, rocksdb::ListJni::getIteratorMethod(env));

  // iterate over keys and convert java byte array to slice
  while(env->CallBooleanMethod(
      iteratorObj, rocksdb::ListJni::getHasNextMethod(env)) == JNI_TRUE) {
    jbyteArray jkey = (jbyteArray) env->CallObjectMethod(
       iteratorObj, rocksdb::ListJni::getNextMethod(env));
    jint key_length = env->GetArrayLength(jkey);

    jbyte* key = new jbyte[key_length];
    env->GetByteArrayRegion(jkey, 0, key_length, key);
    // store allocated jbyte to free it after multiGet call
    keys_to_free.push_back(key);

    rocksdb::Slice key_slice(
      reinterpret_cast<char*>(key), key_length);
    keys.push_back(key_slice);
  }

  std::vector<std::string> values;
  std::vector<rocksdb::Status> s = db->MultiGet(rOpt, keys, &values);

  // Don't reuse class pointer
  jclass jclazz = env->FindClass("java/util/ArrayList");
  jmethodID mid = rocksdb::ListJni::getArrayListConstructorMethodId(
      env, jclazz);
  jobject jvalue_list = env->NewObject(jclazz, mid, jkeys_count);

  // insert in java list
  for(std::vector<rocksdb::Status>::size_type i = 0; i != s.size(); i++) {
    if(s[i].ok()) {
      jbyteArray jvalue = env->NewByteArray(values[i].size());
      env->SetByteArrayRegion(
          jvalue, 0, values[i].size(),
          reinterpret_cast<const jbyte*>(values[i].c_str()));
      env->CallBooleanMethod(
          jvalue_list, rocksdb::ListJni::getListAddMethodId(env), jvalue);
    }
    else {
      env->CallBooleanMethod(
          jvalue_list, rocksdb::ListJni::getListAddMethodId(env), nullptr);
    }
  }

  // free up allocated byte arrays
  for(std::vector<jbyte*>::size_type i = 0; i != keys_to_free.size(); i++) {
    delete[] keys_to_free[i];
  }
  keys_to_free.clear();

  return jvalue_list;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature: (JLjava/util/List;I)Ljava/util/List;
 */
jobject Java_org_rocksdb_RocksDB_multiGet__JLjava_util_List_2I(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jobject jkey_list, jint jkeys_count) {
  return multi_get_helper(env, jdb, reinterpret_cast<rocksdb::DB*>(jdb_handle),
      rocksdb::ReadOptions(), jkey_list, jkeys_count);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    multiGet
 * Signature: (JJLjava/util/List;I)Ljava/util/List;
 */
jobject Java_org_rocksdb_RocksDB_multiGet__JJLjava_util_List_2I(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jlong jropt_handle, jobject jkey_list, jint jkeys_count) {
  return multi_get_helper(env, jdb, reinterpret_cast<rocksdb::DB*>(jdb_handle),
      *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle), jkey_list,
      jkeys_count);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (J[BI[BI)I
 */
jint Java_org_rocksdb_RocksDB_get__J_3BI_3BI(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  return rocksdb_get_helper(env,
      reinterpret_cast<rocksdb::DB*>(jdb_handle),
      rocksdb::ReadOptions(),
      jkey, jkey_len, jvalue, jvalue_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    get
 * Signature: (JJ[BI[BI)I
 */
jint Java_org_rocksdb_RocksDB_get__JJ_3BI_3BI(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_len,
    jbyteArray jvalue, jint jvalue_len) {
  return rocksdb_get_helper(env,
      reinterpret_cast<rocksdb::DB*>(jdb_handle),
      *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle),
      jkey, jkey_len, jvalue, jvalue_len);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::Delete()
void rocksdb_remove_helper(
    JNIEnv* env, rocksdb::DB* db, const rocksdb::WriteOptions& write_options,
    jbyteArray jkey, jint jkey_len) {
  jbyte* key = env->GetByteArrayElements(jkey, 0);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  rocksdb::Status s = db->Delete(write_options, key_slice);

  // trigger java unref on key and value.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
  return;
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    remove
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_RocksDB_remove__J_3BI(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jbyteArray jkey, jint jkey_len) {
  auto db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options =
      rocksdb::WriteOptions();

  rocksdb_remove_helper(env, db, default_write_options, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    remove
 * Signature: (JJ[BI)V
 */
void Java_org_rocksdb_RocksDB_remove__JJ_3BI(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jlong jwrite_options, jbyteArray jkey, jint jkey_len) {
  auto db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto write_options = reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options);

  rocksdb_remove_helper(env, db, *write_options, jkey, jkey_len);
}

//////////////////////////////////////////////////////////////////////////////
// rocksdb::DB::~DB()

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksDB_disposeInternal(
    JNIEnv* env, jobject java_db, jlong jhandle) {
  delete reinterpret_cast<rocksdb::DB*>(jhandle);
}

/*
 * Class:     org_rocksdb_RocksDB
 * Method:    iterator0
 * Signature: (J)J
 */
jlong Java_org_rocksdb_RocksDB_iterator0(
    JNIEnv* env, jobject jdb, jlong db_handle) {
  auto db = reinterpret_cast<rocksdb::DB*>(db_handle);
  rocksdb::Iterator* iterator = db->NewIterator(rocksdb::ReadOptions());
  return reinterpret_cast<jlong>(iterator);
}
