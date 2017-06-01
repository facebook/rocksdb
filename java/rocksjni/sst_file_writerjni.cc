// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ rocksdb::SstFileWriter methods
// from Java side.

#include <jni.h>
#include <string>

#include "include/org_rocksdb_SstFileWriter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    newSstFileWriter
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_SstFileWriter_newSstFileWriter__JJJ(JNIEnv *env, jclass jcls,
                                                      jlong jenvoptions,
                                                      jlong joptions,
                                                      jlong jcomparator) {
  auto *env_options =
      reinterpret_cast<const rocksdb::EnvOptions *>(jenvoptions);
  auto *options = reinterpret_cast<const rocksdb::Options *>(joptions);
  auto *comparator = reinterpret_cast<const rocksdb::Comparator *>(jcomparator);
  rocksdb::SstFileWriter *sst_file_writer =
      new rocksdb::SstFileWriter(*env_options, *options, comparator);
  return reinterpret_cast<jlong>(sst_file_writer);
}

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    newSstFileWriter
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_SstFileWriter_newSstFileWriter__JJ(JNIEnv *env, jclass jcls,
                                                      jlong jenvoptions,
                                                      jlong joptions) {
  auto *env_options =
      reinterpret_cast<const rocksdb::EnvOptions *>(jenvoptions);
  auto *options = reinterpret_cast<const rocksdb::Options *>(joptions);
  rocksdb::SstFileWriter *sst_file_writer =
      new rocksdb::SstFileWriter(*env_options, *options);
  return reinterpret_cast<jlong>(sst_file_writer);
}

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    open
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_SstFileWriter_open(JNIEnv *env, jobject jobj,
                                         jlong jhandle, jstring jfile_path) {
  const char *file_path = env->GetStringUTFChars(jfile_path, nullptr);
  if(file_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  rocksdb::Status s =
      reinterpret_cast<rocksdb::SstFileWriter *>(jhandle)->Open(file_path);
  env->ReleaseStringUTFChars(jfile_path, file_path);

  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    put
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_SstFileWriter_put(JNIEnv *env, jobject jobj,
                                        jlong jhandle, jlong jkey_handle,
                                        jlong jvalue_handle) {
  auto *key_slice = reinterpret_cast<rocksdb::Slice *>(jkey_handle);
  auto *value_slice = reinterpret_cast<rocksdb::Slice *>(jvalue_handle);
  rocksdb::Status s =
    reinterpret_cast<rocksdb::SstFileWriter *>(jhandle)->Put(*key_slice,
                                                             *value_slice);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    merge
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_SstFileWriter_merge(JNIEnv *env, jobject jobj,
                                          jlong jhandle, jlong jkey_handle,
                                          jlong jvalue_handle) {
  auto *key_slice = reinterpret_cast<rocksdb::Slice *>(jkey_handle);
  auto *value_slice = reinterpret_cast<rocksdb::Slice *>(jvalue_handle);
  rocksdb::Status s =
    reinterpret_cast<rocksdb::SstFileWriter *>(jhandle)->Merge(*key_slice,
                                                               *value_slice);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    delete
 * Signature: (JJJ)V
 */
void Java_org_rocksdb_SstFileWriter_delete(JNIEnv *env, jobject jobj,
                                           jlong jhandle, jlong jkey_handle) {
  auto *key_slice = reinterpret_cast<rocksdb::Slice *>(jkey_handle);
  rocksdb::Status s =
    reinterpret_cast<rocksdb::SstFileWriter *>(jhandle)->Delete(*key_slice);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    finish
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileWriter_finish(JNIEnv *env, jobject jobj,
                                           jlong jhandle) {
  rocksdb::Status s =
      reinterpret_cast<rocksdb::SstFileWriter *>(jhandle)->Finish();
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_SstFileWriter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileWriter_disposeInternal(JNIEnv *env, jobject jobj,
                                                    jlong jhandle) {
  delete reinterpret_cast<rocksdb::SstFileWriter *>(jhandle);
}
