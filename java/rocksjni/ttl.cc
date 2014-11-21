// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::TtlDB methods.
// from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>
#include <vector>

#include "include/org_rocksdb_TtlDB.h"
#include "rocksjni/portal.h"
#include "rocksdb/utilities/db_ttl.h"

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    open
 * Signature: (JJ)V
 */
void Java_org_rocksdb_TtlDB_open(JNIEnv* env, jobject jttldb,
    jlong joptions_handle, jstring jdb_path, jint jttl,
    jboolean jread_only) {
  auto opt = reinterpret_cast<rocksdb::Options*>(joptions_handle);
  rocksdb::DBWithTTL* db = nullptr;
  const char* db_path = env->GetStringUTFChars(jdb_path, 0);
  rocksdb::Status s = rocksdb::DBWithTTL::Open(*opt, db_path, &db,
      jttl, jread_only);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  // as TTLDB extends RocksDB on the java side, we can reuse
  // the RocksDB portal here.
  if (s.ok()) {
      rocksdb::RocksDBJni::setHandle(env, jttldb, db);
      return;
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    createColumnFamilyWithTtl
 * Signature: (JLorg/rocksdb/ColumnFamilyDescriptor;I)J;
 */
jlong Java_org_rocksdb_TtlDB_createColumnFamilyWithTtl(
    JNIEnv* env, jobject jobj, jlong jdb_handle,
    jobject jcf_descriptor, jint jttl) {
  rocksdb::ColumnFamilyHandle* handle;
  auto db_handle = reinterpret_cast<rocksdb::DBWithTTL*>(jdb_handle);

  jstring jstr = (jstring) env->CallObjectMethod(jcf_descriptor,
      rocksdb::ColumnFamilyDescriptorJni::getColumnFamilyNameMethod(
      env));
  // get CF Options
  jobject jcf_opt_obj = env->CallObjectMethod(jcf_descriptor,
      rocksdb::ColumnFamilyDescriptorJni::getColumnFamilyOptionsMethod(
      env));
  rocksdb::ColumnFamilyOptions* cfOptions =
      rocksdb::ColumnFamilyOptionsJni::getHandle(env, jcf_opt_obj);

  const char* cfname = env->GetStringUTFChars(jstr, 0);
  rocksdb::Status s = db_handle->CreateColumnFamilyWithTtl(
      *cfOptions, cfname, &handle, jttl);
  env->ReleaseStringUTFChars(jstr, cfname);

  if (s.ok()) {
    return reinterpret_cast<jlong>(handle);
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return 0;
}
