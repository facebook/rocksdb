// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::TtlDB methods.
// from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <memory>

#include "include/org_rocksdb_TtlDB.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    open
 * Signature: (JLjava/lang/String;IZ)J
 */
jlong Java_org_rocksdb_TtlDB_open(JNIEnv* env,
    jclass jcls, jlong joptions_handle, jstring jdb_path,
    jint jttl, jboolean jread_only) {
  auto* opt = reinterpret_cast<rocksdb::Options*>(joptions_handle);
  rocksdb::DBWithTTL* db = nullptr;
  const char* db_path = env->GetStringUTFChars(jdb_path, 0);
  rocksdb::Status s = rocksdb::DBWithTTL::Open(*opt, db_path, &db,
      jttl, jread_only);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  // as TTLDB extends RocksDB on the java side, we can reuse
  // the RocksDB portal here.
  if (s.ok()) {
    return reinterpret_cast<jlong>(db);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    openCF
 * Signature: (JLjava/lang/String;[[B[J[IZ)[J
 */
jlongArray
    Java_org_rocksdb_TtlDB_openCF(
    JNIEnv* env, jclass jcls, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options,
    jintArray jttls, jboolean jread_only) {
  auto* opt = reinterpret_cast<rocksdb::DBOptions*>(jopt_handle);
  const char* db_path = env->GetStringUTFChars(jdb_path, NULL);

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

  jsize len_cols = env->GetArrayLength(jcolumn_names);
  jlong* jco = env->GetLongArrayElements(jcolumn_options, NULL);
  for(int i = 0; i < len_cols; i++) {
    jobject jcn = env->GetObjectArrayElement(jcolumn_names, i);
    jbyteArray jcn_ba = reinterpret_cast<jbyteArray>(jcn);
    jbyte* jcf_name = env->GetByteArrayElements(jcn_ba, NULL);
    const int jcf_name_len = env->GetArrayLength(jcn_ba);

    //TODO(AR) do I need to make a copy of jco[i] ?

    std::string cf_name (reinterpret_cast<char *>(jcf_name), jcf_name_len);
    rocksdb::ColumnFamilyOptions* cf_options =
      reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jco[i]);
    column_families.push_back(
      rocksdb::ColumnFamilyDescriptor(cf_name, *cf_options));

    env->ReleaseByteArrayElements(jcn_ba, jcf_name, JNI_ABORT);
    env->DeleteLocalRef(jcn);
  }
  env->ReleaseLongArrayElements(jcolumn_options, jco, JNI_ABORT);

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::DBWithTTL* db = nullptr;

  std::vector<int32_t> ttl_values;
  jint* jttlv = env->GetIntArrayElements(jttls, NULL);
  jsize len_ttls = env->GetArrayLength(jttls);
  for(int i = 0; i < len_ttls; i++) {
    ttl_values.push_back(jttlv[i]);
  }
  env->ReleaseIntArrayElements(jttls, jttlv, JNI_ABORT);

  rocksdb::Status s = rocksdb::DBWithTTL::Open(*opt, db_path, column_families,
      &handles, &db, ttl_values, jread_only);

  // check if open operation was successful
  if (s.ok()) {
    jsize resultsLen = 1 + len_cols; //db handle + column family handles
    std::unique_ptr<jlong[]> results =
        std::unique_ptr<jlong[]>(new jlong[resultsLen]);
    results[0] = reinterpret_cast<jlong>(db);
    for(int i = 1; i <= len_cols; i++) {
      results[i] = reinterpret_cast<jlong>(handles[i - 1]);
    }

    jlongArray jresults = env->NewLongArray(resultsLen);
    env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
    return jresults;
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return NULL;
  }
}

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    createColumnFamilyWithTtl
 * Signature: (JLorg/rocksdb/ColumnFamilyDescriptor;[BJI)J;
 */
jlong Java_org_rocksdb_TtlDB_createColumnFamilyWithTtl(
    JNIEnv* env, jobject jobj, jlong jdb_handle,
    jbyteArray jcolumn_name, jlong jcolumn_options, jint jttl) {
  rocksdb::ColumnFamilyHandle* handle;
  auto* db_handle = reinterpret_cast<rocksdb::DBWithTTL*>(jdb_handle);

  jbyte* cfname = env->GetByteArrayElements(jcolumn_name, 0);
  const int len = env->GetArrayLength(jcolumn_name);

  auto* cfOptions =
      reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jcolumn_options);

  rocksdb::Status s = db_handle->CreateColumnFamilyWithTtl(
      *cfOptions, std::string(reinterpret_cast<char *>(cfname),
          len), &handle, jttl);
  env->ReleaseByteArrayElements(jcolumn_name, cfname, 0);

  if (s.ok()) {
    return reinterpret_cast<jlong>(handle);
  }
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return 0;
}
