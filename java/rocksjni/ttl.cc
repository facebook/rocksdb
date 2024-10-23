// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::TtlDB methods.
// from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <string>
#include <vector>

#include "include/org_rocksdb_TtlDB.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    open
 * Signature: (JLjava/lang/String;IZ)J
 */
jlong Java_org_rocksdb_TtlDB_open(JNIEnv* env, jclass, jlong joptions_handle,
                                  jstring jdb_path, jint jttl,
                                  jboolean jread_only) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle);
  ROCKSDB_NAMESPACE::DBWithTTL* db = nullptr;
  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::DBWithTTL::Open(*opt, db_path, &db, jttl, jread_only);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  // as TTLDB extends RocksDB on the java side, we can reuse
  // the RocksDB portal here.
  if (s.ok()) {
    return GET_CPLUSPLUS_POINTER(db);
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    openCF
 * Signature: (JLjava/lang/String;[[B[J[IZ)[J
 */
jlongArray Java_org_rocksdb_TtlDB_openCF(JNIEnv* env, jclass, jlong jopt_handle,
                                         jstring jdb_path,
                                         jobjectArray jcolumn_names,
                                         jlongArray jcolumn_options,
                                         jintArray jttls, jboolean jread_only) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  const jsize len_cols = env->GetArrayLength(jcolumn_names);
  jlong* jco = env->GetLongArrayElements(jcolumn_options, nullptr);
  if (jco == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> column_families;
  jboolean has_exception = JNI_FALSE;
  ROCKSDB_NAMESPACE::JniUtil::byteStrings<std::string>(
      env, jcolumn_names,
      [](const char* str_data, const size_t str_len) {
        return std::string(str_data, str_len);
      },
      [&jco, &column_families](size_t idx, std::string cf_name) {
        ROCKSDB_NAMESPACE::ColumnFamilyOptions* cf_options =
            reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jco[idx]);
        column_families.push_back(
            ROCKSDB_NAMESPACE::ColumnFamilyDescriptor(cf_name, *cf_options));
      },
      &has_exception);

  env->ReleaseLongArrayElements(jcolumn_options, jco, JNI_ABORT);

  if (has_exception == JNI_TRUE) {
    // exception occurred
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }

  std::vector<int32_t> ttl_values;
  jint* jttlv = env->GetIntArrayElements(jttls, nullptr);
  if (jttlv == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }
  const jsize len_ttls = env->GetArrayLength(jttls);
  for (jsize i = 0; i < len_ttls; i++) {
    ttl_values.push_back(jttlv[i]);
  }
  env->ReleaseIntArrayElements(jttls, jttlv, JNI_ABORT);

  auto* opt = reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jopt_handle);
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> handles;
  ROCKSDB_NAMESPACE::DBWithTTL* db = nullptr;
  ROCKSDB_NAMESPACE::Status s = ROCKSDB_NAMESPACE::DBWithTTL::Open(
      *opt, db_path, column_families, &handles, &db, ttl_values, jread_only);

  // we have now finished with db_path
  env->ReleaseStringUTFChars(jdb_path, db_path);

  // check if open operation was successful
  if (s.ok()) {
    const jsize resultsLen = 1 + len_cols;  // db handle + column family handles
    std::unique_ptr<jlong[]> results =
        std::unique_ptr<jlong[]>(new jlong[resultsLen]);
    results[0] = GET_CPLUSPLUS_POINTER(db);
    for (int i = 1; i <= len_cols; i++) {
      results[i] = GET_CPLUSPLUS_POINTER(handles[i - 1]);
    }

    jlongArray jresults = env->NewLongArray(resultsLen);
    if (jresults == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jresults);
      return nullptr;
    }

    return jresults;
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_TtlDB_disposeInternalJni(JNIEnv*, jclass, jlong jhandle) {
  auto* ttl_db = reinterpret_cast<ROCKSDB_NAMESPACE::DBWithTTL*>(jhandle);
  assert(ttl_db != nullptr);
  delete ttl_db;
}

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    closeDatabase
 * Signature: (J)V
 */
void Java_org_rocksdb_TtlDB_closeDatabase(JNIEnv* /* env */, jclass,
                                          jlong /* jhandle */) {
  // auto* ttl_db = reinterpret_cast<ROCKSDB_NAMESPACE::DBWithTTL*>(jhandle);
  // assert(ttl_db != nullptr);
  // ROCKSDB_NAMESPACE::Status s = ttl_db->Close();
  // ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);

  // TODO(AR) this is disabled until
  // https://github.com/facebook/rocksdb/issues/4818 is resolved!
}

/*
 * Class:     org_rocksdb_TtlDB
 * Method:    createColumnFamilyWithTtl
 * Signature: (JLorg/rocksdb/ColumnFamilyDescriptor;[BJI)J;
 */
jlong Java_org_rocksdb_TtlDB_createColumnFamilyWithTtl(JNIEnv* env, jclass,
                                                       jlong jdb_handle,
                                                       jbyteArray jcolumn_name,
                                                       jlong jcolumn_options,
                                                       jint jttl) {
  jbyte* cfname = env->GetByteArrayElements(jcolumn_name, nullptr);
  if (cfname == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  const jsize len = env->GetArrayLength(jcolumn_name);

  auto* cfOptions = reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(
      jcolumn_options);

  auto* db_handle = reinterpret_cast<ROCKSDB_NAMESPACE::DBWithTTL*>(jdb_handle);
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* handle;
  ROCKSDB_NAMESPACE::Status s = db_handle->CreateColumnFamilyWithTtl(
      *cfOptions, std::string(reinterpret_cast<char*>(cfname), len), &handle,
      jttl);

  env->ReleaseByteArrayElements(jcolumn_name, cfname, JNI_ABORT);

  if (s.ok()) {
    return GET_CPLUSPLUS_POINTER(handle);
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  return 0;
}
