// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for ROCKSDB_NAMESPACE::TransactionDB.

#include "rocksdb/utilities/optimistic_transaction_db.h"

#include <jni.h>

#include "api_columnfamilyhandle.h"
#include "api_iterator.h"
#include "api_rocksdb.h"
#include "api_transaction.h"
#include "include/org_rocksdb_OptimisticTransactionDB.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksjni/cplusplus_to_java_convert.h"
#include "rocksjni/portal.h"

using API_OTDB = APIRocksDB<ROCKSDB_NAMESPACE::OptimisticTransactionDB>;
using API_OTCFH =
    APIColumnFamilyHandle<ROCKSDB_NAMESPACE::OptimisticTransactionDB>;
using API_OTXN = APITransaction<ROCKSDB_NAMESPACE::OptimisticTransactionDB>;

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    open
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_open__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong joptions_handle, jstring jdb_path) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions_handle);
  ROCKSDB_NAMESPACE::OptimisticTransactionDB* otdb = nullptr;
  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::OptimisticTransactionDB::Open(*options, db_path,
                                                       &otdb);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    std::shared_ptr<ROCKSDB_NAMESPACE::OptimisticTransactionDB> dbShared =
        APIBase::createSharedPtr(otdb, false /*isDefault*/);
    std::unique_ptr<API_OTDB> dbAPI(new API_OTDB(dbShared));
    return GET_CPLUSPLUS_POINTER(dbAPI.release());
  } else {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    open
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray
Java_org_rocksdb_OptimisticTransactionDB_open__JLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jdb_options_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options_handles) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> column_families;
  const jsize len_cols = env->GetArrayLength(jcolumn_names);
  if (len_cols > 0) {
    if (env->EnsureLocalCapacity(len_cols) != 0) {
      // out of memory
      env->ReleaseStringUTFChars(jdb_path, db_path);
      return nullptr;
    }

    jlong* jco = env->GetLongArrayElements(jcolumn_options_handles, nullptr);
    if (jco == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ReleaseStringUTFChars(jdb_path, db_path);
      return nullptr;
    }

    for (int i = 0; i < len_cols; i++) {
      const jobject jcn = env->GetObjectArrayElement(jcolumn_names, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
        env->ReleaseStringUTFChars(jdb_path, db_path);
        return nullptr;
      }

      const jbyteArray jcn_ba = reinterpret_cast<jbyteArray>(jcn);
      const jsize jcf_name_len = env->GetArrayLength(jcn_ba);
      if (env->EnsureLocalCapacity(jcf_name_len) != 0) {
        // out of memory
        env->DeleteLocalRef(jcn);
        env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
        env->ReleaseStringUTFChars(jdb_path, db_path);
        return nullptr;
      }

      jbyte* jcf_name = env->GetByteArrayElements(jcn_ba, nullptr);
      if (jcf_name == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(jcn);
        env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
        env->ReleaseStringUTFChars(jdb_path, db_path);
        return nullptr;
      }

      const std::string cf_name(reinterpret_cast<char*>(jcf_name),
                                jcf_name_len);
      const ROCKSDB_NAMESPACE::ColumnFamilyOptions* cf_options =
          reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(jco[i]);
      column_families.push_back(
          ROCKSDB_NAMESPACE::ColumnFamilyDescriptor(cf_name, *cf_options));

      env->ReleaseByteArrayElements(jcn_ba, jcf_name, JNI_ABORT);
      env->DeleteLocalRef(jcn);
    }
    env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);
  }

  auto* db_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::DBOptions*>(jdb_options_handle);
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  ROCKSDB_NAMESPACE::OptimisticTransactionDB* otdb = nullptr;
  const ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::OptimisticTransactionDB::Open(
          *db_options, db_path, column_families, &cf_handles, &otdb);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  // check if open operation was successful
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::OptimisticTransactionDB> dbShared(otdb);
  std::unique_ptr<API_OTDB> otdbAPI(new APIRocksDB(dbShared));

  const jsize resultsLen = 1 + len_cols;  // db handle + column family handles
  std::unique_ptr<jlong[]> results =
      std::unique_ptr<jlong[]>(new jlong[resultsLen]);
  for (int i = 1; i <= len_cols; i++) {
    std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfShared =
        APIBase::createSharedPtr(cf_handles[i - 1], false /*isDefault*/);
    std::unique_ptr<APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>> cfhAPI(
        new APIColumnFamilyHandle<ROCKSDB_NAMESPACE::DB>(dbShared, cfShared));
    otdbAPI->columnFamilyHandles.push_back(cfShared);
    results[i] = GET_CPLUSPLUS_POINTER(cfhAPI.release());
  }
  results[0] = GET_CPLUSPLUS_POINTER(otdbAPI.release());

  jlongArray jresults = env->NewLongArray(resultsLen);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    nativeClose
 * Signature: (J)V
 */
void Java_org_rocksdb_OptimisticTransactionDB_nativeClose(JNIEnv*, jobject,
                                                          jlong jhandle) {
  std::unique_ptr<API_OTDB> dbAPI(reinterpret_cast<API_OTDB*>(jhandle));
  dbAPI->check("nativeClose()");
  // Now the unique_ptr destructor will delete() referenced shared_ptr contents
  // in the API object.
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    closeDatabase
 * Signature: (J)V
 */
void Java_org_rocksdb_OptimisticTransactionDB_closeDatabase(
    JNIEnv* env, jclass, jlong jhandle) {
  auto& otdbAPI = *reinterpret_cast<API_OTDB*>(jhandle);
  ROCKSDB_NAMESPACE::Status s = otdbAPI->Close();
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction__JJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle) {
  auto& otdbAPI = *reinterpret_cast<API_OTDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);

  ROCKSDB_NAMESPACE::Transaction* txn =
      otdbAPI->BeginTransaction(*write_options);
  auto* apiTxn = new API_OTXN(
      otdbAPI.db, std::shared_ptr<ROCKSDB_NAMESPACE::Transaction>(txn));

  return GET_CPLUSPLUS_POINTER(apiTxn);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction__JJJ(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jwrite_options_handle, jlong joptimistic_txn_options_handle) {
  auto& otdbAPI = *reinterpret_cast<API_OTDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* optimistic_txn_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::OptimisticTransactionOptions*>(
          joptimistic_txn_options_handle);
  ROCKSDB_NAMESPACE::Transaction* txn =
      otdbAPI->BeginTransaction(*write_options, *optimistic_txn_options);
  auto* apiTxn = new API_OTXN(
      otdbAPI.db, std::shared_ptr<ROCKSDB_NAMESPACE::Transaction>(txn));

  return GET_CPLUSPLUS_POINTER(apiTxn);
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction_1withOld__JJJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle,
    jlong jold_txn_handle) {
  auto& otdbAPI = *reinterpret_cast<API_OTDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* old_txn =
      reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jold_txn_handle);
  ROCKSDB_NAMESPACE::OptimisticTransactionOptions optimistic_txn_options;
  ROCKSDB_NAMESPACE::Transaction* txn = otdbAPI->BeginTransaction(
      *write_options, optimistic_txn_options, old_txn);

  // RocksJava relies on the assumption that
  // we do not allocate a new Transaction object
  // when providing an old_optimistic_txn
  assert(txn == old_txn);

  return jold_txn_handle;
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJJ)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_beginTransaction_1withOld__JJJJ(
    JNIEnv*, jobject, jlong jhandle, jlong jwrite_options_handle,
    jlong joptimistic_txn_options_handle, jlong jold_txn_handle) {
  auto& otdbAPI = *reinterpret_cast<API_OTDB*>(jhandle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* optimistic_txn_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::OptimisticTransactionOptions*>(
          joptimistic_txn_options_handle);
  auto* old_txn =
      reinterpret_cast<ROCKSDB_NAMESPACE::Transaction*>(jold_txn_handle);
  ROCKSDB_NAMESPACE::Transaction* txn = otdbAPI->BeginTransaction(
      *write_options, *optimistic_txn_options, old_txn);

  // RocksJava relies on the assumption that
  // we do not allocate a new Transaction object
  // when providing an old_optimisic_txn
  assert(txn == old_txn);

  return jold_txn_handle;
}

/*
 * Class:     org_rocksdb_OptimisticTransactionDB
 * Method:    getBaseDB
 * Signature: (J)J
 */
jlong Java_org_rocksdb_OptimisticTransactionDB_getBaseDB(JNIEnv* env, jobject,
                                                         jlong /*jhandle*/) {
  // TODO (AP) auto& otdbAPI = *reinterpret_cast<API_OTDB*>(jhandle);
  // TODO (AP) auto* baseDB = otdbAPI->GetBaseDB();

  // TODO (AP) we have no way to find the APIDB wrapper
  // we need to shadow more structures in the API layer
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, ROCKSDB_NAMESPACE::Status::NotFound());
  return 0L;
}
